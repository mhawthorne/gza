"""Tests for git-oriented CLI helpers."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gza.cli.git_ops import (
    _build_auto_merge_args,
    _merge_single_task,
    _resolve_merge_subject,
    _run_task_backed_rebase,
    cmd_advance,
)
from gza.config import Config
from gza.git import Git
from gza.rebase_diff import RebaseDiffResult

from .conftest import make_store, run_gza, setup_config


def _advance_args(tmp_path: Path, task_id: str) -> argparse.Namespace:
    return argparse.Namespace(
        project_dir=tmp_path,
        task_id=task_id,
        dry_run=False,
        auto=True,
        max=None,
        batch=None,
        no_docker=True,
        force=False,
        plans=False,
        unimplemented=False,
        create=False,
        no_resume_failed=False,
        max_resume_attempts=None,
        advance_type=None,
        new=False,
        max_review_cycles=None,
        squash_threshold=None,
    )


def _init_repo_with_remote_only_feature(tmp_path: Path, branch: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Initial commit")

    git._run("checkout", "-b", branch)
    feature_file = tmp_path / f"{branch.replace('/', '_')}.txt"
    feature_file.write_text("remote tip\n")
    git._run("add", str(feature_file.name))
    git._run("commit", "-m", "Remote feature tip")
    remote_sha = git.rev_parse("HEAD")

    git._run("checkout", "main")
    git._run("update-ref", f"refs/remotes/origin/{branch}", remote_sha)
    git._run("branch", "-D", branch)
    return git


def _init_repo_with_stale_local_and_clean_origin(tmp_path: Path, branch: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "shared.txt").write_text("base\n")
    git._run("add", "shared.txt")
    git._run("commit", "-m", "Initial commit")
    base_sha = git.rev_parse("HEAD")

    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("remote tip\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Remote feature tip")
    remote_sha = git.rev_parse("HEAD")

    git._run("checkout", "main")
    git._run("update-ref", f"refs/remotes/origin/{branch}", remote_sha)
    git._run("update-ref", f"refs/heads/{branch}", base_sha)
    return git


def _init_repo_with_stale_origin_and_local_ahead(tmp_path: Path, branch: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Initial commit")

    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("remote tip\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Remote tip")
    remote_sha = git.rev_parse("HEAD")

    (tmp_path / "feature.txt").write_text("remote tip\nlocal tip\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Local tip")
    git._run("update-ref", f"refs/remotes/origin/{branch}", remote_sha)
    git._run("checkout", "main")
    return git


def _init_repo_with_diverged_local_and_origin(tmp_path: Path, branch: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Initial commit")
    base_sha = git.rev_parse("HEAD")

    git._run("checkout", "-b", branch)
    (tmp_path / "local.txt").write_text("local\n")
    git._run("add", "local.txt")
    git._run("commit", "-m", "Local only")
    local_sha = git.rev_parse("HEAD")

    git._run("update-ref", f"refs/heads/{branch}", base_sha)
    git._run("checkout", branch)
    (tmp_path / "remote.txt").write_text("remote\n")
    git._run("add", "remote.txt")
    git._run("commit", "-m", "Remote only")
    remote_sha = git.rev_parse("HEAD")
    git._run("update-ref", f"refs/remotes/origin/{branch}", remote_sha)

    git._run("update-ref", f"refs/heads/{branch}", local_sha)
    git._run("checkout", branch)
    git._run("checkout", "main")
    return git


def _add_mergeable_impl_with_failed_rebase(store, branch: str):
    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=task.id, same_branch=True)
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    failed_rebase.branch = branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    review.report_file = "reviews/fake.md"
    store.update(review)
    return task


def test_merge_single_task_preflights_conflicts_before_merge(tmp_path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement conflicting change", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/conflicts"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=False),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = SimpleNamespace(project_dir=tmp_path)

    rc = _merge_single_task(task.id, config, store, git, args, "main")

    assert rc == 1
    git.can_merge.assert_called_once_with("feature/conflicts", "main")
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "has conflicts against 'main'" in output
    assert f"uv run gza rebase {task.id} --resolve" in output


def test_run_task_backed_rebase_refreshes_merge_unit_provenance(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-new"
    assert refreshed_unit.base_sha == "base-new"


def test_run_task_backed_rebase_surfaces_resolution_warnings_and_preserves_existing_merge_unit_provenance(
    tmp_path, capsys
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = RuntimeError("boom")

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-old"
    assert refreshed_unit.base_sha == "base-old"
    output = capsys.readouterr()
    assert "unexpected error resolving ref 'feature/rebased': boom" in output.err
    assert "unexpected error resolving ref 'main': boom" in output.err


def test_run_task_backed_rebase_preserves_review_state_when_diff_is_unchanged(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.review_cleared_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=False, detail="no (review can be preserved)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.review_cleared_at == parent.review_cleared_at
    refreshed_rebase = store.get(rebase_task.id)
    assert refreshed_rebase is not None
    assert refreshed_rebase.changed_diff is False


def test_run_task_backed_rebase_invalidates_review_state_when_diff_changes(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.review_cleared_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.review_cleared_at is None
    refreshed_rebase = store.get(rebase_task.id)
    assert refreshed_rebase is not None
    assert refreshed_rebase.changed_diff is True


@pytest.mark.functional
def test_advance_explicit_merge_refuses_when_checkout_does_not_match_canonical_target(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_text = config_path.read_text()
    config_path.write_text(config_text + "advance_requires_review: false\n")

    store = make_store(tmp_path)
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "file.txt").write_text("initial")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Initial commit")
    git._run("add", "gza.yaml")
    git._run("commit", "-m", "Track config")

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-explicit-refusal"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED**"
    store.update(review)

    git._run("checkout", "-b", task.branch)
    (tmp_path / "feature.txt").write_text("feature content\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    worktree_path = tmp_path / "worktrees" / "advance-explicit-refusal"
    worktree_path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(worktree_path), task.branch)
    real_worktree_git = Git(worktree_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=task.id,
        dry_run=False,
        auto=True,
        max=None,
        batch=None,
        no_docker=True,
        force=False,
        plans=False,
        unimplemented=False,
        create=False,
        no_resume_failed=False,
        max_resume_attempts=None,
        advance_type=None,
        new=False,
        max_review_cycles=None,
        squash_threshold=None,
    )

    with patch("gza.cli.git_ops.Git", return_value=real_worktree_git):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 1
    assert "Will advance 1 task(s):" in output
    assert "Merge (review APPROVED)" in output
    assert (
        f"Error: Advance merge for task {task.id} targets 'main', but the active checkout is "
        f"'{task.branch}'. Switch to 'main' and rerun."
    ) in output
    assert "1 errors" in output

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    assert git.is_merged(task.branch, "main") is False


def test_advance_execution_merges_remote_tracking_ref_when_local_branch_is_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/advance-remote-only"
    task = _add_mergeable_impl_with_failed_rebase(store, branch)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.side_effect = lambda b: b != branch
    fake_git.ref_exists.side_effect = lambda r: r == f"origin/{branch}"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{branch}")
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.merge.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"

    fake_git.merge.assert_called_once()
    merge_call_args, _ = fake_git.merge.call_args
    assert merge_call_args[0] == f"origin/{branch}"

    output = capsys.readouterr().out
    assert f"Merging 'origin/{branch}' into 'main'" in output
    assert "✓ Merged" in output


def test_advance_execution_prefers_remote_tracking_ref_over_stale_local_branch(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/advance-stale-local"
    task = _add_mergeable_impl_with_failed_rebase(store, branch)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{branch}")
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.merge.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"

    fake_git.merge.assert_called_once()
    merge_call_args, _ = fake_git.merge.call_args
    assert merge_call_args[0] == f"origin/{branch}"

    output = capsys.readouterr().out
    assert f"Merging 'origin/{branch}' into 'main'" in output
    assert f"Merging '{branch}' into 'main'" not in output


def test_advance_execution_prefers_local_branch_when_origin_is_stale(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from gza import advance_engine as advance_engine_module
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    config.merge_squash_threshold = 1
    store = make_store(tmp_path)
    branch = "feature/advance-local-ahead"
    task = _add_mergeable_impl_with_failed_rebase(store, branch)

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(branch)
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.merge.return_value = None

    resolved = _resolve_merge_subject(store, fake_git, task.id, target_branch="main")
    assert resolved is not None
    assert resolved.merge_source_ref == branch
    merge_args = _build_auto_merge_args(config, fake_git, resolved.merge_source_ref, "main")

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert merge_args.squash is True
    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"

    fake_git.merge.assert_called_once()
    merge_call_args, _ = fake_git.merge.call_args
    assert merge_call_args[0] == branch

    output = capsys.readouterr().out
    assert f"Merging '{branch}' into 'main'" in output
    assert f"Merging 'origin/{branch}' into 'main'" not in output


def test_advance_dry_run_surfaces_diverged_merge_source_for_manual_resolution(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/advance-diverged"
    git = _init_repo_with_diverged_local_and_origin(tmp_path, branch)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    args = _advance_args(tmp_path, task.id)
    args.dry_run = True

    with patch("gza.cli.git_ops.Git", return_value=git):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "Needs attention" in output
    assert "merge-source-needs-manual-resolution" in output
    assert f"origin/{branch}" in output
    assert "diverged" in output


def test_rebase_background_creator_phase_failure_cleans_up_created_task_and_artifacts(tmp_path: Path) -> None:
    """Background rebase must roll back the created child when startup preparation fails."""

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.git_ops._spawn_background_worker",
            side_effect=AssertionError("background worker should not spawn"),
        ),
    ):
        result = run_gza("rebase", str(impl_task.id), "--background", "--project", str(tmp_path))

    assert result.returncode == 1
    assert "creator boom" in result.stderr
    assert store.get_based_on_children(impl_task.id) == []

    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []

    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_rebase_background_reuses_prepared_child_without_second_startup_pass(tmp_path: Path) -> None:
    """Background rebase should hand the already-prepared child to the generic spawner."""

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
    )
    captured_spawn: dict[str, object] = {}

    def prepare_once(_config, task, **_kwargs):
        if prepare_once.called:
            raise AssertionError("startup preparation ran twice")
        prepare_once.called = True
        return task

    prepare_once.called = False  # type: ignore[attr-defined]

    def fake_spawn(_args, _config, **kwargs):
        captured_spawn.update(kwargs)
        return 0

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=prepare_once) as prepare_task,
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=fake_spawn),
    ):
        result = run_gza("rebase", str(impl_task.id), "--background", "--project", str(tmp_path))

    assert result.returncode == 0
    assert prepare_task.call_count == 1
    assert captured_spawn["task_id"] is not None
    prepared_task = captured_spawn["prepared_task"]
    assert prepared_task is not None
    assert getattr(prepared_task, "id", None) == captured_spawn["task_id"]
