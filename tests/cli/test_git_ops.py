"""Tests for git-oriented CLI helpers."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from gza.cli.git_ops import (
    _reconcile_diverged_branch_with_origin,
    SquashBranchReconcileResult,
    _build_auto_merge_args,
    _classify_squash_reconcile_push_failure,
    _merge_single_task,
    _print_squash_reconcile_result,
    _reconcile_squash_merged_branch_with_origin,
    _resolve_merge_subject,
    _run_task_backed_rebase,
    _tracking_ref_refresh_command,
    cmd_advance,
)
from gza.config import Config
from gza.git import Git, GitError, ResolvedGitRef, ResolvedMergeSourceRef
from gza.lineage_query import LineageOwnerRow
from gza.rebase_diff import RebaseDiffBaseline, RebaseDiffResult
from gza.worktree_roots import managed_worktree_root_paths

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

    result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
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
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
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
    worktree_git.rev_parse.return_value = "head-new"
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
        "origin/feature/rebased": "head-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.resolve_ref_if_possible",
            side_effect=[
                ResolvedGitRef(None, "unexpected error resolving ref 'feature/rebased': boom"),
                ResolvedGitRef(None, "unexpected error resolving ref 'main': boom"),
            ],
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
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-new"
    assert refreshed_unit.base_sha == "base-new"
    worktree_git.push_force_with_lease.assert_not_called()
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
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
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
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
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

def test_run_task_backed_rebase_passes_managed_roots_to_cleanup(tmp_path: Path) -> None:
    """Foreground rebase setup should constrain branch cleanup to managed roots."""
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(
        config_path.read_text() + "interactive_worktree_dir: interactive-worktrees\n"
    )
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.status = "completed"
    parent.completed_at = datetime.now(UTC)
    parent.branch = "feature/rebased"
    store.update(parent)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = parent.branch
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git._run.return_value = None
    repo_git.worktree_remove.return_value = None

    worktree_git = MagicMock()
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None) as mock_cleanup,
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
    mock_cleanup.assert_called_once_with(
        repo_git,
        "feature/rebased",
        force=True,
        permitted_root_paths=managed_worktree_root_paths(config),
    )


def test_checkout_passes_managed_roots_to_cleanup(tmp_path: Path) -> None:
    """Checkout should pass both task and configured interactive roots to cleanup."""
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(
        config_path.read_text() + "interactive_worktree_dir: interactive-worktrees\n"
    )
    store = make_store(tmp_path)
    task = store.add("Checkout feature", task_type="implement")
    task.branch = "feature/checkout-roots"
    store.update(task)

    git = MagicMock()
    git.branch_exists.return_value = True

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None) as mock_cleanup,
    ):
        result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 0
    config = Config.load(tmp_path)
    mock_cleanup.assert_called_once_with(
        git,
        "feature/checkout-roots",
        force=False,
        permitted_root_paths=managed_worktree_root_paths(config),
    )


def test_run_task_backed_rebase_reconciles_parent_merge_status_when_rebased_branch_is_already_in_target(
    tmp_path,
    capsys,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_text = config_path.read_text()
    config_path.write_text(config_text + "advance_requires_review: false\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.review_cleared_at = datetime(2026, 5, 10, 9, 0, tzinfo=UTC)
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
    worktree_git.branch_exists.return_value = True
    worktree_git.is_merged.return_value = True
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "shared-sha",
        "main": "shared-sha",
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
    assert refreshed_parent.merge_status == "merged"
    assert refreshed_parent.merged_at is not None
    assert refreshed_parent.review_cleared_at is None
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.merged_at == refreshed_parent.merged_at
    refreshed_rebase = store.get(rebase_task.id)
    assert refreshed_rebase is not None
    assert refreshed_rebase.status == "completed"

    advance_git = MagicMock(spec=Git)
    advance_git.repo_dir = tmp_path
    advance_git.default_branch.return_value = "main"
    advance_git.current_branch.return_value = "main"

    args = _advance_args(tmp_path, parent.id)
    args.dry_run = True
    with patch("gza.cli.git_ops.Git", return_value=advance_git):
        advance_rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert advance_rc == 0
    assert f"Task {parent.id} is already merged" in output
    assert "Create review" not in output
    assert "Merge" not in output
    assert "needs_rebase" not in output


def test_run_task_backed_rebase_remote_uses_fetched_target_ref_for_merge_proof(tmp_path) -> None:
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
    repo_git.fetch.return_value = None
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.branch_exists.return_value = True

    def _is_merged(branch, into):
        return into == "origin/main"

    worktree_git.is_merged.side_effect = _is_merged
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-remote",
        "origin/main": "base-origin",
        "main": "base-local-stale",
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
            remote=True,
        )

    assert rc == 0
    worktree_git.rebase.assert_called_once_with("origin/main")
    worktree_git.is_merged.assert_called_once_with("feature/rebased", into="origin/main")

    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.merge_status == "merged"
    assert refreshed_parent.merged_at is not None

    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    assert refreshed_unit.target_branch == "main"
    assert refreshed_unit.base_sha == "base-origin"


def test_run_task_backed_rebase_remote_does_not_mark_merged_from_stale_local_target(tmp_path) -> None:
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
    repo_git.fetch.return_value = None
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.branch_exists.return_value = True
    worktree_git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    def _is_merged(branch, into):
        return into == "main"

    worktree_git.is_merged.side_effect = _is_merged
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-remote",
        "origin/main": "base-origin",
        "main": "base-local-stale",
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
            remote=True,
        )

    assert rc == 0
    worktree_git.rebase.assert_called_once_with("origin/main")
    worktree_git.is_merged.assert_called_once_with("feature/rebased", into="origin/main")

    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.merge_status == "unmerged"
    assert refreshed_parent.merged_at is None

    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.target_branch == "main"
    assert refreshed_unit.base_sha == "base-origin"


def test_run_task_backed_rebase_failure_does_not_reconcile_parent_merge_status(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
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
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=False),
        patch("gza.cli.git_ops.mark_task_failed_from_cause", return_value=None) as mark_failed,
        patch("gza.cli.git_ops.reconcile_task_branch_merge_truth") as reconcile_task_branch_merge_truth,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    refreshed_parent = store.get(parent.id)
    assert refreshed_parent is not None
    assert refreshed_parent.merge_status == "unmerged"
    mark_failed.assert_called_once()
    reconcile_task_branch_merge_truth.assert_not_called()


def test_run_task_backed_rebase_provider_resolve_publishes_via_shared_helper(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
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
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    publish_rebased_branch.assert_called_once()


def test_run_task_backed_rebase_clean_publish_failure_does_not_fall_back_to_provider(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
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

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops.publish_rebased_branch", side_effect=GitError("push boom")),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
        patch("gza.cli.git_ops.mark_task_failed_from_cause", return_value=None) as mark_failed,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    invoke_provider_resolve.assert_not_called()
    worktree_git.rebase_abort.assert_not_called()
    mark_failed.assert_called_once()
    assert mark_failed.call_args.kwargs["explicit_reason"] == "GIT_ERROR"


def test_run_task_backed_rebase_remote_ref_lookup_failure_marks_task_failed(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
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
    worktree_git.rev_parse.return_value = "head-new"
    worktree_git.rev_parse_if_exists.side_effect = RuntimeError("remote lookup boom")

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    invoke_provider_resolve.assert_not_called()
    worktree_git.push_force_with_lease.assert_not_called()

    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"


def test_run_task_backed_rebase_non_advancing_publish_succeeds_when_branch_is_already_up_to_date(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)
    assert rebase_task.id is not None

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse.return_value = "same-head"
    worktree_git.is_ancestor.return_value = True
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "same-head",
        "main": "base-old",
        "origin/feature/rebased": "remote-stale",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="same-head",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=False, detail="no (review preserved)"),
        ),
        patch("gza.cli.git_ops.invoke_provider_resolve") as invoke_provider_resolve,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    invoke_provider_resolve.assert_not_called()
    worktree_git.push_force_with_lease.assert_called_once_with("feature/rebased", remote="origin")

    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None


@pytest.mark.timeout(4, method="signal")
def test_advance_explicit_merge_refuses_when_checkout_does_not_match_canonical_target(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_text = config_path.read_text()
    config_path.write_text(config_text + "advance_requires_review: false\n")

    store = make_store(tmp_path)

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

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = task.branch
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(task.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

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

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
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
    fake_git.merge.assert_not_called()
    fake_git.is_merged.assert_called()


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


def test_reconcile_diverged_branch_with_origin_force_pushes_gza_rewrite(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-1", branch="feature/rewrite")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/rewrite": "remote-old",
        "feature/rewrite": "local-new",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/rewrite' and remote-tracking ref 'origin/feature/rewrite' diverged. "
            "Push, fetch, or reconcile them before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.return_value = True

    result = _reconcile_diverged_branch_with_origin(config, git, task)

    assert result.status == "reconciled"
    assert "force-with-lease" in result.message
    git.push_ref_force_with_lease.assert_called_once_with(
        "feature/rewrite",
        "feature/rewrite",
        remote="origin",
        expected_remote_oid="remote-old",
    )
    git.fetch.assert_not_called()
    assert git.is_merged.call_args_list == [
        call("feature/rewrite", into="origin/feature/rewrite", use_cherry=True),
        call("origin/feature/rewrite", into="feature/rewrite", use_cherry=True),
    ]


def test_reconcile_diverged_branch_with_origin_force_pushes_stale_origin_rewrite(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-1b", branch="feature/rebased-rewrite")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/rebased-rewrite": "remote-pre-rebase-tip",
        "feature/rebased-rewrite": "local-rebased-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/rebased-rewrite' and remote-tracking ref "
            "'origin/feature/rebased-rewrite' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [2, 2]
    git.is_merged.side_effect = [True, True]

    result = _reconcile_diverged_branch_with_origin(config, git, task)

    assert result.status == "reconciled"
    assert "force-with-lease" in result.message
    git.push_ref_force_with_lease.assert_called_once_with(
        "feature/rebased-rewrite",
        "feature/rebased-rewrite",
        remote="origin",
        expected_remote_oid="remote-pre-rebase-tip",
    )
    git.fetch.assert_not_called()
    assert git.is_merged.call_args_list == [
        call("feature/rebased-rewrite", into="origin/feature/rebased-rewrite", use_cherry=True),
        call("origin/feature/rebased-rewrite", into="feature/rebased-rewrite", use_cherry=True),
    ]


def test_reconcile_diverged_branch_with_origin_rebases_already_fetched_external_commits(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-2a", branch="feature/already-fetched-external")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/already-fetched-external": "remote-visible",
        "feature/already-fetched-external": "local-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/already-fetched-external' and remote-tracking ref "
            "'origin/feature/already-fetched-external' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.side_effect = [True, False]

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task)

    assert result.status == "reconciled"
    git.push_ref_force_with_lease.assert_not_called()
    git.fetch.assert_called_once_with("origin")
    worktree_git.rebase.assert_called_once_with("origin/feature/already-fetched-external")
    publish_rebased_branch.assert_called_once()
    assert git.is_merged.call_args_list == [
        call(
            "feature/already-fetched-external",
            into="origin/feature/already-fetched-external",
            use_cherry=True,
        ),
        call(
            "origin/feature/already-fetched-external",
            into="feature/already-fetched-external",
            use_cherry=True,
        ),
    ]


def test_reconcile_diverged_branch_with_origin_reports_already_aligned_after_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-2b", branch="feature/already-aligned")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/already-aligned": "remote-visible",
        "feature/already-aligned": "local-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/already-aligned' and remote-tracking ref "
            "'origin/feature/already-aligned' diverged. Push, fetch, or reconcile them "
            "before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.side_effect = [True, False]

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
        patch(
            "gza.cli.git_ops.publish_rebased_branch",
            return_value=SimpleNamespace(pushed=False),
        ),
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task)

    assert result.status == "reconciled"
    assert "verified origin was already aligned" in result.message
    assert "and pushed" not in result.message


def test_reconcile_diverged_branch_with_origin_rebases_after_remote_moves(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-2", branch="feature/external")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = ["remote-old", "local-tip", "remote-new"]
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef("feature/external")
    git.count_commits_ahead.side_effect = [1, 0]
    git.push_ref_force_with_lease.side_effect = GitError("stale info")

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.return_value = None

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task)

    assert result.status == "reconciled"
    assert "Rebased 'feature/external' onto 'origin/feature/external'" in result.message
    git.fetch.assert_called_once_with("origin")
    worktree_git.rebase.assert_called_once_with("origin/feature/external")
    publish_rebased_branch.assert_called_once()


def test_reconcile_diverged_branch_with_origin_routes_conflicts_to_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-3", branch="feature/conflict")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = ["remote-old", "local-tip", "remote-new"]
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef("feature/conflict")
    git.count_commits_ahead.side_effect = [1, 0]
    git.push_ref_force_with_lease.side_effect = GitError("stale info")

    worktree_git = MagicMock(spec=Git)
    worktree_git.rebase.side_effect = GitError("conflict")

    with (
        patch("gza.cli.git_ops.Git", return_value=worktree_git),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline("old", "target", "base"),
        ),
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task)

    assert result.status == "needs_rebase"
    assert result.rebase_target == "origin/feature/conflict"
    worktree_git.rebase_abort.assert_called_once()


def test_advance_batch_limit_skips_reconcile_conflict_fallback_without_spawning_rebase(
    tmp_path: Path,
    capsys,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    first = store.add("Needs explicit rebase", task_type="implement")
    second = store.add("Needs reconcile fallback", task_type="implement")
    for task, branch in ((first, "feature/needs-rebase"), (second, "feature/reconcile-fallback")):
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

    first_row = LineageOwnerRow(
        owner_task=first,
        members=(first,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "needs_rebase", "description": "Create rebase task"},
        next_action_reason="test",
        unresolved_tasks=(first,),
        unresolved_leaf_summary=(),
    )
    second_row = LineageOwnerRow(
        owner_task=second,
        members=(second,),
        tree=None,
        lineage_status="actionable",
        next_action={
            "type": "reconcile_branch_divergence",
            "description": "Reconcile diverged local/origin refs",
        },
        next_action_reason="test",
        unresolved_tasks=(second,),
        unresolved_leaf_summary=(),
    )

    created_rebases: list[str] = []

    def _create_rebase_task(_store, parent_id: str, branch: str, target: str, *, trigger_source: str):
        created = _store.add(
            prompt=f"Rebase {branch} onto {target}",
            task_type="rebase",
            based_on=parent_id,
            same_branch=True,
            trigger_source=trigger_source,
        )
        assert created.id is not None
        created_rebases.append(created.id)
        return created

    args = _advance_args(tmp_path, first.id)
    args.task_id = None
    args.batch = 1

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[first_row, second_row]),
        patch("gza.cli.git_ops._create_rebase_task", side_effect=_create_rebase_task),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=lambda _config, task, **_k: task),
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0) as spawn_worker,
        patch(
            "gza.cli.git_ops._reconcile_diverged_branch_with_origin",
            return_value=SimpleNamespace(
                status="needs_rebase",
                message="Mechanical rebase conflicted",
                rebase_target="origin/feature/reconcile-fallback",
            ),
        ),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    assert spawn_worker.call_count == 1
    assert len(created_rebases) == 1
    output = capsys.readouterr().out
    assert "batch limit reached (1/1), cannot start rebase worker" in output


@pytest.mark.timeout(4, method="signal")
def test_advance_dry_run_surfaces_diverged_merge_source_for_reconcile(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.git import ResolvedMergeSourceRef

    setup_config(tmp_path)
    store = make_store(tmp_path)
    branch = "feature/advance-diverged"

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

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "merge-source-needs-manual-resolution: "
            f"local branch '{branch}' and remote tracking ref 'origin/{branch}' diverged"
        ),
    )
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True

    with patch("gza.cli.git_ops.Git", return_value=fake_git):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "Would advance 1 task(s):" in output
    assert "Reconcile diverged local/origin refs" in output
    assert "Needs attention" not in output


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


def test_reconcile_squash_merge_skips_when_no_remote_tracking_ref() -> None:
    git = MagicMock(spec=Git)

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid=None,
    )

    assert result.status == "skipped_no_remote_tracking_ref"
    git.update_ref.assert_not_called()
    git.push_ref_force_with_lease.assert_not_called()


def test_reconcile_squash_merge_updates_local_and_remote_tracking_refs_before_and_after_push() -> None:
    git = MagicMock(spec=Git)

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "updated"
    assert git.update_ref.call_args_list == [
        call("refs/heads/feature/demo", "squash-oid", "local-oid"),
        call("refs/remotes/origin/feature/demo", "squash-oid"),
    ]
    git.push_ref_force_with_lease.assert_called_once_with(
        "refs/heads/feature/demo",
        "feature/demo",
        remote="origin",
        expected_remote_oid="remote-oid",
    )


def test_reconcile_squash_merge_local_ref_update_failure_prevents_push() -> None:
    git = MagicMock(spec=Git)
    git.update_ref.side_effect = GitError("branch is checked out elsewhere")

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "failed_local_ref_update"
    assert result.manual_source_ref is None
    git.push_ref_force_with_lease.assert_not_called()


def test_reconcile_squash_merge_lease_rejection_is_reported_without_updating_tracking_ref() -> None:
    git = MagicMock(spec=Git)
    git.push_ref_force_with_lease.side_effect = GitError("git push failed:\n! [rejected] (stale info)")

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid=None,
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "failed_push_rejected"
    assert git.update_ref.call_args_list == []


def test_reconcile_squash_merge_remote_tracking_ref_update_failure_reports_post_push_status() -> None:
    git = MagicMock(spec=Git)
    git.update_ref.side_effect = [
        None,
        GitError("cannot lock ref 'refs/remotes/origin/feature/demo'"),
    ]

    result = _reconcile_squash_merged_branch_with_origin(
        git,
        branch="feature/demo",
        squash_oid="squash-oid",
        pre_squash_local_oid="local-oid",
        pre_squash_remote_oid="remote-oid",
    )

    assert result.status == "failed_remote_tracking_ref_update"
    git.push_ref_force_with_lease.assert_called_once_with(
        "refs/heads/feature/demo",
        "feature/demo",
        remote="origin",
        expected_remote_oid="remote-oid",
    )
    assert git.update_ref.call_args_list == [
        call("refs/heads/feature/demo", "squash-oid", "local-oid"),
        call("refs/remotes/origin/feature/demo", "squash-oid"),
    ]


def test_classify_squash_reconcile_push_failure_keeps_policy_rejections_distinct() -> None:
    exc = GitError(
        "git push failed:\n"
        "! [remote rejected] feature/demo -> feature/demo (protected branch hook declined)"
    )

    assert _classify_squash_reconcile_push_failure(exc) == "failed_push_unavailable"


def test_print_squash_reconcile_result_does_not_emit_lease_guidance_for_policy_rejection(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_push_unavailable",
            branch="feature/demo",
            reason="git push failed: protected branch hook declined",
            manual_source_ref="HEAD",
            expected_remote_oid="remote-oid",
        )
    )

    output = capsys.readouterr().out
    assert "changed since it was last observed" not in output
    assert "protected branch hook declined" in output
    assert "git push --force-with-lease=refs/heads/feature/demo:remote-oid" in output


def test_print_squash_reconcile_result_emits_lease_guidance_for_stale_info_rejection(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_push_rejected",
            branch="feature/demo",
            reason="git push failed: stale info",
            manual_source_ref="HEAD",
            expected_remote_oid="remote-oid",
        )
    )

    output = capsys.readouterr().out
    assert "changed since it was last observed" in output
    assert "git push --force-with-lease=refs/heads/feature/demo:remote-oid" in output


def test_print_squash_reconcile_result_failed_local_ref_update_fails_closed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_local_ref_update",
            branch="feature/demo",
            reason="branch is checked out elsewhere",
            expected_remote_oid="remote-oid",
        )
    )

    output = capsys.readouterr().out
    assert "branch is checked out elsewhere" in output
    assert "Reconcile the local branch 'feature/demo' first" in output
    assert "known to point at the squash merge commit" in output
    assert "origin feature/demo:refs/heads/feature/demo" not in output
    assert "Manual repair:" not in output


def test_tracking_ref_refresh_command_forces_non_fast_forward_tracking_update() -> None:
    assert _tracking_ref_refresh_command(
        remote="origin",
        branch="feature/demo",
    ) == "git fetch origin +refs/heads/feature/demo:refs/remotes/origin/feature/demo"


def test_print_squash_reconcile_result_failed_remote_tracking_ref_update_reports_refresh_action(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="failed_remote_tracking_ref_update",
            branch="feature/demo",
            reason="cannot lock ref 'refs/remotes/origin/feature/demo'",
        )
    )

    output = capsys.readouterr().out
    assert "remote push succeeded" in output
    assert "could not be reconciled" not in output
    assert "could not be updated" in output
    assert "git push --force-with-lease" not in output
    assert "git fetch origin +refs/heads/feature/demo:refs/remotes/origin/feature/demo" in output
