"""Tests for git-oriented CLI helpers."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, MagicMock, call, patch

import pytest

from gza.cli.git_ops import (
    _execute_merge_action,
    _MergeSingleTaskResult,
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

from .conftest import make_store, invoke_gza, setup_config


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


def _add_completed_impl_with_approved_review(store, branch: str, *, when: datetime) -> tuple[Any, Any]:
    task = store.add(f"Implement {branch}", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = when
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = when
    review.report_file = "reviews/fake.md"
    store.update(review)
    return task, review


def _make_preload_recording_git(tmp_path: Path) -> tuple[MagicMock, list[tuple[tuple[str, ...], str]], list[tuple[str, ...]]]:
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = True
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    ref_calls: list[tuple[tuple[str, ...], str]] = []
    branch_calls: list[tuple[str, ...]] = []

    def _resolve_refs(refs, peel="commit"):
        ref_tuple = tuple(refs)
        ref_calls.append((ref_tuple, peel))
        return {ref: f"{ref}-{peel}-sha" for ref in ref_tuple}

    def _branches_exist(branches):
        branch_tuple = tuple(branches)
        branch_calls.append(branch_tuple)
        return {branch: True for branch in branch_tuple}

    fake_git.resolve_refs.side_effect = _resolve_refs
    fake_git.branches_exist.side_effect = _branches_exist
    return fake_git, ref_calls, branch_calls


def _assert_scoped_preload_refs(
    ref_calls: list[tuple[tuple[str, ...], str]],
    branch_calls: list[tuple[str, ...]],
    *,
    requested_branch: str,
    unrelated_branches: tuple[str, ...],
    target_branch: str = "main",
) -> None:
    preloaded_refs = {ref for refs, _peel in ref_calls for ref in refs}
    preloaded_branches = {branch for branches in branch_calls for branch in branches}

    assert requested_branch in preloaded_branches
    assert requested_branch in preloaded_refs
    assert f"origin/{requested_branch}" in preloaded_refs
    assert target_branch in preloaded_refs

    for branch in unrelated_branches:
        assert branch not in preloaded_branches
        assert branch not in preloaded_refs
        assert f"origin/{branch}" not in preloaded_refs


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


def test_merge_single_task_returns_blocked_dirty_checkout_status(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement dirty checkout guard", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/dirty-checkout-guard"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=True),
        can_merge=MagicMock(),
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
    assert result.status == "blocked_dirty_checkout"
    assert result.block_reason == "main checkout has uncommitted changes"
    git.can_merge.assert_not_called()
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "You have uncommitted changes. Please commit or stash them first." in output


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


def test_execute_merge_action_mark_merged_rejects_failed_owner_without_marking_unit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TIMEOUT"
    failed.completed_at = datetime.now(UTC)
    failed.branch = "feature/failed-owner-mark-merged"
    failed.has_commits = True
    failed.merge_status = "unmerged"
    store.update(failed)

    merge_git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=True),
    )
    git = SimpleNamespace(repo_dir=tmp_path)

    result = _execute_merge_action(
        config,
        store,
        git,
        failed,
        {"type": "merge", "description": "Merge"},
        target_branch="main",
        current_branch="main",
        merge_git=merge_git,
        merge_current_branch="main",
        already_merged_behavior="mark_merged",
    )

    assert result.rc == 1
    output = capsys.readouterr().out
    assert f"Error: Task {failed.id} is not completed or unmerged (execution status: failed)" in output
    refreshed = store.resolve_merge_unit_for_task(failed.id)
    assert refreshed is not None
    assert refreshed.state == "unmerged"


def test_execute_merge_action_propagates_blocked_dirty_checkout_status(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed implementation", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/dirty-checkout-execute"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(repo_dir=tmp_path)

    with patch(
        "gza.cli.git_ops._merge_single_task",
        return_value=_MergeSingleTaskResult(
            rc=1,
            status="blocked_dirty_checkout",
            block_reason="main checkout has uncommitted changes",
            pending_squash_reconcile=None,
        ),
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            task,
            {"type": "merge", "description": "Merge"},
            target_branch="main",
            current_branch="main",
        )

    assert result.rc == 1
    assert result.status == "blocked_dirty_checkout"
    assert result.block_reason == "main checkout has uncommitted changes"


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
        result = invoke_gza("checkout", str(task.id), "--project", str(tmp_path))

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
    config_path.write_text(config_text + "require_review_before_merge: false\n")
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
    config_path.write_text(config_text + "require_review_before_merge: false\n")

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


def test_cmd_advance_wraps_planning_in_git_cache(tmp_path: Path) -> None:
    from contextlib import contextmanager

    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance cached planning", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/cache-planning"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    cached_entries: list[str] = []

    @contextmanager
    def _cached_scope():
        cached_entries.append("entered")
        yield fake_git

    fake_git.cached.side_effect = _cached_scope

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
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
        )

    assert rc == 0
    assert cached_entries == ["entered"]


def test_cmd_advance_batches_ref_preloads_during_lifecycle_planning(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance cached lifecycle planning", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/cache-lifecycle"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = Git(tmp_path)
    git.current_branch = MagicMock(return_value="main")  # type: ignore[method-assign]
    git.default_branch = MagicMock(return_value="main")  # type: ignore[method-assign]

    git_calls: list[tuple[str, ...]] = []
    batch_stdins: list[bytes] = []

    def _fake_run(*args: str, check: bool = True, stdin: bytes | None = None):
        del check
        git_calls.append(args)
        if args == ("for-each-ref", "--format=%(refname:strip=2)", "refs/heads/"):
            return SimpleNamespace(returncode=0, stdout="feature/cache-lifecycle\n", stderr="")
        if args == ("cat-file", "--batch-check"):
            assert stdin is not None
            batch_stdins.append(stdin)
            request = stdin.decode().splitlines()
            response_lines: list[str] = []
            for line in request:
                if line == "feature/cache-lifecycle^{commit}":
                    response_lines.append("a" * 40 + " commit 1")
                elif line == "origin/feature/cache-lifecycle^{commit}":
                    response_lines.append("origin/feature/cache-lifecycle^{commit} missing")
                elif line == "main^{commit}":
                    response_lines.append("b" * 40 + " commit 1")
                elif line == "main^{tree}":
                    response_lines.append("c" * 40 + " tree 1")
                else:
                    raise AssertionError(f"Unexpected batch ref: {line!r}")
            return SimpleNamespace(returncode=0, stdout="\n".join(response_lines) + "\n", stderr="")
        if args == ("merge-base", "--is-ancestor", "main", "feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args == ("rev-list", "--count", "main..feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="2\n", stderr="")
        if args == ("merge-tree", "--write-tree", "main", "feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="target-tree\n", stderr="")
        if args == ("rev-list", "--count", "origin/feature/cache-lifecycle..feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="2\n", stderr="")
        if args == ("rev-list", "--count", "feature/cache-lifecycle..origin/feature/cache-lifecycle"):
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected git command: {args!r}")

    git._run = _fake_run  # type: ignore[method-assign]

    def _query_rows(*_args, **_kwargs):
        assert git._cache is not None
        assert git.branch_exists("feature/cache-lifecycle") is True
        assert git.rev_parse_if_exists("feature/cache-lifecycle") == "a" * 40
        assert git.ref_exists("origin/feature/cache-lifecycle") is False
        assert git.branch_exists("feature/cache-lifecycle") is True
        return iter(
            [
                LineageOwnerRow(
                    owner_task=task,
                    members=(task,),
                    tree=None,
                    lineage_status="skipped",
                    next_action={"type": "skip", "description": "nothing to do"},
                    next_action_reason="precomputed",
                    unresolved_tasks=(task,),
                    unresolved_leaf_summary=(),
                    lifecycle_action_task=task,
                    recovery_action_task=None,
                    recovery_leaf_task=None,
                )
            ]
        )

    def _determine_next_action(*_args, **_kwargs):
        assert git._cache is not None
        assert git.branch_exists("feature/cache-lifecycle") is True
        assert git.rev_parse_if_exists("main") == "b" * 40
        assert git.ref_exists("origin/feature/cache-lifecycle") is False
        assert git.branch_exists("feature/cache-lifecycle") is True
        return {"type": "skip", "description": "nothing to do"}

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_determine_next_action),
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
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
        )

    assert rc == 0
    assert ("show-ref", "--verify", "--quiet", "refs/heads/feature/cache-lifecycle") not in git_calls
    assert ("rev-parse", "--verify", "--quiet", "feature/cache-lifecycle^{commit}") not in git_calls
    assert ("rev-parse", "--verify", "--quiet", "origin/feature/cache-lifecycle^{commit}") not in git_calls
    assert ("rev-parse", "--verify", "--quiet", "main^{commit}") not in git_calls
    assert ("for-each-ref", "--format=%(refname:strip=2)", "refs/heads/") in git_calls
    assert ("cat-file", "--batch-check") in git_calls
    assert len(set(git_calls)) == 2
    assert any(b"feature/cache-lifecycle^{commit}\n" in payload for payload in batch_stdins)


def test_cmd_advance_uses_shared_ref_preload_helper(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Advance shared preload helper", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-preload"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
        patch("gza.cli.git_ops.prime_advance_planning_refs") as preload,
    ):
        rc = cmd_advance(
            argparse.Namespace(
                project_dir=tmp_path,
                task_id=task.id,
                dry_run=True,
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
        )

    assert rc == 0
    preload.assert_called_once_with(
        fake_git,
        branch_names=["feature/shared-preload"],
        target_branch="main",
        warning_logger=ANY,
    )


def test_cmd_advance_explicit_child_member_scopes_query_to_owner_lineage(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl, review = _add_completed_impl_with_approved_review(
        store,
        "feature/member-owner-scope",
        when=datetime.now(UTC),
    )

    row = LineageOwnerRow(
        owner_task=impl,
        members=(impl, review),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(impl,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=impl,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"

    captured_queries: list = []

    def _query_rows(_store, query, **_kwargs):
        captured_queries.append(query)
        return [row]

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, review.id)), "dry_run": True}))

    assert rc == 0
    assert len(captured_queries) == 1
    assert captured_queries[0].task_ids == (review.id,)
    assert captured_queries[0].owner_task_ids is None


def test_cmd_advance_explicit_failed_leaf_scopes_query_to_owner_lineage(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/failed-leaf-owner-scope",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )

    failed_rebase = store.add("Failed rebase leaf", task_type="rebase", based_on=impl.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    failed_rebase.branch = impl.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    row = LineageOwnerRow(
        owner_task=impl,
        members=(impl, failed_rebase),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "needs_discussion",
            "description": "failed rebase still blocks merge",
            "needs_attention_reason": "rebase-failed",
            "subject_task_id": failed_rebase.id,
        },
        next_action_reason="rebase-failed",
        unresolved_tasks=(failed_rebase,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=impl,
        recovery_action_task=failed_rebase,
        recovery_leaf_task=failed_rebase,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"

    captured_queries: list = []

    def _query_rows(_store, query, **_kwargs):
        captured_queries.append(query)
        return [row]

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed_rebase.id)), "dry_run": True}))

    assert rc == 0
    assert len(captured_queries) == 1
    assert captured_queries[0].task_ids == (failed_rebase.id,)
    assert captured_queries[0].owner_task_ids is None


def test_cmd_advance_explicit_dropped_owner_fallback_scopes_second_query_to_owner_lineage(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Dropped owner", task_type="implement")
    assert owner.id is not None
    owner.status = "dropped"
    owner.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    owner.branch = "feature/dropped-owner-scope"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    descendant = store.add("Completed descendant", task_type="rebase", based_on=owner.id, same_branch=True)
    assert descendant.id is not None
    descendant.status = "completed"
    descendant.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    descendant.branch = owner.branch
    descendant.has_commits = True
    descendant.merge_status = "unmerged"
    store.update(descendant)

    dropped_row = LineageOwnerRow(
        owner_task=owner,
        members=(owner, descendant),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "dropped"},
        next_action_reason="dropped",
        unresolved_tasks=(descendant,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=descendant,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(owner.branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    captured_queries: list = []

    def _query_rows(_store, query, **_kwargs):
        captured_queries.append(query)
        if query.exclude_dropped_from_planning:
            return []
        return [dropped_row]

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", side_effect=_query_rows),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, descendant.id)), "dry_run": True}))

    assert rc == 0
    assert len(captured_queries) == 2
    assert captured_queries[0].task_ids == (descendant.id,)
    assert captured_queries[0].owner_task_ids is None
    assert captured_queries[0].exclude_dropped_from_planning is True
    assert captured_queries[1].task_ids == (descendant.id,)
    assert captured_queries[1].owner_task_ids is None
    assert captured_queries[1].exclude_dropped_from_planning is False
    assert "No eligible tasks to advance" in capsys.readouterr().out


def test_cmd_advance_explicit_task_plans_only_requested_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    requested, _ = _add_completed_impl_with_approved_review(
        store,
        "feature/requested-lineage",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )
    for index in range(3):
        _add_completed_impl_with_approved_review(
            store,
            f"feature/unrelated-lineage-{index}",
            when=datetime(2026, 5, 10, 10 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)

    merge_sources: list[str] = []
    merge_checks: list[str] = []

    def _resolve_fresh_merge_source(branch: str):
        merge_sources.append(branch)
        return ResolvedMergeSourceRef(f"origin/{branch}")

    def _can_merge(source_ref: str, target_branch: str):
        assert target_branch == "main"
        merge_checks.append(source_ref)
        return True

    fake_git.resolve_fresh_merge_source.side_effect = _resolve_fresh_merge_source
    fake_git.can_merge.side_effect = _can_merge

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, requested.id)), "dry_run": True}))

    assert rc == 0
    assert merge_sources
    assert set(merge_sources) == {requested.branch}
    assert set(merge_checks) == {f"origin/{requested.branch}"}
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=requested.branch,
        unrelated_branches=tuple(f"feature/unrelated-lineage-{index}" for index in range(3)),
    )


def test_cmd_advance_explicit_child_member_preloads_only_owner_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    requested, review = _add_completed_impl_with_approved_review(
        store,
        "feature/member-owner-scope",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )
    unrelated_branches = tuple(f"feature/member-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 10, 10 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{requested.branch}")

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, review.id)), "dry_run": True}))

    assert rc == 0
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=requested.branch,
        unrelated_branches=unrelated_branches,
    )


def test_cmd_advance_explicit_failed_leaf_preloads_only_owner_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)
    requested, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/failed-leaf-owner-scope",
        when=datetime(2026, 5, 10, 9, 0, tzinfo=UTC),
    )

    failed_rebase = store.add("Failed rebase leaf", task_type="rebase", based_on=requested.id, same_branch=True)
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    failed_rebase.branch = requested.branch
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    store.update(failed_rebase)

    unrelated_branches = tuple(f"feature/failed-leaf-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 10, 11 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(f"origin/{requested.branch}")

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed_rebase.id)), "dry_run": True}))

    assert rc == 0
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=requested.branch,
        unrelated_branches=unrelated_branches,
    )


def test_cmd_advance_explicit_orphan_same_branch_leaf_uses_representative_row(
    tmp_path: Path,
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "in_progress"
    impl.branch = "feature/canonical"
    impl.has_commits = True
    store.update(impl)

    orphan = store.add("Completed orphan rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    assert orphan.id is not None
    orphan.status = "completed"
    orphan.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    orphan.branch = "feature/orphan"
    orphan.has_commits = True
    orphan.merge_status = "unmerged"
    store.update(orphan)
    orphan_unit = store.create_merge_unit(
        source_branch=orphan.branch,
        target_branch="main",
        owner_task_id=orphan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(orphan.id, orphan_unit.id, "owner")

    unrelated_branches = tuple(f"feature/orphan-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 12, 11 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(impl.branch)

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, orphan.id)), "dry_run": True}))

    output = capsys.readouterr().out
    assert rc == 0
    assert "Needs attention (1 task):" in output
    assert f'{impl.id} implement "Implement feature"' in output
    assert "reason=no-descendant-on-the-impl-branch" in output
    assert "pending command evaluation" not in output
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=impl.branch,
        unrelated_branches=unrelated_branches,
    )


def test_cmd_advance_explicit_dropped_owner_fallback_preloads_only_requested_lineage_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from gza import advance_engine as advance_engine_module

    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Dropped owner", task_type="implement")
    assert owner.id is not None
    owner.status = "dropped"
    owner.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    owner.branch = "feature/dropped-owner-scope"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    requested = store.add("Completed descendant", task_type="rebase", based_on=owner.id, same_branch=True)
    assert requested.id is not None
    requested.status = "completed"
    requested.completed_at = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    requested.branch = owner.branch
    requested.has_commits = True
    requested.merge_status = "unmerged"
    store.update(requested)

    unrelated_branches = tuple(f"feature/dropped-owner-unrelated-{index}" for index in range(2))
    for index, branch in enumerate(unrelated_branches):
        _add_completed_impl_with_approved_review(
            store,
            branch,
            when=datetime(2026, 5, 12, 11 + index, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        advance_engine_module,
        "get_review_report",
        lambda _project_dir, _review_task: SimpleNamespace(
            verdict="APPROVED",
            findings=(),
            format_version="legacy",
        ),
    )

    fake_git, ref_calls, branch_calls = _make_preload_recording_git(tmp_path)
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(owner.branch)

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, requested.id)), "dry_run": True}))

    assert rc == 0
    _assert_scoped_preload_refs(
        ref_calls,
        branch_calls,
        requested_branch=owner.branch,
        unrelated_branches=unrelated_branches,
    )


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
    git._run.assert_not_called()


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
    git._run.assert_not_called()


def test_reconcile_diverged_branch_with_origin_force_pushes_dead_wip_savepoint_divergence(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-1c", branch="feature/dead-wip")

    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "origin/feature/dead-wip": "remote-wip-tip",
        "feature/dead-wip": "local-final-tip",
    }.get(ref)
    git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(
        None,
        (
            "Local branch 'feature/dead-wip' and remote-tracking ref 'origin/feature/dead-wip' diverged. "
            "Push, fetch, or reconcile them before advancing or merging."
        ),
    )
    git.count_commits_ahead.side_effect = [1, 1]
    git.is_merged.side_effect = [True, False]
    git._run.side_effect = lambda *args, **kwargs: (
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n")
        if args[:3] == ("merge-base", "feature/already-fetched-external", "origin/feature/already-fetched-external")
        else (
            SimpleNamespace(returncode=0, stdout="External commit\n")
            if args[:3] == ("log", "--format=%s", "merge-base-oid..origin/feature/already-fetched-external")
            else SimpleNamespace(returncode=0, stdout="")
        )
    )
    git._run.side_effect = [
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n"),
        SimpleNamespace(returncode=0, stdout="WIP: gza task interrupted\n"),
    ]

    result = _reconcile_diverged_branch_with_origin(config, git, task)

    assert result.status == "reconciled"
    assert "force-with-lease" in result.message
    git.push_ref_force_with_lease.assert_called_once_with(
        "feature/dead-wip",
        "feature/dead-wip",
        remote="origin",
        expected_remote_oid="remote-wip-tip",
    )
    git.fetch.assert_not_called()
    assert git._run.call_args_list == [
        call("merge-base", "feature/dead-wip", "origin/feature/dead-wip", check=False),
        call(
            "log",
            "--format=%s",
            "merge-base-oid..origin/feature/dead-wip",
            "--not",
            "feature/dead-wip",
            check=False,
        ),
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
    git._run.side_effect = lambda *args, **kwargs: (
        SimpleNamespace(returncode=0, stdout="merge-base-oid\n")
        if args[:3]
        == ("merge-base", "feature/already-fetched-external", "origin/feature/already-fetched-external")
        else (
            SimpleNamespace(returncode=0, stdout="External commit\n")
            if args[:3] == ("log", "--format=%s", "merge-base-oid..origin/feature/already-fetched-external")
            else SimpleNamespace(returncode=0, stdout="")
        )
    )

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
    git._run.assert_any_call(
        "merge-base",
        "feature/already-fetched-external",
        "origin/feature/already-fetched-external",
        check=False,
    )
    git._run.assert_any_call(
        "log",
        "--format=%s",
        "merge-base-oid..origin/feature/already-fetched-external",
        "--not",
        "feature/already-fetched-external",
        check=False,
    )


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

    assert result.status == "needs_attention"
    assert result.attention_reason == "reconcile-needs-manual-resolution"
    assert "sandboxed rebase worker cannot access that remote-tracking ref" in result.message
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


def test_advance_retryable_provider_attention_recommends_fix_even_without_actionable_work(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Investigate flaky provider run", task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "RETRYABLE_PROVIDER_ERROR"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "needs_discussion",
            "description": "Fresh retry already consumed; retryable provider error now requires manual review",
            "needs_attention_reason": "retryable-provider-error",
            "subject_task_id": task.id,
        },
        next_action_reason="retryable-provider-error",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
    )

    args = _advance_args(tmp_path, task.id)
    args.task_id = None

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "No eligible tasks to advance" in output
    assert "reason=retryable-provider-error" in output
    assert f"Recommended next step: uv run gza fix {task.id}" in output


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
        result = invoke_gza("rebase", str(impl_task.id), "--background", "--project", str(tmp_path))

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
        result = invoke_gza("rebase", str(impl_task.id), "--background", "--project", str(tmp_path))

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
