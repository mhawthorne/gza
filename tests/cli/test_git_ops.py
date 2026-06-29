"""Tests for git-oriented CLI helpers."""

import argparse
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, MagicMock, call, patch

import pytest

from gza.cli._lifecycle_actions import should_execute_lifecycle_action as real_should_execute_lifecycle_action
from gza.cli.advance_executor import AdvanceActionExecutionResult
from gza.cli.git_ops import (
    _execute_merge_action,
    _MergeSingleTaskResult,
    _remove_watch_merge_checkout,
    _classify_rebase_git_failure,
    _reconcile_diverged_branch_with_origin,
    SquashBranchReconcileResult,
    _build_auto_merge_args,
    _classify_squash_reconcile_push_failure,
    ensure_watch_main_checkout,
    _merge_single_task,
    _print_squash_reconcile_result,
    _reconcile_squash_merged_branch_with_origin,
    _resolve_merge_subject,
    _run_task_backed_rebase,
    _tracking_ref_refresh_command,
    cmd_advance,
    cmd_rebase,
)
from gza.concurrency import launch_permit
from gza.config import Config
from gza.db import DuplicateActiveChildError
from gza.git import Git, GitError, ResolvedGitRef, ResolvedMergeSourceRef
from gza.lineage_query import LineageOwnerRow
from gza.rebase_diff import RebaseDiffBaseline, RebaseDiffResult
from gza.review_verify_state import persist_verify_gate_artifact
from gza.review_verdict import ReviewFinding
from gza.worktree_roots import managed_worktree_root_paths

from .conftest import make_store, invoke_gza, setup_config


@pytest.fixture(autouse=True)
def _stub_main_integration_verify() -> object:
    with patch(
        "gza.cli.git_ops.check_main_integration_verify",
        return_value=SimpleNamespace(
            merges_halted=False,
            state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
        ),
    ) as mocked:
        yield mocked


@contextmanager
def _force_merge_planner_action():
    with patch(
        "gza.cli.git_ops.determine_next_action",
        return_value={"type": "merge", "description": "Merge"},
    ):
        yield


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


def _make_read_session_reconciliation_git(tmp_path: Path, branch: str) -> MagicMock:
    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.local_branch_names.return_value = [branch]
    fake_git.resolve_fresh_merge_source.return_value = ResolvedMergeSourceRef(branch)
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    return fake_git


def _add_prerequisite_unmerged_failed_child(
    store,
    *,
    owner_status: str = "completed",
    owner_branch: str,
) -> tuple[Any, Any, Any]:
    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    owner = store.add("Owner task", task_type="implement")
    assert owner.id is not None
    owner.status = owner_status
    owner.completed_at = datetime(2026, 5, 16, 8, 30, tzinfo=UTC)
    owner.branch = owner_branch
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    failed = store.add(
        "Historical blocked child",
        task_type="implement",
        based_on=owner.id,
        depends_on=dependency.id,
        same_branch=True,
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = owner_branch
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)
    return dependency, owner, failed


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


def _write_review_report(tmp_path: Path, *, name: str, content: str) -> str:
    report_path = tmp_path / "reviews" / name
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(content, encoding="utf-8")
    return str(report_path.relative_to(tmp_path))


def _create_worktree_registration(common_dir: Path, *, name: str, worktree_path: Path) -> Path:
    registration_dir = common_dir / "worktrees" / name
    registration_dir.mkdir(parents=True, exist_ok=True)
    (registration_dir / "gitdir").write_text(str(worktree_path / ".git"), encoding="utf-8")
    return registration_dir


def _changes_requested_review_with_blocker(
    *,
    title: str,
    evidence: str,
    required_fix: str,
    open_state_citation: str | None = None,
) -> str:
    citation_line = (
        f"Open-state citation: {open_state_citation}\n"
        if open_state_citation is not None
        else ""
    )
    return (
        "## Summary\n\n- Review summary.\n\n"
        "## Blockers\n\n"
        f"### B1 {title}\n"
        f"Evidence: {evidence}\n"
        "Impact: merge should stay blocked until handled.\n"
        f"Required fix: {required_fix}\n"
        "Required tests: add focused coverage.\n"
        f"{citation_line}\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _add_same_merge_unit_owner_representative_with_review(
    tmp_path: Path,
    store,
    *,
    review_content: str,
) -> tuple[Any, Any, Any]:
    owner = store.add("Historic merge-unit owner", task_type="implement")
    assert owner.id is not None
    owner.status = "completed"
    owner.completed_at = datetime(2026, 6, 20, 9, 0, tzinfo=UTC)
    owner.branch = "feature/merge-unit-blockers"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)

    representative = store.add(
        "Actionable merge-unit representative",
        task_type="improve",
        based_on=owner.id,
        same_branch=True,
    )
    assert representative.id is not None
    representative.status = "completed"
    representative.completed_at = datetime(2026, 6, 20, 10, 0, tzinfo=UTC)
    representative.branch = owner.branch
    representative.has_commits = True
    representative.merge_status = "unmerged"
    store.update(representative)

    unit = store.get_or_create_merge_unit_for_task(representative)
    assert unit is not None
    assert unit.owner_task_id == owner.id

    review = store.add(
        f"Review {representative.id}",
        task_type="review",
        depends_on=representative.id,
        based_on=representative.id,
    )
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 6, 20, 11, 0, tzinfo=UTC)
    review.report_file = _write_review_report(
        tmp_path,
        name=f"{representative.id}-merge-unit-review.md",
        content=review_content,
    )
    store.update(review)

    attached_unit = store.get_or_create_merge_unit_for_task(review)
    assert attached_unit is not None
    assert attached_unit.id == unit.id
    return owner, representative, review


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
        get_diff_name_status=MagicMock(return_value=""),
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
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
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

    config = Config.load(tmp_path)
    config.require_review_before_merge = False
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=task,
        source_task=task,
        result=SimpleNamespace(
            command="./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime.now(UTC),
            reviewed_branch=task.branch,
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory=str(tmp_path),
            failure=None,
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=True),
        get_diff_name_status=MagicMock(return_value=""),
        rev_parse_if_exists=MagicMock(return_value="head-1"),
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
    with patch(
        "gza.cli.git_ops.determine_next_action",
        return_value={"type": "merge", "description": "Merge"},
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    assert result.status == "blocked_dirty_checkout"
    assert result.block_reason == "main checkout has uncommitted changes"
    git.can_merge.assert_not_called()
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "You have uncommitted changes. Please commit or stash them first." in output


def test_merge_single_task_runs_shared_verify_gate_before_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement gated merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/gated-merge"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.determine_next_action",
            side_effect=[
                {
                    "type": "verify_gate",
                    "description": "Run verify gate before merge",
                    "verify_gate_phase": "pre_merge",
                    "verify_owner_task": task,
                },
                {"type": "merge", "description": "Merge (review APPROVED)"},
            ],
        ) as determine,
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="success",
                success_message="Verify gate passed for the current tip before merge.",
                work_done=True,
            ),
        ) as execute_action,
    ):
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    assert determine.call_count == 2
    assert determine.call_args.kwargs["selected_for_merge"] is True
    execute_action.assert_called_once()
    git.merge.assert_called_once_with("feature/gated-merge", squash=False, commit_message=None)


def test_merge_single_task_default_keeps_merge_mechanics_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement merge output", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/default-merge-output"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
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
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    output = capsys.readouterr().out
    assert "Merging 'feature/default-merge-output' into 'main'..." in output
    assert "✓ Successfully merged feature/default-merge-output" in output


def test_merge_single_task_quiet_mechanics_suppresses_default_success_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement quiet merge output", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/quiet-merge-output"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
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
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(
            task.id,
            config,
            store,
            git,
            args,
            "main",
            quiet_mechanics=True,
        )

    assert result.rc == 0
    output = capsys.readouterr().out
    assert "Merging 'feature/quiet-merge-output' into 'main'..." not in output
    assert "✓ Successfully merged feature/quiet-merge-output" not in output


def test_merge_single_task_quiet_mechanics_keeps_squash_reconcile_warning_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement quiet squash merge output", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/quiet-squash-merge-output"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
        rev_parse=MagicMock(return_value="squash-oid"),
        rev_parse_if_exists=MagicMock(side_effect=lambda ref: f"{ref}-oid"),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=True,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = Config.load(tmp_path)

    with patch(
        "gza.cli.git_ops._reconcile_squash_merged_branch_with_origin",
        return_value=SquashBranchReconcileResult(
            status="failed_remote_tracking_ref_update",
            branch="feature/quiet-squash-merge-output",
            reason="cannot lock ref 'refs/remotes/origin/feature/quiet-squash-merge-output'",
        ),
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(
                task.id,
                config,
                store,
                git,
                args,
                "main",
                quiet_mechanics=True,
            )

    assert result.rc == 0
    output = capsys.readouterr().out
    assert "Merging 'feature/quiet-squash-merge-output' into 'main'..." not in output
    assert "✓ Successfully squash merged feature/quiet-squash-merge-output and created commit" not in output
    assert "✓ Reconciled origin/feature/quiet-squash-merge-output to the squash merge commit" not in output
    assert "could not be updated" in output
    assert "git fetch origin +refs/heads/feature/quiet-squash-merge-output:refs/remotes/origin/feature/quiet-squash-merge-output" in output


def test_merge_single_task_auto_defers_verify_only_blockers_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement verify-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/verify-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B1",
        severity="BLOCKER",
        title="Verify timed out",
        body="",
        evidence="./bin/tests timed out",
        impact="Flaky verify evidence would be lost",
        fix_or_followup="stabilize verify",
        tests="add regression coverage",
        open_state_citation="status: open",
    )
    deferred_task = store.add("Deferred blocker B1", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.is_verify_blocked_only_review", return_value=True),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks", return_value=([deferred_task], [])) as materialize,
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    git.merge.assert_called_once_with("feature/verify-only-blocker", squash=False, commit_message=None)
    materialize.assert_called_once()
    output = capsys.readouterr().out
    assert f"DEFERRED-BLOCKER {deferred_task.id} created from {task.id}" in output


def test_merge_single_task_auto_defers_verify_only_report_file_blockers_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement report-file verify-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/report-file-verify-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.report_file = _write_review_report(
        tmp_path,
        name="verify-only-review.md",
        content=_changes_requested_review_with_blocker(
            title="verify_command failure: command exited nonzero",
            evidence="verify_command failed while running `./bin/tests`; the assertion failure is in the verify output.",
            required_fix="rerun verify_command on the current tip.",
        ),
    )
    store.update(review)

    deferred_task = store.add("Deferred blocker B1", task_type="implement", based_on=review.id, depends_on=task.id)
    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with patch(
        "gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks",
        return_value=([deferred_task], []),
    ) as materialize:
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    git.merge.assert_called_once_with(
        "feature/report-file-verify-only-blocker",
        squash=False,
        commit_message=None,
    )
    materialize.assert_called_once()
    assert materialize.call_args.kwargs["findings"][0].id == "B1"
    output = capsys.readouterr().out
    assert f"DEFERRED-BLOCKER {deferred_task.id} created from {task.id}" in output


def test_merge_single_task_refuses_non_verify_blockers_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement blocked merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/non-verify-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B2",
        severity="BLOCKER",
        title="Missing data migration",
        body="Canonical blocker context.",
        evidence=None,
        impact=None,
        fix_or_followup="add migration",
        tests=None,
        open_state_citation="finding remains open",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.is_verify_blocked_only_review", return_value=False),
        patch(
            "gza.cli.git_ops.summarize_review_blockers",
            return_value=SimpleNamespace(
                blocker_count=1,
                verify_timeout_count=0,
                verify_failure_count=0,
                unknown_or_code_count=1,
            ),
        ),
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"
    output = capsys.readouterr().out
    assert f"Error: Task {task.id} has open BLOCKER findings in review {review.id}." in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_refuses_non_verify_report_file_blockers_with_normal_hint(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement report-file non-verify blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/report-file-non-verify-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.report_file = _write_review_report(
        tmp_path,
        name="non-verify-review.md",
        content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )
    store.update(review)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert f"Error: Task {task.id} has open BLOCKER findings in review {review.id}." in output
    assert "blocker classification did not match the parsed blocker set" not in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_defer_blockers_flag_materializes_and_proceeds(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement deferred blocker merge", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/defer-blockers"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blockers = (
        ReviewFinding(
            id="B3",
            severity="BLOCKER",
            title="Missing migration",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="add migration",
            tests=None,
            open_state_citation="citation",
        ),
        ReviewFinding(
            id="B4",
            severity="BLOCKER",
            title="Missing cleanup",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="add cleanup",
            tests=None,
            open_state_citation="citation",
        ),
    )
    deferred_task = store.add("Deferred blocker B3", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=blockers, format_version="v2"),
        ),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks", return_value=([deferred_task], [])) as materialize,
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    git.merge.assert_called_once()
    materialize.assert_called_once()
    assert materialize.call_args.kwargs["findings"] == blockers
    output = capsys.readouterr().out
    assert f"DEFERRED-BLOCKER {deferred_task.id} created from {task.id}" in output


def test_merge_single_task_mark_only_materializes_blockers_before_marking_merged(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement mark-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/mark-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B5",
        severity="BLOCKER",
        title="Missing guard",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="add guard",
        tests=None,
        open_state_citation="citation",
    )
    deferred_task = store.add("Deferred blocker B5", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)
    order: list[str] = []
    original_set_merge_status = store.set_merge_status
    original_set_merge_unit_state = store.set_merge_unit_state

    def _record_set_merge_status(*call_args, **call_kwargs):
        order.append("mark")
        return original_set_merge_status(*call_args, **call_kwargs)

    def _record_set_merge_unit_state(*call_args, **call_kwargs):
        order.append("mark")
        return original_set_merge_unit_state(*call_args, **call_kwargs)

    store.set_merge_status = MagicMock(side_effect=_record_set_merge_status)  # type: ignore[method-assign]
    store.set_merge_unit_state = MagicMock(side_effect=_record_set_merge_unit_state)  # type: ignore[method-assign]

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch(
            "gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks",
            side_effect=lambda *a, **k: (order.append("defer") or ([deferred_task], [])),
        ),
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    assert order == ["defer", "mark"]


def test_merge_single_task_no_followups_does_not_suppress_deferred_blockers(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement no-followups blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/no-followups-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B6",
        severity="BLOCKER",
        title="Missing invariant",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="add invariant",
        tests=None,
        open_state_citation="citation",
    )
    deferred_task = store.add("Deferred blocker B6", task_type="implement", based_on=review.id, depends_on=task.id)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=True,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops._create_or_reuse_deferred_blocker_tasks", return_value=([deferred_task], [])),
        patch("gza.cli.git_ops._materialize_merge_followups") as materialize_followups,
    ):
        with _force_merge_planner_action():
            result = _merge_single_task(task.id, config, store, git, args, "main")

    assert result.rc == 0
    materialize_followups.assert_not_called()
    output = capsys.readouterr().out
    assert f"DEFERRED-BLOCKER {deferred_task.id} created from {task.id}" in output


def test_merge_single_task_mark_only_reuses_existing_deferred_blocker_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement idempotent mark-only blocker path", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/idempotent-mark-only-blocker"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id, based_on=task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    blocker = ReviewFinding(
        id="B7",
        severity="BLOCKER",
        title="Verify timed out",
        body="Body",
        evidence=None,
        impact=None,
        fix_or_followup="stabilize verify",
        tests=None,
        open_state_citation="citation",
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with (
        patch(
            "gza.cli.git_ops.get_review_report",
            return_value=SimpleNamespace(verdict="CHANGES_REQUESTED", findings=(blocker,), format_version="v2"),
        ),
        patch("gza.cli.git_ops.is_verify_blocked_only_review", return_value=True),
    ):
        first = _merge_single_task(task.id, config, store, git, args, "main")
        store.set_merge_status(task.id, "unmerged")
        second = _merge_single_task(task.id, config, store, git, args, "main")

    assert first.rc == 0
    assert second.rc == 0
    children = [
        child for child in store.get_based_on_children(review.id)
        if child.prompt.startswith(f"Deferred blocker {blocker.id} from review {review.id} for task {task.id}:")
    ]
    assert len(children) == 1


def test_merge_single_task_same_merge_unit_review_on_representative_refuses_without_flag(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The representative branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 1
    git.merge.assert_not_called()
    assert store.get_reviews_for_task(owner.id) == []
    output = capsys.readouterr().out
    assert f"Error: Task {owner.id} has open BLOCKER findings in review {review.id}." in output
    assert "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks." in output


def test_merge_single_task_same_merge_unit_review_on_representative_defer_flag_materializes_before_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="Missing data migration",
            evidence="The representative branch adds a new column without a migration.",
            required_fix="add the migration and backfill path.",
        ),
    )

    merge_order: list[str] = []

    def _assert_deferred_before_merge(*_args, **_kwargs):
        blockers = [
            child for child in store.get_based_on_children(review.id)
            if child.prompt.startswith(
                f"Deferred blocker B1 from review {review.id} for task {owner.id}:"
            )
        ]
        assert len(blockers) == 1
        merge_order.append("merge")

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(side_effect=_assert_deferred_before_merge),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
        defer_blockers=True,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    with _force_merge_planner_action():
        result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 0
    assert merge_order == ["merge"]
    blockers = [
        child for child in store.get_based_on_children(review.id)
        if child.prompt.startswith(f"Deferred blocker B1 from review {review.id} for task {owner.id}:")
    ]
    assert len(blockers) == 1


def test_merge_single_task_same_merge_unit_verify_only_review_on_representative_mark_only_materializes_before_mark(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    owner, representative, review = _add_same_merge_unit_owner_representative_with_review(
        tmp_path,
        store,
        review_content=_changes_requested_review_with_blocker(
            title="verify_command failure: command exited nonzero",
            evidence="verify_command failed while running `./bin/tests`; the assertion failure is in the verify output.",
            required_fix="rerun verify_command on the current tip.",
        ),
    )

    order: list[str] = []
    original_set_merge_status = store.set_merge_status
    original_set_merge_unit_state = store.set_merge_unit_state

    def _record_set_merge_status(*call_args, **call_kwargs):
        order.append("mark")
        return original_set_merge_status(*call_args, **call_kwargs)

    def _record_set_merge_unit_state(*call_args, **call_kwargs):
        blockers = [
            child for child in store.get_based_on_children(review.id)
            if child.prompt.startswith(
                f"Deferred blocker B1 from review {review.id} for task {owner.id}:"
            )
        ]
        assert len(blockers) == 1
        order.append("mark")
        return original_set_merge_unit_state(*call_args, **call_kwargs)

    store.set_merge_status = MagicMock(side_effect=_record_set_merge_status)  # type: ignore[method-assign]
    store.set_merge_unit_state = MagicMock(side_effect=_record_set_merge_unit_state)  # type: ignore[method-assign]

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=True),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=True,
        remote=False,
        resolve=False,
        defer_blockers=False,
        no_followups=False,
    )
    config = Config.load(tmp_path)

    result = _merge_single_task(representative.id, config, store, git, args, "main")

    assert result.rc == 0
    assert order == ["mark"]
    blockers = [
        child for child in store.get_based_on_children(review.id)
        if child.prompt.startswith(f"Deferred blocker B1 from review {review.id} for task {owner.id}:")
    ]
    assert len(blockers) == 1


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
    repo_git.worktree_add_existing.assert_called_once_with(config.worktree_path / str(rebase_task.id), "feature/rebased")


def test_run_task_backed_rebase_uses_worktree_add_existing_for_setup(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.status = "completed"
    parent.completed_at = datetime.now(UTC)
    parent.branch = "feature/rebased"
    store.update(parent)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None

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
    repo_git.worktree_add_existing.assert_called_once_with(config.worktree_path / str(rebase_task.id), "feature/rebased")
    assert call("worktree", "add", str(config.worktree_path / str(rebase_task.id)), "feature/rebased") not in repo_git._run.call_args_list


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

    review = store.add("Review feature", task_type="review", depends_on=parent.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 5, 10, 10, 0, tzinfo=UTC)
    review.review_verify_status = "failed"
    review.review_verify_branch = "feature/rebased"
    review.review_verify_head_sha = "head-old"
    store.update(review)

    improve = store.add(
        "No-op improve",
        task_type="improve",
        based_on=parent.id,
        depends_on=review.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    improve.branch = "feature/rebased"
    improve.changed_diff = False
    improve.review_verify_status = "passed"
    improve.review_verify_branch = "feature/rebased"
    improve.review_verify_head_sha = "head-old"
    improve.review_verify_captured_at = datetime(2026, 5, 10, 11, 0, 1, tzinfo=UTC)
    store.update(improve)

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
    refreshed_review = store.get(review.id)
    assert refreshed_review is not None
    assert refreshed_review.review_verify_head_sha == "head-new"
    refreshed_improve = store.get(improve.id)
    assert refreshed_improve is not None
    assert refreshed_improve.review_verify_head_sha == "head-new"
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


def test_run_task_backed_rebase_provider_cleanup_does_not_prune_or_remove_other_registrations(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    parent.status = "completed"
    parent.completed_at = datetime.now(UTC)
    parent.branch = "feature/rebased"
    store.update(parent)

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = parent.branch
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    canonical_worktree = config.worktree_path / str(rebase_task.id)
    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git._run.return_value = None
    repo_git.worktree_remove.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    worktree_git = MagicMock()
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

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
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch("gza.cli.git_ops.import_isolated_rebase_tip"),
        patch("gza.cli.git_ops.remove_worktree_registration_for_path") as remove_worktree_registration_for_path,
        patch("gza.cli.git_ops.publish_rebased_branch"),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    repo_git.worktree_remove.assert_called_once_with(canonical_worktree, force=True)
    remove_worktree_registration_for_path.assert_not_called()
    assert call("worktree", "prune") not in repo_git._run.call_args_list


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


def test_remove_watch_merge_checkout_deregisters_only_managed_checkout(tmp_path: Path) -> None:
    """Managed watch cleanup must not prune unrelated prunable worktree registrations."""
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    checkout_path = config.main_checkout_integration_path
    checkout_path.mkdir(parents=True)
    (checkout_path / "tracked.txt").write_text("watch checkout", encoding="utf-8")

    foreign_worktree = tmp_path.parent / "inline-worktree"
    common_dir = tmp_path / ".git"
    managed_registration = _create_worktree_registration(
        common_dir,
        name="watch-main",
        worktree_path=checkout_path,
    )
    foreign_registration = _create_worktree_registration(
        common_dir,
        name="inline-feature",
        worktree_path=foreign_worktree,
    )

    git = MagicMock(spec=Git)
    git.worktree_list.return_value = [
        {
            "path": str(foreign_worktree),
            "branch": "refs/heads/feature/inline",
            "detached": False,
            "prunable": "gitdir file points to non-existent location",
        }
    ]
    git._run.return_value = SimpleNamespace(stdout=str(common_dir), returncode=0, stderr="")

    _remove_watch_merge_checkout(git, checkout_path)

    git.worktree_remove.assert_called_once_with(checkout_path, force=True)
    assert not managed_registration.exists()
    assert foreign_registration.exists()
    assert not checkout_path.exists()
    assert call("worktree", "prune", "--expire", "now", check=False) not in git._run.call_args_list


def test_ensure_watch_main_checkout_recreates_prunable_registration_without_pruning_foreign_one(
    tmp_path: Path,
) -> None:
    """Reviving the isolated watch checkout must only remove its own stale registration."""
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    checkout_path = config.main_checkout_integration_path
    foreign_worktree = tmp_path.parent / "inline-worktree"
    common_dir = tmp_path / ".git"
    managed_registration = _create_worktree_registration(
        common_dir,
        name="watch-main",
        worktree_path=checkout_path,
    )
    foreign_registration = _create_worktree_registration(
        common_dir,
        name="inline-feature",
        worktree_path=foreign_worktree,
    )

    prunable_managed_entry = {
        "path": str(checkout_path),
        "branch": None,
        "detached": True,
        "prunable": "gitdir file points to non-existent location",
    }
    prunable_foreign_entry = {
        "path": str(foreign_worktree),
        "branch": "refs/heads/feature/inline",
        "detached": False,
        "prunable": "gitdir file points to non-existent location",
    }
    recreated_entry = {
        "path": str(checkout_path),
        "branch": None,
        "detached": True,
        "prunable": False,
    }

    git = MagicMock(spec=Git)
    git.worktree_list.side_effect = [
        [prunable_managed_entry, prunable_foreign_entry],
        [prunable_foreign_entry],
        [prunable_foreign_entry, recreated_entry],
    ]

    def _run(*args, **kwargs):
        if args == ("rev-parse", "--git-common-dir"):
            return SimpleNamespace(stdout=str(common_dir), returncode=0, stderr="")
        raise AssertionError(f"unexpected git._run call: {args!r}")

    git._run.side_effect = _run

    workspace_git = MagicMock()
    workspace_git.current_branch.return_value = "HEAD"
    workspace_git.has_changes.return_value = False

    with patch("gza.cli.git_ops.Git", return_value=workspace_git) as git_cls:
        isolated_git = ensure_watch_main_checkout(config, git, "main")

    assert isolated_git is workspace_git
    git_cls.assert_called_once_with(checkout_path)
    assert not managed_registration.exists()
    assert foreign_registration.exists()
    assert call("worktree", "prune", "--expire", "now", check=False) not in git._run.call_args_list
    git.worktree_add_existing.assert_called_once_with(checkout_path, "main", detach=True)
    workspace_git.checkout_detached.assert_called_once_with("main")
    workspace_git.reset_hard.assert_called_once_with("main")
    workspace_git.clean_force.assert_called_once_with()


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
    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
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
    assert mark_failed.call_args.kwargs["explicit_reason"] == "REBASE_CONFLICT"
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
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)
    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
        patch(
            "gza.cli.git_ops.compute_rebase_changed_diff",
            return_value=RebaseDiffResult(changed_diff=True, detail="yes (review must be refreshed)"),
        ),
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.import_isolated_rebase_tip"),
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


def test_run_task_backed_rebase_provider_resolve_uses_isolated_checkout_and_guarded_import(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout_path = tmp_path / "isolated-checkout"
    private_checkout = SimpleNamespace(path=private_checkout_path)

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

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
        patch(
            "gza.cli.git_ops.reconcile_task_branch_merge_truth",
            return_value=SimpleNamespace(warnings=[], skipped_reason=None, errors=[]),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm) as mock_isolated_checkout,
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True) as invoke_provider_resolve,
        patch("gza.cli.git_ops.import_isolated_rebase_tip") as import_isolated_rebase_tip,
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
    mock_isolated_checkout.assert_called_once_with(
        config=config,
        source_git=repo_git,
        branch="feature/rebased",
        target_ref="main",
        checkout_name="rebase-feature",
    )
    invoke_provider_resolve.assert_called_once_with(
        rebase_task,
        "feature/rebased",
        "main",
        config,
        log_file=ANY,
        logger=ANY,
        worktree_path=private_checkout_path,
    )
    import_isolated_rebase_tip.assert_called_once_with(
        destination_git=repo_git,
        checkout=private_checkout,
        branch="feature/rebased",
        expected_old_sha="head-old",
        temp_ref_name="rebase-feature",
    )
    publish_rebased_branch.assert_called_once_with(
        repo_git,
        branch="feature/rebased",
        baseline=RebaseDiffBaseline(
            old_tip="head-old",
            target_at_start="base-old",
            merge_base_at_start="merge-base",
        ),
        logger=ANY,
    )
    repo_git.worktree_remove.assert_called_once_with(config.worktree_path / str(rebase_task.id), force=True)


def test_run_task_backed_rebase_provider_resolve_stale_import_fails_closed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    rebase_task.slug = "rebase-feature"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.side_effect = GitError("rebase boom")
    worktree_git.rebase_abort.return_value = None

    private_checkout = SimpleNamespace(path=tmp_path / "isolated-checkout")

    @contextmanager
    def _isolated_checkout_cm(**kwargs):
        yield private_checkout

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch(
            "gza.cli.git_ops.capture_rebase_diff_baseline",
            return_value=RebaseDiffBaseline(
                old_tip="head-old",
                target_at_start="base-old",
                merge_base_at_start="merge-base",
            ),
        ),
        patch("gza.cli.git_ops.isolated_rebase_checkout", side_effect=_isolated_checkout_cm),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True),
        patch(
            "gza.cli.git_ops.import_isolated_rebase_tip",
            side_effect=GitError("Refusing to import rebased tip for feature/rebased: expected old SHA head-old"),
        ),
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 1
    publish_rebased_branch.assert_not_called()
    refreshed = store.get(rebase_task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "GIT_ERROR"


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


def test_run_task_backed_rebase_container_git_metadata_failure_retries_as_infra(tmp_path) -> None:
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

    invalid_path_error = GitError(
        "git worktree list --porcelain failed: fatal: Invalid path '/gza-git': No such file or directory"
    )

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", side_effect=invalid_path_error),
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
    mark_failed.assert_called_once()
    assert mark_failed.call_args.kwargs["explicit_reason"] == "INFRASTRUCTURE_ERROR"


def test_classify_rebase_git_failure_marks_readonly_sqlite_error_as_infra() -> None:
    error = sqlite3.OperationalError("attempt to write a readonly database")

    assert _classify_rebase_git_failure(error) == "INFRASTRUCTURE_ERROR"


def test_run_task_backed_rebase_readonly_db_git_failure_retries_as_infra(tmp_path) -> None:
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

    readonly_db_error = GitError("sqlite3.OperationalError: attempt to write a readonly database")

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", side_effect=readonly_db_error),
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
    mark_failed.assert_called_once()
    assert mark_failed.call_args.kwargs["explicit_reason"] == "INFRASTRUCTURE_ERROR"


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
    assert rc == 0
    assert "Will advance 1 task(s):" in output
    assert "Run verify gate before merge" in output
    assert "verify epoch is unavailable; merge is blocked" in output
    assert "1 skipped" in output

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
    assert refreshed.merge_status == "unmerged"

    fake_git.merge.assert_not_called()

    output = capsys.readouterr().out
    assert "Run verify gate before merge" in output
    assert "verify epoch is unavailable; merge is blocked" in output


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
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
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
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
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
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
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
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
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


def test_cmd_advance_explicit_dropped_owner_fallback_uses_one_read_session_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Dropped owner", task_type="implement")
    assert owner.id is not None
    owner.status = "dropped"
    owner.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    owner.branch = "feature/dropped-owner-read-session"
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

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

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

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.resolve_task_merge_state_for_target", return_value="unmerged"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "skip", "description": "nothing to do"}),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, descendant.id)), "dry_run": True}))

    assert rc == 0
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_cmd_advance_explicit_failed_leaf_persists_deferred_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/explicit-prereq-reconcile",
    )

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.advance_engine.determine_next_action", return_value={"type": "skip", "description": "nothing to do"}),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="empty"),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed.id)), "dry_run": True}))

    assert rc == 0
    merge_unit = store.resolve_merge_unit_for_task(failed.id)
    assert merge_unit is not None
    assert merge_unit.state == "empty"


def test_cmd_advance_explicit_dropped_owner_fallback_persists_deferred_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_status="dropped",
        owner_branch="feature/dropped-owner-prereq-reconcile",
    )

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.advance_engine.determine_next_action", return_value={"type": "skip", "description": "nothing to do"}),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="redundant"),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, failed.id)), "dry_run": True}))

    assert rc == 0
    merge_unit = store.resolve_merge_unit_for_task(failed.id)
    assert merge_unit is not None
    assert merge_unit.state == "redundant"


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

    row = LineageOwnerRow(
        owner_task=requested,
        members=(requested, failed_rebase),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(failed_rebase,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=failed_rebase,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
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

    row = LineageOwnerRow(
        owner_task=owner,
        members=(owner, requested),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "nothing to do"},
        next_action_reason="precomputed",
        unresolved_tasks=(requested,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter([row])),
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
    assert refreshed.merge_status == "unmerged"

    fake_git.merge.assert_not_called()

    output = capsys.readouterr().out
    assert "Run verify gate before merge" in output
    assert "verify epoch is unavailable; merge is blocked" in output


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
    assert refreshed.merge_status == "unmerged"

    fake_git.merge.assert_not_called()

    output = capsys.readouterr().out
    assert "Run verify gate before merge" in output
    assert "verify epoch is unavailable; merge is blocked" in output


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

    result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

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

    result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

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

    result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

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
    git.branch_exists.return_value = True
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
        ) as capture_baseline,
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    git.push_ref_force_with_lease.assert_not_called()
    git.fetch.assert_called_once_with("origin")
    worktree_git.rebase.assert_called_once_with("main")
    capture_baseline.assert_called_once_with(
        worktree_git,
        branch="feature/already-fetched-external",
        target="main",
    )
    publish_rebased_branch.assert_called_once()
    assert "origin/feature/already-fetched-external" not in result.message
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
    git.branch_exists.return_value = True
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
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    assert "verified origin was already aligned" in result.message
    assert "and pushed" not in result.message
    assert "local target 'main'" in result.message


def test_reconcile_diverged_branch_with_origin_rebases_after_remote_moves(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-2", branch="feature/external")

    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
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
        ) as capture_baseline,
        patch("gza.cli.git_ops.publish_rebased_branch") as publish_rebased_branch,
    ):
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "reconciled"
    assert "Rebased 'feature/external' onto local target 'main'" in result.message
    git.fetch.assert_called_once_with("origin")
    worktree_git.rebase.assert_called_once_with("main")
    capture_baseline.assert_called_once_with(
        worktree_git,
        branch="feature/external",
        target="main",
    )
    publish_rebased_branch.assert_called_once()


def test_reconcile_diverged_branch_with_origin_routes_conflicts_to_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    task = SimpleNamespace(id="gza-3", branch="feature/conflict")

    git = MagicMock(spec=Git)
    git.branch_exists.return_value = True
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
        result = _reconcile_diverged_branch_with_origin(config, git, task, target_branch="main")

    assert result.status == "needs_attention"
    assert result.attention_reason == "reconcile-needs-manual-resolution"
    assert "local target 'main'" in result.message
    assert "origin/feature/conflict" not in result.message
    assert "remote-tracking ref" not in result.message
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
        patch(
            "gza.cli.advance_executor._prepare_task_for_reserved_launch",
            side_effect=lambda _config, task, permit, rollback_on_failure: task,
        ),
        patch("gza.cli.git_ops._spawn_background_worker", return_value=0) as spawn_worker,
        patch(
            "gza.cli.git_ops._reconcile_diverged_branch_with_origin",
            return_value=SimpleNamespace(
                status="needs_rebase",
                message="Mechanical rebase conflicted",
                rebase_target="main",
            ),
        ),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    assert spawn_worker.call_count == 1
    assert len(created_rebases) == 1
    output = capsys.readouterr().out
    assert output.index(str(second.id)) < output.index(str(first.id))
    assert "Created rebase task" in output
    assert "SKIP: batch limit reached (1/1), cannot start rebase worker" in output


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

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert "Would advance 1 task(s):" in output
    assert "Reconcile diverged local/origin refs" in output
    assert "Needs attention" not in output


def test_cmd_advance_uses_shared_lifecycle_execution_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/shared-lifecycle-gate"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "create_review", "description": "Create review before merge"},
        next_action_reason="review",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"

    gate_calls: list[tuple[str, int]] = []

    def _record_gate(action, *, free_worker_slots):
        gate_calls.append((str(action.get("type")), free_worker_slots))
        return real_should_execute_lifecycle_action(action, free_worker_slots=free_worker_slots)

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli._lifecycle_actions.should_execute_lifecycle_action", side_effect=_record_gate),
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="create_review",
                status="success",
                message="Started review",
                success_message="Started review",
                handled_task_id="testproject-2",
                attempted_spawn=True,
                worker_started=True,
                worker_label="review",
                worker_consuming=True,
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    assert rc == 0
    assert any(action_type == "verify_gate" and free_worker_slots > 0 for action_type, free_worker_slots in gate_calls)
    assert "Will advance 1 task(s):" in capsys.readouterr().out


def test_cmd_advance_orders_direct_non_worker_actions_before_slot_gated_worker_actions(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    review_owner = store.add("Implementation still needs review", task_type="implement")
    assert review_owner.id is not None
    review_owner.status = "completed"
    review_owner.completed_at = datetime.now(UTC)
    review_owner.branch = "feature/advance-slot-gated-review"
    review_owner.merge_status = "unmerged"
    review_owner.has_commits = True
    store.update(review_owner)

    plan_owner = store.add("Approved plan ready to materialize", task_type="plan")
    assert plan_owner.id is not None
    plan_owner.status = "completed"
    plan_owner.completed_at = datetime.now(UTC)
    store.update(plan_owner)

    rows = [
        LineageOwnerRow(
            owner_task=review_owner,
            members=(review_owner,),
            tree=None,
            lineage_status="actionable",
            next_action=None,
            next_action_reason="review",
            unresolved_tasks=(review_owner,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=review_owner,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
        LineageOwnerRow(
            owner_task=plan_owner,
            members=(plan_owner,),
            tree=None,
            lineage_status="actionable",
            next_action=None,
            next_action_reason="materialize",
            unresolved_tasks=(plan_owner,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=plan_owner,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
    ]

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"

    executed_actions: list[str] = []

    def _fake_determine(_config, _store, _git, task, _target_branch, **_kwargs):
        if task.id == review_owner.id:
            return {"type": "create_review", "description": "Create review before merge"}
        if task.id == plan_owner.id:
            return {
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            }
        raise AssertionError(f"unexpected task: {task.id}")

    def _fake_execute(*, task, action, context):
        if not context.dry_run:
            executed_actions.append(f"{task.id}:{action['type']}")
        return AdvanceActionExecutionResult(
            action_type=action["type"],
            status="success",
            message="Materialized plan slices",
            success_message="Materialized plan slices",
            work_done=True,
            worker_consuming=False,
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch(
            "gza.cli.git_ops.get_concurrency_snapshot",
            return_value=SimpleNamespace(available=0, running=1, limit=1),
        ),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=rows),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=_fake_execute),
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, review_owner.id)), "task_id": None}))

    output = capsys.readouterr().out
    assert rc == 0
    assert "Will advance 1 task(s):" in output
    assert executed_actions == [f"{plan_owner.id}:materialize_plan_slices"]
    assert output.index(str(plan_owner.id)) < output.index(str(review_owner.id))
    assert "Materialize implementation slices from approved plan review" in output
    assert str(review_owner.id) in output
    assert "already at max concurrent tasks: 1 running, limit is 1, skipping" in output
    assert "skipping" in output


def test_cmd_advance_reprojects_selected_merge_candidate_for_preview_and_execution(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance selected merge candidate", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-selected-merge"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    selected_flags: list[bool] = []
    executed_action_types: list[tuple[bool, str]] = []

    def _fake_determine(_config, _store, _git, planned_task, _target_branch, **kwargs):
        assert planned_task.id == task.id
        selected = bool(kwargs.get("selected_for_merge", False))
        selected_flags.append(selected)
        if selected:
            return {"type": "needs_rebase", "description": "rebase --resolve (conflicts detected)"}
        return {"type": "merge", "description": "Merge"}

    def _fake_execute(*, task, action, context):
        executed_action_types.append((context.dry_run, str(action["type"])))
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=str(action["type"]),
                status="dry_run",
                message="Would create rebase task",
                worker_consuming=True,
            )
        return AdvanceActionExecutionResult(
            action_type=str(action["type"]),
            status="success",
            message="Started rebase",
            success_message="Started rebase",
            attempted_spawn=True,
            worker_started=True,
            worker_label="rebase",
            worker_consuming=True,
        )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch(
            "gza.cli.git_ops.get_concurrency_snapshot",
            return_value=SimpleNamespace(available=1, running=0, limit=1),
        ),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.git_ops.execute_advance_action", side_effect=_fake_execute),
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        dry_run_args = argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "dry_run": True})
        dry_run_rc = cmd_advance(dry_run_args)
        execute_rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert dry_run_rc == 0
    assert execute_rc == 0
    assert selected_flags.count(True) >= 2
    assert "Would create rebase task" in output
    assert "Started rebase" in output
    assert executed_action_types == [
        (True, "needs_rebase"),
        (True, "needs_rebase"),
        (False, "needs_rebase"),
    ]
    execute_merge.assert_not_called()


def test_cmd_advance_reprojected_selected_merge_rebase_respects_zero_worker_capacity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance selected merge candidate at capacity", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-selected-merge-at-capacity"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    selected_flags: list[bool] = []

    def _fake_determine(_config, _store, _git, planned_task, _target_branch, **kwargs):
        assert planned_task.id == task.id
        selected = bool(kwargs.get("selected_for_merge", False))
        selected_flags.append(selected)
        if selected:
            return {"type": "needs_rebase", "description": "rebase --resolve (conflicts detected)"}
        return {"type": "merge", "description": "Merge"}

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch(
            "gza.cli.git_ops.get_concurrency_snapshot",
            return_value=SimpleNamespace(available=0, running=1, limit=1),
        ),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.git_ops.execute_advance_action") as execute_action,
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        dry_run_args = argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "dry_run": True})
        dry_run_rc = cmd_advance(dry_run_args)
        execute_rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert dry_run_rc == 0
    assert execute_rc == 0
    assert selected_flags.count(True) >= 2
    assert "Would create rebase task" not in output
    assert "Started rebase" not in output
    assert "already at max concurrent tasks: 1 running, limit is 1, skipping" in output
    execute_action.assert_not_called()
    execute_merge.assert_not_called()


def test_cmd_advance_all_tasks_query_uses_one_read_session_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance all-tasks read session", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 5, 12, 9, 0, tzinfo=UTC)
    task.branch = "feature/advance-all-read-session"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

    args = _advance_args(tmp_path, task.id)
    args.task_id = None
    args.dry_run = True

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.current_branch.return_value = "main"
    fake_git.default_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "skip", "description": "nothing to do"}),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_cmd_advance_all_tasks_persists_deferred_prerequisite_reconciliation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _dependency, _owner, failed = _add_prerequisite_unmerged_failed_child(
        store,
        owner_branch="feature/all-tasks-prereq-reconcile",
    )

    args = _advance_args(tmp_path, failed.id)
    args.task_id = None
    args.dry_run = True
    args.no_resume_failed = True

    fake_git = _make_read_session_reconciliation_git(tmp_path, failed.branch)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.prime_advance_planning_refs"),
        patch("gza.cli.advance_engine.determine_next_action", return_value={"type": "skip", "description": "nothing to do"}),
        patch("gza.recovery_engine.resolve_task_merge_state_for_target", return_value="redundant"),
    ):
        rc = cmd_advance(args)

    assert rc == 0
    merge_unit = store.resolve_merge_unit_for_task(failed.id)
    assert merge_unit is not None
    assert merge_unit.state == "redundant"


def test_advance_retryable_provider_attention_recommends_retry_or_reimplement_for_failed_impl(
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
    assert f"Recommended next step: uv run gza fix {task.id}" not in output
    assert "Recommended next step: retry or re-implement instead." in output


def test_cmd_advance_merge_renders_off_topic_investigation_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Advance merge should surface investigations", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/advance-investigations"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge (previous review addressed)"},
        next_action_reason="merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    fake_git = MagicMock(spec=Git)
    fake_git.repo_dir = tmp_path
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.is_merged.return_value = False
    fake_git.has_changes.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1

    action = {
        "type": "merge",
        "description": "Merge (previous review addressed)",
        "created_investigation_task_ids": ("gza-7010",),
        "reused_investigation_task_ids": ("gza-7009",),
    }

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value=action),
        patch(
            "gza.cli.git_ops._execute_merge_action",
            return_value=SimpleNamespace(
                rc=0,
                created_followups=[],
                reused_followups=[],
                created_investigation_task_ids=["gza-7010"],
                reused_investigation_task_ids=["gza-7009"],
            ),
        ),
    ):
        rc = cmd_advance(_advance_args(tmp_path, task.id))

    output = capsys.readouterr().out
    assert rc == 0
    assert "✓ Created investigation task(s): gza-7010" in output
    assert "↺ Reused investigation task(s): gza-7009" in output
    assert "✓ Merged" in output


def test_advance_post_merge_red_main_skips_later_merges_and_surfaces_attention(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    first, _first_review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-main-red-1",
        when=datetime.now(UTC),
    )
    second, _second_review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-main-red-2",
        when=datetime.now(UTC),
    )

    args = _advance_args(tmp_path, first.id)
    args.task_id = None

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

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    merge_calls: list[str] = []

    def fake_execute_merge_action(_config, _store, _git, task, _action, **_kwargs):
        merge_calls.append(task.id)
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    green = SimpleNamespace(
        merges_halted=False,
        state=SimpleNamespace(task=main_verify_task, alert_message=None),
    )
    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `deadbeefcafe` - merges halted; phase `unit` failing",
        ),
    )
    first_row = LineageOwnerRow(
        owner_task=first,
        members=(first,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(first,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=first,
    )
    second_row = LineageOwnerRow(
        owner_task=second,
        members=(second,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(second,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=second,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[first_row, second_row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", side_effect=[green, red]),
        patch("gza.cli.git_ops._execute_merge_action", side_effect=fake_execute_merge_action),
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert merge_calls == [first.id]
    assert "main verify RED at `deadbeefcafe` - merges halted; phase `unit` failing" in output
    assert f"{second.id}" in output
    assert "1 advanced" in output
    assert "1 skipped" in output


def test_advance_refreshes_red_main_before_preview_and_skips_confirmation_prompt(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task, _review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-preview-main-red",
        when=datetime.now(UTC),
    )

    args = argparse.Namespace(**{**vars(_advance_args(tmp_path, task.id)), "auto": False})

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

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    row = LineageOwnerRow(
        owner_task=task,
        members=(task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=task,
    )
    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `cafebabe1234` - merges halted; phase `unit` failing",
        ),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[row]),
        patch("gza.cli.git_ops.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=red) as verify_check,
        patch("builtins.input", side_effect=AssertionError("confirmation prompt should not run")),
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(args)

    output = capsys.readouterr().out
    assert rc == 0
    assert verify_check.call_args.kwargs["reason"] == "advance-pre-merge"
    assert "No eligible tasks to advance" in output
    assert "main verify RED at `cafebabe1234` - merges halted; phase `unit` failing" in output
    assert "Will advance 1 task(s):" not in output
    assert "Proceed? [Y/n]" not in output
    execute_merge.assert_not_called()


def test_advance_dedupes_persisted_and_live_red_main_attention_in_final_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    merge_task, _merge_review = _add_completed_impl_with_approved_review(
        store,
        "feature/advance-main-red-dedupe-merge",
        when=datetime.now(UTC),
    )
    plan_task = store.add("Approved plan ready to materialize", task_type="plan")
    assert plan_task.id is not None
    plan_task.status = "completed"
    plan_task.completed_at = datetime.now(UTC)
    store.update(plan_task)

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    persisted_main_row = LineageOwnerRow(
        owner_task=main_verify_task,
        members=(main_verify_task,),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "needs_discussion",
            "description": "SKIP: main verify RED at `facefeed9999` - merges halted; phase `unit` failing",
            "needs_attention_reason": "main-integration-verify-red",
            "subject_task_id": main_verify_task.id,
        },
        next_action_reason="red main verify",
        unresolved_tasks=(main_verify_task,),
        unresolved_leaf_summary=(),
    )
    merge_row = LineageOwnerRow(
        owner_task=merge_task,
        members=(merge_task,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "merge", "description": "Merge"},
        next_action_reason="Merge",
        unresolved_tasks=(merge_task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=merge_task,
    )
    plan_row = LineageOwnerRow(
        owner_task=plan_task,
        members=(plan_task,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="materialize",
        unresolved_tasks=(plan_task,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=plan_task,
    )

    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `facefeed9999` - merges halted; phase `unit` failing",
        ),
    )

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

    def _fake_determine(_config, _store, _git, task, _target_branch, **_kwargs):
        if task.id == merge_task.id:
            return {"type": "merge", "description": "Merge"}
        if task.id == plan_task.id:
            return {
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            }
        raise AssertionError(f"unexpected task: {task.id}")

    with (
        patch("gza.cli.git_ops.Git", return_value=fake_git),
        patch("gza.git.Git", return_value=fake_git),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=[persisted_main_row, merge_row, plan_row]),
        patch("gza.cli.git_ops.check_main_integration_verify", return_value=red),
        patch("gza.cli.git_ops.determine_next_action", side_effect=_fake_determine),
        patch(
            "gza.cli.git_ops.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="materialize_plan_slices",
                status="success",
                message="Materialized plan slices",
                success_message="Materialized plan slices",
                work_done=True,
                worker_consuming=False,
            ),
        ),
        patch("gza.cli.git_ops._execute_merge_action") as execute_merge,
    ):
        rc = cmd_advance(argparse.Namespace(**{**vars(_advance_args(tmp_path, merge_task.id)), "task_id": None}))

    output = capsys.readouterr().out
    assert rc == 0
    execute_merge.assert_not_called()
    assert "Will advance 1 task(s):" in output
    final_attention = output[output.rfind("Needs attention"):]
    assert final_attention.startswith("Needs attention (1 task):")
    assert final_attention.count("main verify RED at `facefeed9999` - merges halted; phase `unit` failing") == 1
    assert final_attention.count("main-integration-verify-red") == 1


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
        branch_exists=MagicMock(return_value=True),
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
        branch_exists=MagicMock(return_value=True),
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


def test_rebase_defaults_to_queue_without_running_or_spawning(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
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
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=False,
        queue=False,
        background=False,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli.git_ops._run_task_backed_rebase", side_effect=AssertionError("should stay queued")),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=AssertionError("should stay queued")),
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=AssertionError("should stay queued")),
    ):
        rc = cmd_rebase(args)

    assert rc == 0
    output = capsys.readouterr().out
    assert "Created rebase task" in output
    created = store.get_based_on_children(impl_task.id)
    assert len(created) == 1
    assert created[0].task_type == "rebase"
    assert created[0].status == "pending"
    assert created[0].branch == impl_task.branch


def test_rebase_run_uses_foreground_task_backed_path(tmp_path: Path) -> None:
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
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=True,
        queue=False,
        background=False,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli.git_ops._run_task_backed_rebase", return_value=0) as run_rebase,
    ):
        rc = cmd_rebase(args)

    assert rc == 0
    run_rebase.assert_called_once()


def test_rebase_background_duplicate_active_rebase_returns_phase1_error_and_releases_capacity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    active_rebase = store.add("Rebase", task_type="rebase", based_on=impl_task.id, same_branch=True)
    assert active_rebase.id is not None

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=False,
        queue=False,
        background=True,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch(
            "gza.cli.git_ops._create_rebase_task",
            side_effect=DuplicateActiveChildError(active_rebase),
        ),
        patch("gza.cli.git_ops._prepare_task_for_immediate_execution", side_effect=AssertionError("unused")),
        patch("gza.cli.git_ops._spawn_background_worker", side_effect=AssertionError("unused")),
    ):
        rc = cmd_rebase(args)

    assert rc == 1
    captured = capsys.readouterr()
    combined_output = captured.out + captured.err
    assert f"Error: rebase already pending/in progress for {impl_task.id}: {active_rebase.id}" in combined_output
    permit = launch_permit(config, store)
    permit.release()


def test_rebase_foreground_duplicate_active_rebase_returns_phase1_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    assert impl_task.id is not None
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    active_rebase = store.add("Rebase", task_type="rebase", based_on=impl_task.id, same_branch=True)
    assert active_rebase.id is not None

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
    )
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=impl_task.id,
        onto=None,
        remote=False,
        force=False,
        resolve=False,
        run=True,
        queue=False,
        background=False,
        no_docker=True,
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch(
            "gza.cli.git_ops._create_rebase_task",
            side_effect=DuplicateActiveChildError(active_rebase),
        ),
        patch("gza.cli.git_ops._run_task_backed_rebase", side_effect=AssertionError("unused")),
    ):
        rc = cmd_rebase(args)

    assert rc == 1
    captured = capsys.readouterr()
    combined_output = captured.out + captured.err
    assert f"Error: rebase already pending/in progress for {impl_task.id}: {active_rebase.id}" in combined_output


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


def test_print_squash_reconcile_result_suppresses_only_success_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_squash_reconcile_result(
        SquashBranchReconcileResult(
            status="updated",
            branch="feature/demo",
            remote="origin",
        ),
        suppress_success=True,
    )

    assert capsys.readouterr().out == ""


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
