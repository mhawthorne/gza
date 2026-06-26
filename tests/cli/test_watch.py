"""Tests for `gza watch` scheduler behavior."""

import argparse
import contextlib
import io
import json
import os
import re
import signal
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from rich.console import Console

from gza.concurrency import MaxConcurrentTasksError
from gza.dispatch_preview import (
    DispatchPreview,
    DispatchPreviewEntry,
    build_dispatch_preview,
    plan_watch_dispatch_entries,
)
from gza.cli.git_ops import (
    _execute_merge_action,
    _MergeSingleTaskResult,
    _PendingSquashBranchReconcile,
    _ResolvedMergeSubject,
    ensure_watch_main_checkout,
)
import gza.cli.watch as watch_module
from gza.cli.watch import (
    WatchSlotAllocation,
    _collect_advance_completed_tasks,
    _collect_completed_transition_ids,
    _collect_live_running_state,
    _collect_unhandled_failures,
    _compute_failure_backoff_seconds,
    _count_live_workers,
    _CycleResult,
    _emit_cycle_attention_summary,
    _emit_transition_events,
    _format_elapsed,
    _format_wake_message,
    _installed_gza_package_fingerprint,
    _InstalledPackageDriftState,
    _finalize_watch_no_progress_after_execution,
    _maybe_repair_target_already_merged_skip,
    _maybe_park_watch_no_progress,
    _maybe_finalize_watch_no_progress_for_background_action,
    _maybe_skip_watch_no_progress_for_transient_terminal,
    _query_owner_rows_with_context,
    _resolve_watch_attention_display_task,
    _run_cycle,
    _system_can_run_tasks,
    _find_open_main_verify_remediation_task,
    _should_reexec_watch,
    _task_snapshot,
    _watch_needs_attention_message,
    _watch_no_progress_result_deferred_for_transient_backoff,
    _watch_reexec_argv,
    _warn_if_installed_gza_changed,
    _watch_iterate_impl_target,
    _WatchLog,
    allocate_watch_slots,
    cmd_watch,
)
from gza.recovery_read_context import RecoveryReadContext
from gza.advance_engine import classify_advance_action, failed_recovery_decision_to_action
from gza.cli._recovery_lane import collect_recovery_lane_entries
from gza.cli._common import reconcile_in_progress_tasks, set_task_queue_position_scoped
from gza.cli.advance_executor import AdvanceActionExecutionResult, execute_advance_action as real_execute_advance_action
from gza.cli._lifecycle_actions import should_execute_lifecycle_action as real_should_execute_lifecycle_action
import gza.colors as colors
from gza.branch_publication import BranchPublicationState, persist_branch_publication_state
from gza.config import Config
from gza.db import Task, WatchProgressObservation, WatchRecoveryBackoff
from gza.plan_review_verdict import validate_plan_review_manifest
import gza.recovery_engine as recovery_engine
from gza.git import Git, GitError
from gza.lineage_query import LineageOwnerRow
from gza.recovery_engine import decide_failed_task_recovery
from gza.watch_progress import (
    WATCH_NO_PROGRESS_BACKSTOP_REASON,
    WatchProgressCandidate,
    build_watch_progress_candidate,
    clear_watch_progress_subject,
    reconcile_stale_watch_no_progress_parks,
    get_active_watch_no_progress_attention,
)
from gza.workers import WorkerMetadata, WorkerRegistry

from .conftest import make_store, invoke_gza, setup_config


def _task_count(store) -> int:
    with store._connect() as conn:  # noqa: SLF001 - test helper
        row = conn.execute("SELECT COUNT(*) AS c FROM tasks").fetchone()
    assert row is not None
    return int(row["c"])


def _make_watch_git() -> Git:
    class _UnitWatchGit(Git):
        def __init__(self) -> None:
            self.repo_dir = Path("/watch-test-repo")
            self._cache = None

        def _unexpected_git_subprocess(self, method_name: str) -> AssertionError:
            return AssertionError(
                f"{method_name} should not be reached in unit tests; "
                "patch the watch git seam instead of spawning real git subprocesses"
            )

        def _run(self, *args: object, **kwargs: object) -> object:  # type: ignore[override]
            raise self._unexpected_git_subprocess("_run")

        def _run_readonly_cached(self, *args: object, **kwargs: object) -> object:  # type: ignore[override]
            raise self._unexpected_git_subprocess("_run_readonly_cached")

        def _run_readonly_success_cached(self, *args: object, **kwargs: object) -> object:  # type: ignore[override]
            raise self._unexpected_git_subprocess("_run_readonly_success_cached")

    git = _UnitWatchGit()
    git.default_branch = MagicMock(return_value="main")  # type: ignore[method-assign]
    git.current_branch = MagicMock(return_value="main")  # type: ignore[method-assign]
    git.local_branch_names = MagicMock(return_value=frozenset({"main"}))  # type: ignore[method-assign]
    git.branch_exists = MagicMock(return_value=True)  # type: ignore[method-assign]
    git.branches_exist = MagicMock(return_value={})  # type: ignore[method-assign]
    git.ref_exists = MagicMock(return_value=False)  # type: ignore[method-assign]
    git.resolve_refs = MagicMock(return_value={})  # type: ignore[method-assign]
    git.can_merge = MagicMock(return_value=True)  # type: ignore[method-assign]
    git.is_merged = MagicMock(return_value=False)  # type: ignore[method-assign]
    git.count_commits_ahead = MagicMock(return_value=1)  # type: ignore[method-assign]
    git.count_commits_ahead_checked = MagicMock(return_value=1)  # type: ignore[method-assign]
    git.get_diff_name_status = MagicMock(return_value="M\tfeature.txt\n")  # type: ignore[method-assign]
    git.get_diff_stat_parsed = MagicMock(return_value=(1, 1, 0))  # type: ignore[method-assign]
    git.get_diff_numstat = MagicMock(return_value="1\t0\tfeature.txt\n")  # type: ignore[method-assign]
    return git


def _seed_watch_lifecycle_summary_fixture(tmp_path: Path) -> tuple[object, object]:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Completed plan for lifecycle summary", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    impl = store.add("Completed impl for lifecycle summary", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-lifecycle-summary"
    impl.has_commits = True
    impl.merge_status = "unmerged"
    store.update(impl)

    return store, {plan.id: plan, impl.id: impl}


def _make_watch_startup_git() -> Git:
    git = _make_watch_git()
    git.branch_exists.return_value = False  # type: ignore[attr-defined]
    return git


def _append_watch_config(tmp_path: Path, extra: str) -> None:
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + extra)


def _run_cycle_and_emit_transition_events(
    *,
    config: Config,
    store,
    log: _WatchLog,
    **run_cycle_kwargs,
) -> _CycleResult:
    before = _task_snapshot(store)
    result = _run_cycle(config=config, store=store, log=log, **run_cycle_kwargs)
    after = _task_snapshot(store)
    _emit_transition_events(before, after, store=store, config=config, log=log)
    return result


@pytest.fixture(autouse=True)
def _patch_watch_git_runtime() -> object:
    with (
        patch("gza.cli.watch.Git", side_effect=lambda *_args, **_kwargs: _make_watch_git()) as watch_git_cls,
        patch("gza.runner.Git", side_effect=lambda *_args, **_kwargs: _make_watch_startup_git()) as runner_git_cls,
        patch(
            "gza.cli.watch.check_main_integration_verify",
            return_value=SimpleNamespace(
                merges_halted=False,
                state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
            ),
        ),
        patch(
            "gza.recovery_engine._load_merge_context",
            side_effect=lambda *_args, **_kwargs: recovery_engine.build_merge_context_from_git(
                _make_watch_git(),
                "main",
            ),
        ) as load_merge_context,
    ):
        yield watch_git_cls, runner_git_cls, load_merge_context


@pytest.fixture(autouse=True)
def _patch_watch_system_readiness() -> object:
    with patch("gza.cli.watch.wait_for_docker_ready", return_value=True):
        yield


def test_watch_attention_uses_declared_subject_for_held_plan(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Merged implement", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 18, 9, 0, tzinfo=UTC)
    impl.branch = "feature/merged-parent"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "merged")

    plan = store.add("Held plan", task_type="plan", based_on=impl.id, auto_implement=False)
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    store.update(plan)

    git = _make_watch_git()
    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="new-fingerprint"):
        rows, _ = _query_owner_rows_with_context(
            store=store,
            config=config,
            git=git,
            target_branch="main",
            max_recovery_attempts=config.max_resume_attempts,
            include_skipped=True,
        )

    assert len(rows) == 1
    row = rows[0]
    assert row.next_action is not None
    assert row.next_action["type"] == "awaiting_human"
    assert row.next_action["needs_attention_reason"] == "awaiting-human-review"

    subject_task = _resolve_watch_attention_display_task(store, row)
    assert subject_task.id == plan.id

    message = _watch_needs_attention_message(subject_task, row.next_action)
    assert plan.id in message
    assert impl.id not in message


def test_watch_query_owner_rows_uses_one_read_session_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    failed = store.add("Failed implement owner", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "watch-session"
    failed.branch = "feature/watch-read-session"
    failed.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    store.update(failed)

    opened_connections: list[tuple[bool, object]] = []
    original_open_connection = store._open_connection

    def _tracking_open_connection(*, close_on_exit: bool):
        conn = original_open_connection(close_on_exit=close_on_exit)
        opened_connections.append((close_on_exit, conn))
        return conn

    monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

    rows, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=_make_watch_git(),
        target_branch="main",
        max_recovery_attempts=config.max_resume_attempts,
        include_skipped=True,
    )

    assert [row.owner_task.id for row in rows] == [failed.id]
    assert len([conn for close_on_exit, conn in opened_connections if close_on_exit is False]) == 1


def test_watch_query_owner_rows_flushes_prerequisite_reconciliation_after_read_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
    store.update(dependency)

    failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/watch-prereq-empty"
    failed.completed_at = datetime(2026, 5, 16, 9, 0, tzinfo=UTC)
    store.update(failed)

    class _EmptyBranchGit:
        def resolve_fresh_merge_source(self, branch: str):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:
            if ref in {"main", failed.branch}:
                return "same-sha"
            return None

        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            return False

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: recovery_engine._MergeContext(
            git=_EmptyBranchGit(),
            default_branch="main",
            existing_branches=frozenset({failed.branch}),
        ),
    )

    depths: list[tuple[str, int]] = []
    original_get_or_create = store.get_or_create_merge_unit_for_task
    original_set_state = store.set_merge_unit_state

    def _record_get_or_create(task):
        depths.append(("get_or_create", store._read_session_depth))
        return original_get_or_create(task)

    def _record_set_state(unit_id: str, state: str) -> None:
        depths.append(("set_merge_unit_state", store._read_session_depth))
        original_set_state(unit_id, state)

    monkeypatch.setattr(store, "get_or_create_merge_unit_for_task", _record_get_or_create)
    monkeypatch.setattr(store, "set_merge_unit_state", _record_set_state)

    rows, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=_make_watch_git(),
        target_branch="main",
        max_recovery_attempts=config.max_resume_attempts,
        include_skipped=True,
    )

    assert [row.owner_task.id for row in rows] == [failed.id]
    assert depths
    assert all(depth == 0 for _name, depth in depths)
    merge_unit = store.resolve_merge_unit_for_task(failed.id)
    assert merge_unit is not None
    assert merge_unit.state == "empty"

    rows_after, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=_make_watch_git(),
        target_branch="main",
        max_recovery_attempts=config.max_resume_attempts,
        include_skipped=True,
    )

    assert rows_after == []


def test_watch_cycle_surfaces_manual_review_creation_as_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "advance_create_reviews: false\n")
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    impl.branch = "feature/manual-review-creation"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    config.main_checkout_isolate = False
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    lines = log_path.read_text().splitlines()
    attention_lines = [line for line in lines if "ATTENTION" in line]
    assert len(attention_lines) == 1
    assert "reason=review-needs-manual-creation" in attention_lines[0]
    assert "run gza review manually" in attention_lines[0]
    assert not any("SKIP: no review exists and advance_create_reviews=false" in line for line in lines)


def _setup_watch_owner_with_failed_rebase(tmp_path: Path, *, failure_reason: str):
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Watch implement owner", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/watch-owner-attention")
    assert impl.id is not None
    store.get_or_create_merge_unit_for_task(impl)

    failed_rebase = store.add(
        "Failed rebase descendant",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_rebase.id is not None
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = failure_reason
    failed_rebase.completed_at = datetime.now(UTC)
    failed_rebase.branch = "feature/watch-owner-attention"
    failed_rebase.has_commits = True
    store.update(failed_rebase)
    store.get_or_create_merge_unit_for_task(failed_rebase)

    return store, impl, failed_rebase


def _setup_watch_unknown_failed_empty_branch_owner(tmp_path: Path):
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed_leaf = store.add(
        "Failed empty-branch descendant",
        task_type="implement",
        recovery_origin="manual",
    )
    assert failed_leaf.id is not None
    failed_leaf.status = "failed"
    failed_leaf.failure_reason = "UNKNOWN"
    failed_leaf.session_id = "sess-watch-empty-unknown"
    failed_leaf.branch = "feature/watch-empty-unknown"
    failed_leaf.has_commits = False
    failed_leaf.output_content = "provider produced analysis before failing"
    failed_leaf.completed_at = datetime(2026, 6, 5, 9, 0, tzinfo=UTC)
    store.update(failed_leaf)

    blocked = store.add("Blocked dependent", task_type="plan", depends_on=failed_leaf.id)
    assert blocked.id is not None

    return store, failed_leaf, blocked


def _patch_watch_empty_branch_merge_context(
    monkeypatch: pytest.MonkeyPatch,
    *,
    empty_branch: str,
) -> None:
    class _EmptyMergedBranchGit:
        def resolve_fresh_merge_source(self, branch: str):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:
            if ref in {"main", empty_branch}:
                return "shared-tip"
            return None

        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            return into == "main" and branch == empty_branch

        def count_commits_ahead_checked(self, branch: str, base: str) -> int | None:
            if branch == empty_branch and base == "main":
                return 0
            return 1

        def is_on_first_parent_history(self, commit: str, target: str) -> bool:
            return commit == "shared-tip" and target == "main"

    monkeypatch.setattr(
        recovery_engine,
        "_load_merge_context",
        lambda _project_dir=None: recovery_engine._MergeContext(
            git=_EmptyMergedBranchGit(),
            default_branch="main",
        ),
    )


def _setup_watch_plan_owned_branch_action_row(tmp_path: Path):
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Watch branch owner plan", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    impl = store.add("Watch branch owner implement", task_type="implement", depends_on=plan.id)
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-plan-owned-attention"
    impl.has_commits = True
    store.update(impl)

    rebase = store.add("Watch branch owner rebase", task_type="rebase", based_on=impl.id)
    assert rebase.id is not None
    rebase.status = "completed"
    rebase.completed_at = datetime.now(UTC)
    rebase.branch = "feature/watch-plan-owned-attention"
    rebase.has_commits = True
    store.update(rebase)

    unit = store.create_merge_unit(
        source_branch="feature/watch-plan-owned-attention",
        target_branch="main",
        owner_task_id=plan.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(impl.id, unit.id, "owner")
    store.attach_task_to_merge_unit(rebase.id, unit.id, "rebase")

    return store, plan, impl, rebase


def test_format_elapsed_handles_mixed_naive_and_aware_timestamps() -> None:
    """_format_elapsed must tolerate legacy naive DB timestamps mixed with newer UTC-aware ones."""
    # Naive start, aware end — the exact shape that crashed in production.
    assert _format_elapsed("2026-05-15T04:33:01", "2026-05-15T04:33:05+00:00") == "4s"
    # Aware start, naive end — the reverse direction.
    assert _format_elapsed("2026-05-15T04:33:01+00:00", "2026-05-15T04:33:05") == "4s"
    # Both naive (legacy) — should still work.
    assert _format_elapsed("2026-05-15T04:33:01", "2026-05-15T04:33:05") == "4s"
    # Both aware — should still work.
    assert _format_elapsed("2026-05-15T04:33:01+00:00", "2026-05-15T04:33:05+00:00") == "4s"


def test_watch_cycle_spawns_iterate_for_implement_and_plain_for_plan(tmp_path: Path) -> None:
    """Pending implement tasks use iterate workers, while plan tasks use plain workers."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    plan = store.add("Plan follow-up", task_type="plan")
    assert impl.id is not None
    assert plan.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    prepared_impl_ids: list[str] = []

    def prepare_pending_impl(_config, task, **_kwargs):
        assert task.id is not None
        prepared_impl_ids.append(task.id)
        return task

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.launch_permit"),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=prepare_pending_impl) as prepare_task,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert prepare_task.call_count == 1
    assert prepared_impl_ids == [impl.id]
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert spawn_iterate.call_args.kwargs["prepared_task_id"] == impl.id
    assert spawn_iterate.call_args.kwargs["prepared_phase"] == "preloop"
    assert spawn_iterate.call_args.kwargs["startup_quiet"] is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == plan.id
    assert spawn_worker.call_args.kwargs["startup_quiet"] is True


def test_watch_cycle_pending_resume_row_on_empty_branch_preserves_resume_startup(tmp_path: Path) -> None:
    """Pending resume rows in the general queue must launch iterate in resume mode even when empty."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pending = store.add("Pending resumed implement", task_type="implement", recovery_origin="resume")
    assert pending.id is not None
    pending.status = "pending"
    pending.session_id = "sess-watch-pending"
    pending.branch = "feature/watch-pending-empty-resume"
    store.update(pending)

    unit = store.create_merge_unit(
        source_branch=pending.branch,
        target_branch="main",
        owner_task_id=pending.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(pending.id, unit.id, "owner")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == pending.id
    assert spawn_iterate.call_args.kwargs["prepared_task_id"] == pending.id
    assert spawn_iterate.call_args.kwargs["prepared_resume"] is True
    assert spawn_iterate.call_args.kwargs["prepared_phase"] == "preloop"


def test_watch_cycle_pending_implement_startup_failure_surfaces_without_spawning_iterate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Watch must fail pending implement startup in the parent before detach."""

    from gza.log_paths import resolve_task_log_paths

    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    def fail_log_setup(config, _store, pending_task):
        paths = resolve_task_log_paths(config, pending_task)
        paths.conversation.parent.mkdir(parents=True, exist_ok=True)
        paths.conversation.touch()
        raise RuntimeError("watch creator boom")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.runner.Git", return_value=MagicMock()),
        patch("gza.runner.generate_slug", return_value="20260510-test-project-implement-feature"),
        patch("gza.runner.ensure_task_log_paths", side_effect=fail_log_setup),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("iterate worker should not spawn"),
        ),
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    assert result.work_done is False
    assert "watch creator boom" not in capsys.readouterr().out
    refreshed = store.get(impl.id)
    assert refreshed is not None
    assert refreshed.slug is None
    assert refreshed.log_file is None
    log_text = log_path.read_text()
    assert "START_FAILED" in log_text
    assert f"{impl.id} implement: iterate startup preparation failed" in log_text
    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert not any(path.is_file() for path in logs_dir.rglob("*"))
    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_watch_collects_legacy_unmerged_owner_after_lazy_merge_unit_backfill(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    legacy = store.add("Legacy watch branch", task_type="implement")
    legacy.status = "completed"
    legacy.completed_at = datetime.now(UTC)
    legacy.branch = "feature/legacy-watch"
    legacy.has_commits = True
    legacy.merge_status = "unmerged"
    store.update(legacy)

    assert legacy.id is not None
    assert store.resolve_merge_unit_for_task(legacy.id) is None

    tasks, impl_based_on_ids = _collect_advance_completed_tasks(store, target_branch="main")

    assert legacy.id not in impl_based_on_ids
    assert [task.id for task in tasks if task.task_type == "implement"] == [legacy.id]
    unit = store.resolve_merge_unit_for_task(legacy.id)
    assert unit is not None
    assert unit.state == "unmerged"


def test_watch_collects_only_merge_unit_owner_for_same_branch_descendants(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Watch implement owner", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/watch-owner-only")
    assert impl.id is not None

    improve = store.add("Watch improve descendant", task_type="improve", based_on=impl.id, same_branch=True)
    store.mark_completed(improve, has_commits=True, branch="feature/watch-owner-only")
    assert improve.id is not None

    tasks, _ = _collect_advance_completed_tasks(store, target_branch="main")

    assert [task.id for task in tasks if task.task_type == "implement"] == [impl.id]
    assert improve.id not in [task.id for task in tasks]


def test_watch_query_owner_rows_filters_target_branch_and_keeps_legacy_fallback(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    main_task = store.add("Watch main owner", task_type="implement")
    release_task = store.add("Watch release owner", task_type="implement")
    legacy_task = store.add("Watch legacy owner", task_type="implement")
    assert main_task.id is not None
    assert release_task.id is not None
    assert legacy_task.id is not None

    for task, branch in (
        (main_task, "feature/watch-main"),
        (release_task, "feature/watch-release"),
        (legacy_task, "feature/watch-legacy"),
    ):
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)
    assert store.resolve_merge_unit_for_task(legacy_task.id) is None

    main_unit = store.create_merge_unit(
        source_branch="feature/watch-main",
        target_branch="main",
        owner_task_id=main_task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(main_task.id, main_unit.id, "owner")
    store.dual_write_legacy_merge_status(main_unit.id)

    release_unit = store.create_merge_unit(
        source_branch="feature/watch-release",
        target_branch="release",
        owner_task_id=release_task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(release_task.id, release_unit.id, "owner")
    store.dual_write_legacy_merge_status(release_unit.id)

    config = Config.load(tmp_path)
    git = _make_watch_git()

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="new-fingerprint"):
        rows, _ = _query_owner_rows_with_context(
            store=store,
            config=config,
            git=git,
            target_branch="main",
            max_recovery_attempts=config.max_resume_attempts,
            include_skipped=True,
        )

    owner_ids = {row.owner_task.id for row in rows}
    assert owner_ids == {main_task.id, legacy_task.id}


def test_watch_owner_rows_keep_lifecycle_merge_candidate_and_failed_recovery_separately(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Watch merge owner", task_type="implement")
    store.mark_completed(impl, has_commits=True, branch="feature/watch-split-owner")
    assert impl.id is not None

    review = store.add("Review merge owner", task_type="review", based_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
    store.update(review)
    assert review.id is not None
    store.get_or_create_merge_unit_for_task(review)

    failed_rebase = store.add(
        "Failed rebase descendant",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = "MERGE_CONFLICT"
    failed_rebase.completed_at = datetime.now(UTC)
    failed_rebase.branch = "feature/watch-split-owner"
    failed_rebase.has_commits = True
    store.update(failed_rebase)
    assert failed_rebase.id is not None
    store.get_or_create_merge_unit_for_task(failed_rebase)

    owner_rows, _ = _query_owner_rows_with_context(
        store=store,
        config=Config.load(tmp_path),
        git=_make_watch_git(),
        target_branch="main",
        max_recovery_attempts=1,
        include_skipped=True,
    )

    assert len(owner_rows) == 1
    row = owner_rows[0]
    assert row.owner_task.id == impl.id
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.status != "failed"
    assert row.recovery_action_task is not None
    assert row.recovery_action_task.id == failed_rebase.id
    assert row.recovery_leaf_task is not None
    assert row.recovery_leaf_task.id == failed_rebase.id

    lifecycle_rows = [
        candidate
        for candidate in owner_rows
        if candidate.lifecycle_action_task is not None and candidate.lifecycle_action_task.status != "failed"
    ]
    recovery_rows = [
        candidate
        for candidate in owner_rows
        if candidate.recovery_action_task is not None
        and candidate.recovery_leaf_task is not None
        and candidate.recovery_action_task.id == candidate.recovery_leaf_task.id
    ]

    assert [candidate.owner_task.id for candidate in lifecycle_rows] == [impl.id]
    assert [candidate.recovery_leaf_task.id for candidate in recovery_rows] == [failed_rebase.id]


def test_watch_cycle_prefers_freshly_bumped_task_over_older_urgent(tmp_path: Path) -> None:
    """Queue bump semantics should be respected by watch pending pickup."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    older_urgent = store.add("Older urgent plan", task_type="plan", urgent=True)
    bumped = store.add("Bumped plan", task_type="plan")
    assert older_urgent.id is not None
    assert bumped.id is not None
    store.set_urgent(bumped.id, True)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == bumped.id


def test_cmd_watch_repeated_tag_filters_use_or_by_default(tmp_path: Path) -> None:
    """CLI watch should forward repeated tag filters as OR unless --all-tags is set."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=1,
        max_iterations=10,
        dry_run=True,
        quiet=True,
        yes=True,
        tags=["release-1", "system"],
        all_tags=False,
    )

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(False, 0, 0)) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
        patch("gza.cli.watch.time.sleep"),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_args.kwargs["tags"] == ("release-1", "system")
    assert run_cycle.call_args.kwargs["any_tag"] is True


def test_watch_cycle_tag_prefers_explicit_queue_order(tmp_path: Path) -> None:
    """Tag-scoped watch should respect explicit queue positions before urgent/FIFO fallback."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    urgent = store.add("Urgent release plan", task_type="plan", group="release", urgent=True)
    ordered = store.add("Ordered release plan", task_type="plan", group="release")
    assert urgent.id is not None
    assert ordered.id is not None
    store.set_queue_position(ordered.id, 1)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("release",),
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == ordered.id


def test_watch_cycle_recovery_mode_resumes_failed_task_before_starting_new_pending(tmp_path: Path) -> None:
    """With restart-failed enabled, failed recoveries consume slots before pending work."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_impl = store.add("Pending implement", task_type="implement")
    assert pending_impl.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_resume.call_count == 0
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    prepared_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert isinstance(prepared_child_id, str)
    assert spawned_task.id == prepared_child_id
    assert spawn_iterate.call_args.kwargs["prepared_resume"] is True


def test_watch_cycle_restart_failed_prioritizes_oldest_created_failed_task(tmp_path: Path) -> None:
    """With limited slots, restart-failed should recover the oldest created failed task first."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    older = store.add("Older failed plan", task_type="plan")
    assert older.id is not None
    older.status = "failed"
    older.failure_reason = "INFRASTRUCTURE_ERROR"
    older.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(older)

    newer = store.add("Newer failed plan", task_type="plan")
    assert newer.id is not None
    newer.status = "failed"
    newer.failure_reason = "INFRASTRUCTURE_ERROR"
    newer.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(newer)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    recovered = max(
        [task for task in store.get_all() if task.based_on in {older.id, newer.id}],
        key=lambda task: int(str(task.id).split("-")[-1]),
    )
    assert recovered.based_on == older.id
    assert spawn_worker.call_args.kwargs["task_id"] == recovered.id


def test_watch_cycle_default_auto_resume_prioritizes_oldest_created_failed_task(tmp_path: Path) -> None:
    """Plain watch auto-resume should use the same oldest-created failed-task ordering."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    older = store.add("Older failed implement", task_type="implement")
    assert older.id is not None
    older.status = "failed"
    older.failure_reason = "MAX_TURNS"
    older.session_id = "sess-older"
    older.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(older)

    newer = store.add("Newer failed implement", task_type="implement")
    assert newer.id is not None
    newer.status = "failed"
    newer.failure_reason = "MAX_TURNS"
    newer.session_id = "sess-newer"
    newer.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(newer)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    prepared_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert isinstance(prepared_child_id, str)
    assert spawned_args.resume is False
    assert spawned_task.id == prepared_child_id


@pytest.mark.parametrize(
    (
        "slots",
        "recovery_slots_config",
        "actionable_recovery_count",
        "worker_consuming_recovery_count",
        "pending_count",
        "gate_pending_on_actionable_recovery",
        "expected",
    ),
    [
        (1, 1, 1, 1, 3, False, WatchSlotAllocation(recovery_slots=1, pending_slots=0)),
        (5, 1, 1, 1, 7, False, WatchSlotAllocation(recovery_slots=1, pending_slots=4)),
        (5, 0, 3, 3, 7, False, WatchSlotAllocation(recovery_slots=0, pending_slots=5)),
        (3, 9, 4, 4, 2, False, WatchSlotAllocation(recovery_slots=3, pending_slots=0)),
        (4, 1, 0, 0, 9, False, WatchSlotAllocation(recovery_slots=0, pending_slots=4)),
        (1, 1, 1, 0, 1, True, WatchSlotAllocation(recovery_slots=0, pending_slots=0)),
        (2, 2, 1, 0, 3, True, WatchSlotAllocation(recovery_slots=0, pending_slots=0)),
    ],
)
def test_allocate_watch_slots(
    slots: int,
    recovery_slots_config: int,
    actionable_recovery_count: int,
    worker_consuming_recovery_count: int,
    pending_count: int,
    gate_pending_on_actionable_recovery: bool,
    expected: WatchSlotAllocation,
) -> None:
    assert (
        allocate_watch_slots(
            slots=slots,
            recovery_slots_config=recovery_slots_config,
            actionable_recovery_count=actionable_recovery_count,
            worker_consuming_recovery_count=worker_consuming_recovery_count,
            pending_count=pending_count,
            gate_pending_on_actionable_recovery=gate_pending_on_actionable_recovery,
        )
        == expected
    )


@pytest.mark.parametrize("task_type", ["implement", "review", "improve", "rebase"])
def test_watch_cycle_plain_mode_batch_one_defaults_to_recovery_first(
    tmp_path: Path, task_type: str
) -> None:
    """Plain watch should spend the single batch-1 slot on recovery before pending work."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add(f"Failed {task_type}", task_type=task_type)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_impl = store.add("Pending implement", task_type="implement")
    assert pending_impl.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_resume.call_count + spawn_iterate.call_count == 1
    if spawn_iterate.call_count == 1:
        spawned_args = spawn_iterate.call_args.args[0]
        spawned_task = spawn_iterate.call_args.args[2]
        assert spawned_args.resume is False
        prepared_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
        assert isinstance(prepared_child_id, str)
        assert spawned_task.id == prepared_child_id
    assert store.get(pending_impl.id).status == "pending"
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert any("RECOVR" in line and failed.id in line for line in log_text.splitlines())


def test_watch_cycle_default_mode_reuses_existing_pending_resume_child(tmp_path: Path) -> None:
    """Plain watch should reuse an existing pending resume child instead of creating a sibling."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    resume_child = store.add("Pending resume child", task_type="implement", based_on=failed.id)
    assert resume_child.id is not None
    resume_child.status = "pending"
    resume_child.session_id = failed.session_id
    store.update(resume_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    children = store.get_based_on_children(failed.id)
    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == resume_child.id
    assert spawn_iterate.call_args.args[0].resume is False
    assert spawn_iterate.call_args.kwargs["prepared_resume"] is True
    assert len(children) == 1
    assert children[0].id == resume_child.id


def test_watch_cycle_default_mode_keeps_reusable_pending_recovery_child_runnable_when_slots_are_saturated(
    tmp_path: Path,
) -> None:
    """Plain watch should still run a reusable pending recovery child through pending pickup when recovery slots are zero."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    resume_child = store.add("Pending resume child", task_type="implement", based_on=failed.id)
    assert resume_child.id is not None
    resume_child.status = "pending"
    resume_child.session_id = failed.session_id
    store.update(resume_child)

    unrelated_pending = store.add("Unrelated pending plan", task_type="plan")
    assert unrelated_pending.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_resume.call_count == 0
    assert spawn_worker.call_count == 0
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == resume_child.id
    assert spawn_iterate.call_args.args[0].resume is False


def test_watch_cycle_default_mode_starts_queued_retry_child_as_pending_work(tmp_path: Path) -> None:
    """Plain watch should run queued retry children through the normal pending queue."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    retry_child = store.add(failed.prompt, task_type="implement", based_on=failed.id, depends_on=failed.depends_on)
    assert retry_child.id is not None
    retry_child.status = "pending"
    store.update(retry_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_resume.call_count == 0
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == retry_child.id
    assert spawn_iterate.call_args.args[0].resume is False
    assert [task.id for task in store.get_based_on_children(failed.id)] == [retry_child.id]


def test_watch_cycle_default_mode_suppresses_existing_in_progress_resume_child(tmp_path: Path) -> None:
    """Plain watch should not create or launch another resume child while one is already running."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    resume_child = store.add("Running resume child", task_type="implement", based_on=failed.id)
    assert resume_child.id is not None
    resume_child.status = "in_progress"
    resume_child.session_id = failed.session_id
    store.update(resume_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    children = store.get_based_on_children(failed.id)
    assert result.work_done is False
    assert spawn_resume.call_count == 0
    assert len(children) == 1
    assert children[0].id == resume_child.id


def test_watch_cycle_default_mode_does_not_treat_unrelated_in_progress_child_as_resume_blocker(
    tmp_path: Path,
) -> None:
    """Plain watch should ignore unrelated in-progress descendants instead of broadening auto-resume logic."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    unrelated_child = store.add("Running retry child", task_type="implement", based_on=failed.id)
    assert unrelated_child.id is not None
    unrelated_child.status = "in_progress"
    unrelated_child.session_id = "different-session"
    store.update(unrelated_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    prepared_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert isinstance(prepared_child_id, str)
    assert spawn_iterate.call_args.args[2].id == prepared_child_id
    assert spawn_iterate.call_args.args[0].resume is False
    prepared_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert spawn_iterate.call_args.kwargs["prepared_resume"] is True
    assert spawn_iterate.call_args.kwargs["prepared_phase"] == "preloop"
    assert spawn_iterate.call_args.kwargs["startup_quiet"] is True
    assert [task.id for task in store.get_based_on_children(failed.id)] == [unrelated_child.id, prepared_child_id]


def test_watch_cycle_default_mode_spawn_failure_reuses_pending_resume_child_next_cycle(tmp_path: Path) -> None:
    """Plain watch should not attempt recovery while the failed task remains dependency-blocked."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Blocked dependency", task_type="plan")
    assert dependency.id is not None
    dependency.status = "in_progress"
    store.update(dependency)

    failed = store.add("Failed implement", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=1) as spawn_iterate,
    ):
        result_first = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )
        result_second = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    children = store.get_based_on_children(failed.id)
    pending_children = [child for child in children if child.status == "pending"]
    assert result_first.work_done is False
    assert result_second.work_done is False
    assert spawn_iterate.call_count == 0
    assert len(children) == 0
    assert len(pending_children) == 0


def test_watch_cycle_default_mode_non_actionable_failed_row_starts_pending(tmp_path: Path) -> None:
    """Plain watch should move on to pending work when the scoped failed row is not auto-recoverable."""
    (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: 0\n")
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement", tags=("backlog",))
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed = store.add("Failed resume child", task_type="implement", based_on=root.id, tags=("release",))
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-root"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending = store.add("Pending fallback plan", task_type="plan", tags=("release",))
    assert pending.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
            tags=("release",),
        )

    assert result.work_done is True
    assert spawn_resume.call_count == 0
    assert spawn_iterate.call_count == 0
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending.id


def test_watch_cycle_default_watch_emits_owner_attention_for_manual_failed_recovery(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store, owner, failed_rebase = _setup_watch_owner_with_failed_rebase(tmp_path, failure_reason="MERGE_CONFLICT")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={"type": "wait_review", "description": "SKIP: waiting for review"},
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is False
    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=config.max_resume_attempts,
    )
    expected_owner_ids = {
        entry.owner_task.id
        for entry in entries
        if entry.attention_action is not None and classify_advance_action(entry.attention_action) == "needs_attention"
    }
    text = log_path.read_text()
    assert "ATTENTION" in text
    attention_lines = [line for line in text.splitlines() if "ATTENTION" in line]
    emitted_owner_ids = {
        match.group(1)
        for line in attention_lines
        if (match := re.search(r"ATTENTION\s+([a-z0-9]+-\d+)\s", line)) is not None
    }
    assert emitted_owner_ids == expected_owner_ids == {owner.id}
    assert f"{owner.id} implement" in text
    assert f"failed leaf {failed_rebase.id}" in text
    assert "Needs attention (1 task):" in text


def test_watch_cycle_owner_plan_attention_emits_once_even_with_skipped_failed_descendant(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store, impl, failed_rebase = _setup_watch_owner_with_failed_rebase(tmp_path, failure_reason="MERGE_CONFLICT")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={
                "type": "wait_review",
                "description": "Owner requires manual review",
                "needs_attention_reason": "owner-needs-attention",
            },
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is False
    attention_lines = [line for line in log_path.read_text().splitlines() if "ATTENTION" in line]
    assert len(attention_lines) == 2
    assert any("reason=owner-needs-attention" in line and impl.id in line for line in attention_lines)
    assert any("reason=manual-failure-reason" in line and f"failed leaf {failed_rebase.id}" in line for line in attention_lines)


def test_watch_cycle_actionable_failed_descendant_still_spawns_recovery_worker(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store, _, failed_rebase = _setup_watch_owner_with_failed_rebase(tmp_path, failure_reason="INFRASTRUCTURE_ERROR")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={"type": "wait_review", "description": "SKIP: waiting for review"},
        ),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count + spawn_worker.call_count == 1
    recovery_children = store.get_based_on_children(failed_rebase.id)
    assert len(recovery_children) == 1
    assert recovery_children[0].id != failed_rebase.id
    assert recovery_children[0].trigger_source == "watch"


def test_watch_cycle_show_skipped_keeps_manual_failed_recovery_on_attention_channel(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store, impl, failed_rebase = _setup_watch_owner_with_failed_rebase(tmp_path, failure_reason="MERGE_CONFLICT")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={"type": "wait_review", "description": "SKIP: waiting for review"},
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
            show_skipped=True,
        )

    assert result.work_done is False
    text = log_path.read_text()
    assert "ATTENTION" in text
    assert any("ATTENTION" in line and impl.id in line for line in text.splitlines())
    assert f"failed leaf {failed_rebase.id}" in text
    assert not any(
        "SKIP" in line and f"{impl.id} failed rebase: manual_failure_reason" in line
        for line in text.splitlines()
    )


def test_watch_cycle_unknown_manual_empty_branch_matches_incomplete_owner_surface(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store, failed_leaf, _blocked = _setup_watch_unknown_failed_empty_branch_owner(tmp_path)
    watch_git = _make_empty_merged_watch_git(tmp_path, empty_branch=str(failed_leaf.branch))

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    entries = collect_recovery_lane_entries(
        store,
        tags=None,
        any_tag=False,
        max_recovery_attempts=config.max_resume_attempts,
        git=watch_git,
        target_branch="main",
    )
    expected_owner_ids = {
        entry.owner_task.id
        for entry in entries
        if entry.attention_action is not None and classify_advance_action(entry.attention_action) == "needs_attention"
    }

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=watch_git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={"type": "wait_review", "description": "SKIP: waiting for review"},
        ),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("manual recovery row should not spawn")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("manual recovery row should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=0,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    attention_lines = [line for line in log_path.read_text().splitlines() if "ATTENTION" in line]
    emitted_owner_ids = {
        match.group(1)
        for line in attention_lines
        if (match := re.search(r"ATTENTION\s+([a-z0-9]+-\d+)\s", line)) is not None
    }
    assert emitted_owner_ids == expected_owner_ids == {failed_leaf.id}
    assert any(
        failed_leaf.id in line and 'implement "Failed empty-branch descendant"' in line
        for line in attention_lines
    )
    text = log_path.read_text()
    assert "Needs attention (1 task):" in text


def test_watch_cycle_default_watch_false_positives_stay_silent(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    resume = store.add("Resume candidate", task_type="implement")
    assert resume.id is not None
    resume.status = "failed"
    resume.failure_reason = "MAX_TURNS"
    resume.session_id = "sess-resume"
    resume.completed_at = datetime.now(UTC)
    store.update(resume)

    retry = store.add("Retry candidate", task_type="plan")
    assert retry.id is not None
    retry.status = "failed"
    retry.failure_reason = "INFRASTRUCTURE_ERROR"
    retry.completed_at = datetime.now(UTC)
    store.update(retry)

    hidden = store.add("Hidden failed candidate", task_type="implement")
    assert hidden.id is not None
    hidden.status = "failed"
    hidden.failure_reason = "MAX_TURNS"
    hidden.session_id = "sess-hidden"
    hidden.branch = "feature/hidden-watch-row"
    hidden.completed_at = datetime.now(UTC)
    store.update(hidden)

    pending_retry = store.add(hidden.prompt, task_type="implement", based_on=hidden.id)
    assert pending_retry.id is not None
    pending_retry.status = "pending"
    pending_retry.branch = "feature/hidden-watch-row-retry"
    store.update(pending_retry)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert "Needs attention" not in text


def test_watch_cycle_manual_failed_recovery_emits_one_steady_attention_per_cycle(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store, failed_leaf, _blocked = _setup_watch_unknown_failed_empty_branch_owner(tmp_path)
    watch_git = _make_empty_merged_watch_git(tmp_path, empty_branch=str(failed_leaf.branch))

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=False)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=watch_git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={"type": "wait_review", "description": "SKIP: waiting for review"},
        ),
    ):
        first = _run_cycle(
            config=config,
            store=store,
            batch=0,
            max_iterations=10,
            dry_run=False,
            log=log,
        )
        second = _run_cycle(
            config=config,
            store=store,
            batch=0,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert first.work_done is False
    assert second.work_done is False
    text = log_path.read_text()
    stdout = capsys.readouterr().out
    assert text.count("ATTENTION") == 1
    assert text.count("Needs attention (1 task):") == 2
    assert stdout.count("Needs attention (1 task):") == 2
    assert text.count(f"{failed_leaf.id} implement") == 3


def test_watch_cycle_recovery_mode_retries_failed_implement_via_iterate_child(tmp_path: Path) -> None:
    """Restart-failed retry path for implement tasks should launch iterate on the new child without retry flags."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    spawned_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert isinstance(spawned_child_id, str)
    assert spawned_task.id == spawned_child_id
    assert spawn_iterate.call_args.kwargs["prepared_resume"] is False
    assert spawn_iterate.call_args.kwargs["prepared_phase"] == "preloop"
    assert spawn_iterate.call_args.kwargs["startup_quiet"] is True
    log_text = log_path.read_text()
    assert any("RECOVR" in line and f"{failed.id} retry via iterate -> {spawned_child_id}" in line for line in log_text.splitlines())
    assert not any("RECOVR" in line and f"{failed.id} resume via iterate -> {spawned_child_id}" in line for line in log_text.splitlines())


def test_watch_cycle_restart_failed_terminalizes_dead_pending_retry_child_before_next_bounded_attempt(
    tmp_path: Path,
) -> None:
    """Restart-failed should fail a stale dead pending retry row as NO_ACTIVITY before launching the next attempt."""
    import os
    from datetime import timedelta

    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    retry_child = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert retry_child.id is not None
    retry_child.status = "pending"
    store.update(retry_child)

    config = Config.load(tmp_path)
    config.watch.no_activity_timeout = 1
    registry = WorkerRegistry(config.workers_path)
    registry.register(
        WorkerMetadata(
            worker_id="w-dead-watch-retry",
            task_id=retry_child.id,
            pid=os.getpid(),
            started_at=(datetime.now(UTC) - timedelta(seconds=90)).isoformat(),
            status="running",
            startup_log_file=".gza/workers/dead-watch-retry.startup.log",
        )
    )
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.WorkerRegistry.is_running", return_value=False),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is False
    refreshed_retry_child = store.get(retry_child.id)
    assert refreshed_retry_child is not None
    assert refreshed_retry_child.status == "failed"
    assert refreshed_retry_child.failure_reason == "NO_ACTIVITY"
    assert spawn_iterate.call_count == 0
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "BACKOFF" in log_text
    assert f"{failed.id} retry delayed" in log_text


def test_watch_cycle_repeated_recovery_evaluation_does_not_park_pending_descendant(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-pending-child"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    recovery_child = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert recovery_child.id is not None
    recovery_child.status = "pending"
    recovery_child.session_id = failed.session_id
    recovery_child.started_at = datetime.now(UTC)
    recovery_child.running_pid = 4343
    store.update(recovery_child)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert store.list_watch_progress_observations(subject_kind="lineage", subject_id=failed.id) == []
    text = log_path.read_text()
    assert "ATTENTION" not in text


def test_watch_cycle_restart_failed_reuses_existing_deep_recovery_chain_without_creating_sibling(
    tmp_path: Path,
) -> None:
    """Restart-failed should preserve and launch a pending deep descendant without creating siblings."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-root"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed_retry = store.add("Failed retry", task_type="implement", based_on=root.id)
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "MAX_TURNS"
    failed_retry.session_id = root.session_id
    failed_retry.completed_at = datetime.now(UTC)
    store.update(failed_retry)

    pending_grandchild = store.add("Pending grandchild", task_type="implement", based_on=failed_retry.id)
    assert pending_grandchild.id is not None
    pending_grandchild.status = "pending"
    pending_grandchild.session_id = root.session_id
    store.update(pending_grandchild)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=3,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    assert spawned_task.id == pending_grandchild.id
    assert [task.id for task in store.get_based_on_children(root.id)] == [failed_retry.id]
    assert [task.id for task in store.get_based_on_children(failed_retry.id)] == [pending_grandchild.id]


def test_watch_cycle_dry_run_recovery_mode_reports_actions_without_mutation(tmp_path: Path) -> None:
    """Recovery-mode dry-run should log a non-mutating recovery action preview."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    log_text = log_path.read_text()
    assert any("RECOVR" in line and f"{failed.id} resume via iterate -> (new task)" in line for line in log_text.splitlines())
    assert len(store.get_based_on_children(failed.id)) == 0


def test_watch_cycle_recovery_startup_failure_rolls_back_child_and_skips_success_log(tmp_path: Path) -> None:
    """Restart-failed startup failures should not leave recovery children or RECOVR success output behind."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed review", task_type="review")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.watch._spawn_background_resume_worker",
            side_effect=AssertionError("resume worker should not spawn"),
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is False
    assert store.get_based_on_children(failed.id) == []
    text = log_path.read_text()
    assert "creator boom" not in text
    assert not any("RECOVR" in line and str(failed.id) in line for line in text.splitlines())
    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_watch_cycle_recovery_mode_does_not_resume_test_failure_tasks(tmp_path: Path) -> None:
    """TEST_FAILURE is excluded from unattended recovery."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TEST_FAILURE"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_impl = store.add("Pending implement", task_type="implement")
    assert pending_impl.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_resume.call_count == 0
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == pending_impl.id


def test_watch_cycle_resume_spawn_failure_does_not_fall_back_to_generic_iterate(tmp_path: Path) -> None:
    """Implement recovery should retry the failed root iterate launch while reusing one prepared resume child."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=1) as spawn_resume,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result_first = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )
        result_second = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    children = store.get_based_on_children(failed.id)
    assert result_first.work_done is True
    assert result_second.work_done is True
    assert spawn_resume.call_count == 0
    assert spawn_iterate.call_count == 2
    first_task = spawn_iterate.call_args_list[0].args[2]
    second_args = spawn_iterate.call_args_list[1].args[0]
    second_task = spawn_iterate.call_args_list[1].args[2]
    assert first_task.id == second_task.id
    assert first_task.id != failed.id
    first_prepared_child = spawn_iterate.call_args_list[0].kwargs["prepared_task_id"]
    second_prepared_child = spawn_iterate.call_args_list[1].kwargs["prepared_task_id"]
    assert first_prepared_child == second_prepared_child
    assert second_args.resume is False
    assert second_args.retry is False
    assert [task.id for task in children] == [first_prepared_child]


def test_watch_cycle_reuses_preexisting_pending_resume_child_with_resume_semantics(tmp_path: Path) -> None:
    """Restart-failed should launch iterate on the failed root and let iterate reuse the pending resume child."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    resume_child = store.add("Resume child", task_type="implement", based_on=failed.id)
    assert resume_child.id is not None
    resume_child.status = "pending"
    resume_child.session_id = failed.session_id
    store.update(resume_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    assert spawned_task.id == resume_child.id
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    assert [task.id for task in store.get_based_on_children(failed.id)] == [resume_child.id]


def test_watch_cycle_blocked_pending_recovery_child_waits_for_dependency_then_reuses_child(
    tmp_path: Path,
) -> None:
    """Restart-failed must not launch a blocked reused child until its dependency completes."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="plan")
    failed = store.add("Blocked failed implement", task_type="implement", depends_on=dependency.id)
    assert dependency.id is not None
    assert failed.id is not None
    dependency.status = "in_progress"
    store.update(dependency)
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    resume_child = store.add(
        "Pending resume child",
        task_type="implement",
        based_on=failed.id,
        depends_on=dependency.id,
    )
    assert resume_child.id is not None
    resume_child.status = "pending"
    resume_child.session_id = failed.session_id
    store.update(resume_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        blocked_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

        dependency.status = "completed"
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)

        ready_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert blocked_result.work_done is False
    assert ready_result.work_done is True
    assert spawn_worker.call_count == 0
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    assert spawned_task.id == resume_child.id
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    assert len(store.get_based_on_children(failed.id)) == 1


def test_watch_cycle_two_slot_batch_launches_recovery_and_pending_in_same_pass(tmp_path: Path) -> None:
    """A two-slot pass should launch one recovery and one pending task together."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan.id


def test_cmd_watch_without_explicit_batch_uses_default_capacity_for_recovery_and_pending(
    tmp_path: Path,
) -> None:
    """Plain watch with omitted --batch should still launch recovery and pending work in the same pass."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=None,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
    )

    def run_real_cycle_once_then_idle(**kwargs):
        if not hasattr(run_real_cycle_once_then_idle, "seen"):
            run_real_cycle_once_then_idle.seen = True  # type: ignore[attr-defined]
            return _run_cycle(**kwargs)
        return _CycleResult(work_done=False, running=0, pending=0)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
        patch("gza.cli.watch._run_cycle", side_effect=run_real_cycle_once_then_idle),
        patch("gza.cli.watch._sleep_interruptibly"),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert spawn_iterate.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan.id


def test_watch_cycle_pending_only_mode_skips_recovery_and_runs_pending(tmp_path: Path) -> None:
    """Pending-only mode should leave failed recovery untouched and spend all slots on pending work."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=4,
            max_iterations=10,
            dry_run=False,
            log=log,
            recovery_slots=0,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 0
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan.id
    assert store.get_based_on_children(failed.id) == []


def test_watch_cycle_pending_only_mode_skips_reconcile_recovery_and_runs_pending(tmp_path: Path) -> None:
    """Pending-only mode should not execute direct reconcile recovery actions."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/pending-only-reconcile"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.cli.watch.execute_advance_action",
            side_effect=AssertionError("pending-only should not execute reconcile recovery"),
        ),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
            recovery_slots=0,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan.id
    log_text = log_path.read_text()
    assert "RECOVR" not in log_text
    assert "reconcile branch publication" not in log_text


def test_watch_cycle_default_recovery_slot_caps_pending_implement_starts(tmp_path: Path) -> None:
    """One actionable recovery with batch 3 should leave room for only two pending implement starts."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-recovery-slot-cap"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_impls = [store.add(f"Pending implement {index}", task_type="implement") for index in range(4)]
    pending_ids = {task.id for task in pending_impls}

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=3,
            max_iterations=10,
            dry_run=False,
            log=log,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 3
    recovery_calls = [call for call in spawn_iterate.call_args_list if call.args[2].based_on == failed.id]
    pending_calls = [call for call in spawn_iterate.call_args_list if call.args[2].id in pending_ids]
    assert len(recovery_calls) == 1
    assert len(pending_calls) == 2


def test_watch_cycle_dry_run_matches_shared_preview_dispatch_plan_order(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-preview-parity"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    pending_impl = store.add("Pending implement", task_type="implement")
    pending_plan_two = store.add("Pending plan two", task_type="plan")
    assert pending_plan.id is not None
    assert pending_impl.id is not None
    assert pending_plan_two.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    preview = build_dispatch_preview(
        store,
        config=config,
        tags=None,
        any_tag=False,
        max_recovery_attempts=config.max_resume_attempts,
    )
    plan = plan_watch_dispatch_entries(
        preview.runnable_entries,
        slots=3,
        recovery_slot_cap=config.watch.recovery_slots,
        selection_mode="default",
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("dry-run should not spawn")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("dry-run should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=3,
            max_iterations=10,
            dry_run=True,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    log_lines = log_path.read_text().splitlines()
    dispatched_ids: list[str] = []
    for line in log_lines:
        if "RECOVR" in line:
            dispatched_ids.append(failed.id)
        elif "START" in line and "[dry-run]" in line:
            for task_id in (pending_plan.id, pending_impl.id, pending_plan_two.id):
                if isinstance(task_id, str) and task_id in line:
                    dispatched_ids.append(task_id)
                    break
    assert dispatched_ids == [entry.task.id for entry in plan.entries]


def test_watch_cycle_dry_run_caps_pending_implement_preview_to_allocated_slots(tmp_path: Path) -> None:
    """Dry-run should preview only the pending implement starts allowed after recovery reservation."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-dry-run-cap"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_impls = [store.add(f"Pending implement {index}", task_type="implement") for index in range(4)]

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("dry-run should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=3,
            max_iterations=10,
            dry_run=True,
            log=log,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    log_lines = log_path.read_text().splitlines()
    start_lines = [line for line in log_lines if "START" in line and "[dry-run]" in line]
    assert len(start_lines) == 2
    assert any("RECOVR" in line and "[dry-run]" in line for line in log_lines)
    assert any(str(pending_impls[0].id) in line for line in start_lines)
    assert any(str(pending_impls[1].id) in line for line in start_lines)
    assert all(str(pending_impls[2].id) not in line for line in start_lines)
    assert all(str(pending_impls[3].id) not in line for line in start_lines)


def test_watch_cycle_dry_run_caps_pending_nonimplement_preview_to_allocated_slots(tmp_path: Path) -> None:
    """Dry-run should apply the same pending slot cap to non-implement queue previews."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-dry-run-plan-cap"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plans = [store.add(f"Pending plan {index}", task_type="plan") for index in range(4)]

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("dry-run should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=3,
            max_iterations=10,
            dry_run=True,
            log=log,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    log_lines = log_path.read_text().splitlines()
    start_lines = [line for line in log_lines if "START" in line and "[dry-run]" in line]
    assert len(start_lines) == 2
    assert any("RECOVR" in line and "[dry-run]" in line for line in log_lines)
    assert any(str(pending_plans[0].id) in line for line in start_lines)
    assert any(str(pending_plans[1].id) in line for line in start_lines)
    assert all(str(pending_plans[2].id) not in line for line in start_lines)
    assert all(str(pending_plans[3].id) not in line for line in start_lines)


def test_watch_cycle_pending_only_dry_run_skips_reconcile_preview_and_shows_pending(tmp_path: Path) -> None:
    """Pending-only dry-run should suppress reconcile preview while still previewing pending pickup."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/pending-only-dry-run-reconcile"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.cli.watch.execute_advance_action",
            side_effect=AssertionError("pending-only dry-run should not execute reconcile recovery"),
        ),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("dry-run should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=True,
            log=log,
            recovery_slots=0,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    log_lines = log_path.read_text().splitlines()
    start_lines = [line for line in log_lines if "START" in line and "[dry-run]" in line]
    assert len(start_lines) == 1
    assert str(pending_plan.id) in start_lines[0]
    assert all("RECOVR" not in line for line in log_lines)
    assert all("reconcile branch publication" not in line for line in log_lines)


def test_watch_cycle_restart_failed_manual_failure_child_does_not_block_pending_queue(tmp_path: Path) -> None:
    """Manual-only failed chains should not keep restart-failed sessions stuck in recovery phase."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Broken plan", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TEST_FAILURE"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    child = store.add("Manual retry child", task_type="plan", based_on=failed.id)
    assert child.id is not None
    child.status = "in_progress"
    store.update(child)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._collect_live_running_state", return_value=(set(), [], 0)),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan.id


def test_watch_cycle_plain_mode_starts_manually_queued_pending_recovery_child(tmp_path: Path) -> None:
    """Plain watch should pick pending manual recovery descendants from the normal pending queue."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Broken plan", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TEST_FAILURE"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    manual_child = store.add("Manual retry child", task_type="plan", based_on=failed.id)
    assert manual_child.id is not None
    manual_child.status = "pending"
    store.update(manual_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == manual_child.id


def test_watch_cycle_restart_failed_starts_manually_queued_child_after_recovery_budget_exhaustion(
    tmp_path: Path,
) -> None:
    """--restart-failed should drain both bounded retries before starting manual pending children."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    actionable = store.add("Actionable failed plan", task_type="plan")
    assert actionable.id is not None
    actionable.status = "failed"
    actionable.failure_reason = "INFRASTRUCTURE_ERROR"
    actionable.completed_at = datetime.now(UTC)
    store.update(actionable)

    manual_parent = store.add("Manual-only failed plan", task_type="plan")
    assert manual_parent.id is not None
    manual_parent.status = "failed"
    manual_parent.failure_reason = "TEST_FAILURE"
    manual_parent.completed_at = datetime.now(UTC)
    store.update(manual_parent)

    manual_child = store.add("Manual pending retry", task_type="plan", based_on=manual_parent.id)
    assert manual_child.id is not None
    manual_child.status = "pending"
    store.update(manual_child)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        first_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )
        actionable_child = store.get_based_on_children(actionable.id)[0]
        actionable_child.status = "failed"
        actionable_child.failure_reason = "TEST_FAILURE"
        store.update(actionable_child)
        second_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )
        final_actionable_child = sorted(store.get_based_on_children(actionable.id), key=lambda task: str(task.id))[-1]
        final_actionable_child.status = "failed"
        final_actionable_child.failure_reason = "TEST_FAILURE"
        store.update(final_actionable_child)
        third_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert first_result.work_done is True
    assert second_result.work_done is True
    assert third_result.work_done is True
    assert spawn_worker.call_count == 3
    assert spawn_worker.call_args_list[-1].kwargs["task_id"] == manual_child.id


@pytest.mark.parametrize(
    ("failure_reason", "session_id", "max_recovery_attempts", "expected_reason_code"),
    [
        ("MAX_TURNS", None, 1, "retry_limit_reached"),
        ("MAX_TURNS", "sess-123", 0, "automatic_recovery_disabled"),
    ],
)
def test_watch_cycle_pending_manual_recovery_child_not_suppressed_for_stop_reasons(
    tmp_path: Path,
    failure_reason: str,
    session_id: str | None,
    max_recovery_attempts: int,
    expected_reason_code: str,
) -> None:
    """Pending manual recovery descendants remain runnable for manual-review and disabled-recovery stops."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed parent", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = failure_reason
    failed.session_id = session_id
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    manual_child = store.add("Manual pending recovery", task_type="plan", based_on=failed.id)
    assert manual_child.id is not None
    manual_child.status = "pending"
    manual_child.session_id = session_id
    store.update(manual_child)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=max_recovery_attempts)
    assert decision.reason_code == expected_reason_code

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=max_recovery_attempts,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == manual_child.id


def test_watch_cycle_executes_branch_unpushable_reconcile_without_worker_slots(tmp_path: Path) -> None:
    """Failed BRANCH_UNPUSHABLE tasks should reconcile directly without spawning resume/retry workers."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/reconcile-publish"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.cli.watch.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="reconcile_branch_divergence",
                status="success",
                message="Reconciled branch publication",
                success_message="Reconciled branch publication",
                work_done=True,
                worker_consuming=False,
            ),
        ) as execute_action,
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("worker should not spawn")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not spawn")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=1,
        )

    assert result.work_done is True
    execute_action.assert_called_once()
    action = execute_action.call_args.kwargs["action"]
    assert action["type"] == "reconcile_branch_divergence"
    assert action["decision"].action == "reconcile"
    text = log_path.read_text()
    assert "RECOVR" in text
    assert "reconcile branch publication" in text
    assert "REPAIR" in text


@pytest.mark.parametrize(
    ("status", "message", "success_message"),
    [
        ("error", "Reconcile attempt failed", None),
        ("skip", "Reconcile requires manual repair", None),
        ("success", "Reconciled branch publication", "Reconciled branch publication"),
    ],
)
def test_watch_cycle_direct_reconcile_does_not_block_pending_worker_slot(
    tmp_path: Path,
    status: str,
    message: str,
    success_message: str | None,
) -> None:
    """Direct reconcile recovery should not suppress pending pickup when no worker slot is consumed."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = f"feature/reconcile-pending-{status}"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.cli.watch.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="reconcile_branch_divergence",
                status=status,
                message=message,
                success_message=success_message,
                work_done=status == "success",
                worker_consuming=False,
            ),
        ) as execute_action,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            recovery_slots=1,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    execute_action.assert_called_once()
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan.id
    log_text = log_path.read_text()
    assert "START" in log_text
    assert str(pending_plan.id) in log_text


def test_watch_cycle_logs_off_topic_clearance_success_message_without_starting_impl_task(
    tmp_path: Path,
) -> None:
    """Watch should log clear_off_topic_verify_blocker success output instead of a misleading START."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/off-topic-watch-log"
    impl.has_commits = True
    impl.merge_status = "unmerged"
    store.update(impl)

    row = LineageOwnerRow(
        owner_task=impl,
        members=(impl,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="clear_off_topic_verify_blocker",
        unresolved_tasks=(impl,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=impl,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()
    success_message = (
        "Cleared verify-only review blocker as off-topic; "
        "created investigation task(s): gza-999"
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[]),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={
                "type": "clear_off_topic_verify_blocker",
                "description": "Clear off-topic verify blocker",
            },
        ),
        patch(
            "gza.cli.watch.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="clear_off_topic_verify_blocker",
                status="success",
                message="",
                success_message=success_message,
                handled_task_id=impl.id,
                work_done=True,
                worker_consuming=False,
            ),
        ) as execute_action,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    execute_action.assert_called_once()
    log_text = log_path.read_text()
    assert "REPAIR" in log_text
    assert success_message in log_text
    assert f"START     {impl.id}" not in log_text


@pytest.mark.parametrize("dry_run", [False, True])
def test_watch_cycle_recovery_only_direct_reconcile_blocks_pending_pickup(
    tmp_path: Path,
    dry_run: bool,
) -> None:
    """Recovery-only watch should drain actionable direct recovery before pending pickup."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = f"feature/recovery-only-direct-reconcile-{dry_run}"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.cli.watch.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="reconcile_branch_divergence",
                status="success",
                message="Reconciled branch publication",
                success_message="Reconciled branch publication",
                work_done=True,
                worker_consuming=False,
            ),
        ) as execute_action,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=dry_run,
            log=log,
            recovery_slots=1,
            recovery_mode="recovery-only",
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    if dry_run:
        execute_action.assert_not_called()
    else:
        execute_action.assert_called_once()
    spawn_worker.assert_not_called()
    log_text = log_path.read_text()
    assert "RECOVR" in log_text
    assert "START" not in log_text
    assert str(pending_plan.id) not in log_text


def test_watch_cycle_parks_branch_unpushable_after_direct_reconcile_budget_is_consumed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed direct reconcile attempt should consume the shared recovery budget."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/reconcile-budget"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    preseeded_git = _make_preseeded_watch_git(tmp_path)

    def _seeded_merge_context(_project_dir: object = None) -> object:
        return recovery_engine._MergeContext(  # noqa: SLF001
            git=preseeded_git,
            default_branch="main",
            existing_branches=frozenset(),
        )

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _seeded_merge_context)

    def _execute_and_fail_once(*, task, action, context):
        persist_branch_publication_state(
            store=store,
            task=task,
            config=config,
            state=BranchPublicationState(reconcile_attempts_consumed=1),
            status="BRANCH_UNPUSHABLE",
            exit_status="reconcile_retry_failed",
        )
        return AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="error",
            message="Reconciled branch publication; completion retry ended in BRANCH_UNPUSHABLE",
        )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=preseeded_git),
        patch("gza.cli.watch.execute_advance_action", side_effect=_execute_and_fail_once) as execute_action,
    ):
        first = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=1,
        )
        second = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=1,
        )

    assert first.work_done is False
    assert second.work_done is False
    assert execute_action.call_count == 1
    refreshed = store.get(failed.id)
    assert refreshed is not None
    decision = decide_failed_task_recovery(
        store,
        refreshed,
        max_recovery_attempts=1,
        merge_context=recovery_engine._MergeContext(git=None, default_branch=None),  # noqa: SLF001
    )
    assert decision.reason_code == "retry_limit_reached"


def test_watch_cycle_branchless_pr_required_parks_without_reconcile_attempt(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Branchless legacy publication failures should surface as attention, not attempted reconcile work."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Legacy failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PR_REQUIRED"
    failed.has_commits = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=False,
        quiet=True,
        yes=True,
        group=None,
        recovery_mode="recovery-only",
        recovery_slots=None,
        max_resume_attempts=None,
    )

    with (
        patch("gza.cli.watch.execute_advance_action", side_effect=AssertionError("reconcile should not execute")),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    normalized = " ".join(stdout.split())
    assert "Needs attention" in stdout
    assert "reason=branch-publication-needs-manual-repair" in normalized
    assert "no branch to reconcile" in normalized
    assert "reconcile branch publication" not in stdout
    assert "REPAIR" not in stdout


def test_watch_cycle_reconcile_completes_failed_branch_unpushable_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/watch-reconcile-complete"
    failed.has_commits = True
    failed.log_file = "logs/watch-reconcile.log"
    failed.output_content = "summary"
    failed.diff_files_changed = 1
    failed.diff_lines_added = 3
    failed.diff_lines_removed = 0
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    ensure_result = SimpleNamespace(ok=True, status="created", error=None, pr_url="https://example.test/pr/2")
    git = SimpleNamespace(
        default_branch=lambda: "main",
        current_branch=lambda: "main",
        count_commits_ahead=lambda *_args: 1,
        rev_parse_if_exists=lambda ref: {"feature/watch-reconcile-complete": "head123", "main": "base456"}.get(ref),
        can_merge=lambda *_args: True,
        get_diff_name_status=lambda *_args: "",
    )

    def _execute_with_mocked_git(*, task, action, context):
        context.git = git
        return real_execute_advance_action(task=task, action=action, context=context)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch._reconcile_diverged_branch_with_origin",
            return_value=SimpleNamespace(
                status="reconciled",
                message="Reconciled branch publication",
            ),
        ),
        patch("gza.cli.watch.execute_advance_action", side_effect=_execute_with_mocked_git),
        patch("gza.runner.ensure_task_pr", return_value=ensure_result) as ensure_pr,
        patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None),
        patch("gza.runner.task_footer"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("worker should not spawn")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not spawn")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=1,
        )

    assert result.work_done is True
    ensure_pr.assert_called_once()
    refreshed = store.get(failed.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.failure_reason is None
    text = log_path.read_text()
    assert "reconcile branch publication" in text


def test_watch_cycle_recovery_reconcile_durable_progress_does_not_record_no_progress(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/watch-reconcile-progress"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    def _execute_and_complete(*, task, action, context):
        refreshed = store.get(task.id)
        assert refreshed is not None
        refreshed.status = "completed"
        refreshed.failure_reason = None
        refreshed.completed_at = datetime.now(UTC)
        store.update(refreshed)
        return AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="success",
            message="Reconciled branch publication",
            success_message="Reconciled branch publication",
            work_done=True,
            worker_consuming=False,
        )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.execute_advance_action", side_effect=_execute_and_complete) as execute_action,
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("worker should not spawn")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not spawn")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=1,
        )

    assert result.work_done is True
    execute_action.assert_called_once()
    refreshed = store.get(failed.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert store.list_watch_progress_observations(subject_kind="lineage", subject_id=failed.id) == []
    text = log_path.read_text()
    assert "ATTENTION" not in text


def test_watch_cycle_recovery_reconcile_no_op_success_parks_at_no_progress_threshold(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed publish", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/watch-reconcile-no-op"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.cli.watch.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="reconcile_branch_divergence",
                status="success",
                message="Reconciled branch publication",
                success_message="Reconciled branch publication",
                work_done=True,
                worker_consuming=False,
            ),
        ) as execute_action,
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("worker should not spawn")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not spawn")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume should not spawn")),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=False,
            max_recovery_attempts=1,
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=False,
            max_recovery_attempts=1,
        )

    assert execute_action.call_count == 2
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=failed.id)
    assert len(observations) == 1
    assert observations[0].streak == 2
    assert observations[0].parked_reason == WATCH_NO_PROGRESS_BACKSTOP_REASON
    text = log_path.read_text()
    assert "ATTENTION" in text
    assert "without durable progress for 2 cycles" in text


def test_watch_cycle_restart_failed_hides_skipped_logs_by_default(tmp_path: Path) -> None:
    """Restart-failed should suppress skipped recovery log lines unless explicitly requested."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Broken review", task_type="review")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
    )

    assert result.work_done is True
    assert spawn_resume.call_count == 1
    text = log_path.read_text()
    assert "recovery-skip" not in text
    assert any("RECOVR" in line and f"{failed.id} resume via worker" in line for line in text.splitlines())


def test_watch_cycle_restart_failed_show_skipped_emits_skipped_logs(tmp_path: Path) -> None:
    """Restart-failed should emit skipped recovery log lines when show_skipped is enabled."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Broken review", task_type="review")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
            show_skipped=True,
        )

    assert result.work_done is True
    assert spawn_resume.call_count == 1
    text = log_path.read_text()
    assert "SKIP" not in text
    assert any("RECOVR" in line and f"{failed.id} resume via worker" in line for line in text.splitlines())


def test_watch_cycle_recovery_only_running_recovery_child_keeps_pending_queue_blocked_without_execution(
    tmp_path: Path,
) -> None:
    """A merely re-evaluated running recovery child no longer parks and therefore still blocks recovery-only pending pickup."""
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed review", task_type="review")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    child = store.add("Review resume child", task_type="review", based_on=failed.id)
    assert child.id is not None
    child.status = "in_progress"
    child.session_id = failed.session_id
    store.update(child)

    pending_plan = store.add("Pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._collect_live_running_state", return_value=(set(), [], 0)),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        first = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )
        result = _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert first.work_done is False
    assert result.work_done is False
    assert spawn_worker.call_count == 0


@pytest.mark.parametrize(
    ("action_type", "child_type"),
    [
        ("create_review", "review"),
        ("improve", "improve"),
        ("create_implement", "implement"),
        ("needs_rebase", "rebase"),
    ],
)
def test_watch_cycle_task_creating_advance_spawn_failure_is_not_retried_in_step3(
    tmp_path: Path, action_type: str, child_type: str
) -> None:
    """Task-creating advance children should not be retried via generic pickup in the same watch pass."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root_type = "plan" if action_type == "create_implement" else "implement"
    root = store.add("Root task", task_type=root_type)
    assert root.id is not None
    root.status = "completed"
    root.completed_at = datetime.now(UTC)
    if action_type != "create_implement":
        root.branch = "feature/same-watch-pass-no-retry"
    store.update(root)
    if action_type != "create_implement":
        store.set_merge_status(root.id, "unmerged")

    review_task = None
    if action_type == "improve":
        review_task = store.add("Completed review", task_type="review", depends_on=root.id)
        assert review_task.id is not None
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

    rebase_task = None
    if action_type == "needs_rebase":
        rebase_task = store.add("Pending rebase", task_type="rebase", based_on=root.id, depends_on=root.id)
        assert rebase_task.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    action: dict[str, object] = {"type": action_type}
    if action_type == "improve":
        assert review_task is not None
        action["review_task"] = review_task

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value=action),
        patch(
            "gza.cli.watch._prepare_create_review_action",
            return_value=SimpleNamespace(
                status="created",
                review_task=review_task,
                message="created",
            ),
        ) as create_review,
        patch("gza.cli.watch._create_rebase_task", return_value=rebase_task) as create_rebase,
        patch("gza.cli.watch._spawn_background_worker", side_effect=[1, 0]) as spawn_worker,
        patch("gza.cli.watch._spawn_background_iterate", side_effect=[1, 0]) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    if action_type in {"create_review", "improve", "create_implement"}:
        assert spawn_iterate.call_count == 1
        if action_type == "create_implement":
            assert spawn_worker.call_count == 0
            created_children = [
                task for task in store.get_all() if task.task_type == "implement" and task.depends_on == root.id
            ]
            assert len(created_children) == 1
            child_id = str(created_children[0].id)
        else:
            assert spawn_worker.call_count == 0
            child_id = str(root.id)
            if action_type == "create_review":
                assert create_review.call_count == 0
            else:
                improved_children = [
                    task for task in store.get_all() if task.task_type == "improve" and task.depends_on == review_task.id
                ]
                assert improved_children == []
    else:
        assert spawn_worker.call_count == 1
        if action_type == "needs_rebase":
            assert create_rebase.call_count == 1
            assert rebase_task is not None
            child_id = str(rebase_task.id)
        else:
            assert review_task is not None
            child_id = str(review_task.id)

    log_lines = log_path.read_text().splitlines()
    assert any("START_FAILED" in line and child_id in line for line in log_lines)
    assert not any(
        line.split(maxsplit=2)[1] == "START" and f"{child_id} {child_type}" in line
        for line in log_lines
    )


def test_count_live_workers_dedupes_registry_and_in_progress_rows_by_pid(tmp_path: Path) -> None:
    """Iterate worker plus foreground child rows must consume one slot."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    review.status = "in_progress"
    review.running_pid = 4242
    store.update(review)

    config = Config.load(tmp_path)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=impl.id, pid=4242, status="running"),
    ]
    registry.is_running.return_value = True

    with (
        patch("gza.concurrency.WorkerRegistry", return_value=registry),
        patch("gza.concurrency._pid_alive", return_value=True),
    ):
        assert _count_live_workers(config, store) == 1


def test_count_live_workers_counts_live_worker_for_terminal_task(tmp_path: Path) -> None:
    """A live terminal-task worker still consumes a slot until its PID dies."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "INFRASTRUCTURE_ERROR"
    task.running_pid = None
    store.update(task)

    config = Config.load(tmp_path)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=task.id, pid=4242, status="running"),
    ]
    registry.is_running.return_value = True

    with (
        patch("gza.concurrency.WorkerRegistry", return_value=registry),
        patch("gza.concurrency._pid_alive", return_value=True),
    ):
        assert _count_live_workers(config, store) == 1


def test_collect_live_running_state_counts_live_terminal_task_worker_as_anonymous_capacity(
    tmp_path: Path,
) -> None:
    """A live terminal-task worker counts as capacity without reviving its task ID."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    worker_task = store.add("Worker-backed task", task_type="implement")
    pid_only_task = store.add("PID-only task", task_type="review")
    terminal_task = store.add("Terminal task", task_type="plan")
    assert worker_task.id is not None
    assert pid_only_task.id is not None
    assert terminal_task.id is not None

    worker_task.status = "in_progress"
    worker_task.running_pid = None
    store.update(worker_task)

    pid_only_task.status = "in_progress"
    pid_only_task.running_pid = 5252
    store.update(pid_only_task)

    terminal_task.status = "completed"
    terminal_task.running_pid = None
    store.update(terminal_task)

    config = Config.load(tmp_path)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=worker_task.id, pid=4242, status="running"),
        WorkerMetadata(worker_id="w-2", task_id=terminal_task.id, pid=4343, status="running"),
    ]
    registry.is_running.return_value = True

    with (
        patch("gza.concurrency.WorkerRegistry", return_value=registry),
        patch("gza.concurrency._pid_alive", side_effect=lambda pid: pid == 5252),
    ):
        live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)

    assert live_pids == {4242, 4343, 5252}
    assert running_task_ids == [worker_task.id, pid_only_task.id]
    assert anonymous_worker_count == 1


def test_collect_live_running_state_ignores_dead_terminal_task_worker(
    tmp_path: Path,
) -> None:
    """A dead terminal-task worker entry alone must not count as live or anonymous."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    terminal_task = store.add("Terminal task", task_type="plan")
    assert terminal_task.id is not None
    terminal_task.status = "completed"
    store.update(terminal_task)

    config = Config.load(tmp_path)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=terminal_task.id, pid=4343, status="running"),
    ]
    registry.is_running.return_value = False

    with patch("gza.concurrency.WorkerRegistry", return_value=registry):
        live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)

    assert live_pids == set()
    assert running_task_ids == []
    assert anonymous_worker_count == 0


def test_collect_live_running_state_counts_pending_task_with_live_worker(tmp_path: Path) -> None:
    """A spawned explicit worker for a still-pending task must consume a watch slot."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    pending_task = store.add("Pending worker-claimed soon", task_type="implement")
    assert pending_task.id is not None

    config = Config.load(tmp_path)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=pending_task.id, pid=4242, status="running"),
    ]
    registry.is_running.return_value = True

    with patch("gza.concurrency.WorkerRegistry", return_value=registry):
        live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)

    assert live_pids == {4242}
    assert running_task_ids == [pending_task.id]
    assert anonymous_worker_count == 0


def test_collect_live_running_state_counts_anonymous_live_worker(tmp_path: Path) -> None:
    """An anonymous live worker must still count and surface as anonymous."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    config = Config.load(tmp_path)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=None, pid=4242, status="running"),
    ]
    registry.is_running.return_value = True

    with patch("gza.concurrency.WorkerRegistry", return_value=registry):
        live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)

    assert live_pids == {4242}
    assert running_task_ids == []
    assert anonymous_worker_count == 1


def test_watch_cycle_leaves_no_slots_when_terminal_task_worker_is_still_alive(
    tmp_path: Path,
) -> None:
    """A live terminal-task worker must block new starts when batch=1."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    terminal_task = store.add("Recently failed implement", task_type="implement")
    pending_task = store.add("Pending implement", task_type="implement")
    assert terminal_task.id is not None
    assert pending_task.id is not None

    terminal_task.status = "failed"
    terminal_task.failure_reason = "TERMINATED"
    store.update(terminal_task)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=terminal_task.id, pid=4242, status="running"),
    ]
    registry.is_running.return_value = True

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.concurrency.WorkerRegistry", return_value=registry),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert result.running == 1
    assert spawn_iterate.call_count == 0


def test_format_wake_message_includes_running_task_ids() -> None:
    """WAKE line should append task IDs when tasks are actively running."""
    assert _format_wake_message(running=1, runnable_pending=3, blocked_pending=0, slots=0, running_task_ids=["gza-42"]) == (
        "checking... (1 running, pending=3 runnable, blocked=0, 0 slots)\n"
        "live workers:\n"
        "- gza-42"
    )
    assert _format_wake_message(running=0, runnable_pending=2, blocked_pending=0, slots=2, running_task_ids=[]) == (
        "checking... (0 running, pending=2 runnable, blocked=0, 2 slots)"
    )
    assert _format_wake_message(
        running=2,
        runnable_pending=3,
        blocked_pending=0,
        slots=0,
        running_task_ids=["gza-42"],
        anonymous_worker_count=1,
    ) == (
        "checking... (2 running, pending=3 runnable, blocked=0, 0 slots)\n"
        "live workers:\n"
        "- gza-42\n"
        "- 1 worker without an active task id"
    )


def test_watch_log_aligns_multiline_messages(tmp_path: Path) -> None:
    """Multiline events should indent continuation lines under the event prefix."""
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with patch("gza.cli.watch._format_hms", return_value="18:08:47"):
        log.emit("WAKE", "checking...\nlive workers:\n- gza-42")

    assert log_path.read_text() == (
        "18:08:47 WAKE      checking...\n"
        "                   live workers:\n"
        "                   - gza-42\n"
    )


def test_watch_log_stdout_themes_task_ids_but_file_stays_plain(tmp_path: Path) -> None:
    """watch.log should stay plain while stdout highlights task IDs via the shared theme."""
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=False)
    output = io.StringIO()
    themed_console = Console(file=output, force_terminal=True, color_system="truecolor", highlight=False)

    try:
        # Import-order regression guard: watch may already hold module globals before theme load.
        import gza.cli.watch as watch_module

        colors.set_theme(None, {"task_id": "bold red"})
        with (
            patch.object(watch_module, "console", themed_console),
            patch.object(watch_module, "_format_hms", return_value="18:08:47"),
        ):
            log.emit("START", "gza-4216 rebase")

        plain_log = log_path.read_text()
        stdout = output.getvalue()
        stripped_stdout = re.sub(r"\x1b\[[0-9;]*m", "", stdout)

        assert plain_log == "18:08:47 START     gza-4216 rebase\n"
        assert "\x1b[" not in plain_log
        assert stripped_stdout == plain_log
        assert "\x1b[1;31mgza-4216\x1b[0m" in stdout
    finally:
        colors.set_theme(None)


def test_watch_cycle_logs_tag_scoped_pending_count_in_wake_line(tmp_path: Path) -> None:
    """WAKE line should report runnable pending tasks using the selected tag filter."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    store.add("Release runnable", task_type="plan", group="release-1")
    release_blocker = store.add("Release blocker", task_type="plan", group="release-1")
    assert release_blocker.id is not None
    store.add("Release blocked", task_type="plan", group="release-1", depends_on=release_blocker.id)
    store.add("Backlog runnable", task_type="plan", group="backlog")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("release-1",),
        )

    assert "WAKE      checking... (0 running, pending=2 runnable, blocked=1, 1 slots)" in log_path.read_text()


def test_watch_cycle_logs_tag_scope_with_all_mode(tmp_path: Path) -> None:
    """Tag-scoped watch should log when all-tag matching is enabled."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    store.add("Release task", task_type="plan", tags=("release-1.2",))

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("Release-1.2", "backend"),
            any_tag=False,
        )

    assert "INFO      scope: tags=backend,release-1.2 mode=all" in log_path.read_text()


def test_watch_cycle_logs_tag_scope_with_any_mode(tmp_path: Path) -> None:
    """Tag-scoped watch should log when any-tag matching is enabled."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    store.add("Release task", task_type="plan", tags=("release-1.2",))

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("release-1.2", "backend"),
            any_tag=True,
        )

    assert "INFO      scope: tags=backend,release-1.2 mode=any" in log_path.read_text()


def test_watch_cycle_reports_out_of_scope_runnable_child_without_starting_it(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add(
        "Scoped recovery owner",
        task_type="plan",
        tags=("202606-recovery", "v0.5.0"),
        auto_implement=False,
    )
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(plan)

    child = store.add(
        "Out of scope implement child",
        task_type="implement",
        based_on=plan.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
        )

    log_text = log_path.read_text()
    assert "out-of-scope child" in log_text
    assert plan.id in log_text
    assert child.id in log_text
    assert f"START     {child.id}" not in log_text


def test_watch_cycle_reports_depends_on_only_out_of_scope_runnable_child_without_starting_it(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    explore = store.add(
        "Scoped recovery owner",
        task_type="explore",
        tags=("202606-recovery", "v0.5.0"),
    )
    assert explore.id is not None
    explore.status = "completed"
    explore.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(explore)

    child = store.add(
        "Out of scope implement child",
        task_type="implement",
        depends_on=explore.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
        )

    log_text = log_path.read_text()
    assert "out-of-scope child" in log_text
    assert explore.id in log_text
    assert child.id in log_text
    assert f"START     {child.id}" not in log_text


def test_watch_cycle_reports_blocked_out_of_scope_pending_child_without_starting_it(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    blocked_by = store.add("Blocking dependency")
    assert blocked_by.id is not None

    plan = store.add(
        "Scoped recovery owner",
        task_type="plan",
        tags=("202606-recovery", "v0.5.0"),
        auto_implement=False,
    )
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(plan)

    child = store.add(
        "Blocked out of scope implement child",
        task_type="implement",
        based_on=plan.id,
        depends_on=blocked_by.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
        )

    log_text = log_path.read_text()
    assert "out-of-scope child" in log_text
    assert plan.id in log_text
    assert child.id in log_text
    assert "blocked implement" in log_text
    assert f"START     {child.id}" not in log_text


def test_watch_cycle_reports_failed_owner_with_out_of_scope_resume_child_without_starting_it(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add(
        "Scoped failed implementation",
        task_type="implement",
        tags=("202606-recovery", "v0.5.0"),
    )
    assert failed.id is not None
    store.mark_failed(failed, "timed out")

    resume_child = store.add(
        "Pre-existing out of scope resume child",
        task_type="implement",
        based_on=failed.id,
        recovery_origin="resume",
        tags=("v0.5.0",),
    )
    assert resume_child.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
        )

    log_text = log_path.read_text()
    assert "out-of-scope child" in log_text
    assert failed.id in log_text
    assert resume_child.id in log_text
    assert f"START     {resume_child.id}" not in log_text


def test_watch_cycle_does_not_warn_when_depends_on_only_child_inherits_scope_tag(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    explore = store.add(
        "Scoped recovery owner",
        task_type="explore",
        tags=("202606-recovery", "v0.5.0"),
    )
    assert explore.id is not None
    explore.status = "completed"
    explore.completed_at = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    store.update(explore)

    child = store.add(
        "Inherited scope implement child",
        task_type="implement",
        depends_on=explore.id,
        tags=("202606-recovery", "v0.5.0"),
    )
    assert child.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
        )

    log_text = log_path.read_text()
    assert "out-of-scope child" not in log_text
    assert f"START     {child.id}" in log_text


def test_watch_cycle_does_not_warn_when_runnable_scoped_owner_has_out_of_scope_dependent_child(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add(
        "Scoped runnable owner",
        task_type="implement",
        tags=("202606-recovery",),
    )
    assert owner.id is not None

    child = store.add(
        "Out of scope dependent child",
        task_type="review",
        depends_on=owner.id,
        tags=("v0.5.0",),
    )
    assert child.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
        )

    log_text = log_path.read_text()
    assert "out-of-scope child" not in log_text
    assert f"START     {owner.id}" in log_text
    assert child.id not in log_text


def test_watch_cycle_keeps_free_slot_when_iterate_child_task_shares_pid(tmp_path: Path) -> None:
    """batch=2 should still schedule one task when one iterate process is active."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    review.status = "in_progress"
    review.running_pid = 7777
    store.update(review)

    plan = store.add("Plan follow-up", task_type="plan")
    assert plan.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    registry = MagicMock()
    registry.list_all.return_value = [
        WorkerMetadata(worker_id="w-1", task_id=impl.id, pid=7777, status="running"),
    ]
    registry.is_running.return_value = True

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.concurrency.WorkerRegistry", return_value=registry),
        patch("gza.concurrency._pid_alive", return_value=True),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == plan.id


def test_watch_cycle_skips_merge_off_default_branch(tmp_path: Path, capsys) -> None:
    """Watch merge path must enforce the same default-branch guard as merge."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-merge"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "feature/local"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    output = capsys.readouterr().out
    assert "`gza merge` must be run from the default branch 'main'" in output
    execute_merge.assert_not_called()
    assert " MERGE " not in log_path.read_text()


def test_watch_cycle_uses_default_branch_for_advance_planning_off_default_branch(tmp_path: Path) -> None:
    """Advance planning in watch should target default branch even when run elsewhere."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-plan-target"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "feature/local"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "skip"}) as determine_action,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert determine_action.call_count == 1
    assert determine_action.call_args.args[4] == "main"


def test_watch_cycle_with_isolation_enabled_preflights_and_merges_in_isolated_checkout(tmp_path: Path) -> None:
    """Isolation mode should preflight checkout and route merge execution through that checkout."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-merge"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", return_value=isolated_git) as ensure_isolated,
        patch("gza.cli.watch._require_default_branch") as require_default_branch,
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=lambda *_args, **_kwargs: (
                store.set_merge_status(task.id, "merged"),
                SimpleNamespace(rc=0, created_followups=[], reused_followups=[]),
            )[1],
        ) as execute_merge,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    ensure_isolated.assert_called_once_with(config, repo_git, "main")
    require_default_branch.assert_not_called()
    assert execute_merge.call_count == 1
    assert execute_merge.call_args.kwargs["merge_git"] is isolated_git
    assert execute_merge.call_args.kwargs["merge_current_branch"] == "main"


def test_watch_cycle_with_isolation_enabled_rebuilds_checkout_after_preflight_failure_and_merges(
    tmp_path: Path,
) -> None:
    """A stale isolated checkout at watch-pass start should rebuild once and still allow same-pass merges."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-preflight-rebuild"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    rebuilt_git = MagicMock()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch(
            "gza.cli.watch.ensure_watch_main_checkout",
            side_effect=[GitError("stale checkout"), rebuilt_git],
        ) as ensure_isolated,
        patch("gza.cli.watch._require_default_branch") as require_default_branch,
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=lambda *_args, **_kwargs: (
                store.set_merge_status(task.id, "merged"),
                SimpleNamespace(rc=0, created_followups=[], reused_followups=[]),
            )[1],
        ) as execute_merge,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert ensure_isolated.call_count == 2
    assert ensure_isolated.call_args_list[0].args == (config, repo_git, "main")
    assert ensure_isolated.call_args_list[1].args == (config, repo_git, "main")
    assert ensure_isolated.call_args_list[1].kwargs["rebuild"] is True
    require_default_branch.assert_not_called()
    assert execute_merge.call_count == 1
    assert execute_merge.call_args.kwargs["merge_git"] is rebuilt_git
    assert execute_merge.call_args.kwargs["merge_current_branch"] == "main"
    assert execute_merge.call_args.kwargs["quiet_mechanics"] is True
    log_text = log_path.read_text()
    assert "isolated merge checkout refresh failed; rebuilding: stale checkout" in log_text
    assert "isolated merge checkout rebuilt" in log_text
    assert log_text.count(f"MERGE     {task.id} -> main") == 1


def test_watch_cycle_with_isolation_enabled_dry_run_does_not_mutate_checkout(tmp_path: Path) -> None:
    """Isolation dry-run should preview merges without reconciling isolated checkout."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-dry-run"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout") as ensure_isolated,
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    assert result.work_done is True
    ensure_isolated.assert_not_called()
    execute_merge.assert_not_called()
    assert f"MERGE     {task.id} -> main [dry-run]" in log_path.read_text()


def test_watch_cycle_dry_run_logs_only_merge_unit_owner(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Completed owner", task_type="implement")
    assert owner.id is not None
    review = store.add("Attached review", task_type="review", depends_on=owner.id)
    assert review.id is not None
    for task in (owner, review):
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/watch-dry-run-merge-unit"
        task.has_commits = True
        store.update(task)

    unit = store.create_merge_unit(
        source_branch="feature/watch-dry-run-merge-unit",
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    store.attach_task_to_merge_unit(review.id, unit.id, "review")
    store.dual_write_legacy_merge_status(unit.id)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    assert result.work_done is True
    execute_merge.assert_not_called()
    log_text = log_path.read_text()
    assert f"MERGE     {owner.id} -> main [dry-run]" in log_text
    assert f"MERGE     {review.id} -> main [dry-run]" not in log_text


def test_ensure_watch_main_checkout_detaches_existing_shared_default_branch_worktree(tmp_path: Path) -> None:
    """Isolation helper should not leave the integration worktree attached to the shared default-branch ref."""
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    checkout_path = config.main_checkout_integration_path

    attached_entry = {
        "path": str(checkout_path),
        "branch": "refs/heads/main",
        "detached": False,
        "prunable": False,
    }
    detached_entry = {
        "path": str(checkout_path),
        "branch": None,
        "detached": True,
        "prunable": False,
    }
    git = MagicMock()
    git.worktree_list.side_effect = [[attached_entry], [detached_entry]]

    workspace_git = MagicMock()
    workspace_git.current_branch.return_value = "HEAD"
    workspace_git.has_changes.return_value = False

    with patch("gza.cli.git_ops.Git", return_value=workspace_git) as git_cls:
        isolated_git = ensure_watch_main_checkout(config, git, "main")

    assert isolated_git is workspace_git
    git_cls.assert_called_once_with(checkout_path)
    git._run.assert_not_called()
    workspace_git.checkout_detached.assert_called_once_with("main")
    workspace_git.reset_hard.assert_called_once_with("main")
    workspace_git.clean_force.assert_called_once_with()
    workspace_git.current_branch.assert_called_once_with()
    workspace_git.has_changes.assert_called_once_with(include_untracked=True)


def test_execute_merge_action_with_followups_aborts_on_merge_source_warning_before_side_effects(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """merge_with_followups must fail closed on divergence before mutating task state."""
    from gza.cli.git_ops import _ResolvedMergeSubject
    from gza.review_verdict import ReviewFinding

    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/diverged"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: APPROVED_WITH_FOLLOWUPS**"
    store.update(review)

    finding = ReviewFinding(
        id="F1",
        severity="FOLLOWUP",
        title="Harden",
        body="",
        evidence=None,
        impact=None,
        fix_or_followup="add input guard",
        tests=None,
    )

    resolved = _ResolvedMergeSubject(
        trigger_task=impl,
        execution_task=impl,
        merge_subject=impl,
        merge_unit_id=None,
        merge_branch=impl.branch,
        merge_source_ref=f"origin/{impl.branch}",
        merge_source_warning=(
            f"Branch '{impl.branch}' diverged between local and origin "
            "(merge-source-needs-manual-resolution)"
        ),
    )

    git = MagicMock()

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops._create_or_reuse_followup_tasks") as create_followups,
        patch("gza.cli.git_ops._merge_single_task") as merge_single,
    ):
        result = _execute_merge_action(
            config,
            store,
            git,
            impl,
            {
                "type": "merge_with_followups",
                "review_task": review,
                "followup_findings": (finding,),
            },
            target_branch="main",
            current_branch="main",
        )

    assert result.rc == 1
    assert result.created_followups == []
    assert result.reused_followups == []
    create_followups.assert_not_called()
    merge_single.assert_not_called()
    output = capsys.readouterr().out
    assert "merge-source-needs-manual-resolution" in output

    refreshed = store.get(impl.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_isolated_watch_merge_promotion_rollback_keeps_task_unmerged_when_attached_checkout_reset_fails(
    tmp_path: Path,
) -> None:
    """Promotion rollback must restore refs and keep merge_status unmerged after attached checkout reset failure."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Promotion rollback regression", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-rollback"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse.return_value = "previous-main-oid"

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path
    merge_git.rev_parse.return_value = "isolated-merge-oid"

    attached_target_git = MagicMock()
    attached_target_git.has_changes.return_value = False
    attached_target_git.reset_hard.side_effect = [
        GitError("attached checkout reset failed"),
        None,
    ]

    with (
        patch("gza.cli.git_ops._merge_single_task", return_value=0),
        patch(
            "gza.cli.git_ops.active_worktree_path_for_branch",
            return_value=config.project_dir,
        ),
        patch("gza.cli.git_ops.Git", return_value=attached_target_git),
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 1
    assert result.created_followups == []
    assert result.reused_followups == []
    assert repo_git.update_ref.call_args_list == [
        call("refs/heads/main", "isolated-merge-oid", "previous-main-oid"),
        call("refs/heads/main", "previous-main-oid", "isolated-merge-oid"),
    ]
    assert attached_target_git.reset_hard.call_args_list == [
        call("refs/heads/main"),
        call("refs/heads/main"),
    ]
    merge_git.reset_hard.assert_called_once_with("refs/heads/main")
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "unmerged"


def test_execute_merge_action_reconciles_pending_squash_only_after_isolated_promotion(
    tmp_path: Path,
) -> None:
    """Deferred squash reconciliation must run after isolated promotion succeeds."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Isolated squash promotion success", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-squash-success"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=None,
        merge_branch=task.branch,
        merge_source_ref=f"origin/{task.branch}",
        merge_source_warning=None,
    )
    merge_result = _MergeSingleTaskResult(
        rc=0,
        pending_squash_reconcile=_PendingSquashBranchReconcile(
            branch=task.branch,
            pre_squash_local_oid="local-oid",
            pre_squash_remote_oid="remote-oid",
        ),
    )

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse.return_value = "promoted-target-oid"

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path

    promoted = False

    def fake_promote(*_args, **_kwargs):
        nonlocal promoted
        promoted = True

    reconcile_result = SimpleNamespace(status="updated")

    def fake_reconcile(_git, **kwargs):
        assert promoted is True
        assert kwargs["branch"] == task.branch
        assert kwargs["squash_oid"] == "promoted-target-oid"
        assert kwargs["pre_squash_local_oid"] == "local-oid"
        assert kwargs["pre_squash_remote_oid"] == "remote-oid"
        return reconcile_result

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", return_value=merge_result),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch", side_effect=fake_promote) as promote,
        patch(
            "gza.cli.git_ops._reconcile_squash_merged_branch_with_origin",
            side_effect=fake_reconcile,
        ) as reconcile,
        patch("gza.cli.git_ops._print_squash_reconcile_result") as print_reconcile,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 0
    promote.assert_called_once_with(repo_git, merge_git, "main")
    reconcile.assert_called_once()
    print_reconcile.assert_called_once_with(reconcile_result, suppress_success=False)
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "merged"


def test_execute_merge_action_skips_pending_squash_reconcile_when_isolated_promotion_fails(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deferred squash reconciliation must not run if isolated promotion fails."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Isolated squash promotion failure", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-squash-failure"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=None,
        merge_branch=task.branch,
        merge_source_ref=f"origin/{task.branch}",
        merge_source_warning=None,
    )
    merge_result = _MergeSingleTaskResult(
        rc=0,
        pending_squash_reconcile=_PendingSquashBranchReconcile(
            branch=task.branch,
            pre_squash_local_oid="local-oid",
            pre_squash_remote_oid="remote-oid",
        ),
    )

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace()),
        patch("gza.cli.git_ops._merge_single_task", return_value=merge_result),
        patch(
            "gza.cli.git_ops._promote_isolated_merge_to_target_branch",
            side_effect=GitError("promotion failed"),
        ) as promote,
        patch("gza.cli.git_ops._reconcile_squash_merged_branch_with_origin") as reconcile,
        patch("gza.cli.git_ops._print_squash_reconcile_result") as print_reconcile,
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 1
    promote.assert_called_once_with(repo_git, merge_git, "main")
    reconcile.assert_not_called()
    print_reconcile.assert_not_called()
    assert "Error finalizing isolated merge success: promotion failed" in capsys.readouterr().out
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "unmerged"


def test_execute_merge_action_isolated_squash_uses_real_checkout_pre_squash_refs(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Isolated squash uses real checkout refs", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-real-refs"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=None,
        merge_branch=task.branch,
        merge_source_ref=f"origin/{task.branch}",
        merge_source_warning=None,
    )
    merge_result = _MergeSingleTaskResult(
        rc=0,
        pending_squash_reconcile=_PendingSquashBranchReconcile(
            branch=task.branch,
            pre_squash_local_oid="isolated-local-oid",
            pre_squash_remote_oid="isolated-remote-oid",
        ),
    )

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse.return_value = "promoted-target-oid"
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        f"refs/heads/{task.branch}": "real-local-oid",
        f"refs/remotes/origin/{task.branch}": "real-remote-oid",
    }.get(ref)

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path

    def fake_reconcile(_git, **kwargs):
        assert kwargs["branch"] == task.branch
        assert kwargs["squash_oid"] == "promoted-target-oid"
        assert kwargs["pre_squash_local_oid"] == "real-local-oid"
        assert kwargs["pre_squash_remote_oid"] == "real-remote-oid"
        return SimpleNamespace(status="updated")

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace(squash=True)),
        patch("gza.cli.git_ops._merge_single_task", return_value=merge_result),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch"),
        patch(
            "gza.cli.git_ops._reconcile_squash_merged_branch_with_origin",
            side_effect=fake_reconcile,
        ) as reconcile,
        patch("gza.cli.git_ops._print_squash_reconcile_result"),
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 0
    reconcile.assert_called_once()


def test_execute_merge_action_isolated_squash_uses_real_checkout_missing_remote_tracking_ref(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    task = store.add("Isolated squash preserves real missing remote ref", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-no-real-remote"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    resolved = _ResolvedMergeSubject(
        trigger_task=task,
        execution_task=task,
        merge_subject=task,
        merge_unit_id=None,
        merge_branch=task.branch,
        merge_source_ref=f"origin/{task.branch}",
        merge_source_warning=None,
    )
    merge_result = _MergeSingleTaskResult(
        rc=0,
        pending_squash_reconcile=_PendingSquashBranchReconcile(
            branch=task.branch,
            pre_squash_local_oid="isolated-local-oid",
            pre_squash_remote_oid="isolated-stale-remote-oid",
        ),
    )

    repo_git = MagicMock()
    repo_git.repo_dir = tmp_path
    repo_git.rev_parse.return_value = "promoted-target-oid"
    repo_git.rev_parse_if_exists.side_effect = lambda ref: {
        f"refs/heads/{task.branch}": "real-local-oid",
        f"refs/remotes/origin/{task.branch}": None,
    }.get(ref)

    merge_git = MagicMock()
    merge_git.repo_dir = config.main_checkout_integration_path

    def fake_reconcile(_git, **kwargs):
        assert kwargs["pre_squash_local_oid"] == "real-local-oid"
        assert kwargs["pre_squash_remote_oid"] is None
        return SimpleNamespace(status="skipped_no_remote_tracking_ref")

    with (
        patch("gza.cli.git_ops._resolve_merge_subject", return_value=resolved),
        patch("gza.cli.git_ops._build_auto_merge_args", return_value=argparse.Namespace(squash=True)),
        patch("gza.cli.git_ops._merge_single_task", return_value=merge_result),
        patch("gza.cli.git_ops._promote_isolated_merge_to_target_branch"),
        patch(
            "gza.cli.git_ops._reconcile_squash_merged_branch_with_origin",
            side_effect=fake_reconcile,
        ) as reconcile,
        patch("gza.cli.git_ops._print_squash_reconcile_result"),
    ):
        result = _execute_merge_action(
            config,
            store,
            repo_git,
            task,
            {"type": "merge"},
            target_branch="main",
            current_branch="main",
            merge_git=merge_git,
            merge_current_branch="main",
        )

    assert result.rc == 0
    reconcile.assert_called_once()


def test_watch_cycle_with_isolation_enabled_rebuilds_after_cleanup_failure_and_continues_merging(tmp_path: Path) -> None:
    """Cleanup failures in isolated mode should rebuild checkout and continue later merges in the same pass."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task_a = store.add("Task A", task_type="implement")
    task_b = store.add("Task B", task_type="implement")
    assert task_a.id is not None
    assert task_b.id is not None
    for task, branch in ((task_a, "feature/watch-isolated-a"), (task_b, "feature/watch-isolated-b")):
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        store.update(task)
        store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()
    rebuilt_git = MagicMock()
    isolated_git.branch_exists.return_value = True
    isolated_git.is_merged.return_value = False
    isolated_git.can_merge.side_effect = [False]

    rebase_task = SimpleNamespace(id="gza-rebase-1")

    def choose_action(_cfg, _store, _git, task, _target, *, impl_based_on_ids, **_kwargs):  # noqa: ARG001
        if task.id == task_a.id:
            return {"type": "merge"}
        if task.id == task_b.id:
            return {"type": "merge"}
        return {"type": "skip"}

    def merge_side_effect(*_args, **_kwargs):
        if merge_side_effect.calls == 0:
            merge_side_effect.calls += 1
            return SimpleNamespace(rc=1, created_followups=[], reused_followups=[])
        merge_side_effect.calls += 1
        store.set_merge_status(task_b.id, "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    merge_side_effect.calls = 0  # type: ignore[attr-defined]

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", side_effect=[isolated_git, rebuilt_git]) as ensure_isolated,
        patch("gza.cli.determine_next_action", side_effect=choose_action),
        patch("gza.cli.watch._execute_merge_action", side_effect=merge_side_effect) as execute_merge,
        patch("gza.cli.watch.cleanup_failed_merge_checkout", side_effect=GitError("cleanup failed")),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._create_rebase_task", return_value=rebase_task),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert ensure_isolated.call_count == 2
    assert ensure_isolated.call_args_list[1].kwargs["rebuild"] is True
    assert execute_merge.call_count == 2
    assert execute_merge.call_args_list[0].kwargs["merge_git"] is isolated_git
    assert execute_merge.call_args_list[1].kwargs["merge_git"] is rebuilt_git
    assert spawn_worker.call_count == 1
    log_text = log_path.read_text()
    assert "isolated merge checkout rebuilt" in log_text
    assert "merge conflict routed to rebase" in log_text
    assert log_text.count(" MERGE ") == 1


def test_watch_cycle_with_isolation_enabled_merge_conflict_preparation_failure_rolls_back_rebase(
    tmp_path: Path,
) -> None:
    """Isolated conflict rebases must prepare in the watch parent and roll back on failure."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-prep-failure"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()
    isolated_git.branch_exists.return_value = True
    isolated_git.is_merged.return_value = False
    isolated_git.can_merge.return_value = False

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", return_value=isolated_git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            return_value=SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
        ),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.watch._spawn_background_worker",
            side_effect=AssertionError("rebase worker should not spawn"),
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert store.get_based_on_children(task.id) == []
    log_text = log_path.read_text()
    assert "failed to prepare merge-conflict rebase task" in log_text
    assert "merge conflict routed to rebase" not in log_text
    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert not any(path.is_file() for path in logs_dir.rglob("*"))
    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_watch_cycle_with_isolation_enabled_rebuild_failure_skips_later_merges_but_runs_other_actions(
    tmp_path: Path,
) -> None:
    """When rebuild fails, later merge actions should skip while non-merge actions still proceed."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    merge_task = store.add("Merge task", task_type="implement")
    plan_task = store.add("Plan task", task_type="plan")
    assert merge_task.id is not None
    assert plan_task.id is not None
    merge_task.status = "completed"
    merge_task.completed_at = datetime.now(UTC)
    merge_task.branch = "feature/watch-isolated-rebuild-fail"
    store.update(merge_task)
    store.set_merge_status(merge_task.id, "unmerged")

    plan_task.status = "completed"
    plan_task.completed_at = datetime.now(UTC)
    store.update(plan_task)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()
    isolated_git.branch_exists.return_value = True
    isolated_git.is_merged.return_value = False
    isolated_git.can_merge.side_effect = [False]

    def choose_action(_cfg, _store, _git, task, _target, *, impl_based_on_ids, **_kwargs):  # noqa: ARG001
        if task.id == merge_task.id:
            return {"type": "merge"}
        if task.id == plan_task.id:
            return {"type": "create_implement"}
        return {"type": "skip"}

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch(
            "gza.cli.watch.ensure_watch_main_checkout",
            side_effect=[isolated_git, GitError("rebuild failed")],
        ),
        patch("gza.cli.determine_next_action", side_effect=choose_action),
        patch(
            "gza.cli.watch._execute_merge_action",
            return_value=SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
        ),
        patch("gza.cli.watch.cleanup_failed_merge_checkout", side_effect=GitError("cleanup failed")),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._create_rebase_task", return_value=SimpleNamespace(id="gza-rebase-fail")),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    log_text = log_path.read_text()
    assert "isolated merge checkout rebuild failed" in log_text
    assert "START" in log_text


def test_watch_cycle_with_isolation_enabled_missing_branch_failure_does_not_route_to_rebase(
    tmp_path: Path,
) -> None:
    """Missing task branches must not be misclassified as isolated merge conflicts."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-missing-branch"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()
    isolated_git.branch_exists.return_value = False

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", return_value=isolated_git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            return_value=SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
        ),
        patch("gza.cli.watch._create_rebase_task") as create_rebase,
        patch("gza.cli.watch.cleanup_failed_merge_checkout") as cleanup_checkout,
        patch("gza.cli.watch._spawn_background_worker") as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    create_rebase.assert_not_called()
    cleanup_checkout.assert_not_called()
    spawn_worker.assert_not_called()
    isolated_git.is_merged.assert_not_called()
    isolated_git.can_merge.assert_not_called()
    log_text = log_path.read_text()
    assert "merge conflict routed to rebase" not in log_text
    assert f"{task.id}: merge failed (branch missing); not routing to rebase" in log_text


def test_watch_cycle_with_isolation_enabled_already_merged_failure_does_not_route_to_rebase(
    tmp_path: Path,
) -> None:
    """Already-merged branches should repair canonical merge state instead of creating rebase work."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-already-merged"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()
    isolated_git.branch_exists.return_value = True
    isolated_git.is_merged.return_value = True
    repaired_result = SimpleNamespace(ok=True, merge_status="merged")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", return_value=isolated_git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            return_value=SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
        ),
        patch("gza.cli.watch.reconcile_task_branch_merge_truth", return_value=repaired_result) as reconcile_branch,
        patch("gza.cli.watch._create_rebase_task") as create_rebase,
        patch("gza.cli.watch.cleanup_failed_merge_checkout") as cleanup_checkout,
        patch("gza.cli.watch._spawn_background_worker") as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    reconcile_branch.assert_called_once_with(
        store,
        repo_git,
        str(task.id),
        target_branch="main",
        include_diff_stats=True,
        persist=True,
    )
    create_rebase.assert_not_called()
    cleanup_checkout.assert_not_called()
    spawn_worker.assert_not_called()
    isolated_git.can_merge.assert_not_called()
    log_text = log_path.read_text()
    assert "merge conflict routed to rebase" not in log_text
    assert f"{task.id}: marked merged after shared reconciliation against main" in log_text


def test_watch_cycle_repairs_owner_unit_from_target_already_merged_same_branch_followup(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Watch implement owner", task_type="implement")
    store.mark_completed(owner, has_commits=True, branch="feature/watch-target-already-merged")
    assert owner.id is not None
    store.set_merge_status(owner.id, "unmerged")

    follow_up = store.add(
        "Watch improve descendant",
        task_type="improve",
        based_on=owner.id,
        same_branch=True,
    )
    store.mark_completed(follow_up, has_commits=True, branch="feature/watch-target-already-merged")
    assert follow_up.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
        ref_exists=MagicMock(
            side_effect=lambda ref: ref == "origin/feature/watch-target-already-merged"
        ),
        is_merged=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: {
                "origin/feature/watch-target-already-merged": "head-watch-target-already-merged",
                "main": "base-watch-target-already-merged",
            }.get(ref)
        ),
        count_commits_ahead_checked=MagicMock(return_value=1),
        get_diff_name_status=MagicMock(return_value=""),
    )
    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch(
            "gza.cli.watch.check_main_integration_verify",
            return_value=SimpleNamespace(merges_halted=False),
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    owner_unit = store.resolve_merge_unit_for_task(owner.id)
    follow_up_unit = store.resolve_merge_unit_for_task(follow_up.id)
    assert owner_unit is not None
    assert follow_up_unit is not None
    assert owner_unit.id == follow_up_unit.id
    assert owner_unit.state == "merged"
    assert owner_unit.merged_by_task_id == owner.id
    refreshed_owner = store.get(owner.id)
    refreshed_follow_up = store.get(follow_up.id)
    assert refreshed_owner is not None
    assert refreshed_follow_up is not None
    assert refreshed_owner.merge_status == "merged"
    assert refreshed_follow_up.merge_status is None
    assert repo_git.ref_exists.call_args_list
    repo_git.is_merged.assert_called_once_with(
        "origin/feature/watch-target-already-merged",
        "main",
    )
    log_text = log_path.read_text()
    assert f"{owner.id}: already merged into target branch" in log_text


def test_watch_target_already_merged_skip_repairs_owner_unit_via_shared_reconcile(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Watch implement owner", task_type="implement")
    store.mark_completed(owner, has_commits=True, branch="feature/watch-target-already-merged")
    assert owner.id is not None
    store.set_merge_status(owner.id, "unmerged")

    follow_up = store.add(
        "Watch improve descendant",
        task_type="improve",
        based_on=owner.id,
        same_branch=True,
    )
    store.mark_completed(follow_up, has_commits=True, branch="feature/watch-target-already-merged")
    assert follow_up.id is not None

    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    repo_git = SimpleNamespace(
        branch_exists=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(
            side_effect=lambda ref: {
                "feature/watch-target-already-merged": "head-watch-target-already-merged",
                "main": "base-watch-target-already-merged",
            }.get(ref)
        ),
        count_commits_ahead_checked=MagicMock(return_value=1),
    )
    skip_action = {
        "type": "skip",
        "description": "SKIP: target implementation already merged (merge-unit-merged)",
        "advance_reason": "target-already-merged",
    }

    repaired = _maybe_repair_target_already_merged_skip(
        store=store,
        git=repo_git,
        task=owner,
        display_task=owner,
        action=skip_action,
        target_branch="main",
        dry_run=False,
        log=log,
    )

    assert repaired is True
    owner_unit = store.resolve_merge_unit_for_task(owner.id)
    follow_up_unit = store.resolve_merge_unit_for_task(follow_up.id)
    assert owner_unit is not None
    assert follow_up_unit is not None
    assert owner_unit.id == follow_up_unit.id
    assert owner_unit.state == "merged"
    assert owner_unit.merged_by_task_id == owner.id
    assert owner_unit.head_sha == "head-watch-target-already-merged"
    assert owner_unit.base_sha == "base-watch-target-already-merged"
    refreshed_owner = store.get(owner.id)
    refreshed_follow_up = store.get(follow_up.id)
    assert refreshed_owner is not None
    assert refreshed_follow_up is not None
    assert refreshed_owner.merge_status == "merged"
    assert refreshed_follow_up.merge_status is None
    repo_git.branch_exists.assert_called_once_with("feature/watch-target-already-merged")
    repo_git.is_merged.assert_called_once_with("feature/watch-target-already-merged", into="main")
    log_text = log_path.read_text()
    assert f"{owner.id}: marked merged after shared reconciliation against main" in log_text


def test_watch_cycle_repairs_owner_unit_from_target_already_merged_same_branch_skip_during_runtime(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Watch implement owner", task_type="implement")
    store.mark_completed(owner, has_commits=True, branch="feature/watch-target-already-merged")
    assert owner.id is not None
    store.set_merge_status(owner.id, "unmerged")

    follow_up = store.add(
        "Watch improve descendant",
        task_type="improve",
        based_on=owner.id,
        same_branch=True,
    )
    store.mark_completed(follow_up, has_commits=True, branch="feature/watch-target-already-merged")
    assert follow_up.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = _make_watch_git()
    repo_git.current_branch.return_value = "main"  # type: ignore[attr-defined]
    repo_git.default_branch.return_value = "main"  # type: ignore[attr-defined]
    repo_git.branch_exists.return_value = True  # type: ignore[attr-defined]
    repo_git.is_merged = MagicMock(side_effect=[False, True])  # type: ignore[method-assign]
    repo_git.rev_parse_if_exists = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda ref: {
            "feature/watch-target-already-merged": "head-watch-target-already-merged",
            "main": "base-watch-target-already-merged",
        }.get(ref)
    )
    skip_action = {
        "type": "skip",
        "description": "SKIP: target implementation already merged (merge-unit-merged)",
        "advance_reason": "target-already-merged",
    }
    original_query_owner_rows = _query_owner_rows_with_context

    def _force_runtime_target_already_merged_action(*args, **kwargs):
        rows, read_context = original_query_owner_rows(*args, **kwargs)
        return tuple(replace(row, next_action=None) for row in rows), read_context

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch(
            "gza.cli.watch._query_owner_rows_with_context",
            side_effect=_force_runtime_target_already_merged_action,
        ),
        patch("gza.cli.watch.determine_next_action", return_value=skip_action),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    owner_unit = store.resolve_merge_unit_for_task(owner.id)
    follow_up_unit = store.resolve_merge_unit_for_task(follow_up.id)
    assert owner_unit is not None
    assert follow_up_unit is not None
    assert owner_unit.id == follow_up_unit.id
    assert owner_unit.state == "merged"
    assert owner_unit.merged_by_task_id == owner.id
    assert owner_unit.head_sha == "head-watch-target-already-merged"
    assert owner_unit.base_sha == "base-watch-target-already-merged"
    repo_git.is_merged.assert_any_call(  # type: ignore[attr-defined]
        "feature/watch-target-already-merged",
        into="main",
    )
    log_text = log_path.read_text()
    assert f"{owner.id}: marked merged after shared reconciliation against main" in log_text


def test_watch_cycle_logs_already_merged_reconciliation_as_info(tmp_path: Path) -> None:
    """Watch should log stale merge-state reconciliation as expected info, not a merge failure."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-stale-merged"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=lambda *_args, **_kwargs: (
                store.set_merge_status(task.id, "merged"),
                SimpleNamespace(
                    rc=0,
                    status="already_merged",
                    created_followups=[],
                    reused_followups=[],
                ),
            )[1],
        ) as execute_merge,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    execute_merge.assert_called_once()
    log_text = log_path.read_text()
    assert f"INFO      {task.id} already merged into main; marked merged" not in log_text
    assert log_text.count(f"MERGE     {task.id} -> main") == 1


def test_watch_cycle_without_isolation_preserves_default_branch_merge_guard(tmp_path: Path) -> None:
    """Isolation disabled should preserve legacy default-branch guard behavior."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-no-isolation-guard"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout") as ensure_isolated,
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    ensure_isolated.assert_not_called()
    execute_merge.assert_not_called()
    assert "merge actions skipped: not on default branch" in log_path.read_text()


def test_watch_cycle_uses_auto_squash_merge_args_from_shared_logic(tmp_path: Path) -> None:
    """Watch merge execution should honor merge_squash_threshold auto-squash."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "merge_squash_threshold: 2\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-squash"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.count_commits_ahead.return_value = 3
    source_ref = "origin/feature/watch-squash"
    captured: dict[str, object] = {}

    def fake_execute_merge_action(
        config_arg,
        store_arg,
        git_arg,
        task_arg,
        action_arg,
        *,
        target_branch,
        current_branch,
        **_kwargs,
    ):
        del store_arg, action_arg, current_branch
        from types import SimpleNamespace

        from gza.cli.git_ops import _build_auto_merge_args

        assert task_arg.branch == "feature/watch-squash"
        captured["merge_args"] = _build_auto_merge_args(config_arg, git_arg, source_ref, target_branch)
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
    ):
        from gza.cli.git_ops import _build_auto_merge_args

        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )
        watch_merge_args = captured["merge_args"]
        advance_merge_args = _build_auto_merge_args(config, git, source_ref, "main")

    assert watch_merge_args.squash is True
    assert advance_merge_args.squash is True
    git.count_commits_ahead.assert_called_with(source_ref, "main")


def test_watch_cycle_logs_inline_merge_before_same_cycle_start(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    merged_task = store.add("Completed task", task_type="implement")
    assert merged_task.id is not None
    merged_task.status = "completed"
    merged_task.completed_at = datetime.now(UTC)
    merged_task.branch = "feature/watch-inline-merge-order"
    store.update(merged_task)
    store.set_merge_status(merged_task.id, "unmerged")

    pending_review = store.add("Pending review", task_type="review")
    assert pending_review.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    def choose_action(_cfg, _store, _git, task, _target, *, impl_based_on_ids, **_kwargs):  # noqa: ARG001
        if task.id == merged_task.id:
            return {"type": "merge"}
        return {"type": "skip"}

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", side_effect=choose_action),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=lambda *_args, **_kwargs: (
                store.set_merge_status(merged_task.id, "merged"),
                SimpleNamespace(rc=0, created_followups=[], reused_followups=[]),
            )[1],
        ),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    lines = log_path.read_text().splitlines()
    merge_index = next(i for i, line in enumerate(lines) if f"MERGE     {merged_task.id} -> main" in line)
    start_index = next(i for i, line in enumerate(lines) if f"START     {pending_review.id} review" in line)
    assert merge_index < start_index


def test_watch_cycle_non_quiet_passes_quiet_mechanics_and_keeps_structured_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-non-quiet-merge"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=False)
    git = _make_watch_git()

    def fake_execute_merge_action(*_args, **kwargs):
        assert kwargs["quiet_mechanics"] is True
        store.set_merge_status(task.id, "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert f"MERGE     {task.id} -> main" in log_path.read_text()


def test_watch_cycle_quiet_suppresses_merge_stdout_and_logs_merge_event(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Quiet merge path should not print helper output and must emit MERGE log event."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-quiet-merge"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    def noisy_merge(*_args, **_kwargs):
        print("Merging 'feature/watch-quiet-merge' into 'main'...")
        print("✓ Successfully merged feature/watch-quiet-merge")
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=lambda *_args, **_kwargs: (
                store.set_merge_status(task.id, "merged"),
                SimpleNamespace(
                    rc=noisy_merge(),
                    created_followups=[],
                    reused_followups=[],
                ),
            )[1],
        ),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    stdout = capsys.readouterr().out
    assert "Merging 'feature/watch-quiet-merge' into 'main'..." not in stdout
    assert log_path.read_text().count(f"MERGE     {task.id} -> main") == 1


def test_watch_cycle_non_quiet_keeps_reconcile_failure_warning(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-warning-merge"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=False)
    git = _make_watch_git()

    def fake_execute_merge_action(*_args, **kwargs):
        assert kwargs["quiet_mechanics"] is True
        print(
            "Warning: Squash merge landed, but origin/feature/watch-warning-merge could not be reconciled: "
            "git push failed: stale info"
        )
        print(
            "origin/feature/watch-warning-merge changed since it was last observed; "
            "reconcile it manually before relying on watch."
        )
        store.set_merge_status(task.id, "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    stdout = capsys.readouterr().out
    assert "could not be reconciled: git push failed: stale info" in stdout
    assert "reconcile it manually before relying on watch." in stdout


def test_watch_cycle_dirty_checkout_blocks_merge_pass_and_stops_later_merges(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    first = store.add("First completed task", task_type="implement")
    second = store.add("Second completed task", task_type="implement")
    for task, branch in ((first, "feature/watch-dirty-1"), (second, "feature/watch-dirty-2")):
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        store.update(task)
        store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    calls: list[str] = []

    def fake_execute_merge_action(*args, **kwargs):
        task = args[3]
        calls.append(task.id)
        return SimpleNamespace(
            rc=1,
            status="blocked_dirty_checkout",
            block_reason="main checkout has uncommitted changes",
            created_followups=[],
            reused_followups=[],
        )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    assert result.work_done is False
    assert len(calls) == 1
    assert calls[0] in {first.id, second.id}
    assert "ATTENTION merges blocked: main checkout has uncommitted changes - commit or stash them first" in (
        log_path.read_text()
    )


def test_watch_cycle_red_main_after_merge_halts_later_merges_and_emits_single_attention(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    first = store.add("First completed task", task_type="implement")
    second = store.add("Second completed task", task_type="implement")
    for task, branch in ((first, "feature/watch-main-red-1"), (second, "feature/watch-main-red-2")):
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.has_commits = True
        store.update(task)
        store.set_merge_status(task.id, "unmerged")

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    merge_calls: list[str] = []

    def fake_execute_merge_action(*args, **kwargs):
        task = args[3]
        merge_calls.append(task.id)
        store.set_merge_status(task.id, "merged")
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

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
        patch("gza.cli.watch.check_main_integration_verify", side_effect=[green, red]),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert len(merge_calls) == 1
    skipped_task_id = second.id if merge_calls[0] == first.id else first.id
    log_text = log_path.read_text()
    assert "main verify RED at `deadbeefcafe` - merges halted; phase `unit` failing" in log_text
    assert "Needs attention (1 task):" in log_text
    assert f"SKIP      {skipped_task_id}: merges halted while local main verify is red" in log_text


def test_watch_cycle_green_main_after_merge_keeps_later_merges(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    first = store.add("First completed task", task_type="implement")
    second = store.add("Second completed task", task_type="implement")
    for task, branch in ((first, "feature/watch-main-green-1"), (second, "feature/watch-main-green-2")):
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.has_commits = True
        store.update(task)
        store.set_merge_status(task.id, "unmerged")

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    merge_calls: list[str] = []

    def fake_execute_merge_action(*args, **kwargs):
        task = args[3]
        merge_calls.append(task.id)
        store.set_merge_status(task.id, "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    green = SimpleNamespace(
        merges_halted=False,
        state=SimpleNamespace(task=main_verify_task, alert_message=None),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
        patch("gza.cli.watch.check_main_integration_verify", side_effect=[green, green, green]),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert set(merge_calls) == {first.id, second.id}


def test_watch_cycle_flaky_main_verify_files_one_deflake_task_and_keeps_merging(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-main-flaky"
    task.has_commits = True
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "deadbeefcafe"
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    merge_calls: list[str] = []

    def fake_execute_merge_action(*args, **kwargs):
        merge_task = args[3]
        merge_calls.append(str(merge_task.id))
        store.set_merge_status(str(merge_task.id), "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    flaky = SimpleNamespace(
        merges_halted=False,
        remediation=SimpleNamespace(
            kind="deflake",
            signature="phase:functional",
            tree_fingerprint="fp-functional-a",
            failing_phase="functional",
            failure="verify_command failed",
        ),
        state=SimpleNamespace(task=main_verify_task, head_sha="deadbeefcafe", alert_message=None),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=flaky),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )
        remediation_task = next(
            candidate
            for candidate in store.get_all()
            if candidate.trigger_source == "watch-main-integration-verify-remediation"
        )

    assert merge_calls == [task.id]
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 1
    remediation_task = remediation_tasks[0]
    assert remediation_task.status == "pending"
    assert remediation_task.urgent is True
    assert remediation_task.queue_position == 1
    assert set(remediation_task.tags or ()) == {"system", "202606-recovery"}
    assert "De-flake local main integration verify phase `functional`" in remediation_task.prompt
    assert "Failure signature: phase:functional" in remediation_task.prompt
    assert "Tree fingerprint: fp-functional-a" in remediation_task.prompt


def test_watch_cycle_reuses_failed_flaky_main_verify_remediation_as_pending_front_of_queue(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-main-flaky-reuse"
    task.has_commits = True
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    blocker = store.add("Scoped pending task", task_type="implement", tags=("202606-recovery",))
    assert blocker.id is not None
    assert set_task_queue_position_scoped(store, blocker.id, position=1, tags=("202606-recovery",))

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "deadbeefcafe"
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    merge_calls: list[str] = []

    def fake_execute_merge_action(*args, **kwargs):
        merge_task = args[3]
        merge_calls.append(str(merge_task.id))
        store.set_merge_status(str(merge_task.id), "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    flaky = SimpleNamespace(
        merges_halted=False,
        remediation=SimpleNamespace(
            kind="deflake",
            signature="phase:functional",
            tree_fingerprint="fp-functional-a",
            failing_phase="functional",
            failure="verify_command failed",
        ),
        state=SimpleNamespace(task=main_verify_task, head_sha="deadbeefcafe", alert_message=None),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=flaky),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )
        remediation_task = next(
            candidate
            for candidate in store.get_all()
            if candidate.trigger_source == "watch-main-integration-verify-remediation"
        )
        remediation_task.status = "failed"
        remediation_task.completed_at = datetime.now(UTC)
        remediation_task.failure_reason = "UNKNOWN"
        store.update(remediation_task)
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    assert merge_calls == [task.id]
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 1
    remediation_task = remediation_tasks[0]
    assert remediation_task.status == "pending"
    assert remediation_task.urgent is True
    assert remediation_task.queue_position == 1
    assert set(remediation_task.tags or ()) == {"system", "202606-recovery"}
    assert [candidate.id for candidate in store.get_pending_pickup(tags=("202606-recovery",), any_tag=False)] == [
        remediation_task.id,
        blocker.id,
    ]


def test_watch_cycle_deterministic_main_verify_halts_and_files_fix_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", tags=("202606-recovery",))
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-main-deterministic"
    task.has_commits = True
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint="fp-functional-a",
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 1
    remediation_task = remediation_tasks[0]
    assert remediation_task.urgent is True
    assert remediation_task.queue_position == 1
    assert "Fix local main integration verify phase `functional`" in remediation_task.prompt
    assert "Failure signature: phase:functional" in remediation_task.prompt
    assert "Tree fingerprint: fp-functional-a" in remediation_task.prompt
    assert "main verify RED at `feedfacecafe` - merges halted; phase `functional` failing" in log_path.read_text()


@pytest.mark.parametrize(
    ("existing_signature", "new_signature"),
    [
        pytest.param("phase:functional-long", "phase:functional", id="existing-has-prefix-of-new"),
        pytest.param("phase:functional", "phase:functional-long", id="new-has-prefix-of-existing"),
    ],
)
def test_watch_cycle_main_verify_remediation_dedup_matches_signature_exactly(
    tmp_path: Path,
    existing_signature: str,
    new_signature: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    existing_phase = existing_signature.removeprefix("phase:")
    remediation_task = store.add(
        "\n".join(
            [
                f"Fix local main integration verify phase `{existing_phase}`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                f"Failure signature: {existing_signature}",
                "Tree fingerprint: fp-existing",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("202606-recovery", "system"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert remediation_task.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    new_phase = new_signature.removeprefix("phase:")
    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature=new_signature,
            tree_fingerprint="fp-new",
            failing_phase=new_phase,
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message=f"main verify RED at `feedfacecafe` - merges halted; phase `{new_phase}` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "wait"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 2
    unchanged_existing = store.get(remediation_task.id)
    assert unchanged_existing is not None
    assert unchanged_existing.prompt == remediation_task.prompt
    assert sorted(
        _task.prompt.split("Failure signature: ", 1)[1].splitlines()[0]
        for _task in remediation_tasks
    ) == sorted([existing_signature, new_signature])


def test_watch_cycle_main_verify_remediation_dedup_requires_matching_tree_fingerprint(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    remediation_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: fp-old",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("202606-recovery", "system"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert remediation_task.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint="fp-new",
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "wait"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 2
    assert sorted(
        _task.prompt.split("Tree fingerprint: ", 1)[1].splitlines()[0]
        for _task in remediation_tasks
    ) == ["fp-new", "fp-old"]


def test_find_open_main_verify_remediation_task_does_not_fallback_unknown_for_concrete_fingerprint(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    unknown_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: unavailable",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert unknown_task.id is not None

    assert (
        _find_open_main_verify_remediation_task(
            store,
            signature="phase:functional",
            tree_fingerprint="fp-new",
        )
        is None
    )


def test_watch_cycle_main_verify_remediation_reuses_existing_concrete_task_when_current_fingerprint_unavailable(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    blocker = store.add("Scoped pending task", task_type="implement", tags=("202606-recovery",))
    assert blocker.id is not None
    assert set_task_queue_position_scoped(store, blocker.id, position=1, tags=("202606-recovery",))

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    remediation_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: fp-existing",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("legacy-recovery", "triaged"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert remediation_task.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint=None,
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "wait"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 1
    updated = remediation_tasks[0]
    assert updated.id == remediation_task.id
    assert updated.urgent is True
    assert updated.queue_position == 1
    assert set(updated.tags or ()) == {"system", "202606-recovery", "legacy-recovery", "triaged"}
    assert "Tree fingerprint: unavailable" in updated.prompt
    assert [task.id for task in store.get_pending_pickup(tags=("202606-recovery",), any_tag=False)] == [
        updated.id,
        blocker.id,
    ]


def test_watch_cycle_main_verify_remediation_keeps_unknown_task_and_creates_concrete_match(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    blocker = store.add("Scoped pending task", task_type="implement", tags=("202606-recovery",))
    assert blocker.id is not None
    assert set_task_queue_position_scoped(store, blocker.id, position=1, tags=("202606-recovery",))

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    unknown_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: unavailable",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("legacy-recovery", "system"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert unknown_task.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint="fp-new",
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "wait"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 2

    unchanged_unknown = store.get(unknown_task.id)
    assert unchanged_unknown is not None
    assert unchanged_unknown.prompt == unknown_task.prompt
    assert unchanged_unknown.queue_position != 1

    concrete_tasks = [task for task in remediation_tasks if task.id != unknown_task.id]
    assert len(concrete_tasks) == 1
    created = concrete_tasks[0]
    assert created.urgent is True
    assert created.queue_position == 1
    assert set(created.tags or ()) == {"system", "202606-recovery"}
    assert "Tree fingerprint: fp-new" in created.prompt
    assert [task.id for task in store.get_pending_pickup(tags=("202606-recovery",), any_tag=False)] == [
        created.id,
        blocker.id,
    ]


def test_watch_cycle_main_verify_remediation_prefers_exact_fingerprint_over_unknown_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    blocker = store.add("Scoped pending task", task_type="implement", tags=("202606-recovery",))
    assert blocker.id is not None
    assert set_task_queue_position_scoped(store, blocker.id, position=1, tags=("202606-recovery",))

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    unknown_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: unavailable",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("legacy-recovery", "system"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert unknown_task.id is not None

    exact_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: fp-new",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("exact-recovery", "system"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert exact_task.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint="fp-new",
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "wait"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 2

    unchanged_unknown = store.get(unknown_task.id)
    assert unchanged_unknown is not None
    assert unchanged_unknown.prompt == unknown_task.prompt
    assert unchanged_unknown.queue_position != 1

    updated_exact = store.get(exact_task.id)
    assert updated_exact is not None
    assert updated_exact.urgent is True
    assert updated_exact.queue_position == 1
    assert set(updated_exact.tags or ()) == {"system", "202606-recovery", "exact-recovery"}
    assert "Tree fingerprint: fp-new" in updated_exact.prompt
    assert [task.id for task in store.get_pending_pickup(tags=("202606-recovery",), any_tag=False)] == [
        updated_exact.id,
        blocker.id,
    ]


def test_watch_cycle_reuses_same_signature_remediation_task_but_updates_kind(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    remediation_task = store.add(
        "\n".join(
            [
                "De-flake local main integration verify phase `functional`",
                "",
                "The verify gate went red once, passed on rerun, and should be stabilized so watch does not keep rediscovering the flake.",
                "",
                "Remediation kind: deflake",
                "Failure signature: phase:functional",
                "Tree fingerprint: fp-functional-a",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("202606-recovery", "system"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert remediation_task.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint="fp-functional-a",
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 1
    updated = remediation_tasks[0]
    assert updated.id == remediation_task.id
    assert updated.urgent is True
    assert updated.queue_position == 1
    assert "Fix local main integration verify phase `functional`" in updated.prompt
    assert "Remediation kind: fix" in updated.prompt
    assert "Tree fingerprint: fp-functional-a" in updated.prompt
    assert "Verify failure: verify_command failed twice" in updated.prompt


def test_watch_cycle_reuses_failed_deterministic_main_verify_remediation_as_pending_front_of_queue(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    blocker = store.add("Scoped pending task", task_type="implement", tags=("202606-recovery",))
    assert blocker.id is not None
    assert set_task_queue_position_scoped(store, blocker.id, position=1, tags=("202606-recovery",))

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    remediation_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: fp-functional-a",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("legacy-recovery", "triaged"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert remediation_task.id is not None
    remediation_task.status = "failed"
    remediation_task.completed_at = datetime.now(UTC)
    remediation_task.failure_reason = "UNKNOWN"
    store.update(remediation_task)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint="fp-functional-a",
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "wait"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 1
    updated = remediation_tasks[0]
    assert updated.id == remediation_task.id
    assert updated.status == "pending"
    assert updated.urgent is True
    assert updated.queue_position == 1
    assert set(updated.tags or ()) == {"system", "202606-recovery", "legacy-recovery", "triaged"}
    assert [task.id for task in store.get_pending_pickup(tags=("202606-recovery",), any_tag=False)] == [
        updated.id,
        blocker.id,
    ]


def test_watch_cycle_reused_main_verify_remediation_inherits_active_scope_tags(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    blocker = store.add("Scoped pending task", task_type="implement", tags=("202606-recovery",))
    assert blocker.id is not None
    assert set_task_queue_position_scoped(store, blocker.id, position=1, tags=("202606-recovery",))

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_head_sha = "feedfacecafe"
    store.update(main_verify_task)

    remediation_task = store.add(
        "\n".join(
            [
                "Fix local main integration verify phase `functional`",
                "",
                "The verify gate stayed red across bounded reruns and is currently halting merges onto local main.",
                "",
                "Remediation kind: fix",
                "Failure signature: phase:functional",
                "Tree fingerprint: fp-functional-a",
                "Observed main HEAD: deadbeefcafe",
            ]
        ),
        task_type="implement",
        tags=("legacy-recovery", "triaged"),
        trigger_source="watch-main-integration-verify-remediation",
    )
    assert remediation_task.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = _make_watch_git()

    deterministic_red = SimpleNamespace(
        merges_halted=True,
        remediation=SimpleNamespace(
            kind="fix",
            signature="phase:functional",
            tree_fingerprint="fp-functional-a",
            failing_phase="functional",
            failure="verify_command failed twice",
        ),
        state=SimpleNamespace(
            task=main_verify_task,
            head_sha="feedfacecafe",
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `functional` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "wait"}),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch("gza.cli.watch.check_main_integration_verify", return_value=deterministic_red),
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("202606-recovery",),
            any_tag=False,
        )

    execute_merge.assert_not_called()
    remediation_tasks = [
        candidate
        for candidate in store.get_all()
        if candidate.trigger_source == "watch-main-integration-verify-remediation"
    ]
    assert len(remediation_tasks) == 1
    updated = remediation_tasks[0]
    assert updated.id == remediation_task.id
    assert updated.urgent is True
    assert set(updated.tags or ()) == {"system", "202606-recovery", "legacy-recovery", "triaged"}
    assert [task.id for task in store.get_pending_pickup(tags=("202606-recovery",), any_tag=False)] == [
        updated.id,
        blocker.id,
    ]


def test_watch_cycle_configured_unavailable_main_verify_halts_later_merges(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    first = store.add("First completed task", task_type="implement")
    second = store.add("Second completed task", task_type="implement")
    for task, branch in ((first, "feature/watch-main-unavailable-1"), (second, "feature/watch-main-unavailable-2")):
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.has_commits = True
        store.update(task)
        store.set_merge_status(task.id, "unmerged")

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    merge_calls: list[str] = []

    def fake_execute_merge_action(*args, **kwargs):
        task = args[3]
        merge_calls.append(task.id)
        store.set_merge_status(task.id, "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    green = SimpleNamespace(
        merges_halted=False,
        state=SimpleNamespace(task=main_verify_task, alert_message=None),
    )
    unavailable = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `deadbeefcafe` - merges halted; verify status `unavailable`",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
        patch("gza.cli.watch.check_main_integration_verify", side_effect=[green, unavailable]),
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert len(merge_calls) == 1
    skipped_task_id = second.id if merge_calls[0] == first.id else first.id
    log_text = log_path.read_text()
    assert "main verify RED at `deadbeefcafe` - merges halted; verify status `unavailable`" in log_text
    assert f"SKIP      {skipped_task_id}: merges halted while local main verify is red" in log_text


def test_watch_cycle_head_change_reverifies_main_and_surfaces_attention_without_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-main-head-change"
    task.has_commits = True
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    config.main_checkout_isolate = False
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `unit` failing",
        ),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.lineage_query.current_main_integration_verify_alert", return_value=None),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch.check_main_integration_verify", return_value=red) as check_main_verify,
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    execute_merge.assert_not_called()
    check_main_verify.assert_called_once()
    assert f"SKIP      {task.id}: merges halted while local main verify is red" in log_path.read_text()
    assert "main verify RED at `feedfacecafe` - merges halted; phase `unit` failing" in log_path.read_text()


def test_watch_cycle_idle_head_change_reverifies_main_and_surfaces_attention_row(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    main_verify_task = store.add("System alert: local main integration verify", task_type="internal", skip_learnings=True)
    assert main_verify_task.id is not None
    main_verify_task.status = "completed"
    main_verify_task.completed_at = datetime.now(UTC)
    main_verify_task.review_verify_command = "./bin/tests"
    main_verify_task.review_verify_status = "passed"
    main_verify_task.review_verify_exit_status = "0"
    main_verify_task.review_verify_head_sha = "deadbeefcafe"
    main_verify_task.output_content = json.dumps(
        {
            "alert_message": None,
            "captured_at": "2026-06-23T00:00:00+00:00",
            "failing_phase": None,
            "gate_enabled": True,
            "head_sha": "deadbeefcafe",
            "tree_fingerprint": "old-fingerprint",
            "verify_command": "./bin/tests",
            "verify_timeout_grace_seconds": 5.0,
            "verify_timeout_seconds": 120,
        },
        sort_keys=True,
    )
    store.update(main_verify_task)

    config = Config.load(tmp_path)
    config.verify_command = "./bin/tests"
    config.main_checkout_isolate = False
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()
    git.rev_parse_if_exists = MagicMock(return_value="feedfacecafe")  # type: ignore[method-assign]

    red = SimpleNamespace(
        merges_halted=True,
        state=SimpleNamespace(
            task=main_verify_task,
            alert_message="main verify RED at `feedfacecafe` - merges halted; phase `unit` failing",
        ),
    )

    def _persist_red_main_verify(*args, **kwargs):
        del args, kwargs
        main_verify_task.review_verify_command = "./bin/tests"
        main_verify_task.review_verify_status = "failed"
        main_verify_task.review_verify_exit_status = "1"
        main_verify_task.review_verify_failure = "verify_command failed"
        main_verify_task.review_verify_head_sha = "feedfacecafe"
        main_verify_task.completed_at = datetime.now(UTC)
        main_verify_task.output_content = json.dumps(
            {
                "alert_message": red.state.alert_message,
                "captured_at": "2026-06-23T00:05:00+00:00",
                "failing_phase": "unit",
                "gate_enabled": True,
                "head_sha": "feedfacecafe",
                "tree_fingerprint": "new-fingerprint",
                "verify_command": "./bin/tests",
                "verify_timeout_grace_seconds": 5.0,
                "verify_timeout_seconds": 120,
            },
            sort_keys=True,
        )
        store.update(main_verify_task)
        return red

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="new-fingerprint"),
        patch("gza.cli.watch.check_main_integration_verify", side_effect=_persist_red_main_verify) as check_main_verify,
        patch("gza.cli.watch._execute_merge_action") as execute_merge,
    ):
        _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    execute_merge.assert_not_called()
    check_main_verify.assert_called_once()
    log_text = log_path.read_text()
    assert "main verify RED at `feedfacecafe` - merges halted; phase `unit` failing" in log_text

    with patch("gza.main_integration_verify._compute_tree_fingerprint", return_value="new-fingerprint"):
        rows, _ = _query_owner_rows_with_context(
            store=store,
            config=config,
            git=git,
            target_branch="main",
            max_recovery_attempts=config.max_resume_attempts,
            include_skipped=True,
        )
    main_rows = [row for row in rows if row.owner_task.id == main_verify_task.id]
    assert len(main_rows) == 1
    assert main_rows[0].next_action is not None
    assert main_rows[0].next_action["needs_attention_reason"] == "main-integration-verify-red"
    assert "main verify RED at `feedfacecafe` - merges halted; phase `unit` failing" in main_rows[0].next_action["description"]


def test_watch_cycle_merges_approved_with_followups_and_materializes_followup_tasks(tmp_path: Path) -> None:
    """Watch should treat APPROVED_WITH_FOLLOWUPS as mergeable and create follow-up tasks first."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", group="release-1")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-followups-merge"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    review = store.add("Review task", task_type="review", depends_on=task.id)
    assert review.id is not None

    finding = SimpleNamespace(id="F1")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    created_followup = SimpleNamespace(id="gza-999")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch(
            "gza.cli.determine_next_action",
            return_value={
                "type": "merge_with_followups",
                "review_task": review,
                "followup_findings": (finding,),
            },
        ),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=lambda *_args, **_kwargs: (
                store.set_merge_status(task.id, "merged"),
                SimpleNamespace(
                    rc=0,
                    created_followups=[created_followup],
                    reused_followups=[],
                ),
            )[1],
        ) as execute_merge,
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    assert result.work_done is True
    execute_merge.assert_called_once()
    args = execute_merge.call_args.args
    kwargs = execute_merge.call_args.kwargs
    assert args[0] is config
    assert args[1] is store
    assert args[2] is git
    assert args[3].id == task.id
    assert args[4]["type"] == "merge_with_followups"
    assert args[4]["review_task"].id == review.id
    assert args[4]["followup_findings"] == (finding,)
    assert kwargs["target_branch"] == "main"
    assert kwargs["current_branch"] == "main"
    assert kwargs["merge_source"] == "watch"
    assert kwargs["quiet_mechanics"] is True
    assert log_path.read_text().count(f"MERGE     {task.id} -> main") == 1
    assert any(
        line.split(maxsplit=2)[1] == "FOLLOW" and "gza-999 created from" in line
        for line in log_path.read_text().splitlines()
    )


def test_watch_cycle_logs_off_topic_investigation_ids_for_cleared_merge(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement", group="release-1")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-investigation-merge"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
        patch(
            "gza.cli.determine_next_action",
            return_value={
                "type": "merge",
                "description": "Merge (previous review addressed)",
                "created_investigation_task_ids": ("gza-7001",),
                "reused_investigation_task_ids": ("gza-7000",),
            },
        ),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=lambda *_args, **_kwargs: (
                store.set_merge_status(task.id, "merged"),
                SimpleNamespace(
                    rc=0,
                    created_followups=[],
                    reused_followups=[],
                    created_investigation_task_ids=["gza-7001"],
                    reused_investigation_task_ids=["gza-7000"],
                ),
            )[1],
        ),
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    assert result.work_done is True
    log_text = log_path.read_text()
    assert f"MERGE     {task.id} -> main" in log_text
    assert "FOLLOW    gza-7001 investigation created from" in log_text
    assert "FOLLOW    gza-7000 investigation reused from" in log_text


def test_watch_cycle_dry_run_merges_approved_with_followups_without_creating_followup_tasks(tmp_path: Path) -> None:
    """Dry-run should preview merge_with_followups without mutating follow-up tasks."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-followups-dry-run"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    review = store.add("Review task", task_type="review", depends_on=task.id)
    assert review.id is not None

    finding = SimpleNamespace(id="F1")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={
                "type": "merge_with_followups",
                "review_task": review,
                "followup_findings": (finding,),
            },
        ),
        patch(
            "gza.cli.watch._execute_merge_action",
            return_value=SimpleNamespace(
                rc=0,
                created_followups=[],
                reused_followups=[],
            ),
        ) as execute_merge,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
            quiet=True,
        )

    assert result.work_done is True
    execute_merge.assert_not_called()
    assert f"MERGE     {task.id} -> main [dry-run]" in log_path.read_text()


def test_emit_transition_events_includes_followup_ids_for_review(tmp_path: Path) -> None:
    """Completed review transition logs parsed follow-up IDs."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Completed impl", task_type="implement")
    assert impl.id is not None

    review = store.add("Review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.output_content = (
        "## Summary\n\nLooks good.\n\n"
        "## Blockers\n\nNone.\n\n"
        "## Follow-Ups\n\n"
        "### F1 Input validation\n"
        "Recommended follow-up: add validation.\n\n"
        "## Verdict\n\n"
        "Verdict: APPROVED_WITH_FOLLOWUPS\n"
    )
    review.completed_at = datetime.now(UTC)
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    _emit_transition_events(
        {review.id: {"status": "in_progress"}},
        {
            review.id: {
                "status": "completed",
                "task_type": "review",
                "started_at": None,
                "completed_at": review.completed_at.isoformat() if review.completed_at else None,
                "failure_reason": None,
                "depends_on": impl.id,
            }
        },
        store=store,
        config=config,
        log=log,
    )

    assert f"{review.id} for {impl.id}: APPROVED_WITH_FOLLOWUPS [follow-ups: F1]" in log_path.read_text()


def test_emit_transition_events_logs_external_merge_status_flip(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    before = _task_snapshot(store)
    store.set_merge_status(task.id, "merged")
    after = _task_snapshot(store)

    _emit_transition_events(before, after, store=store, config=config, log=log)

    assert log_path.read_text().count(f"MERGE     {task.id} -> main") == 1


def test_emit_transition_events_logs_merge_unit_target_branch_on_external_merge_flip(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed release task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-release-merge"
    task.has_commits = True
    store.update(task)

    unit = store.create_merge_unit(
        source_branch="feature/watch-release-merge",
        target_branch="release",
        owner_task_id=task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.dual_write_legacy_merge_status(unit.id)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    before = _task_snapshot(store)
    store.set_merge_unit_state(unit.id, "merged")
    after = _task_snapshot(store)

    _emit_transition_events(before, after, store=store, config=config, log=log)

    log_text = log_path.read_text()
    assert log_text.count(f"MERGE     {task.id} -> release") == 1
    assert f"MERGE     {task.id} -> main" not in log_text


def test_emit_transition_events_logs_only_merge_unit_owner(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Owner task", task_type="implement")
    assert owner.id is not None
    review = store.add("Attached review", task_type="review", depends_on=owner.id)
    rebase = store.add("Attached rebase", task_type="rebase", based_on=owner.id)
    assert review.id is not None
    assert rebase.id is not None
    for task in (owner, review, rebase):
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/watch-transition-merge-unit"
        task.has_commits = True
        store.update(task)

    unit = store.create_merge_unit(
        source_branch="feature/watch-transition-merge-unit",
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    store.attach_task_to_merge_unit(review.id, unit.id, "review")
    store.attach_task_to_merge_unit(rebase.id, unit.id, "same_branch")
    store.dual_write_legacy_merge_status(unit.id)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    before = _task_snapshot(store)
    store.set_merge_unit_state(unit.id, "merged")
    after = _task_snapshot(store)

    _emit_transition_events(before, after, store=store, config=config, log=log)

    log_text = log_path.read_text()
    assert log_text.count(f"MERGE     {owner.id} -> main") == 1
    assert f"MERGE     {review.id} -> main" not in log_text
    assert f"MERGE     {rebase.id} -> main" not in log_text


def test_watch_cycle_logs_one_merge_line_for_merge_unit_owner_and_keeps_member_credit_on_unit(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    owner = store.add("Owner task", task_type="implement")
    assert owner.id is not None
    review = store.add("Attached review", task_type="review", depends_on=owner.id)
    rebase = store.add("Attached rebase", task_type="rebase", based_on=owner.id)
    failed_impl = store.add("Failed sibling", task_type="implement", based_on=owner.id)
    recovery = store.add("Recovery child", task_type="implement", based_on=failed_impl.id)
    assert owner.id is not None
    assert review.id is not None
    assert rebase.id is not None
    assert failed_impl.id is not None
    assert recovery.id is not None
    for task in (owner, review, rebase, recovery):
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/watch-merge-unit"
        task.has_commits = True
        store.update(task)
    failed_impl.status = "failed"
    failed_impl.failure_reason = "simulated failure"
    failed_impl.branch = "feature/watch-merge-unit"
    failed_impl.has_commits = True
    store.update(failed_impl)

    unit = store.create_merge_unit(
        source_branch="feature/watch-merge-unit",
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    store.attach_task_to_merge_unit(review.id, unit.id, "review")
    store.attach_task_to_merge_unit(rebase.id, unit.id, "same_branch")
    store.attach_task_to_merge_unit(failed_impl.id, unit.id, "same_branch")
    store.attach_task_to_merge_unit(recovery.id, unit.id, "same_branch")
    store.dual_write_legacy_merge_status(unit.id)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    def choose_action(_cfg, _store, _git, task, _target, *, impl_based_on_ids, **_kwargs):  # noqa: ARG001
        if task.id == owner.id:
            return {"type": "merge"}
        return {"type": "skip"}

    def merge_unit_side_effect(*_args, **_kwargs):
        store.set_merge_unit_state(unit.id, "merged")
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", side_effect=choose_action),
        patch(
            "gza.cli.watch._execute_merge_action",
            side_effect=merge_unit_side_effect,
        ),
    ):
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    log_text = log_path.read_text()
    assert log_text.count(f"MERGE     {owner.id} -> main") == 1
    assert f"MERGE     {review.id} -> main" not in log_text
    assert f"MERGE     {rebase.id} -> main" not in log_text
    assert f"MERGE     {failed_impl.id} -> main" not in log_text
    assert f"MERGE     {recovery.id} -> main" not in log_text
    for task_id in (owner.id, review.id, rebase.id, failed_impl.id, recovery.id):
        member_unit = store.resolve_merge_unit_for_task(task_id)
        assert member_unit is not None
        assert member_unit.state == "merged"
    owner_task = store.get(owner.id)
    review_task = store.get(review.id)
    rebase_task = store.get(rebase.id)
    failed_impl_task = store.get(failed_impl.id)
    recovery_task = store.get(recovery.id)
    assert owner_task is not None
    assert review_task is not None
    assert rebase_task is not None
    assert failed_impl_task is not None
    assert recovery_task is not None
    assert owner_task.merge_status == "merged"
    assert review_task.merge_status is None
    assert rebase_task.merge_status is None
    assert failed_impl_task.merge_status is None
    assert recovery_task.merge_status is None


def test_cmd_watch_logs_completed_review_before_same_cycle_merge(tmp_path: Path) -> None:
    """Pre-pass transitions should land before merge logs from the same watch pass."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    impl_task = store.add("Impl", task_type="implement")
    assert impl_task.id is not None
    review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
    assert review_task.id is not None
    review_task.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    review_task.status = "completed"
    review_task.completed_at = datetime.now(UTC)
    store.update(review_task)

    impl_id = impl_task.id
    review_id = review_task.id
    snapshots = [
        {
            impl_id: {"status": "completed", "task_type": "implement", "merge_status": "unmerged"},
            review_id: {
                "status": "in_progress",
                "task_type": "review",
                "started_at": None,
                "completed_at": None,
                "failure_reason": None,
                "depends_on": impl_id,
                "merge_status": None,
            },
        },
        {
            impl_id: {"status": "completed", "task_type": "implement", "merge_status": "unmerged"},
            review_id: {
                "status": "completed",
                "task_type": "review",
                "started_at": None,
                "completed_at": datetime.now(UTC).isoformat(),
                "failure_reason": None,
                "depends_on": impl_id,
                "merge_status": None,
            },
        },
        {
            impl_id: {"status": "completed", "task_type": "implement", "merge_status": "merged"},
            review_id: {
                "status": "completed",
                "task_type": "review",
                "started_at": None,
                "completed_at": datetime.now(UTC).isoformat(),
                "failure_reason": None,
                "depends_on": impl_id,
                "merge_status": None,
            },
        },
        {
            impl_id: {"status": "completed", "task_type": "implement", "merge_status": "merged"},
            review_id: {
                "status": "completed",
                "task_type": "review",
                "started_at": None,
                "completed_at": datetime.now(UTC).isoformat(),
                "failure_reason": None,
                "depends_on": impl_id,
                "merge_status": None,
            },
        },
        {
            impl_id: {"status": "completed", "task_type": "implement", "merge_status": "merged"},
            review_id: {
                "status": "completed",
                "task_type": "review",
                "started_at": None,
                "completed_at": datetime.now(UTC).isoformat(),
                "failure_reason": None,
                "depends_on": impl_id,
                "merge_status": None,
            },
        },
    ]

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=1,
        max_idle=1,
        max_iterations=3,
        dry_run=False,
        quiet=True,
        group=None,
        yes=True,
    )

    def fake_run_cycle(**_kwargs):
        if not hasattr(fake_run_cycle, "seen"):
            fake_run_cycle.seen = True  # type: ignore[attr-defined]
            return _CycleResult(work_done=True, pending=0, running=0)
        return _CycleResult(work_done=False, pending=0, running=0)

    with (
        patch("gza.cli.watch.Config.load", return_value=config),
        patch("gza.cli.watch.get_store", return_value=store),
        patch("gza.cli.watch._task_snapshot", side_effect=snapshots),
        patch("gza.cli.watch._run_cycle", side_effect=fake_run_cycle),
        patch("gza.cli.watch._sleep_interruptibly"),
    ):
        assert cmd_watch(args) == 0

    lines = log_path.read_text().splitlines()
    review_index = next(i for i, line in enumerate(lines) if " REVIEW " in line)
    merge_index = next(i for i, line in enumerate(lines) if " MERGE " in line)
    assert review_index < merge_index


def test_watch_cycle_quiet_off_default_branch_suppresses_stdout_and_logs_skip(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Quiet branch guard should suppress helper output while keeping SKIP log event."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-quiet-default-guard"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    git = MagicMock()
    git.current_branch.return_value = "feature/local"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action") as merge_exec,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    stdout = capsys.readouterr().out
    assert "`gza merge` must be run from the default branch" not in stdout
    assert merge_exec.call_count == 0
    assert "SKIP      merge actions skipped: not on default branch" in log_path.read_text()


def test_watch_cycle_starts_pending_review_with_plain_worker(tmp_path: Path) -> None:
    """Watch should start pending non-implement tasks with plain workers."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    review = store.add("Pending review", task_type="review")
    assert review.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == review.id


def test_watch_cycle_advances_create_review_action(tmp_path: Path) -> None:
    """Completed unmerged implement with no review should launch iterate on the impl."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/create-review"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = True

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._prepare_create_review_action", side_effect=AssertionError("plain review creation should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id


def test_watch_cycle_creates_exactly_one_closing_review_after_completed_improve_without_review_clear(
    tmp_path: Path,
) -> None:
    """Watch should route the shared closing review action through iterate on the impl."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    impl.branch = "feature/watch-closing-review"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
    assert stale_review.id is not None
    stale_review.status = "completed"
    stale_review.output_content = "**Verdict: CHANGES_REQUESTED**"
    stale_review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(stale_review)

    improve = store.add(
        "Improve feature",
        task_type="improve",
        based_on=impl.id,
        depends_on=stale_review.id,
        same_branch=True,
    )
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(improve)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = True
    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._prepare_create_review_action", side_effect=AssertionError("plain review creation should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=1,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id


def test_watch_cycle_creates_implement_from_completed_plan_with_iterate_mode(tmp_path: Path) -> None:
    """Completed plan without implement child should create implement and start iterate."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan feature", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=7,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 0
    assert spawn_worker.call_count == 1
    created_plan_review = spawn_worker.call_args.kwargs["prepared_task"]
    assert created_plan_review.task_type == "plan_review"
    assert created_plan_review.depends_on == plan.id


def test_watch_create_plan_review_inherits_tags_from_completed_plan(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Plan scoped slice", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    plan.tags = ("lifecycle", "planner")
    store.update(plan)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=7,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 0
    created_plan_review = spawn_worker.call_args.kwargs["prepared_task"]
    assert created_plan_review.depends_on == plan.id
    assert created_plan_review.tags == plan.tags


@pytest.mark.parametrize(
    ("action_type", "child_type", "action_key"),
    [
        ("run_plan_review", "plan_review", "plan_review_task"),
        ("run_plan_improve", "plan_improve", "plan_improve_task"),
    ],
)
def test_watch_cycle_does_not_double_start_pending_plan_review_children_started_in_advance_step(
    tmp_path: Path,
    action_type: str,
    child_type: str,
    action_key: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan feature", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    child_kwargs: dict[str, object] = {"task_type": child_type}
    if child_type == "plan_review":
        child_kwargs["depends_on"] = plan.id
    else:
        review = store.add("Review plan", task_type="plan_review", depends_on=plan.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "## Verdict\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)
        child_kwargs["depends_on"] = review.id
        child_kwargs["based_on"] = plan.id
    child = store.add("Child task", **child_kwargs)
    assert child.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": action_type, action_key: child}),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not spawn")),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == child.id


def test_watch_cycle_plan_review_action_counts_against_batch_capacity(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan feature", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    pending_plan_review = store.add("Pending plan review", task_type="plan_review", depends_on=plan.id)
    assert pending_plan_review.id is not None
    unrelated_pending = store.add("Pending queue plan", task_type="plan")
    assert unrelated_pending.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            side_effect=[
                {"type": "run_plan_review", "plan_review_task": pending_plan_review},
                {"type": "skip", "description": "SKIP: pending plan review already running"},
            ],
        ),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not spawn")),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan_review.id


@pytest.mark.parametrize(
    ("action_type", "child_type", "action_key"),
    [("run_review", "review", "review_task"), ("run_improve", "improve", "improve_task")],
)
def test_watch_cycle_does_not_double_start_pending_child_started_in_advance_step(
    tmp_path: Path,
    action_type: str,
    child_type: str,
    action_key: str,
) -> None:
    """Child task started by advance action must not be started again in step 3."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/no-double-start"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    child = store.add("Child task", task_type=child_type)
    assert child.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": action_type, action_key: child}),
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == child.id


def test_watch_cycle_advances_run_improve_action(tmp_path: Path) -> None:
    """Completed task with pending improve child should launch iterate on the impl."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/run-improve"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
    store.update(review)

    improve = store.add(
        "Improve feature",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = True

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "run_improve", "improve_task": improve}),
        patch("gza.cli.git_ops.get_review_verdict", return_value="CHANGES_REQUESTED"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id


def test_watch_cycle_improve_action_with_disabled_auto_recovery_routes_to_iterate(tmp_path: Path) -> None:
    """Watch should hand failed-improve recovery state to iterate instead of stopping locally."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/improve-cap-zero"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
    store.update(review)

    failed_improve = store.add(
        "Improve feature",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "MAX_TURNS"
    failed_improve.session_id = "sess-123"
    failed_improve.completed_at = datetime.now(UTC)
    store.update(failed_improve)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = True

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            max_recovery_attempts=0,
        )
    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    log_text = log_path.read_text()
    assert "ATTENTION" not in log_text
    assert f"{impl.id} iterate" in log_text


def test_watch_cycle_failed_improve_dry_run_routes_through_iterate(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-improve-skip"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
    store.update(review)

    failed_improve = store.add(
        "Improve feature",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "MAX_TURNS"
    failed_improve.session_id = "sess-improve"
    failed_improve.completed_at = datetime.now(UTC)
    store.update(failed_improve)

    dependency = store.add("Mismatched dependency", task_type="plan")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    running_child = store.add(
        "Running resumed improve",
        task_type="improve",
        based_on=failed_improve.id,
        depends_on=dependency.id,
    )
    assert running_child.id is not None
    running_child.status = "in_progress"
    running_child.session_id = failed_improve.session_id
    store.update(running_child)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
    ):
        _ = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
            max_recovery_attempts=1,
        )
    log_text = log_path.read_text()
    assert "ATTENTION" not in log_text
    assert f"START     {impl.id} iterate [dry-run]" in log_text


def test_watch_cycle_improve_action_routes_to_iterate_without_creating_local_child(tmp_path: Path) -> None:
    """Watch should leave improve creation details to iterate."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-improve-comments"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    store.add_comment(impl.id, "Please fix edge-case validation from QA feedback.")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    improves = store.get_improve_tasks_for(impl.id, review.id)
    assert improves == []
    assert f"{impl.id} iterate" in log_path.read_text()


def test_watch_cycle_improve_action_routes_failed_chain_to_iterate(tmp_path: Path) -> None:
    """Watch should route failed improve chains to iterate instead of resuming them directly."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/improve-resume"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    failed_improve = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "MAX_TURNS"
    failed_improve.session_id = "sess-improve-1"
    failed_improve.completed_at = datetime.now(UTC)
    store.update(failed_improve)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    improves = [t for t in store.get_all() if t.task_type == "improve"]
    assert [t.id for t in improves] == [failed_improve.id]
    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert f"{impl.id} iterate" in log_path.read_text()


def test_watch_cycle_improve_action_with_manual_review_failure_routes_to_iterate(tmp_path: Path) -> None:
    """Watch should not surface failed-improve manual-review stops before iterate runs."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/improve-retry"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    failed_improve = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "TEST_FAILURE"
    failed_improve.completed_at = datetime.now(UTC)
    store.update(failed_improve)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    improves = [t for t in store.get_all() if t.task_type == "improve"]
    assert [t.id for t in improves] == [failed_improve.id]
    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    log_text = log_path.read_text()
    assert "ATTENTION" not in log_text
    assert f"{impl.id} iterate" in log_text


def test_watch_cycle_attempt_capped_improve_chain_stays_parked_without_iterate_respawn(tmp_path: Path) -> None:
    """Watch should park attempt-capped improve chains instead of re-spawning iterate."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/improve-attempt-cap"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    previous_id = impl.id
    for attempt in range(config.max_resume_attempts + 1):
        task = store.add(
            "Improve attempt",
            task_type="improve",
            depends_on=review.id,
            based_on=previous_id,
            same_branch=True,
        )
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "MAX_STEPS"
        task.completed_at = datetime.now(UTC)
        store.update(task)
        previous_id = task.id

    before_count = _task_count(store)
    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("iterate should not respawn for parked manual review"),
        ) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert _task_count(store) == before_count
    assert spawn_iterate.call_count == 0
    text = log_path.read_text()
    assert "ATTENTION" in text
    assert "reason=retry-limit-reached" in text
    assert f"{impl.id} iterate" not in text


def test_watch_cycle_runs_pending_improve_even_when_failed_chain_is_parked(tmp_path: Path) -> None:
    """A pending improve should still run even if older improve attempts exhausted auto-recovery."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/parked-run-improve"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = True

    previous_id = impl.id
    for _attempt in range(config.max_resume_attempts + 1):
        failed = store.add(
            "Failed improve attempt",
            task_type="improve",
            depends_on=review.id,
            based_on=previous_id,
            same_branch=True,
        )
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_STEPS"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)
        previous_id = failed.id

    pending_improve = store.add(
        "Pending improve after manual intervention",
        task_type="improve",
        depends_on=review.id,
        based_on=previous_id,
        same_branch=True,
    )
    assert pending_improve.id is not None

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={"type": "run_improve", "improve_task": pending_improve},
        ),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    text = log_path.read_text()
    assert f"START     {impl.id} iterate" in text
    assert "ATTENTION" not in text
    assert "reason=retry-limit-reached" not in text


def test_watch_cycle_advances_needs_rebase_action(tmp_path: Path) -> None:
    """Conflict path should create and run rebase tasks in watch cycles."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/rebase-me"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = False

    rebase_task = SimpleNamespace(id="test-rebase-id")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch.launch_permit"),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._create_rebase_task", return_value=rebase_task) as create_rebase,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert create_rebase.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == rebase_task.id
    lines = (tmp_path / ".gza" / "watch.log").read_text().splitlines()
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{rebase_task.id} rebase" in line
        for line in lines
    )
    assert not any(" REBASE " in line for line in lines)


def test_watch_cycle_reconcile_conflict_respects_zero_slots(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-reconcile-zero-slots"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={
                "type": "reconcile_branch_divergence",
                "description": "Reconcile diverged local/origin refs",
            },
        ),
        patch(
            "gza.cli.watch._reconcile_diverged_branch_with_origin",
            return_value=SimpleNamespace(
                status="needs_rebase",
                message="Mechanical rebase conflicted",
                rebase_target="main",
            ),
        ),
        patch("gza.cli.watch._create_rebase_task", side_effect=AssertionError("rebase task should not be created")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("worker should not spawn")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=0,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert "no watch worker slots available for rebase" in log_path.read_text()


def test_watch_cycle_reconcile_conflict_spawns_rebase_and_logs_start(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-reconcile-conflict"
    impl.merge_status = "unmerged"
    impl.has_commits = True
    store.update(impl)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()
    rebase_task = SimpleNamespace(id="watch-rebase-id")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={
                "type": "reconcile_branch_divergence",
                "description": "Reconcile diverged local/origin refs",
            },
        ),
        patch(
            "gza.cli.watch._reconcile_diverged_branch_with_origin",
            return_value=SimpleNamespace(
                status="needs_rebase",
                message="Mechanical rebase conflicted",
                rebase_target="main",
            ),
        ),
        patch("gza.cli.watch.launch_permit"),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._create_rebase_task", return_value=rebase_task) as create_rebase,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert create_rebase.call_count == 1
    assert create_rebase.call_args.args[3] == "main"
    assert spawn_worker.call_count == 1
    assert f"START     {rebase_task.id} rebase" in log_path.read_text()


def test_watch_cycle_skips_iterate_for_already_reachable_branch_with_stale_merge_state(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/already-reachable"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    store.get_or_create_merge_unit_for_task(impl)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()
    git.is_merged.return_value = True

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch.determine_next_action", return_value={"type": "create_review"}),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not run")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert (
        "already merged into target branch" in log_path.read_text()
        or "implementation chain already merged; not starting iterate" in log_path.read_text()
    )


def test_watch_cycle_next_pass_skips_iterate_after_child_reconciles_merged_state(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-iterate-reconcile"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "unmerged")

    queued = store.add("Queued follow-up plan", task_type="plan")
    assert queued.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    def fake_spawn_iterate(_args, _config, impl_task, **_kwargs):
        assert impl_task.id == impl.id
        store.set_merge_status(impl_task.id, "merged")
        return 0

    def fake_spawn_worker(_args, _config, task_id=None, **_kwargs):
        assert task_id == queued.id
        task = store.get(task_id)
        assert task is not None
        task.status = "in_progress"
        store.update(task)
        return 0

    def fake_determine_next_action(_config, _store, _git, task, _target_branch, **_kwargs):
        if task.id == impl.id:
            return {"type": "create_review"}
        raise AssertionError(f"unexpected completed-task planning for {task.id}")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch.determine_next_action", side_effect=fake_determine_next_action),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=fake_spawn_iterate) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", side_effect=fake_spawn_worker) as spawn_worker,
    ):
        first_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )
        second_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    refreshed_unit = store.resolve_merge_unit_for_task(impl.id)
    assert first_result.work_done is True
    assert second_result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_worker.call_count == 1
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"
    log_lines = log_path.read_text().splitlines()
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{impl.id} iterate" in line
        for line in log_lines
        if line.strip()
    )
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{queued.id} plan" in line
        for line in log_lines
        if line.strip()
    )


def test_watch_cycle_with_isolation_enabled_merge_conflict_spawns_prepared_rebase_task(tmp_path: Path) -> None:
    """Isolated conflict rebases should pass the prepared child into the worker spawn helper."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-prepared-rebase"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()
    isolated_git.branch_exists.return_value = True
    isolated_git.is_merged.return_value = False
    isolated_git.can_merge.return_value = False

    created_rebase = SimpleNamespace(id="test-rebase-raw")
    prepared_rebase = SimpleNamespace(id="test-rebase-prepared")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", return_value=isolated_git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            return_value=SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
        ),
        patch("gza.cli.watch._create_rebase_task", return_value=created_rebase),
        patch("gza.cli.watch.launch_permit"),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", return_value=prepared_rebase) as prepare_task,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    prepare_task.assert_called_once_with(
        config,
        created_rebase,
        rollback_on_failure=True,
    )
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == prepared_rebase.id
    assert spawn_worker.call_args.kwargs["prepared_task"] is prepared_rebase
    assert f"START     {prepared_rebase.id} rebase" in log_path.read_text()


def test_watch_cycle_merge_conflict_skips_rebase_at_max_concurrent_without_creating_task(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "main_checkout_isolate: true\n"
    )
    store = make_store(tmp_path)

    task = store.add("Completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/watch-isolated-rebase-cap"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "feature/local"
    repo_git.default_branch.return_value = "main"
    isolated_git = MagicMock()
    isolated_git.branch_exists.return_value = True
    isolated_git.is_merged.return_value = False
    isolated_git.can_merge.return_value = False

    before_count = len(store.get_all())

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", return_value=isolated_git),
        patch(
            "gza.cli.watch.get_concurrency_snapshot",
            return_value=SimpleNamespace(
                limit=1,
                running=0,
                available=1,
                running_task_ids=(),
                live_pids=frozenset(),
                anonymous_worker_count=0,
                current_pid_counted=False,
            ),
        ),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch(
            "gza.cli.watch._execute_merge_action",
            return_value=SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
        ),
        patch(
            "gza.cli.watch.launch_permit",
            side_effect=MaxConcurrentTasksError("already at max concurrent tasks: 1 running, limit is 1"),
        ),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("spawn must not run")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert len(store.get_all()) == before_count
    assert [row for row in store.get_all() if row.task_type == "rebase"] == []
    assert "already at max concurrent tasks: 1 running, limit is 1" in log_path.read_text()


def test_watch_cycle_off_default_branch_targets_rebase_to_default_branch(tmp_path: Path) -> None:
    """Off-default watch should still create rebases against the default branch."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/rebase-off-default"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "feature/local"
    git.default_branch.return_value = "main"

    rebase_task = SimpleNamespace(id="test-rebase-off-default")

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "needs_rebase"}),
        patch("gza.cli.watch.launch_permit"),
        patch("gza.cli.advance_executor._prepare_task_for_reserved_launch", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._create_rebase_task", return_value=rebase_task) as create_rebase,
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert create_rebase.call_count == 1
    assert create_rebase.call_args.args[3] == "main"
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == rebase_task.id


@pytest.mark.parametrize(
    ("action_type", "expected_fragment"),
    [
        ("create_review", "review"),
        ("improve", "improve"),
        ("create_implement", "implement"),
        ("needs_rebase", "rebase"),
    ],
)
def test_watch_cycle_dry_run_does_not_create_tasks_for_task_creating_advance_actions(
    tmp_path: Path,
    action_type: str,
    expected_fragment: str,
) -> None:
    """Dry-run must never mutate task rows for task-creating advance actions."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    if action_type == "create_implement":
        root = store.add("Plan feature", task_type="plan")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        store.update(root)
        action: dict[str, object] = {"type": action_type}
    else:
        root = store.add("Implement feature", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        root.branch = "feature/dry-run-no-mutate"
        store.update(root)
        store.set_merge_status(root.id, "unmerged")
        action = {"type": action_type}
        if action_type == "improve":
            review = store.add("Review feature", task_type="review", depends_on=root.id)
            action["review_task"] = review

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    before_count = _task_count(store)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value=action),
        patch("gza.cli.watch._prepare_create_review_action") as create_review,
        patch("gza.cli.watch._create_rebase_task") as create_rebase,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    assert result.work_done is True
    assert _task_count(store) == before_count
    if action_type == "create_review":
        assert create_review.call_count == 0
    if action_type == "needs_rebase":
        assert create_rebase.call_count == 0
    log_lines = log_path.read_text().splitlines()
    if action_type in {"create_review", "improve"}:
        assert any(f"{root.id} iterate [dry-run]" in line for line in log_lines)
    else:
        assert any("[dry-run]" in line and expected_fragment in line for line in log_lines)
    if action_type == "create_implement":
        assert any(f"(new) implement for {root.id} [dry-run]" in line for line in log_lines)


def test_watch_dry_run_command_does_not_reconcile_or_prune_dead_in_progress_task(tmp_path: Path) -> None:
    """watch --dry-run must not mutate dead in-progress tasks or worker registry rows."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dead_pid = 999_999_999

    task = store.add("Dead worker in progress", task_type="implement")
    assert task.id is not None
    task.status = "in_progress"
    task.started_at = datetime.now(UTC)
    task.running_pid = dead_pid
    store.update(task)

    registry = WorkerRegistry(tmp_path / ".gza" / "workers")
    registry.register(
        WorkerMetadata(
            worker_id="w-watch-dry-run-command",
            task_id=task.id,
            pid=dead_pid,
            status="running",
            started_at=datetime.now(UTC).isoformat(),
        )
    )

    before_row = store.get(task.id)
    assert before_row is not None
    before_worker = registry.get("w-watch-dry-run-command")
    assert before_worker is not None

    with patch("gza.cli._common.WorkerRegistry.is_running", return_value=False):
        result = invoke_gza(
            "watch",
            "--dry-run",
            "--poll",
            "1",
            "--max-idle",
            "1",
            "--batch",
            "1",
            "--quiet",
            "--project",
            str(tmp_path),
        )

    assert result.returncode == 0
    after_row = store.get(task.id)
    assert after_row is not None
    after_worker = registry.get("w-watch-dry-run-command")
    assert after_worker is not None

    assert after_row.status == "in_progress"
    assert after_row.failure_reason == before_row.failure_reason
    assert after_row.running_pid == before_row.running_pid
    assert after_row.started_at == before_row.started_at
    assert after_row.completed_at == before_row.completed_at
    assert after_worker.status == before_worker.status
    assert after_worker.task_id == before_worker.task_id
    assert after_worker.pid == before_worker.pid


def test_run_cycle_dry_run_real_helpers_does_not_reconcile_or_prune(tmp_path: Path) -> None:
    """_run_cycle(dry_run=True) should leave dead in-progress rows untouched with real helpers."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    dead_pid = 999_999_999

    task = store.add("Dead worker in progress", task_type="implement")
    assert task.id is not None
    task.status = "in_progress"
    task.started_at = datetime.now(UTC)
    task.running_pid = dead_pid
    store.update(task)

    registry = WorkerRegistry(tmp_path / ".gza" / "workers")
    registry.register(
        WorkerMetadata(
            worker_id="w-watch-dry-run-pass",
            task_id=task.id,
            pid=dead_pid,
            status="running",
            started_at=datetime.now(UTC).isoformat(),
        )
    )

    before_row = store.get(task.id)
    assert before_row is not None
    before_worker = registry.get("w-watch-dry-run-pass")
    assert before_worker is not None

    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    with patch("gza.cli._common.WorkerRegistry.is_running", return_value=False):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    assert result.work_done is False
    after_row = store.get(task.id)
    assert after_row is not None
    after_worker = registry.get("w-watch-dry-run-pass")
    assert after_worker is not None

    assert after_row.status == "in_progress"
    assert after_row.failure_reason == before_row.failure_reason
    assert after_row.running_pid == before_row.running_pid
    assert after_row.started_at == before_row.started_at
    assert after_row.completed_at == before_row.completed_at
    assert after_worker.status == before_worker.status
    assert after_worker.task_id == before_worker.task_id
    assert after_worker.pid == before_worker.pid


@pytest.mark.parametrize(
    ("action_type", "description"),
    [
        ("skip", "SKIP: no review exists and advance_create_reviews=false"),
        ("wait_review", "SKIP: review test-review is in_progress"),
        ("wait_improve", "SKIP: improve task test-improve is in_progress"),
    ],
)
def test_watch_cycle_logs_skip_events_for_non_actionable_advance_outcomes(
    tmp_path: Path,
    action_type: str,
    description: str,
) -> None:
    """Silent skip outcomes should emit SKIP events in watch logs."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/skip-visibility"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": action_type, "description": description}),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    text = log_path.read_text()
    assert "SKIP" in text
    assert str(impl.id) in text
    assert "ATTENTION" not in text


@pytest.mark.parametrize(
    "action",
    [
        {
            "type": "needs_discussion",
            "description": "SKIP: review verdict is NEEDS_DISCUSSION, needs manual attention",
        },
        {
            "type": "max_cycles_reached",
            "description": "SKIP: max review cycles (2) reached, needs manual intervention",
        },
    ],
)
def test_watch_cycle_logs_attention_events_for_manual_advance_outcomes(
    tmp_path: Path,
    action: dict[str, str],
) -> None:
    """Pre-execution manual-attention advance outcomes should use ATTENTION instead of deduped SKIP."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/manual-attention"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value=action),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    text = log_path.read_text()
    assert text.count("ATTENTION") == 1
    assert text.count("Needs attention (1 task):") == 2
    assert str(impl.id) in text
    assert "SKIP" not in text


def test_watch_cycle_logs_attention_for_retry_limit_reached_action(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/retry-limit-attention"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={
                "type": "skip",
                "description": "SKIP: automatic recovery stops here; retry limit reached",
                "needs_attention_reason": "retry-limit-reached",
            },
        ),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    text = log_path.read_text()
    assert "ATTENTION" in text
    assert "reason=retry-limit-reached" in text
    assert "SKIP" not in text


def test_watch_cycle_skips_parked_retry_limit_lineage_without_recomputing_or_spawning(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/parked-retry-limit"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "MAX_TURNS"
    failed_improve.session_id = "sess-parked-improve"
    failed_improve.branch = impl.branch
    failed_improve.completed_at = datetime.now(UTC)
    store.update(failed_improve)

    exhausted_improve = store.add(
        "Failed retry improve",
        task_type="improve",
        depends_on=review.id,
        based_on=failed_improve.id,
        same_branch=True,
    )
    assert exhausted_improve.id is not None
    exhausted_improve.status = "failed"
    exhausted_improve.failure_reason = "TIMEOUT"
    exhausted_improve.session_id = failed_improve.session_id
    exhausted_improve.branch = impl.branch
    exhausted_improve.completed_at = datetime.now(UTC)
    store.update(exhausted_improve)

    config = Config.load(tmp_path)
    git = _make_watch_git()
    rows, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=git,
        target_branch="main",
        max_recovery_attempts=config.max_resume_attempts,
        include_skipped=True,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.lifecycle_action_task is not None
    assert row.lifecycle_action_task.id == impl.id
    assert row.next_action is not None
    assert row.next_action["type"] == "improve"

    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("parked lineage should not spawn iterate"),
        ) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert spawn_iterate.call_count == 0

    log_text = log_path.read_text()
    assert "ATTENTION" in log_text
    assert "reason=retry-limit-reached" in log_text
    assert not any(line.split(maxsplit=2)[1] == "START" for line in log_text.splitlines())


def test_watch_cycle_logs_attention_for_rebase_did_not_unblock_merge_without_spawning_rebase(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/rebase-still-blocked-watch"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={
                "type": "needs_discussion",
                "description": "SKIP: completed rebase did not unblock merge; manual decision required",
                "needs_attention_reason": "rebase-did-not-unblock-merge",
            },
        ),
        patch("gza.cli.watch._create_rebase_task") as create_rebase,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    text = log_path.read_text()
    assert "ATTENTION" in text
    assert "reason=rebase-did-not-unblock-merge" in text
    create_rebase.assert_not_called()


def test_watch_cycle_attention_uses_impl_owner_for_plan_owned_rebase_row(tmp_path: Path) -> None:
    """Plan-owned branch rows should reroot ATTENTION output to the implementation owner."""
    store, plan, impl, rebase = _setup_watch_plan_owned_branch_action_row(tmp_path)

    config = Config.load(tmp_path)
    rows, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=_make_watch_git(),
        target_branch="main",
        max_recovery_attempts=config.max_resume_attempts,
        include_skipped=True,
    )
    action_rows = [
        row
        for row in rows
        if row.lifecycle_action_task is not None and row.lifecycle_action_task.id == rebase.id
    ]
    assert len(action_rows) == 1
    assert action_rows[0].owner_task.id == plan.id
    assert any(member.id == impl.id for member in action_rows[0].members)

    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={
                "type": "needs_discussion",
                "description": "SKIP: review verdict is NEEDS_DISCUSSION, needs manual attention",
            },
        ),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    attention_lines = [line for line in log_path.read_text().splitlines() if "ATTENTION" in line]
    assert len(attention_lines) == 1
    assert str(impl.id) in attention_lines[0]
    assert str(plan.id) not in attention_lines[0]


def test_watch_cycle_attention_keeps_plan_when_no_impl_exists(tmp_path: Path) -> None:
    """Plan-only manual ATTENTION rows should keep the plan identity."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Design unresolved branch ownership", task_type="plan")
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={
                "type": "needs_discussion",
                "description": "SKIP: plan needs manual attention",
            },
        ),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    attention_lines = [line for line in log_path.read_text().splitlines() if "ATTENTION" in line]
    assert len(attention_lines) == 1
    assert str(plan.id) in attention_lines[0]


def test_watch_cycle_execution_attention_uses_impl_owner_for_plan_owned_rebase_row(tmp_path: Path) -> None:
    """Execution-time ATTENTION rows should reroot plan-owned branch rows the same way."""
    store, plan, impl, rebase = _setup_watch_plan_owned_branch_action_row(tmp_path)

    config = Config.load(tmp_path)
    rows, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=_make_watch_git(),
        target_branch="main",
        max_recovery_attempts=config.max_resume_attempts,
        include_skipped=True,
    )
    action_rows = [
        row
        for row in rows
        if row.lifecycle_action_task is not None and row.lifecycle_action_task.id == rebase.id
    ]
    assert len(action_rows) == 1
    assert action_rows[0].owner_task.id == plan.id
    assert any(member.id == impl.id for member in action_rows[0].members)

    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()
    exec_result = SimpleNamespace(
        status="skip",
        message="worker needs manual review",
        attempted_spawn=True,
        handled_task_id=None,
        guarded_pending_task_id=None,
    )
    attention = SimpleNamespace(
        action={
            "type": "manual_review_required",
            "description": "SKIP: worker needs manual review",
            "needs_attention_reason": "retry-limit-reached",
        }
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={"type": "create_review", "description": "Create review (required before merge)"},
        ),
        patch("gza.cli.watch.execute_advance_action", return_value=exec_result),
        patch("gza.cli.watch.resolve_execution_needs_attention", return_value=attention),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    attention_lines = [line for line in log_path.read_text().splitlines() if "ATTENTION" in line]
    assert len(attention_lines) == 1
    assert str(impl.id) in attention_lines[0]
    assert str(plan.id) not in attention_lines[0]


def test_watch_cycle_dedupes_non_human_execution_skip_across_cycles(tmp_path: Path) -> None:
    """Execution skips without a human-attention type should keep ordinary SKIP dedupe behavior."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Plan feature", task_type="plan")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/create-review-skip-dedupe"
    store.update(task)
    store.set_merge_status(task.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={"type": "create_review", "description": "Create review (required before merge)"},
        ),
    ):
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)

    text = log_path.read_text()
    assert text.count("SKIP      ") == 1
    assert "ATTENTION" not in text
    assert f"SKIP: Task {task.id} is a plan task. Expected an implementation task." in text


def test_watch_cycle_clears_attention_reminder_when_next_action_changes(tmp_path: Path) -> None:
    """Sticky ATTENTION reminders should stop once the task no longer resolves to a manual-attention action."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/manual-attention-clear"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            side_effect=[
                {"type": "needs_discussion", "description": "SKIP: review verdict is NEEDS_DISCUSSION, needs manual attention"},
                {"type": "wait_review", "description": "SKIP: review test-review is in_progress"},
                {"type": "wait_review", "description": "SKIP: review test-review is in_progress"},
            ],
        ),
    ):
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)

    text = log_path.read_text()
    assert text.count("ATTENTION") == 1
    assert text.count("review test-review is in_progress") == 1
    assert "SKIP" in text


def test_watch_cycle_off_default_branch_still_runs_non_merge_advance_actions(tmp_path: Path, capsys) -> None:
    """Off default branch should only block merge actions, not iterate launches."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/off-default-review"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "feature/local"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "create_review"}),
        patch("gza.cli.watch._prepare_create_review_action", side_effect=AssertionError("plain review creation should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    output = capsys.readouterr().out
    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert "must be run from the default branch" not in output
    assert "merge actions skipped: not on default branch" not in log_path.read_text()


def test_watch_cycle_create_review_routes_impl_chain_through_iterate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/create-review-iterate"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "create_review"}),
        patch("gza.cli.watch._prepare_create_review_action", side_effect=AssertionError("plain review creation should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{impl.id} iterate" in line
        for line in log_path.read_text().splitlines()
    )


def test_watch_cycle_noop_improve_limit_emits_attention_without_iterate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/noop-reverify-watch"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Completed review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    row = LineageOwnerRow(
        owner_task=impl,
        members=(impl, review),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="test",
        unresolved_tasks=(review,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=review,
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch(
            "gza.cli.watch.determine_next_action",
            return_value={
                "type": "needs_discussion",
                "description": "SKIP: no-op improve limit reached; needs manual discussion",
                "needs_attention_reason": "improve-no-op",
                "subject_task_id": impl.id,
            },
        ),
        patch("gza.cli.watch._prepare_create_review_action", side_effect=AssertionError("plain review creation should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    assert spawn_iterate.call_count == 0
    assert "verify_" + "noop_improve_then_review" not in log_path.read_text()


def test_watch_cycle_completed_rebase_without_owner_review_routes_to_iterate_before_merge(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed_owner = store.add("Original implement", task_type="implement")
    assert failed_owner.id is not None
    failed_owner.status = "failed"
    failed_owner.failure_reason = "MAX_STEPS"
    failed_owner.completed_at = datetime.now(UTC)
    failed_owner.branch = "feature/rebase-review-gate"
    failed_owner.has_commits = True
    store.update(failed_owner)
    store.set_merge_status(failed_owner.id, "unmerged")
    store.get_or_create_merge_unit_for_task(failed_owner)

    resumed = store.add("Resumed implement", task_type="implement", based_on=failed_owner.id)
    assert resumed.id is not None
    resumed.status = "completed"
    resumed.completed_at = datetime.now(UTC)
    resumed.branch = failed_owner.branch
    resumed.has_commits = True
    store.update(resumed)
    store.set_merge_status(resumed.id, "unmerged")
    store.get_or_create_merge_unit_for_task(resumed)

    rebase = store.add("Completed rebase", task_type="rebase", based_on=resumed.id, same_branch=True)
    assert rebase.id is not None
    rebase.status = "completed"
    rebase.completed_at = datetime.now(UTC)
    rebase.branch = resumed.branch
    rebase.changed_diff = False
    store.update(rebase)
    store.get_or_create_merge_unit_for_task(rebase)

    config = Config.load(tmp_path)
    rows, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=_make_watch_git(),
        target_branch="main",
        max_recovery_attempts=config.max_resume_attempts,
        include_skipped=True,
    )
    assert any(row.owner_task.id == failed_owner.id for row in rows)

    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._execute_merge_action", side_effect=AssertionError("merge should not run before review")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == resumed.id
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{resumed.id} iterate" in line
        for line in log_path.read_text().splitlines()
    )


def test_watch_review_spawn_logs_start_and_review_transition_logs_verdict(tmp_path: Path) -> None:
    """Standalone review fallback should log START; REVIEW is only for completed verdict transitions."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/review-events"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review")
    assert review.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "run_review", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    log_lines = log_path.read_text().splitlines()
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{review.id} review" in line
        for line in log_lines
    )
    assert not any(" REVIEW " in line for line in log_lines)

    before = _task_snapshot(store)
    review.status = "completed"
    review.started_at = datetime.now(UTC)
    review.completed_at = datetime.now(UTC)
    store.update(review)
    after = _task_snapshot(store)

    with patch("gza.cli.watch.format_review_outcome", return_value="APPROVED"):
        _emit_transition_events(before, after, store=store, config=config, log=log)

    review_lines = [line for line in log_path.read_text().splitlines() if " REVIEW " in line]
    assert len(review_lines) == 1


def test_watch_cycle_run_review_routes_impl_chain_through_iterate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/run-review-iterate"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Pending review", task_type="review", depends_on=impl.id)
    assert review.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "run_review", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{impl.id} iterate" in line
        for line in log_path.read_text().splitlines()
    )


@pytest.mark.parametrize("anchor_type", ["improve", "rebase"])
def test_watch_cycle_run_review_allows_later_same_lineage_anchor(tmp_path: Path, anchor_type: str) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/run-review-later-anchor"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    store.get_or_create_merge_unit_for_task(impl)

    review = store.add("Pending review", task_type="review", depends_on=impl.id)
    assert review.id is not None

    improve = store.add(
        "Completed improve",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None
    improve.status = "completed"
    improve.completed_at = datetime.now(UTC)
    improve.branch = impl.branch
    improve.has_commits = True
    store.update(improve)
    store.get_or_create_merge_unit_for_task(improve)

    anchor = improve
    members: tuple[DbTask, ...] = (impl, review, improve)
    if anchor_type == "rebase":
        rebase = store.add(
            "Completed rebase",
            task_type="rebase",
            based_on=improve.id,
            same_branch=True,
        )
        assert rebase.id is not None
        rebase.status = "completed"
        rebase.completed_at = datetime.now(UTC)
        rebase.branch = impl.branch
        rebase.has_commits = True
        rebase.changed_diff = True
        store.update(rebase)
        store.get_or_create_merge_unit_for_task(rebase)
        anchor = rebase
        members = (impl, review, improve, rebase)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    row = LineageOwnerRow(
        owner_task=impl,
        members=members,
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="test",
        unresolved_tasks=(review,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=anchor,
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.determine_next_action", return_value={"type": "run_review", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    log_lines = log_path.read_text().splitlines()
    assert any(line.split(maxsplit=2)[1] == "START" and f"{impl.id} iterate" in line for line in log_lines)
    assert not any("resolves to" in line and "not completed task" in line for line in log_lines)


def test_watch_cycle_improve_routes_impl_chain_through_iterate_without_creating_child(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/improve-iterate"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    before_count = _task_count(store)
    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert _task_count(store) == before_count
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{impl.id} iterate" in line
        for line in log_path.read_text().splitlines()
    )


def test_watch_cycle_failed_iterate_launch_does_not_park_without_execution(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/no-progress-park"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    store.get_or_create_merge_unit_for_task(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=1) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )
        restarted_store = make_store(tmp_path)
        _run_cycle(
            config=Config.load(tmp_path),
            store=restarted_store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )

    assert spawn_iterate.call_count == 2
    assert store.list_watch_progress_observations(subject_kind="merge_unit", subject_id=str(impl.id)) == []
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert text.count("START_FAILED") == 2


def test_clear_watch_progress_subject_clears_persisted_observation_for_subject(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/no-progress-clear"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action={"type": "improve", "review_task": review},
        action_task=impl,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            streak=1,
            parked_reason=None,
            observed_at=datetime.now(UTC),
        )
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id="gza-999",
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="fp-clear-me",
            streak=1,
            next_retry_at=datetime.now(UTC) + timedelta(seconds=60),
            updated_at=datetime.now(UTC),
        )
    )

    clear_watch_progress_subject(store, subject_task=impl)

    assert store.list_watch_progress_observations(
        subject_kind="merge_unit",
        subject_id=unit.id,
    ) == []
    assert (
        store.get_watch_recovery_backoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
        )
        is None
    )


def test_background_no_progress_finalizer_ignores_nonterminal_launch_state(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None

    action = {"type": "iterate", "description": "pending queue iterate"}

    pending_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=impl,
        action=action,
        action_task_before=impl,
        action_task_after=impl,
        failed_task=None,
        no_progress_cycles=2,
    )

    impl_in_progress = store.get(impl.id)
    assert impl_in_progress is not None
    impl_in_progress.status = "in_progress"
    impl_in_progress.started_at = datetime.now(UTC)
    impl_in_progress.running_pid = 5151
    store.update(impl_in_progress)

    running_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=impl_in_progress,
        action=action,
        action_task_before=impl,
        action_task_after=impl_in_progress,
        failed_task=None,
        no_progress_cycles=2,
    )

    assert pending_attention is None
    assert running_attention is None
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id))
    assert len(observations) == 1
    assert observations[0].launch_evidence_fingerprint is not None
    assert observations[0].streak == 0
    assert observations[0].parked_reason is None


def test_background_no_progress_finalizer_parks_repeated_completed_no_progress_outcomes(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    store.update(impl)

    refreshed_impl = store.get(impl.id)
    assert refreshed_impl is not None
    action = {"type": "iterate", "description": "pending queue iterate"}

    first_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=refreshed_impl,
        action=action,
        action_task_before=refreshed_impl,
        action_task_after=refreshed_impl,
        failed_task=None,
        no_progress_cycles=2,
    )
    second_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=refreshed_impl,
        action=action,
        action_task_before=refreshed_impl,
        action_task_after=refreshed_impl,
        failed_task=None,
        no_progress_cycles=2,
    )

    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id))
    assert first_attention is None
    assert second_attention is not None
    assert second_attention["needs_attention_reason"] == WATCH_NO_PROGRESS_BACKSTOP_REASON
    assert len(observations) == 1
    assert observations[0].parked_reason == WATCH_NO_PROGRESS_BACKSTOP_REASON


def test_background_no_progress_finalizer_completed_noop_improve_still_parks(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    improve = store.add("Improve feature", task_type="improve", based_on=impl.id, depends_on=review.id)
    assert improve.id is not None
    improve.status = "completed"
    improve.changed_diff = False
    improve.completed_at = datetime.now(UTC)
    store.update(improve)

    refreshed_impl = store.get(impl.id)
    refreshed_review = store.get(review.id)
    refreshed_improve = store.get(improve.id)
    assert refreshed_impl is not None
    assert refreshed_review is not None
    assert refreshed_improve is not None
    action = {"type": "run_improve", "description": "Run existing improve", "review_task": refreshed_review}

    first_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=refreshed_impl,
        action=action,
        action_task_before=refreshed_improve,
        action_task_after=refreshed_improve,
        failed_task=None,
        no_progress_cycles=2,
    )
    second_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=refreshed_impl,
        action=action,
        action_task_before=refreshed_improve,
        action_task_after=refreshed_improve,
        failed_task=None,
        no_progress_cycles=2,
    )

    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id))
    assert first_attention is None
    assert second_attention is not None
    assert second_attention["needs_attention_reason"] == WATCH_NO_PROGRESS_BACKSTOP_REASON
    assert len(observations) == 1
    assert observations[0].parked_reason == WATCH_NO_PROGRESS_BACKSTOP_REASON
    assert observations[0].streak == 2


def test_watch_progress_candidate_treats_dispute_artifacts_as_progress(tmp_path: Path) -> None:
    from gza.runner import REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/watch-dispute-progress"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = (
        "## Summary\n\n- Found a blocker.\n\n"
        "## Blockers\n\n"
        "### B1 Missing API guard\n"
        "Evidence: the current code still accepts empty IDs.\n"
        "Open-state citation: `src/api.py:12-18`\n"
        "Impact: invalid requests can crash the handler.\n"
        "Required fix: reject empty IDs before calling the service.\n"
        "Required tests: add regression coverage for empty IDs.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )
    store.update(review)

    action = {
        "type": "create_review_adjudication",
        "description": "Create review-blocker-disputed adjudication for blocker B1",
    }
    before = build_watch_progress_candidate(store, subject_task=impl, action=action, action_task=review)

    store.add_artifact(
        review.id,
        kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND,
        label="disputed-B1",
        path=".gza/artifacts/disputed-b1.txt",
        byte_size=0,
        sha256="0" * 64,
        status="disputed",
        exit_status="already_satisfied",
        metadata={
            "schema_version": 1,
            "state": "disputed",
            "review_task_id": review.id,
            "impl_task_id": impl.id,
            "source_task_id": impl.id,
            "source_task_type": "improve",
            "finding_id": "B1",
            "reason": "already_satisfied",
            "evidence": "The guard already exists on the current branch tip.",
            "current_state_citation": "`src/api.py:12-18`",
            "finding_fingerprint": {
                "title": "missing api guard",
                "anchor": "src/api.py:12-18",
            },
        },
        created_at=datetime.now(UTC),
    )

    after = build_watch_progress_candidate(store, subject_task=impl, action=action, action_task=review)

    assert before.evidence_fingerprint != after.evidence_fingerprint


def test_watch_cycle_pending_dispatch_failed_launch_does_not_park_without_execution(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._spawn_background_iterate", return_value=1) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )
    assert spawn_iterate.call_count == 2
    assert store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id)) == []
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert any("START_FAILED" in line and str(impl.id) in line for line in text.splitlines())
    assert not any("START" in line and f"{impl.id} iterate" in line for line in text.splitlines())


def test_watch_cycle_pending_dispatch_launch_success_without_terminal_outcome_does_not_park(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )

    assert spawn_iterate.call_count == 2
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id))
    assert len(observations) == 1
    assert observations[0].launch_evidence_fingerprint is not None
    assert observations[0].streak == 0
    assert observations[0].parked_reason is None
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert any("START" in line and f"{impl.id} implement" in line for line in text.splitlines())


def test_watch_cycle_pending_dispatch_running_launch_state_does_not_park(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    def mark_running(
        _args: argparse.Namespace,
        _config: Config,
        task: object,
        **_kwargs: object,
    ) -> int:
        task_id = getattr(task, "id", None)
        assert isinstance(task_id, str)
        running_task = store.get(task_id)
        assert running_task is not None
        running_task.status = "in_progress"
        running_task.started_at = datetime.now(UTC)
        running_task.running_pid = 7777
        store.update(running_task)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=mark_running) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )

    assert spawn_iterate.call_count == 1
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id))
    assert len(observations) == 1
    assert observations[0].launch_evidence_fingerprint is not None
    assert observations[0].streak == 0
    assert observations[0].parked_reason is None
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert any("START" in line and f"{impl.id} implement" in line for line in text.splitlines())


def test_watch_cycle_recovery_relaunch_launch_success_without_terminal_outcome_does_not_park(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert spawn_iterate.call_count == 3
    assert len(store.get_based_on_children(failed.id)) == 1
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(failed.id))
    assert len(observations) == 1
    assert observations[0].launch_evidence_fingerprint is not None
    assert observations[0].streak == 0
    assert observations[0].parked_reason is None
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert any("RECOVR" in line and f"{failed.id} resume via iterate" in line for line in text.splitlines())


def test_recovery_deferred_background_terminal_no_progress_parks_after_repeated_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-deferred-terminal"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    real_build_candidate = build_watch_progress_candidate

    def stable_candidate(*args: object, **kwargs: object) -> WatchProgressCandidate:
        candidate = real_build_candidate(*args, **kwargs)
        subject_task = kwargs.get("subject_task")
        if getattr(subject_task, "id", None) == failed.id:
            return replace(candidate, evidence_fingerprint=f"stable:{failed.id}:{candidate.action_type}")
        return candidate

    monkeypatch.setattr("gza.cli.watch.build_watch_progress_candidate", stable_candidate)
    action = {"type": "resume", "description": "Resume failed task after MAX_TURNS"}

    first_child = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert first_child.id is not None
    first_child.status = "in_progress"
    first_child.started_at = datetime.now(UTC)
    first_child.running_pid = 8118
    store.update(first_child)

    first_launch_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=failed,
        action=action,
        action_task_before=failed,
        action_task_after=first_child,
        failed_task=failed,
        no_progress_cycles=2,
    )
    first_child.status = "failed"
    first_child.failure_reason = "MAX_TURNS"
    first_child.completed_at = datetime.now(UTC)
    store.update(first_child)
    first_terminal_attention = _maybe_park_watch_no_progress(
        store=store,
        subject_task=failed,
        action=action,
        action_task=failed,
        failed_task=failed,
        no_progress_cycles=2,
    )

    second_child = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert second_child.id is not None
    second_child.status = "in_progress"
    second_child.started_at = datetime.now(UTC)
    second_child.running_pid = 8229
    store.update(second_child)

    second_launch_attention = _maybe_finalize_watch_no_progress_for_background_action(
        store=store,
        subject_task=failed,
        action=action,
        action_task_before=failed,
        action_task_after=second_child,
        failed_task=failed,
        no_progress_cycles=2,
    )
    second_child.status = "failed"
    second_child.failure_reason = "MAX_TURNS"
    second_child.completed_at = datetime.now(UTC)
    store.update(second_child)
    second_terminal_attention = _maybe_park_watch_no_progress(
        store=store,
        subject_task=failed,
        action=action,
        action_task=failed,
        failed_task=failed,
        no_progress_cycles=2,
    )

    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(failed.id))
    assert first_launch_attention is None
    assert first_terminal_attention is None
    assert second_launch_attention is None
    assert second_terminal_attention is not None
    assert second_terminal_attention["needs_attention_reason"] == WATCH_NO_PROGRESS_BACKSTOP_REASON
    assert len(observations) == 1
    assert observations[0].parked_reason == WATCH_NO_PROGRESS_BACKSTOP_REASON
    assert observations[0].streak == 2


def test_improve_deferred_background_transient_terminal_does_not_park_and_preserves_real_streak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    store.update(impl)

    config = Config.load(tmp_path)
    action = {"type": "improve", "description": "Create improve task", "review_task": review}
    real_build_candidate = build_watch_progress_candidate

    def stable_candidate(*args: object, **kwargs: object) -> WatchProgressCandidate:
        candidate = real_build_candidate(*args, **kwargs)
        subject_task = kwargs.get("subject_task")
        if getattr(subject_task, "id", None) == impl.id and candidate.action_type == "improve":
            return replace(candidate, evidence_fingerprint=f"stable:{impl.id}:improve")
        return candidate

    monkeypatch.setattr("gza.cli.watch.build_watch_progress_candidate", stable_candidate)
    seeded_candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=action,
        action_task=review,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=seeded_candidate.subject_kind,
            subject_id=seeded_candidate.subject_id,
            action_type=seeded_candidate.action_type,
            action_reason=seeded_candidate.action_reason,
            subject_task_id=seeded_candidate.subject_task_id,
            action_task_id=seeded_candidate.action_task_id,
            action_task_status=seeded_candidate.action_task_status,
            action_task_started_at=seeded_candidate.action_task_started_at,
            action_task_running_pid=seeded_candidate.action_task_running_pid,
            failed_task_id=seeded_candidate.failed_task_id,
            recovery_task_id=seeded_candidate.recovery_task_id,
            merge_unit_id=seeded_candidate.merge_unit_id,
            merge_unit_state=seeded_candidate.merge_unit_state,
            merge_unit_head_sha=seeded_candidate.merge_unit_head_sha,
            evidence_fingerprint=seeded_candidate.evidence_fingerprint,
            streak=1,
            parked_reason=None,
            observed_at=datetime.now(UTC),
        )
    )

    first_improve = store.add("Improve attempt 1", task_type="improve", based_on=impl.id, depends_on=review.id)
    assert first_improve.id is not None
    first_improve.status = "in_progress"
    first_improve.started_at = datetime.now(UTC)
    first_improve.running_pid = 8118
    store.update(first_improve)

    first_launch_attention = _maybe_finalize_watch_no_progress_for_background_action(
        config=config,
        store=store,
        subject_task=impl,
        action=action,
        action_task_before=review,
        action_task_after=first_improve,
        failed_task=None,
        no_progress_cycles=2,
    )
    first_improve.status = "failed"
    first_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    first_improve.recovery_origin = "retry"
    first_improve.completed_at = datetime.now(UTC)
    store.update(first_improve)
    first_terminal_attention = _maybe_park_watch_no_progress(
        config=config,
        store=store,
        subject_task=impl,
        action=action,
        action_task=review,
        failed_task=None,
        no_progress_cycles=2,
    )

    second_improve = store.add(
        "Improve attempt 2",
        task_type="improve",
        based_on=first_improve.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert second_improve.id is not None
    second_improve.status = "in_progress"
    second_improve.started_at = datetime.now(UTC)
    second_improve.running_pid = 8229
    store.update(second_improve)

    second_launch_attention = _maybe_finalize_watch_no_progress_for_background_action(
        config=config,
        store=store,
        subject_task=impl,
        action=action,
        action_task_before=review,
        action_task_after=second_improve,
        failed_task=None,
        no_progress_cycles=2,
    )
    second_improve.status = "failed"
    second_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    second_improve.completed_at = datetime.now(UTC)
    store.update(second_improve)
    second_terminal_attention = _maybe_park_watch_no_progress(
        config=config,
        store=store,
        subject_task=impl,
        action=action,
        action_task=review,
        failed_task=None,
        no_progress_cycles=2,
    )

    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id))
    backoff = store.get_watch_recovery_backoff(
        subject_kind="lineage",
        subject_id=str(impl.id),
        action_type="improve",
        action_reason="Create improve task",
    )

    assert first_launch_attention is None
    assert _watch_no_progress_result_deferred_for_transient_backoff(first_terminal_attention)
    assert second_launch_attention is None
    assert _watch_no_progress_result_deferred_for_transient_backoff(second_terminal_attention)
    assert len(observations) == 1
    assert observations[0].streak == 1
    assert observations[0].parked_reason is None
    assert observations[0].launch_evidence_fingerprint is None
    assert backoff is not None
    assert backoff.last_failure_task_id == second_improve.id
    assert backoff.last_failure_reason == "PROVIDER_UNAVAILABLE"
    assert backoff.streak == 2


def test_transient_watch_recovery_backoff_is_idempotent_for_same_failed_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 22, 0, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    failed = store.add("Failed improve", task_type="improve")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PROVIDER_UNAVAILABLE"
    failed.recovery_origin = "retry"
    failed.completed_at = FrozenDateTime.current
    store.update(failed)

    action = {"type": "improve", "description": "Create improve task"}
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=action,
        action_task=failed,
        failed_task=None,
    )

    first = _maybe_skip_watch_no_progress_for_transient_terminal(
        config=Config.load(tmp_path),
        store=store,
        subject_task=failed,
        action=action,
        action_task=failed,
        failed_task=None,
        candidate=candidate,
    )
    first_backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    assert first is True
    assert first_backoff is not None
    assert first_backoff.streak == 1
    assert first_backoff.next_retry_at == FrozenDateTime.current + timedelta(seconds=60)

    FrozenDateTime.current = FrozenDateTime.current + timedelta(seconds=5)
    second = _maybe_skip_watch_no_progress_for_transient_terminal(
        config=Config.load(tmp_path),
        store=store,
        subject_task=failed,
        action=action,
        action_task=failed,
        failed_task=None,
        candidate=candidate,
    )
    second_backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    assert second is True
    assert second_backoff is not None
    assert second_backoff.streak == 1
    assert second_backoff.next_retry_at == first_backoff.next_retry_at


def test_transient_watch_recovery_backoff_advances_for_new_failed_attempt_on_same_subject_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 22, 10, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    first_failed = store.add("Failed improve 1", task_type="improve")
    assert first_failed.id is not None
    first_failed.status = "failed"
    first_failed.failure_reason = "PROVIDER_UNAVAILABLE"
    first_failed.recovery_origin = "retry"
    first_failed.completed_at = FrozenDateTime.current
    store.update(first_failed)

    action = {"type": "improve", "description": "Create improve task"}
    candidate = build_watch_progress_candidate(
        store,
        subject_task=first_failed,
        action=action,
        action_task=first_failed,
        failed_task=None,
    )

    first = _maybe_skip_watch_no_progress_for_transient_terminal(
        config=Config.load(tmp_path),
        store=store,
        subject_task=first_failed,
        action=action,
        action_task=first_failed,
        failed_task=None,
        candidate=candidate,
    )
    first_backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    second_failed = store.add(
        "Failed improve 2",
        task_type="improve",
        based_on=first_failed.id,
        recovery_origin="retry",
    )
    assert second_failed.id is not None
    second_failed.status = "failed"
    second_failed.failure_reason = "WORKER_DIED"
    second_failed.completed_at = FrozenDateTime.current + timedelta(seconds=5)
    store.update(second_failed)

    FrozenDateTime.current = FrozenDateTime.current + timedelta(seconds=5)
    second = _maybe_skip_watch_no_progress_for_transient_terminal(
        config=Config.load(tmp_path),
        store=store,
        subject_task=first_failed,
        action=action,
        action_task=second_failed,
        failed_task=None,
        candidate=candidate,
    )
    second_backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    assert first is True
    assert second is True
    assert first_backoff is not None
    assert second_backoff is not None
    assert first_backoff.streak == 1
    assert first_backoff.next_retry_at == datetime(2026, 6, 24, 22, 11, tzinfo=UTC)
    assert second_backoff.last_failure_task_id == second_failed.id
    assert second_backoff.last_failure_reason == "WORKER_DIED"
    assert second_backoff.last_failure_fingerprint == "transient:improve:WORKER_DIED"
    assert second_backoff.streak == 2
    assert second_backoff.next_retry_at == datetime(2026, 6, 24, 22, 12, 5, tzinfo=UTC)


def test_watch_cycle_recovery_backoff_blocks_until_due_then_launches(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PROVIDER_UNAVAILABLE"
    failed.session_id = "sess-backoff-gate"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=Config.load(tmp_path).max_resume_attempts)
    recovery_action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=recovery_action,
        action_task=failed,
        failed_task=failed,
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="fp-pending-retry",
            streak=1,
            next_retry_at=datetime.now(UTC) + timedelta(seconds=60),
            updated_at=datetime.now(UTC),
        )
    )

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    started_task_ids: list[str] = []

    def record_resume_spawn(
        _args: argparse.Namespace,
        _config: Config,
        task_id: str,
        **_kwargs: object,
    ) -> int:
        started_task_ids.append(task_id)
        return 0

    def record_worker_spawn(
        _args: argparse.Namespace,
        _config: Config,
        task_id: str,
        **_kwargs: object,
    ) -> int:
        started_task_ids.append(task_id)
        return 0

    def record_iterate_spawn(
        _args: argparse.Namespace,
        _config: Config,
        task: object,
        **_kwargs: object,
    ) -> int:
        task_id = getattr(task, "id", None)
        assert isinstance(task_id, str)
        started_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=record_resume_spawn),
        patch("gza.cli.watch._spawn_background_worker", side_effect=record_worker_spawn),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=record_iterate_spawn),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

        store.upsert_watch_recovery_backoff(
            WatchRecoveryBackoff(
                subject_kind=candidate.subject_kind,
                subject_id=candidate.subject_id,
                action_type=candidate.action_type,
                action_reason=candidate.action_reason,
                subject_task_id=candidate.subject_task_id,
                last_failure_task_id=failed.id,
                last_failure_reason="PROVIDER_UNAVAILABLE",
                last_failure_fingerprint="fp-pending-retry",
                streak=1,
                next_retry_at=datetime.now(UTC) - timedelta(seconds=1),
                updated_at=datetime.now(UTC),
            )
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert len(started_task_ids) == 1
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{failed.id} {candidate.action_type} delayed" in log_text


def test_watch_cycle_pending_implement_retry_respects_active_transient_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 22, 20, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PROVIDER_UNAVAILABLE"
    failed.session_id = "sess-pending-implement-retry"
    failed.completed_at = FrozenDateTime.current
    store.update(failed)

    config = Config.load(tmp_path)
    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=config.max_resume_attempts)
    recovery_action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=recovery_action,
        action_task=failed,
        failed_task=failed,
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:implement:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=FrozenDateTime.current + timedelta(seconds=60),
            updated_at=FrozenDateTime.current,
        )
    )

    pending_retry = store.add(
        "Pending implement retry",
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None

    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([], RecoveryReadContext())),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_retry]),
        patch(
            "gza.cli.watch.build_dispatch_preview",
            return_value=DispatchPreview(
                entries=(
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_retry,
                        runnable=True,
                        worker_consuming=True,
                    ),
                )
            ),
        ),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("implement recovery should stay pending during backoff")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("implement recovery should iterate when due")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("implement recovery should not spawn before cooldown is due")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is False
    assert result.pending == 1
    assert store.list_watch_progress_observations(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
    ) == []
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{failed.id} retry delayed" in log_text
    assert not any(
        line.split(maxsplit=2)[1] == "START" and pending_retry.id in line
        for line in log_text.splitlines()
    )


def test_watch_cycle_pending_improve_recovery_worker_respects_active_transient_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 22, 30, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = FrozenDateTime.current
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = FrozenDateTime.current
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    failed_improve.completed_at = FrozenDateTime.current
    store.update(failed_improve)

    improve_action = {
        "type": "improve",
        "description": "Create improve task (review CHANGES_REQUESTED)",
        "review_task": review,
    }
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=improve_action,
        action_task=failed_improve,
        failed_task=failed_improve,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            action_task_started_at=candidate.action_task_started_at,
            action_task_running_pid=candidate.action_task_running_pid,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            streak=1,
            parked_reason=None,
            observed_at=FrozenDateTime.current,
        )
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed_improve.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:improve:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=FrozenDateTime.current + timedelta(seconds=60),
            updated_at=FrozenDateTime.current,
        )
    )

    pending_retry = store.add(
        "Pending improve retry",
        task_type="improve",
        based_on=failed_improve.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    started_task_ids: list[str] = []

    def record_worker_spawn(
        _args: argparse.Namespace,
        _config: Config,
        task_id: str,
        **_kwargs: object,
    ) -> int:
        started_task_ids.append(task_id)
        return 0

    patches = (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([], RecoveryReadContext())),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_retry]),
        patch(
            "gza.cli.watch.build_dispatch_preview",
            return_value=DispatchPreview(
                entries=(
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_retry,
                        runnable=True,
                        worker_consuming=True,
                    ),
                )
            ),
        ),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("improve recovery should stay on worker path")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("improve recovery should stay on worker path")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=record_worker_spawn),
    )

    with contextlib.ExitStack() as stack:
        for active_patch in patches:
            stack.enter_context(active_patch)
        first_result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            max_recovery_attempts=config.max_resume_attempts,
        )
        first_log_text = log_path.read_text()
        first_started_task_ids = list(started_task_ids)

        FrozenDateTime.current = FrozenDateTime.current + timedelta(seconds=5)
        second_blocked_result = _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            max_recovery_attempts=config.max_resume_attempts,
        )
        second_blocked_log_text = log_path.read_text()
        second_blocked_started_task_ids = list(started_task_ids)

        FrozenDateTime.current = FrozenDateTime.current + timedelta(seconds=61)
        due_result = _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert first_result.pending == 1
    assert first_started_task_ids == []
    assert second_blocked_result.pending == 1
    assert second_blocked_started_task_ids == []
    assert second_blocked_log_text.count("BACKOFF") == 1
    assert not any(
        line.split(maxsplit=2)[1] == "START" and pending_retry.id in line
        for line in first_log_text.splitlines()
    )
    assert due_result.work_done is True
    assert started_task_ids == [pending_retry.id]
    log_text = log_path.read_text()
    assert log_text.count("BACKOFF") == 1
    assert f"{impl.id} improve delayed" in log_text


def test_watch_cycle_improve_transient_terminal_defers_same_cycle_relaunch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 23, 0, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = FrozenDateTime.current
    impl.branch = "feature/improve-backoff"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = FrozenDateTime.current
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease retry later."
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    failed_improve.completed_at = FrozenDateTime.current
    store.update(failed_improve)

    improve_action = {
        "type": "improve",
        "description": "Create improve task",
        "review_task": review,
    }
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=improve_action,
        action_task=impl,
        failed_task=None,
    )

    row = LineageOwnerRow(
        owner_task=impl,
        members=(impl, review, failed_improve),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="review",
        unresolved_tasks=(impl,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=impl,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[]),
        patch("gza.cli.watch.determine_next_action", return_value=improve_action),
        patch(
            "gza.cli.watch.execute_advance_action",
            side_effect=AssertionError("improve launch should defer behind initial transient cooldown"),
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )

    backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    assert result.work_done is False
    assert backoff is not None
    assert backoff.last_failure_task_id == failed_improve.id
    assert backoff.streak == 1
    assert backoff.next_retry_at == FrozenDateTime.current + timedelta(seconds=60)
    assert (
        store.get_watch_progress_observation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
        )
        is None
    )
    assert get_active_watch_no_progress_attention(store, candidate=candidate) is None
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{impl.id} improve delayed 60s" in log_text
    assert "START" not in log_text


def test_watch_cycle_pending_improve_retry_respects_active_transient_backoff_without_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 23, 5, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = FrozenDateTime.current
    impl.branch = "feature/improve-backoff"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = FrozenDateTime.current
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease retry later."
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    failed_improve.completed_at = FrozenDateTime.current
    store.update(failed_improve)

    improve_action = {
        "type": "improve",
        "description": "Create improve task",
        "review_task": review,
    }
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=improve_action,
        action_task=impl,
        failed_task=None,
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed_improve.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:improve:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=FrozenDateTime.current + timedelta(seconds=60),
            updated_at=FrozenDateTime.current,
        )
    )

    pending_retry = store.add(
        "Pending improve retry",
        task_type="improve",
        based_on=failed_improve.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None

    unrelated = store.add("Unrelated pending review", task_type="review")
    assert unrelated.id is not None

    log_path = tmp_path / ".gza" / "watch.log"
    started_worker_task_ids: list[str] = []

    def _record_worker_start(_args: argparse.Namespace, _config: Config, *, task_id: str, **_kwargs: object) -> int:
        started_worker_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([], RecoveryReadContext())),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_retry, unrelated]),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("pending improve retry should not iterate before cooldown is due"),
        ),
        patch(
            "gza.cli.watch._spawn_background_resume_worker",
            side_effect=AssertionError("pending improve retry should not resume before cooldown is due"),
        ),
        patch("gza.cli.watch._spawn_background_worker", side_effect=_record_worker_start),
    ):
        result = _run_cycle(
            config=Config.load(tmp_path),
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
        )

    assert result.work_done is True
    assert pending_retry.status == "pending"
    assert started_worker_task_ids == [unrelated.id]
    assert store.list_watch_progress_observations(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
    ) == []
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{impl.id} improve delayed 60s" in log_text
    assert not any(
        line.split(maxsplit=2)[1] == "START" and pending_retry.id in line
        for line in log_text.splitlines()
    )
    assert any(
        line.split(maxsplit=2)[1] == "START" and unrelated.id in line
        for line in log_text.splitlines()
    )


def test_watch_cycle_pending_review_retry_respects_active_transient_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 22, 40, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    failed = store.add("Failed review", task_type="review")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PROVIDER_UNAVAILABLE"
    failed.session_id = "sess-pending-review-retry"
    failed.completed_at = FrozenDateTime.current
    store.update(failed)

    config = Config.load(tmp_path)
    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=config.max_resume_attempts)
    recovery_action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=recovery_action,
        action_task=failed,
        failed_task=failed,
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:review:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=FrozenDateTime.current + timedelta(seconds=60),
            updated_at=FrozenDateTime.current,
        )
    )

    pending_retry = store.add(
        "Pending review retry",
        task_type="review",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None

    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([], RecoveryReadContext())),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_retry]),
        patch(
            "gza.cli.watch.build_dispatch_preview",
            return_value=DispatchPreview(
                entries=(
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_retry,
                        runnable=True,
                        worker_consuming=True,
                    ),
                )
            ),
        ),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("review retry should stay on worker path")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("review retry should not iterate")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("review retry should not spawn before cooldown is due")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is False
    assert result.pending == 1
    assert store.list_watch_progress_observations(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
    ) == []
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{failed.id} retry delayed" in log_text
    assert not any(
        line.split(maxsplit=2)[1] == "START" and pending_retry.id in line
        for line in log_text.splitlines()
    )


def test_watch_cycle_failed_recovery_transient_terminal_defers_same_cycle_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_resume_attempts: 2\n")
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 23, 10, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PROVIDER_UNAVAILABLE"
    failed.session_id = "sess-recovery-transient"
    failed.completed_at = FrozenDateTime.current
    store.update(failed)

    failed_retry = store.add(
        "Failed retry attempt",
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "WORKER_DIED"
    failed_retry.completed_at = FrozenDateTime.current
    store.update(failed_retry)

    config = Config.load(tmp_path)
    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=config.max_resume_attempts)
    recovery_action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=recovery_action,
        action_task=failed,
        failed_task=failed,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=failed_retry.id,
            action_task_status=failed_retry.status,
            action_task_started_at=failed_retry.started_at,
            action_task_running_pid=failed_retry.running_pid,
            failed_task_id=failed_retry.id,
            recovery_task_id=failed_retry.id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            launch_evidence_fingerprint="launch:retry",
            streak=1,
            parked_reason=None,
            observed_at=FrozenDateTime.current,
        )
    )

    row = LineageOwnerRow(
        owner_task=failed,
        members=(failed, failed_retry),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="recovery",
        unresolved_tasks=(failed,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=failed,
        recovery_leaf_task=failed,
    )

    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[]),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("recovery launch should defer behind initial transient cooldown"),
        ),
        patch(
            "gza.cli.watch._spawn_background_worker",
            side_effect=AssertionError("worker retry should not spawn during transient cooldown"),
        ),
        patch(
            "gza.cli.watch._spawn_background_resume_worker",
            side_effect=AssertionError("resume worker should not spawn during transient cooldown"),
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    assert result.work_done is False
    assert backoff is not None
    assert backoff.last_failure_task_id == failed_retry.id
    assert backoff.streak == 1
    assert backoff.next_retry_at == FrozenDateTime.current + timedelta(seconds=60)
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{failed.id} retry delayed 60s" in log_text
    assert "RECOVR" not in log_text


def test_watch_cycle_failed_recovery_transient_terminal_without_deferred_observation_still_persists_backoff_and_skips_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    (tmp_path / "gza.yaml").write_text((tmp_path / "gza.yaml").read_text() + "max_resume_attempts: 2\n")
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 23, 11, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PROVIDER_UNAVAILABLE"
    failed.session_id = "sess-recovery-transient-no-observation"
    failed.completed_at = FrozenDateTime.current
    store.update(failed)

    failed_retry = store.add(
        "Failed retry attempt",
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "WORKER_DIED"
    failed_retry.completed_at = FrozenDateTime.current
    store.update(failed_retry)

    config = Config.load(tmp_path)
    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=config.max_resume_attempts)
    recovery_action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=recovery_action,
        action_task=failed,
        failed_task=failed,
    )

    row = LineageOwnerRow(
        owner_task=failed,
        members=(failed, failed_retry),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="recovery",
        unresolved_tasks=(failed,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=failed,
        recovery_leaf_task=failed,
    )

    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[]),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("recovery launch should defer behind initial transient cooldown"),
        ),
        patch(
            "gza.cli.watch._spawn_background_worker",
            side_effect=AssertionError("worker retry should not spawn during transient cooldown"),
        ),
        patch(
            "gza.cli.watch._spawn_background_resume_worker",
            side_effect=AssertionError("resume worker should not spawn during transient cooldown"),
        ),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            recovery_slots=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    assert result.work_done is False
    assert store.list_watch_progress_observations(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
    ) == []
    assert backoff is not None
    assert backoff.last_failure_task_id == failed_retry.id
    assert backoff.streak == 1
    assert backoff.next_retry_at == FrozenDateTime.current + timedelta(seconds=60)
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{failed.id} retry delayed 60s" in log_text
    assert "RECOVR" not in log_text


def test_watch_cycle_active_recovery_backoff_does_not_consume_pending_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 23, 20, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PROVIDER_UNAVAILABLE"
    failed.session_id = "sess-recovery-backoff-slot"
    failed.completed_at = FrozenDateTime.current
    store.update(failed)

    pending_plan = store.add("Unrelated pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=config.max_resume_attempts)
    recovery_action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=recovery_action,
        action_task=failed,
        failed_task=failed,
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:implement:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=FrozenDateTime.current + timedelta(seconds=60),
            updated_at=FrozenDateTime.current,
        )
    )

    row = LineageOwnerRow(
        owner_task=failed,
        members=(failed,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="recovery",
        unresolved_tasks=(failed,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=None,
        recovery_action_task=failed,
        recovery_leaf_task=failed,
    )

    log_path = tmp_path / ".gza" / "watch.log"
    started_task_ids: list[str] = []

    def record_pending_start(
        _args: argparse.Namespace,
        _config: Config,
        task_id: str,
        **_kwargs: object,
    ) -> int:
        started_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_plan]),
        patch(
            "gza.cli.watch.build_dispatch_preview",
            return_value=DispatchPreview(
                entries=(
                    DispatchPreviewEntry(
                        lane="recovery",
                        task=failed,
                        runnable=True,
                        worker_consuming=True,
                        owner_task=failed,
                        decision=decision,
                    ),
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_plan,
                        runnable=True,
                        worker_consuming=True,
                    ),
                )
            ),
        ),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("recovery launch should stay deferred during active cooldown"),
        ),
        patch(
            "gza.cli.watch._spawn_background_resume_worker",
            side_effect=AssertionError("resume worker should stay deferred during active cooldown"),
        ),
        patch("gza.cli.watch._spawn_background_worker", side_effect=record_pending_start),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            recovery_slots=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert started_task_ids == [pending_plan.id]
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{failed.id} {candidate.action_type} delayed" in log_text
    assert "RECOVR" not in log_text
    assert any(
        line.split(maxsplit=2)[1] == "START" and pending_plan.id in line
        for line in log_text.splitlines()
    )


def test_watch_cycle_active_improve_recovery_backoff_does_not_consume_pending_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 23, 25, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = FrozenDateTime.current
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = FrozenDateTime.current
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    failed_improve.completed_at = FrozenDateTime.current
    store.update(failed_improve)

    improve_action = {
        "type": "improve",
        "description": "Create improve task (review CHANGES_REQUESTED)",
        "review_task": review,
    }
    observed_candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=improve_action,
        action_task=failed_improve,
        failed_task=failed_improve,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=observed_candidate.subject_kind,
            subject_id=observed_candidate.subject_id,
            action_type=observed_candidate.action_type,
            action_reason=observed_candidate.action_reason,
            subject_task_id=observed_candidate.subject_task_id,
            action_task_id=failed_improve.id,
            action_task_status=failed_improve.status,
            action_task_started_at=failed_improve.started_at,
            action_task_running_pid=failed_improve.running_pid,
            failed_task_id=failed_improve.id,
            recovery_task_id=failed_improve.id,
            merge_unit_id=observed_candidate.merge_unit_id,
            merge_unit_state=observed_candidate.merge_unit_state,
            merge_unit_head_sha=observed_candidate.merge_unit_head_sha,
            evidence_fingerprint=observed_candidate.evidence_fingerprint,
            streak=1,
            parked_reason=None,
            observed_at=FrozenDateTime.current,
        )
    )
    pending_retry = store.add(
        "Pending improve retry",
        task_type="improve",
        based_on=failed_improve.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None

    pending_plan = store.add("Unrelated pending plan", task_type="plan")
    assert pending_plan.id is not None

    config = Config.load(tmp_path)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=improve_action,
        action_task=pending_retry,
        failed_task=failed_improve,
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed_improve.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:improve:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=FrozenDateTime.current + timedelta(seconds=60),
            updated_at=FrozenDateTime.current,
        )
    )
    log_path = tmp_path / ".gza" / "watch.log"
    started_task_ids: list[str] = []

    def record_pending_start(
        _args: argparse.Namespace,
        _config: Config,
        task_id: str,
        **_kwargs: object,
    ) -> int:
        started_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_retry, pending_plan]),
        patch(
            "gza.cli.watch.build_dispatch_preview",
            return_value=DispatchPreview(
                entries=(
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_retry,
                        runnable=True,
                        worker_consuming=True,
                    ),
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_plan,
                        runnable=True,
                        worker_consuming=True,
                    ),
                )
            ),
        ),
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("improve recovery launch should stay deferred during active cooldown"),
        ),
        patch(
            "gza.cli.watch._spawn_background_resume_worker",
            side_effect=AssertionError("improve resume worker should stay deferred during active cooldown"),
        ),
        patch("gza.cli.watch._spawn_background_worker", side_effect=record_pending_start),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            recovery_slots=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert started_task_ids == [pending_plan.id]
    log_text = log_path.read_text()
    assert "BACKOFF" in log_text
    assert f"{impl.id} {candidate.action_type} delayed" in log_text
    assert "RECOVR" not in log_text
    assert any(
        line.split(maxsplit=2)[1] == "START" and pending_plan.id in line
        for line in log_text.splitlines()
    )


def test_watch_cycle_due_improve_transient_backoff_launches_again(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 25, 0, 5, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = FrozenDateTime.current
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = FrozenDateTime.current
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    failed_improve.completed_at = FrozenDateTime.current
    store.update(failed_improve)

    pending_retry = store.add(
        "Pending improve retry",
        task_type="improve",
        based_on=failed_improve.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None

    improve_action = {
        "type": "improve",
        "description": "Create improve task (review CHANGES_REQUESTED)",
        "review_task": review,
    }
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=improve_action,
        action_task=pending_retry,
        failed_task=failed_improve,
    )
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed_improve.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:improve:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=FrozenDateTime.current - timedelta(seconds=1),
            updated_at=FrozenDateTime.current,
        )
    )
    no_progress_attention = _maybe_park_watch_no_progress(
        config=Config.load(tmp_path),
        store=store,
        subject_task=impl,
        action=improve_action,
        action_task=pending_retry,
        failed_task=failed_improve,
        no_progress_cycles=2,
    )
    preserved_backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    preserved_observation = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    log_path = tmp_path / ".gza" / "watch.log"
    launched_task_ids: list[str] = []

    def record_launch(
        _args: argparse.Namespace,
        _config: Config,
        task_id: str,
        **_kwargs: object,
    ) -> int:
        launched_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([], RecoveryReadContext())),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_retry]),
        patch(
            "gza.cli.watch.build_dispatch_preview",
            return_value=DispatchPreview(
                entries=(
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_retry,
                        runnable=True,
                        worker_consuming=True,
                    ),
                )
            ),
        ),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("improve retry should iterate")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("due improve retry should not iterate")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=record_launch),
    ):
        result = _run_cycle(
            config=Config.load(tmp_path),
            store=store,
            batch=1,
            recovery_slots=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=Config.load(tmp_path).max_resume_attempts,
        )

    assert result.work_done is True
    assert launched_task_ids == [pending_retry.id]
    log_text = log_path.read_text()
    assert "BACKOFF" not in log_text
    assert not any(
        line.split(maxsplit=2)[1] == "START" and impl.id in line and "delayed" in line
        for line in log_text.splitlines()
    )


def test_watch_cycle_due_improve_transient_backoff_preserves_prior_streak_and_does_not_extend_same_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 25, 0, 15, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = FrozenDateTime.current
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = FrozenDateTime.current
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    failed_improve.completed_at = FrozenDateTime.current
    store.update(failed_improve)

    pending_retry = store.add(
        "Pending improve retry",
        task_type="improve",
        based_on=failed_improve.id,
        depends_on=review.id,
        recovery_origin="retry",
    )
    assert pending_retry.id is not None

    improve_action = {
        "type": "improve",
        "description": "Create improve task (review CHANGES_REQUESTED)",
        "review_task": review,
    }
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=improve_action,
        action_task=pending_retry,
        failed_task=failed_improve,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=failed_improve.id,
            action_task_status=failed_improve.status,
            action_task_started_at=failed_improve.started_at,
            action_task_running_pid=failed_improve.running_pid,
            failed_task_id=failed_improve.id,
            recovery_task_id=failed_improve.id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            launch_evidence_fingerprint="launch:improve",
            streak=1,
            parked_reason=None,
            observed_at=FrozenDateTime.current,
        )
    )
    due_retry_at = FrozenDateTime.current - timedelta(seconds=1)
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed_improve.id,
            last_failure_reason="PROVIDER_UNAVAILABLE",
            last_failure_fingerprint="transient:improve:PROVIDER_UNAVAILABLE",
            streak=1,
            next_retry_at=due_retry_at,
            updated_at=FrozenDateTime.current,
        )
    )
    no_progress_attention = _maybe_park_watch_no_progress(
        config=Config.load(tmp_path),
        store=store,
        subject_task=impl,
        action=improve_action,
        action_task=pending_retry,
        failed_task=failed_improve,
        no_progress_cycles=2,
    )
    preserved_backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    preserved_observation = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )

    log_path = tmp_path / ".gza" / "watch.log"
    launched_task_ids: list[str] = []

    def record_launch(
        _args: argparse.Namespace,
        _config: Config,
        task_id: str,
        **_kwargs: object,
    ) -> int:
        launched_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([], RecoveryReadContext())),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[pending_retry]),
        patch(
            "gza.cli.watch.build_dispatch_preview",
            return_value=DispatchPreview(
                entries=(
                    DispatchPreviewEntry(
                        lane="pending",
                        task=pending_retry,
                        runnable=True,
                        worker_consuming=True,
                    ),
                )
            ),
        ),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("improve retry should iterate")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("due improve retry should not iterate")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=record_launch),
    ):
        result = _run_cycle(
            config=Config.load(tmp_path),
            store=store,
            batch=1,
            recovery_slots=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=Config.load(tmp_path).max_resume_attempts,
        )

    refreshed_backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    assert no_progress_attention is None
    assert preserved_backoff is not None
    assert preserved_backoff.last_failure_task_id == failed_improve.id
    assert preserved_backoff.streak == 1
    assert preserved_backoff.next_retry_at == due_retry_at
    assert preserved_observation is not None
    assert preserved_observation.streak == 1
    assert preserved_observation.parked_reason is None
    assert preserved_observation.launch_evidence_fingerprint is None
    assert result.work_done is True
    assert launched_task_ids == [pending_retry.id]
    if refreshed_backoff is not None:
        assert refreshed_backoff.last_failure_task_id == failed_improve.id
        assert refreshed_backoff.streak == 1
        assert refreshed_backoff.next_retry_at == due_retry_at
    log_text = log_path.read_text()
    assert "BACKOFF" not in log_text
    assert "watch-no-progress-backstop" not in log_text


def test_watch_no_progress_treats_provider_unavailable_with_turn_and_token_evidence_and_no_capacity_proof_as_real_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 25, 0, 10, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.cli.watch.datetime", FrozenDateTime)

    impl = store.add("Implement feature", task_type="implement")
    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None
    impl.status = "completed"
    impl.completed_at = FrozenDateTime.current
    store.update(impl)
    review.status = "completed"
    review.completed_at = FrozenDateTime.current
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease retry later."
    store.update(review)

    action = {"type": "improve", "description": "Create improve task", "review_task": review}
    real_build_candidate = build_watch_progress_candidate

    def stable_candidate(*args: object, **kwargs: object) -> WatchProgressCandidate:
        candidate = real_build_candidate(*args, **kwargs)
        subject = kwargs.get("subject_task")
        if getattr(subject, "id", None) == impl.id and candidate.action_type == "improve":
            return replace(candidate, evidence_fingerprint=f"stable:{impl.id}:improve")
        return candidate

    monkeypatch.setattr("gza.cli.watch.build_watch_progress_candidate", stable_candidate)

    failed_improve = store.add("Improve attempt", task_type="improve", based_on=impl.id, depends_on=review.id)
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "PROVIDER_UNAVAILABLE"
    failed_improve.recovery_origin = "retry"
    failed_improve.num_turns_reported = 1
    failed_improve.output_tokens = 32
    failed_improve.completed_at = FrozenDateTime.current
    store.update(failed_improve)

    first_result = _finalize_watch_no_progress_after_execution(
        config=Config.load(tmp_path),
        store=store,
        subject_task=impl,
        action=action,
        action_task_before=review,
        action_task_after=failed_improve,
        failed_task=None,
        no_progress_cycles=2,
    )
    second_result = _finalize_watch_no_progress_after_execution(
        config=Config.load(tmp_path),
        store=store,
        subject_task=impl,
        action=action,
        action_task_before=review,
        action_task_after=failed_improve,
        failed_task=None,
        no_progress_cycles=2,
    )
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=str(impl.id))
    backoff = store.get_watch_recovery_backoff(
        subject_kind="lineage",
        subject_id=str(impl.id),
        action_type="improve",
        action_reason="Create improve task",
    )

    assert first_result is None
    assert second_result is not None
    assert second_result["needs_attention_reason"] == WATCH_NO_PROGRESS_BACKSTOP_REASON
    assert len(observations) == 1
    assert observations[0].streak == 2
    assert observations[0].parked_reason == WATCH_NO_PROGRESS_BACKSTOP_REASON
    assert backoff is None


def test_watch_cycle_repeated_recovery_evaluation_does_not_park_running_descendant(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-running-child"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    recovery_child = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert recovery_child.id is not None
    recovery_child.status = "in_progress"
    recovery_child.session_id = failed.session_id
    recovery_child.started_at = datetime.now(UTC)
    recovery_child.running_pid = 4242
    store.update(recovery_child)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=AssertionError("iterate should not run")),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert store.list_watch_progress_observations(subject_kind="lineage", subject_id=failed.id) == []

    text = log_path.read_text()
    assert "ATTENTION" not in text


def test_watch_cycle_parked_recovery_head_does_not_starve_later_candidate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    blocked = store.add("Blocked failed implement", task_type="implement")
    assert blocked.id is not None
    blocked.status = "failed"
    blocked.failure_reason = "MAX_TURNS"
    blocked.session_id = "sess-blocked"
    blocked.completed_at = datetime.now(UTC)
    store.update(blocked)

    blocked_child = store.add(blocked.prompt, task_type="implement", based_on=blocked.id)
    assert blocked_child.id is not None
    blocked_child.status = "in_progress"
    blocked_child.session_id = blocked.session_id
    blocked_child.started_at = datetime.now(UTC)
    blocked_child.running_pid = 5151
    store.update(blocked_child)

    actionable = store.add("Actionable failed implement", task_type="implement")
    assert actionable.id is not None
    actionable.status = "failed"
    actionable.failure_reason = "MAX_TURNS"
    actionable.session_id = "sess-actionable"
    actionable.completed_at = datetime.now(UTC)
    store.update(actionable)

    blocked_decision = decide_failed_task_recovery(store, blocked, max_recovery_attempts=1)
    blocked_action = failed_recovery_decision_to_action(blocked, blocked_decision)
    blocked_candidate = build_watch_progress_candidate(
        store,
        subject_task=blocked,
        action=blocked_action,
        action_task=blocked_child,
        failed_task=blocked,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=blocked_candidate.subject_kind,
            subject_id=blocked_candidate.subject_id,
            action_type=blocked_candidate.action_type,
            action_reason=blocked_candidate.action_reason,
            subject_task_id=blocked_candidate.subject_task_id,
            action_task_id=blocked_candidate.action_task_id,
            action_task_status=blocked_candidate.action_task_status,
            action_task_started_at=blocked_candidate.action_task_started_at,
            action_task_running_pid=blocked_candidate.action_task_running_pid,
            failed_task_id=blocked_candidate.failed_task_id,
            recovery_task_id=blocked_candidate.recovery_task_id,
            merge_unit_id=blocked_candidate.merge_unit_id,
            merge_unit_state=blocked_candidate.merge_unit_state,
            merge_unit_head_sha=blocked_candidate.merge_unit_head_sha,
            evidence_fingerprint=blocked_candidate.evidence_fingerprint,
            streak=2,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert spawn_iterate.call_count == 1
    assert len(store.get_based_on_children(actionable.id)) == 1
    text = log_path.read_text()
    assert f"{blocked.id}" in text
    assert any("RECOVR" in line and actionable.id in line for line in text.splitlines())


def test_watch_cycle_parked_pending_recovery_child_is_excluded_from_plain_pending_pickup(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Blocked failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-blocked-pending"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    parked_recovery_child = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert parked_recovery_child.id is not None
    parked_recovery_child.status = "pending"
    parked_recovery_child.session_id = failed.session_id
    store.update(parked_recovery_child)

    ordinary_pending = store.add("Ordinary pending implement", task_type="implement")
    assert ordinary_pending.id is not None

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "recovery_already_pending"
    assert decision.recovery_task_id == parked_recovery_child.id

    action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=action,
        action_task=parked_recovery_child,
        failed_task=failed,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            action_task_started_at=candidate.action_task_started_at,
            action_task_running_pid=candidate.action_task_running_pid,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            streak=2,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    started_task_ids: list[str] = []

    def record_iterate_spawn(
        _args: argparse.Namespace,
        _config: Config,
        task: object,
        **_kwargs: object,
    ) -> int:
        task_id = getattr(task, "id", None)
        assert isinstance(task_id, str)
        started_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=record_iterate_spawn),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert started_task_ids == []
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=failed.id)
    assert observations == []
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert not any(parked_recovery_child.id in line and "START" in line for line in text.splitlines())


def test_watch_cycle_reused_pending_recovery_liveness_transition_resets_no_progress_streak(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-reuse-pending"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    recovery_child = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert recovery_child.id is not None
    recovery_child.status = "pending"
    recovery_child.session_id = failed.session_id
    store.update(recovery_child)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=1),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

        recovery_child = store.get(recovery_child.id)
        assert recovery_child is not None
        recovery_child.started_at = datetime.now(UTC)
        recovery_child.running_pid = 6060
        store.update(recovery_child)

        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert store.list_watch_progress_observations(subject_kind="lineage", subject_id=failed.id) == []

    text = log_path.read_text()
    assert "ATTENTION" not in text


def test_watch_cycle_clears_stale_never_started_pending_backstop_and_launches_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    pending = store.add("Never-started pending implement", task_type="implement")
    assert pending.id is not None

    action = {"type": "iterate", "description": "pending queue iterate"}
    candidate = build_watch_progress_candidate(
        store,
        subject_task=pending,
        action=action,
        action_task=pending,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            action_task_started_at=candidate.action_task_started_at,
            action_task_running_pid=candidate.action_task_running_pid,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            streak=2,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    started_task_ids: list[str] = []

    def record_iterate_spawn(
        _args: argparse.Namespace,
        _config: Config,
        task: object,
        **_kwargs: object,
    ) -> int:
        task_id = getattr(task, "id", None)
        assert isinstance(task_id, str)
        started_task_ids.append(task_id)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=record_iterate_spawn),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert started_task_ids == [pending.id]
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=pending.id)
    assert len(observations) == 1
    assert observations[0].parked_reason is None
    assert observations[0].launch_evidence_fingerprint is not None
    assert "ATTENTION" not in log_path.read_text()


def test_watch_cycle_reused_pending_recovery_relaunch_without_terminal_outcome_does_not_tick_streak(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-reuse-unchanged"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    recovery_child = store.add(failed.prompt, task_type="implement", based_on=failed.id)
    assert recovery_child.id is not None
    recovery_child.status = "pending"
    recovery_child.session_id = failed.session_id
    store.update(recovery_child)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch(
            "gza.cli.watch._maybe_finalize_watch_no_progress_for_background_action",
            wraps=_maybe_finalize_watch_no_progress_for_background_action,
        ) as finalize_spy,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )
        _run_cycle(
            config=Config.load(tmp_path),
            store=make_store(tmp_path),
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(log_path, quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert finalize_spy.call_count == 2
    observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=failed.id)
    assert len(observations) == 1
    assert observations[0].launch_evidence_fingerprint is not None
    assert observations[0].streak == 0


def test_watch_cycle_resets_no_progress_streak_after_merge_unit_progress(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  no_progress_cycles: 2\n")
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/no-progress-reset"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)
        store.refresh_merge_unit_head(unit.id, "new-head-sha", "new-base-sha")
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)

    assert spawn_iterate.call_count == 2
    text = log_path.read_text()
    assert "ATTENTION" not in text


def test_reconcile_stale_watch_no_progress_parks_clears_residue_rows(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed merge-unit residue", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.branch = "feature/stale-failed-park"
    failed.has_commits = True
    failed.started_at = datetime(2026, 6, 1, 9, 0, tzinfo=UTC)
    failed.completed_at = datetime(2026, 6, 1, 10, 0, tzinfo=UTC)
    failed.failure_reason = "MAX_TURNS"
    store.update(failed)
    store.set_merge_status(failed.id, "unmerged")
    store.get_or_create_merge_unit_for_task(failed)
    failed_child = store.add("Completed child for failed residue", task_type="review", depends_on=failed.id)
    assert failed_child.id is not None
    failed_child.status = "completed"
    failed_child.started_at = datetime(2026, 6, 1, 10, 5, tzinfo=UTC)
    failed_child.completed_at = datetime(2026, 6, 1, 10, 10, tzinfo=UTC)
    failed_child.branch = failed.branch
    store.update(failed_child)
    failed_candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action={"type": "create_review", "description": "Create review (required before merge)"},
        action_task=failed_child,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=failed_candidate.subject_kind,
            subject_id=failed_candidate.subject_id,
            action_type=failed_candidate.action_type,
            action_reason=failed_candidate.action_reason,
            subject_task_id=failed_candidate.subject_task_id,
            action_task_id=failed_candidate.action_task_id,
            action_task_status=failed_candidate.action_task_status,
            action_task_started_at=failed_candidate.action_task_started_at,
            action_task_running_pid=failed_candidate.action_task_running_pid,
            failed_task_id=failed_candidate.failed_task_id,
            recovery_task_id=failed_candidate.recovery_task_id,
            merge_unit_id=failed_candidate.merge_unit_id,
            merge_unit_state=failed_candidate.merge_unit_state,
            merge_unit_head_sha=failed_candidate.merge_unit_head_sha,
            evidence_fingerprint=failed_candidate.evidence_fingerprint,
            streak=3,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    completed = store.add("Completed merged residue", task_type="implement")
    assert completed.id is not None
    store.mark_completed(completed, has_commits=True, branch="feature/stale-merged-park")
    completed_unit = store.get_or_create_merge_unit_for_task(completed)
    assert completed_unit is not None
    store.set_merge_unit_state(completed_unit.id, "merged")
    completed = store.get(completed.id)
    assert completed is not None
    completed_candidate = build_watch_progress_candidate(
        store,
        subject_task=completed,
        action={"type": "improve", "description": "Create improve task (review CHANGES_REQUESTED)"},
        action_task=completed,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=completed_candidate.subject_kind,
            subject_id=completed_candidate.subject_id,
            action_type=completed_candidate.action_type,
            action_reason=completed_candidate.action_reason,
            subject_task_id=completed_candidate.subject_task_id,
            action_task_id=completed_candidate.action_task_id,
            action_task_status=completed_candidate.action_task_status,
            action_task_started_at=completed_candidate.action_task_started_at,
            action_task_running_pid=completed_candidate.action_task_running_pid,
            failed_task_id=completed_candidate.failed_task_id,
            recovery_task_id=completed_candidate.recovery_task_id,
            merge_unit_id=completed_candidate.merge_unit_id,
            merge_unit_state=completed_candidate.merge_unit_state,
            merge_unit_head_sha=completed_candidate.merge_unit_head_sha,
            evidence_fingerprint=completed_candidate.evidence_fingerprint,
            streak=3,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    dropped = store.add("Dropped merge-unit residue", task_type="implement")
    assert dropped.id is not None
    dropped.status = "dropped"
    dropped.branch = "feature/stale-dropped-park"
    dropped.has_commits = True
    dropped.started_at = datetime(2026, 6, 2, 9, 0, tzinfo=UTC)
    dropped.completed_at = datetime(2026, 6, 2, 10, 0, tzinfo=UTC)
    store.update(dropped)
    store.set_merge_status(dropped.id, "unmerged")
    store.get_or_create_merge_unit_for_task(dropped)
    dropped_child = store.add("Completed child for dropped residue", task_type="review", depends_on=dropped.id)
    assert dropped_child.id is not None
    dropped_child.status = "completed"
    dropped_child.started_at = datetime(2026, 6, 2, 10, 5, tzinfo=UTC)
    dropped_child.completed_at = datetime(2026, 6, 2, 10, 10, tzinfo=UTC)
    dropped_child.branch = dropped.branch
    store.update(dropped_child)
    dropped_candidate = build_watch_progress_candidate(
        store,
        subject_task=dropped,
        action={"type": "create_review", "description": "Create review (required before merge)"},
        action_task=dropped_child,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=dropped_candidate.subject_kind,
            subject_id=dropped_candidate.subject_id,
            action_type=dropped_candidate.action_type,
            action_reason=dropped_candidate.action_reason,
            subject_task_id=dropped_candidate.subject_task_id,
            action_task_id=dropped_candidate.action_task_id,
            action_task_status=dropped_candidate.action_task_status,
            action_task_started_at=dropped_candidate.action_task_started_at,
            action_task_running_pid=dropped_candidate.action_task_running_pid,
            failed_task_id=dropped_candidate.failed_task_id,
            recovery_task_id=dropped_candidate.recovery_task_id,
            merge_unit_id=dropped_candidate.merge_unit_id,
            merge_unit_state=dropped_candidate.merge_unit_state,
            merge_unit_head_sha=dropped_candidate.merge_unit_head_sha,
            evidence_fingerprint=dropped_candidate.evidence_fingerprint,
            streak=3,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    cleared = reconcile_stale_watch_no_progress_parks(store)

    assert cleared == 3
    assert store.list_watch_progress_observations(
        subject_kind=failed_candidate.subject_kind,
        subject_id=failed_candidate.subject_id,
    ) == []
    assert store.list_watch_progress_observations(
        subject_kind=completed_candidate.subject_kind,
        subject_id=completed_candidate.subject_id,
    ) == []
    assert store.list_watch_progress_observations(
        subject_kind=dropped_candidate.subject_kind,
        subject_id=dropped_candidate.subject_id,
    ) == []


def test_query_owner_rows_surfaces_persisted_watch_no_progress_backstop(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/no-progress-surface"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")
    store.get_or_create_merge_unit_for_task(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    action = {"type": "improve", "review_task": review}
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=action,
        action_task=impl,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            streak=2,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    with patch("gza.cli.advance_engine.determine_next_action", return_value=action):
        rows, _ = _query_owner_rows_with_context(
            store=store,
            config=Config.load(tmp_path),
            git=_make_watch_git(),
            target_branch="main",
            max_recovery_attempts=1,
            include_skipped=True,
        )

    row = next(row for row in rows if row.owner_task.id == impl.id)
    assert row.lineage_status == "needs_attention"
    assert row.next_action is not None
    assert row.next_action["needs_attention_reason"] == WATCH_NO_PROGRESS_BACKSTOP_REASON


def test_get_active_watch_no_progress_attention_preserves_executed_unmerged_merge_unit_loop(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Completed impl with active no-op loop", task_type="implement")
    assert impl.id is not None
    store.mark_completed(impl, has_commits=True, branch="feature/preserve-real-loop")
    store.set_merge_status(impl.id, "unmerged")
    store.get_or_create_merge_unit_for_task(impl)
    impl = store.get(impl.id)
    assert impl is not None

    review = store.add("Changes requested review", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.started_at = datetime.now(UTC)
    review.completed_at = datetime.now(UTC)
    store.update(review)

    action = {"type": "improve", "review_task": review}
    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action=action,
        action_task=impl,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            action_task_started_at=candidate.action_task_started_at,
            action_task_running_pid=candidate.action_task_running_pid,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            streak=3,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    attention = get_active_watch_no_progress_attention(store, candidate=candidate)

    assert attention is not None
    assert attention["needs_attention_reason"] == WATCH_NO_PROGRESS_BACKSTOP_REASON


def test_watch_cycle_dedupes_merge_not_default_skip_across_cycles(tmp_path: Path) -> None:
    """Persistent 'not on default branch' skip should not spam every watch pass."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/not-default"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "feature/local"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
    ):
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)

    assert log_path.read_text().count("merge actions skipped: not on default branch") == 1


def test_watch_cycle_dedupes_attempt_cap_skip_across_cycles(tmp_path: Path) -> None:
    """Persistent attempt-cap skip should stay informational and dedupe across cycles."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed resume attempt", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-1"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    config.max_resume_attempts = 0
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=0,
            show_skipped=True,
        )
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=0,
            show_skipped=True,
        )

    text = log_path.read_text()
    assert text.count("ATTENTION") == 1
    assert text.count("SKIP") == 0
    assert text.count("Needs attention (1 task):") == 2
    assert (
        f'{failed.id} implement "Failed resume attempt" reason=automatic-recovery-disabled '
        "automatic recovery is disabled"
    ) in text


def test_watch_cycle_dedupes_wait_review_skip_across_cycles(tmp_path: Path) -> None:
    """Ordinary advance wait states should keep existing SKIP dedupe behavior."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/wait-review-dedupe"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch(
            "gza.cli.determine_next_action",
            return_value={"type": "wait_review", "description": "SKIP: review test-review is in_progress"},
        ),
    ):
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)

    text = log_path.read_text()
    assert text.count("SKIP") == 1
    assert text.count("ATTENTION") == 0


def test_watch_cycle_surfaces_guarded_pending_skip_as_attention_then_roundup_only(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Guarded pending attention should emit inline once, then stay in the roundup until it changes."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Completed implementation", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/guarded-pending-skip"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    pending_review = store.add("Pending review", task_type="review", depends_on=impl.id)
    assert pending_review.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=False)
    exec_result = AdvanceActionExecutionResult(
        action_type="run_review",
        status="skip",
        message=(
            f"review task {pending_review.id} resolves to {impl.id}, "
            f"not completed task {impl.id}-other"
        ),
        guarded_pending_task_id=pending_review.id,
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.determine_next_action", return_value={"type": "run_review", "review_task": pending_review}),
        patch("gza.cli.watch.execute_advance_action", return_value=exec_result),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("guarded pending task should stay suppressed")),
    ):
        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)
        first_pass = log_path.read_text()
        assert "ATTENTION" in first_pass
        assert "Needs attention (1 task):" in first_pass
        assert "SKIP" in first_pass

        _run_cycle(config=config, store=store, batch=1, max_iterations=10, dry_run=False, log=log)

    text = log_path.read_text()
    stdout = capsys.readouterr().out
    attention_lines = [line for line in text.splitlines() if "ATTENTION" in line]
    assert len(attention_lines) == 1
    assert (
        f'{pending_review.id} review "Pending review" reason=guarded-pending-skip '
        f'{exec_result.message}; will not run automatically'
    ) in attention_lines[0]
    assert text.count("Needs attention (1 task):") == 2
    assert "Summary:" not in text
    assert (
        text.count(
            f'{pending_review.id} review "Pending review" reason=guarded-pending-skip '
            f'{exec_result.message}; will not run automatically'
        )
        == 3
    )
    assert stdout.count("Needs attention (1 task):") == 2
    assert text.count("SKIP") == 1


def test_watch_log_inserts_blank_line_between_cycles(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Each watch pass should be visually separated in stdout and watch.log."""
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=False)

    with patch("gza.cli.watch._format_hms", side_effect=["18:08:47", "18:13:47"]):
        log.begin_cycle()
        log.emit("WAKE", "checking... (0 running, 2 pending, 1 slots)")
        log.end_cycle()

        log.begin_cycle()
        log.emit("WAKE", "checking... (1 running, 0 pending, 0 slots)")
        log.end_cycle()

    assert log_path.read_text() == (
        "18:08:47 WAKE      checking... (0 running, 2 pending, 1 slots)\n"
        "\n"
        "18:13:47 WAKE      checking... (1 running, 0 pending, 0 slots)\n"
    )
    assert capsys.readouterr().out == (
        "18:08:47 WAKE      checking... (0 running, 2 pending, 1 slots)\n"
        "\n"
        "18:13:47 WAKE      checking... (1 running, 0 pending, 0 slots)\n"
    )


def test_watch_cycle_emits_one_lifecycle_summary_line_per_cycle(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store, tasks = _seed_watch_lifecycle_summary_fixture(tmp_path)
    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=False)
    git = _make_watch_git()
    plan_task = next(task_obj for task_obj in tasks.values() if task_obj.task_type == "plan")
    impl_task = next(task_obj for task_obj in tasks.values() if task_obj.task_type == "implement")
    rows = [
        LineageOwnerRow(
            owner_task=plan_task,
            members=(plan_task,),
            tree=None,
            lineage_status="actionable",
            next_action={
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            },
            next_action_reason="materialize",
            unresolved_tasks=(plan_task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=plan_task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
        LineageOwnerRow(
            owner_task=impl_task,
            members=(impl_task,),
            tree=None,
            lineage_status="actionable",
            next_action={"type": "create_review", "description": "Create review before merge"},
            next_action_reason="review",
            unresolved_tasks=(impl_task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=impl_task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
    ]

    def _fake_execute_advance_action(*, task, action, context):
        return AdvanceActionExecutionResult(
            action_type=action["type"],
            status="dry_run",
            message=f"Would {action['type']}",
            worker_label="worker",
        )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=(rows, RecoveryReadContext())),
        patch(
            "gza.cli.watch.determine_next_action",
            side_effect=lambda _config, _store, _git, task, _target_branch, **_kwargs: (
                {"type": "materialize_plan_slices", "description": "Materialize implementation slices from approved plan review"}
                if task.id == plan_task.id
                else {"type": "create_review", "description": "Create review before merge"}
            ),
        ),
        patch("gza.cli.watch.execute_advance_action", side_effect=_fake_execute_advance_action),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    text = log_path.read_text()
    stdout = capsys.readouterr().out
    assert text.count("Lifecycle actions (2):") == 1
    assert stdout.count("Lifecycle actions (2):") == 1
    assert any(f"{task_id}→materialize_plan_slices" in text for task_id, task in tasks.items() if task.task_type == "plan")
    assert any(f"{task_id}→create_review" in text for task_id, task in tasks.items() if task.task_type == "implement")


def test_watch_cycle_quiet_routes_lifecycle_summary_to_watch_log(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store, tasks = _seed_watch_lifecycle_summary_fixture(tmp_path)
    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = _make_watch_git()
    plan_task = next(task_obj for task_obj in tasks.values() if task_obj.task_type == "plan")
    impl_task = next(task_obj for task_obj in tasks.values() if task_obj.task_type == "implement")
    rows = [
        LineageOwnerRow(
            owner_task=plan_task,
            members=(plan_task,),
            tree=None,
            lineage_status="actionable",
            next_action={
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            },
            next_action_reason="materialize",
            unresolved_tasks=(plan_task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=plan_task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
        LineageOwnerRow(
            owner_task=impl_task,
            members=(impl_task,),
            tree=None,
            lineage_status="actionable",
            next_action={"type": "create_review", "description": "Create review before merge"},
            next_action_reason="review",
            unresolved_tasks=(impl_task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=impl_task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
    ]

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=(rows, RecoveryReadContext())),
        patch(
            "gza.cli.watch.determine_next_action",
            side_effect=lambda _config, _store, _git, task, _target_branch, **_kwargs: (
                {"type": "materialize_plan_slices", "description": "Materialize implementation slices from approved plan review"}
                if task.id == plan_task.id
                else {"type": "create_review", "description": "Create review before merge"}
            ),
        ),
        patch(
            "gza.cli.watch.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="create_review",
                status="dry_run",
                message="Would create_review",
                worker_label="worker",
            ),
        ),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=2,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    assert "Lifecycle actions (2):" in log_path.read_text()
    assert "Lifecycle actions (2):" not in capsys.readouterr().out


def test_watch_cycle_executes_direct_lifecycle_actions_before_worker_consuming_actions(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    review_owner = store.add("Needs review before merge", task_type="implement")
    assert review_owner.id is not None
    review_owner.status = "completed"
    review_owner.completed_at = datetime.now(UTC)
    review_owner.branch = "feature/watch-create-review"
    review_owner.has_commits = True
    review_owner.merge_status = "unmerged"
    store.update(review_owner)

    plan_owner = store.add("Approved plan ready to materialize", task_type="plan")
    assert plan_owner.id is not None
    plan_owner.status = "completed"
    plan_owner.completed_at = datetime.now(UTC)
    store.update(plan_owner)

    followup_owner = store.add("Approved with followups", task_type="implement")
    assert followup_owner.id is not None
    followup_owner.status = "completed"
    followup_owner.completed_at = datetime.now(UTC)
    followup_owner.branch = "feature/watch-followups"
    followup_owner.has_commits = True
    followup_owner.merge_status = "unmerged"
    store.update(followup_owner)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=False)
    executed_actions: list[str] = []

    def _row(task: Task) -> LineageOwnerRow:
        return LineageOwnerRow(
            owner_task=task,
            members=(task,),
            tree=None,
            lineage_status="actionable",
            next_action=None,
            next_action_reason="",
            unresolved_tasks=(task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        )

    def _fake_determine(_config, _store, _git, task, _target_branch, **_kwargs):
        if task.id == review_owner.id:
            return {"type": "create_review", "description": "Create review before merge"}
        if task.id == plan_owner.id:
            return {
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            }
        return {"type": "merge_with_followups", "description": "Merge approved task into main and create follow-up tasks"}

    def _fake_execute_advance_action(*, task, action, context):
        executed_actions.append(action["type"])
        return AdvanceActionExecutionResult(
            action_type=action["type"],
            status="dry_run",
            message=f"Would {action['type']}",
            worker_label="worker",
            worker_consuming=action["type"] == "create_review",
        )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch(
            "gza.cli.watch._query_owner_rows_with_context",
            return_value=([_row(review_owner), _row(plan_owner), _row(followup_owner)], RecoveryReadContext()),
        ),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.watch.execute_advance_action", side_effect=_fake_execute_advance_action),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    stdout = capsys.readouterr().out
    assert "Lifecycle actions (3):" in stdout
    assert executed_actions == ["materialize_plan_slices", "create_review"]
    assert stdout.index("MERGE") < stdout.index("(new) review for")


def test_watch_cycle_executes_non_worker_lifecycle_actions_with_zero_slots_and_skips_moot_rows(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    actionable_plan = store.add("Approved plan ready to materialize", task_type="plan")
    assert actionable_plan.id is not None
    actionable_plan.status = "completed"
    actionable_plan.completed_at = datetime.now(UTC)
    store.update(actionable_plan)

    moot_plan = store.add("Moot plan should not rematerialize", task_type="plan")
    assert moot_plan.id is not None
    moot_plan.status = "completed"
    moot_plan.completed_at = datetime.now(UTC)
    store.update(moot_plan)

    review_owner = store.add("Implementation still needs review", task_type="implement")
    assert review_owner.id is not None
    review_owner.status = "completed"
    review_owner.completed_at = datetime.now(UTC)
    review_owner.branch = "feature/watch-slot-gated-review"
    review_owner.has_commits = True
    review_owner.merge_status = "unmerged"
    store.update(review_owner)

    plan_review = store.add("Review actionable plan", task_type="plan_review", depends_on=actionable_plan.id)
    assert plan_review.id is not None
    plan_review.status = "completed"
    plan_review.completed_at = datetime.now(UTC)
    store.update(plan_review)

    manifest = validate_plan_review_manifest(
        {
            "schema_version": 1,
            "source_task_id": actionable_plan.id,
            "source_task_type": "plan",
            "verdict": "APPROVED",
            "slice_quality": {
                "fits_single_task_budget": True,
                "timeout_budget_minutes": 30,
                "max_expected_files_changed_per_slice": 8,
                "rationale": "Bounded slices.",
            },
            "slices": [
                {
                    "slice_id": "S1",
                    "title": "Foundation",
                    "prompt": "Implement slice S1.",
                    "scope": ["Add parser"],
                    "out_of_scope": ["Executor"],
                    "acceptance_criteria": ["Parser works"],
                    "depends_on_slices": [],
                    "based_on_slice": None,
                    "review_scope": "Parser only.",
                    "estimated_complexity": "medium",
                    "expected_timeout_minutes": 30,
                    "requires_code_review": True,
                    "tags": ["watch"],
                }
            ],
        },
        markdown_verdict="APPROVED",
        source_task_id=actionable_plan.id,
        source_task_type="plan",
        max_slice_timeout_minutes=30,
    )

    actionable_row = LineageOwnerRow(
        owner_task=actionable_plan,
        members=(actionable_plan, plan_review),
        tree=None,
        lineage_status="actionable",
        next_action={
            "type": "materialize_plan_slices",
            "description": f"Materialize implementation slices from plan review {plan_review.id}",
            "plan_review_task": plan_review,
            "manifest": manifest,
            "plan_source_task": actionable_plan,
        },
        next_action_reason="materialize",
        unresolved_tasks=(actionable_plan,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=actionable_plan,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )
    moot_row = LineageOwnerRow(
        owner_task=moot_plan,
        members=(moot_plan,),
        tree=None,
        lineage_status="skipped",
        next_action={"type": "skip", "description": "SKIP: moot (commits already present on target)"},
        next_action_reason="moot",
        unresolved_tasks=(moot_plan,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=moot_plan,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )
    review_row = LineageOwnerRow(
        owner_task=review_owner,
        members=(review_owner,),
        tree=None,
        lineage_status="actionable",
        next_action={"type": "create_review", "description": "Create review before merge"},
        next_action_reason="review",
        unresolved_tasks=(review_owner,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=review_owner,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch(
            "gza.cli.watch._query_owner_rows_with_context",
            return_value=([actionable_row, moot_row, review_row], RecoveryReadContext()),
        ),
        patch(
            "gza.cli.watch.determine_next_action",
            side_effect=lambda _config, _store, _git, task, _target_branch, **_kwargs: (
                {"type": "create_review", "description": "Create review before merge"}
                if task.id == review_owner.id
                else {
                    "type": "materialize_plan_slices",
                    "description": f"Materialize implementation slices from plan review {plan_review.id}",
                    "plan_review_task": plan_review,
                    "manifest": manifest,
                    "plan_source_task": actionable_plan,
                }
            ),
        ),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=0,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    implement_tasks = [task for task in store.get_all() if task.task_type == "implement" and task.based_on == actionable_plan.id]
    review_tasks = [task for task in store.get_all() if task.task_type == "review" and task.depends_on == review_owner.id]

    assert len(implement_tasks) == 1
    assert implement_tasks[0].trigger_source == "plan-review"
    assert not any(task.based_on == moot_plan.id for task in store.get_all() if task.task_type == "implement")
    assert review_tasks == []


def test_watch_cycle_uses_shared_lifecycle_execution_gate(tmp_path: Path) -> None:
    store, tasks = _seed_watch_lifecycle_summary_fixture(tmp_path)
    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    gate_calls: list[tuple[str, int]] = []
    plan_task = next(task_obj for task_obj in tasks.values() if task_obj.task_type == "plan")
    impl_task = next(task_obj for task_obj in tasks.values() if task_obj.task_type == "implement")
    merge_task = store.add("Approved task ready to merge", task_type="implement")
    assert merge_task.id is not None
    merge_task.status = "completed"
    merge_task.completed_at = datetime.now(UTC)
    merge_task.branch = "feature/watch-shared-gate-merge"
    merge_task.has_commits = True
    merge_task.merge_status = "unmerged"
    store.update(merge_task)

    rows = [
        LineageOwnerRow(
            owner_task=merge_task,
            members=(merge_task,),
            tree=None,
            lineage_status="actionable",
            next_action={
                "type": "merge_with_followups",
                "description": "Merge approved task into main and create follow-up tasks",
            },
            next_action_reason="merge",
            unresolved_tasks=(merge_task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=merge_task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
        LineageOwnerRow(
            owner_task=plan_task,
            members=(plan_task,),
            tree=None,
            lineage_status="actionable",
            next_action={
                "type": "materialize_plan_slices",
                "description": "Materialize implementation slices from approved plan review",
            },
            next_action_reason="materialize",
            unresolved_tasks=(plan_task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=plan_task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
        LineageOwnerRow(
            owner_task=impl_task,
            members=(impl_task,),
            tree=None,
            lineage_status="actionable",
            next_action={"type": "create_review", "description": "Create review before merge"},
            next_action_reason="review",
            unresolved_tasks=(impl_task,),
            unresolved_leaf_summary=(),
            lifecycle_action_task=impl_task,
            recovery_action_task=None,
            recovery_leaf_task=None,
        ),
    ]

    def _record_gate(action, *, free_worker_slots):
        gate_calls.append((str(action.get("type")), free_worker_slots))
        if action.get("type") == "merge_with_followups":
            return False
        return real_should_execute_lifecycle_action(action, free_worker_slots=free_worker_slots)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=(rows, RecoveryReadContext())),
        patch(
            "gza.cli.watch.determine_next_action",
            side_effect=lambda _config, _store, _git, task, _target_branch, **_kwargs: (
                {"type": "materialize_plan_slices", "description": "Materialize implementation slices from approved plan review"}
                if task.id == plan_task.id
                else {"type": "create_review", "description": "Create review before merge"}
                if task.id == impl_task.id
                else {"type": "merge_with_followups", "description": "Merge approved task into main and create follow-up tasks"}
            ),
        ),
        patch(
            "gza.cli.watch.execute_advance_action",
            side_effect=lambda *, task, action, context: AdvanceActionExecutionResult(
                action_type=action["type"],
                status="dry_run",
                message="Would run",
                worker_label="worker",
                worker_consuming=action["type"] == "create_review",
            ),
        ),
        patch("gza.cli._lifecycle_actions.should_execute_lifecycle_action", side_effect=_record_gate),
        patch("gza.cli.watch._resolve_watch_merge_log_event") as resolve_merge_event,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=log,
        )

    resolve_merge_event.assert_not_called()
    assert ("merge_with_followups", 1) in gate_calls
    assert ("materialize_plan_slices", 1) in gate_calls
    assert ("create_review", 1) in gate_calls


def test_watch_log_suppresses_unchanged_attention_inline_across_cycles_but_keeps_roundups(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with patch(
        "gza.cli.watch._format_hms",
        side_effect=[
            "18:08:47",
            "18:08:48",
            "18:13:47",
            "18:18:47",
            "18:18:48",
        ],
    ):
        log.begin_cycle()
        log.emit_attention(attention_key="task-1", message="gza-1 review needs manual attention")
        _emit_cycle_attention_summary(log)
        log.end_cycle()

        log.begin_cycle()
        log.emit_attention(attention_key="task-1", message="gza-1 review needs manual attention")
        _emit_cycle_attention_summary(log)
        log.end_cycle()

        log.begin_cycle()
        log.emit_attention(attention_key="task-1", message="gza-1 review still needs manual attention")
        _emit_cycle_attention_summary(log)
        log.end_cycle()

    text = log_path.read_text()
    attention_lines = [line for line in text.splitlines() if " ATTENTION " in line]
    assert len(attention_lines) == 2
    assert attention_lines[0].startswith("18:08:47 ATTENTION")
    assert attention_lines[1].startswith("18:18:47 ATTENTION")
    assert text.count("INFO      Needs attention (1 task):") == 3
    assert text.count("  gza-1 review needs manual attention") == 2
    assert text.count("  gza-1 review still needs manual attention") == 1


def test_installed_gza_package_fingerprint_changes_only_when_python_source_changes(tmp_path: Path) -> None:
    """Package fingerprint should ignore mtimes and change when Python contents change."""
    package_root = tmp_path / "gza"
    package_root.mkdir()
    alpha = package_root / "alpha.py"
    beta = package_root / "nested" / "beta.py"
    ignored = package_root / "notes.txt"
    beta.parent.mkdir()
    alpha.write_text("VALUE = 1\n")
    beta.write_text("NAME = 'beta'\n")
    ignored.write_text("ignored\n")

    original = _installed_gza_package_fingerprint(package_root)

    alpha.touch()
    ignored.write_text("changed\n")
    assert _installed_gza_package_fingerprint(package_root) == original

    beta.write_text("NAME = 'beta2'\n")
    assert _installed_gza_package_fingerprint(package_root) != original


def test_watch_warns_once_per_installed_package_drift(tmp_path: Path) -> None:
    """Watch should advertise next-cycle-boundary re-exec once per new drift fingerprint."""
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    drift_state = _InstalledPackageDriftState(startup_fingerprint="startup")

    with patch(
        "gza.cli.watch._installed_gza_package_fingerprint",
        side_effect=["startup", "changed-1", "changed-1", "changed-2"],
    ):
        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state, auto_restart_on_drift=True)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state, auto_restart_on_drift=True)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state, auto_restart_on_drift=True)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state, auto_restart_on_drift=True)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

    warning_lines = [line for line in log_path.read_text().splitlines() if "WARNING" in line]
    assert len(warning_lines) == 2
    assert all(
        line.endswith(
            "WARNING   installed gza changed since watch started -- watch will re-exec at the next cycle boundary to load new code"
        )
        for line in warning_lines
    )
    assert drift_state.warned_fingerprint == "changed-2"
    assert drift_state.pending_restart_fingerprint == "changed-2"


def test_watch_warns_for_manual_restart_when_auto_restart_on_drift_is_disabled(tmp_path: Path) -> None:
    """Opting out should keep the explicit manual-restart operator warning."""
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    drift_state = _InstalledPackageDriftState(startup_fingerprint="startup")

    with patch("gza.cli.watch._installed_gza_package_fingerprint", return_value="changed-1"):
        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state, auto_restart_on_drift=False)
        log.end_cycle()

    warning_lines = [line for line in log_path.read_text().splitlines() if "WARNING" in line]
    assert len(warning_lines) == 1
    assert warning_lines[0].endswith(
        "WARNING   installed gza changed since watch started -- restart watch to pick up new code"
    )
    assert drift_state.pending_restart_fingerprint == "changed-1"


def test_watch_drift_state_does_not_request_reexec_when_fingerprint_is_unchanged(tmp_path: Path) -> None:
    """No drift means no pending watch re-exec."""
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    drift_state = _InstalledPackageDriftState(startup_fingerprint="startup")

    with patch("gza.cli.watch._installed_gza_package_fingerprint", return_value="startup"):
        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state, auto_restart_on_drift=True)
        log.end_cycle()

    assert drift_state.pending_restart_fingerprint is None
    assert _should_reexec_watch(
        auto_restart_on_drift=True,
        dry_run=False,
        stop_requested=False,
        drift_state=drift_state,
    ) is False


def test_watch_requests_reexec_on_pending_drift_even_with_running_and_pending_work() -> None:
    """Pending drift should restart watch at the next cycle boundary regardless of queue state."""
    drift_state = _InstalledPackageDriftState(startup_fingerprint="startup")
    drift_state.pending_restart_fingerprint = "changed-1"

    assert _should_reexec_watch(
        auto_restart_on_drift=True,
        dry_run=False,
        stop_requested=False,
        drift_state=drift_state,
    ) is True


@pytest.mark.parametrize(
    ("auto_restart_on_drift", "dry_run", "stop_requested"),
    [
        (False, False, False),
        (True, True, False),
        (True, False, True),
    ],
)
def test_watch_reexec_guards_still_suppress_pending_drift(
    auto_restart_on_drift: bool,
    dry_run: bool,
    stop_requested: bool,
) -> None:
    """Guard rails should still block re-exec even when drift is pending."""
    drift_state = _InstalledPackageDriftState(startup_fingerprint="startup")
    drift_state.pending_restart_fingerprint = "changed-1"

    assert _should_reexec_watch(
        auto_restart_on_drift=auto_restart_on_drift,
        dry_run=dry_run,
        stop_requested=stop_requested,
        drift_state=drift_state,
    ) is False


@pytest.mark.parametrize(
    ("drift_state", "expected"),
    [
        (None, False),
        (_InstalledPackageDriftState(startup_fingerprint="startup"), False),
        (
            _InstalledPackageDriftState(
                startup_fingerprint="startup",
                pending_restart_fingerprint="changed-1",
            ),
            True,
        ),
    ],
)
def test_watch_reexec_requires_pending_drift_state(
    drift_state: _InstalledPackageDriftState | None,
    expected: bool,
) -> None:
    assert _should_reexec_watch(
        auto_restart_on_drift=True,
        dry_run=False,
        stop_requested=False,
        drift_state=drift_state,
    ) is expected


def test_watch_reexec_argv_preserves_requested_watch_flags(tmp_path: Path) -> None:
    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=3,
        poll=12,
        max_idle=60,
        max_iterations=7,
        recovery_mode="recovery-only",
        recovery_slots=2,
        max_resume_attempts=4,
        dry_run=False,
        show_skipped=True,
        quiet=True,
        yes=True,
        tags=["release", "urgent"],
        all_tags=True,
        auto_restart_on_drift=False,
    )

    argv = _watch_reexec_argv(args)

    assert argv == [
        sys.executable,
        "-m",
        "gza",
        "watch",
        "--project",
        str(tmp_path),
        "--batch",
        "3",
        "--poll",
        "12",
        "--max-idle",
        "60",
        "--max-iterations",
        "7",
        "--recovery-only",
        "--recovery-slots",
        "2",
        "--max-resume-attempts",
        "4",
        "--show-skipped",
        "--quiet",
        "--yes",
        "--resumed-reexec",
        "--tag",
        "release",
        "--tag",
        "urgent",
        "--all-tags",
        "--no-auto-restart-on-drift",
    ]


def test_cmd_watch_exits_when_idle_reaches_max_idle(tmp_path: Path) -> None:
    """Watch should exit as soon as accumulated idle time reaches max-idle."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=10,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
    )

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(False, 0, 0)) as run_cycle,
        patch("gza.cli.watch.time.sleep"),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 2


def test_cmd_watch_resumed_reexec_skips_first_pass_confirmation_and_logs_auto_resume(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=10,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=False,
        resumed_reexec=True,
    )

    signal_handlers: dict[signal.Signals, object] = {}

    def register_signal(sig: signal.Signals, handler: object) -> object:
        signal_handlers[sig] = handler
        return object()

    def fake_sleep(_seconds: int, _stop_requested) -> None:
        handler = signal_handlers[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(False, 0, 0)) as run_cycle,
        patch("builtins.input") as input_mock,
        patch("gza.cli.watch.signal.signal", side_effect=register_signal),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
    ):
        rc = cmd_watch(args)

    assert rc == 128 + signal.SIGTERM
    assert run_cycle.call_count == 1
    input_mock.assert_not_called()
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "auto-resumed after code update (skipping first-pass confirmation)" in log_text


def test_cmd_watch_first_start_preserves_confirmation_prompt(tmp_path: Path) -> None:
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=False,
        resumed_reexec=False,
    )

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(True, 0, 0)) as run_cycle,
        patch("builtins.input", return_value="n") as input_mock,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 1
    input_mock.assert_called_once_with("\nProceed? [y/N] ")


def test_cmd_watch_confirmed_first_start_computes_snapshot_once(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pending = store.add("Pending implement", task_type="implement")
    assert pending.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=False,
        resumed_reexec=False,
    )

    signal_handlers: dict[signal.Signals, object] = {}

    def register_signal(sig: signal.Signals, handler: object) -> object:
        signal_handlers[sig] = handler
        return object()

    def fake_sleep(_seconds: int, _stop_requested) -> None:
        handler = signal_handlers[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    with (
        patch("gza.cli.watch.get_concurrency_snapshot", wraps=watch_module.get_concurrency_snapshot) as snapshot_mock,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0),
        patch("builtins.input", return_value="y") as input_mock,
        patch("gza.cli.watch.signal.signal", side_effect=register_signal),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
    ):
        rc = cmd_watch(args)

    log_text = (tmp_path / ".gza" / "watch.log").read_text(encoding="utf-8")
    assert rc == 128 + signal.SIGTERM
    assert snapshot_mock.call_count == 1
    assert log_text.count(" WAKE ") == 1
    input_mock.assert_called_once_with("\nProceed? [y/N] ")


def test_cmd_watch_yes_runs_exactly_one_cycle(tmp_path: Path) -> None:
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=10,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        resumed_reexec=False,
    )

    signal_handlers: dict[signal.Signals, object] = {}

    def register_signal(sig: signal.Signals, handler: object) -> object:
        signal_handlers[sig] = handler
        return object()

    def fake_sleep(_seconds: int, _stop_requested) -> None:
        handler = signal_handlers[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(False, 0, 0)) as run_cycle,
        patch("builtins.input") as input_mock,
        patch("gza.cli.watch.signal.signal", side_effect=register_signal),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
    ):
        rc = cmd_watch(args)

    assert rc == 128 + signal.SIGTERM
    assert run_cycle.call_count == 1
    input_mock.assert_not_called()


def test_cmd_watch_declining_first_start_aborts_without_mutations(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pending = store.add("Pending implement", task_type="implement")
    assert pending.id is not None
    before = _task_snapshot(store)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=False,
        resumed_reexec=False,
    )

    with (
        patch("gza.cli.watch.get_concurrency_snapshot", wraps=watch_module.get_concurrency_snapshot) as snapshot_mock,
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("builtins.input", return_value="n") as input_mock,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    after = _task_snapshot(store)
    log_text = (tmp_path / ".gza" / "watch.log").read_text(encoding="utf-8")
    assert rc == 0
    assert snapshot_mock.call_count == 1
    assert log_text.count(" WAKE ") == 1
    assert before == after
    spawn_iterate.assert_not_called()
    input_mock.assert_called_once_with("\nProceed? [y/N] ")


def test_cmd_watch_cli_batch_derives_runtime_cap_when_max_concurrent_unset(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  batch: 1\n")

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=2,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
    )

    def assert_runtime_cap(**kwargs) -> _CycleResult:
        assert kwargs["config"].max_concurrent == 2
        assert kwargs["batch"] == 2
        return _CycleResult(False, 0, 0)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=assert_runtime_cap) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 1
    log_text = (tmp_path / ".gza" / "watch.log").read_text(encoding="utf-8")
    assert " WARN " not in log_text


def test_cmd_watch_warns_once_when_explicit_max_concurrent_caps_requested_batch(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "max_concurrent: 2\nwatch:\n  batch: 1\n")

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=4,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
    )

    def assert_explicit_cap(**kwargs) -> _CycleResult:
        assert kwargs["config"].max_concurrent == 2
        assert kwargs["batch"] == 4
        return _CycleResult(False, 0, 0)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=assert_explicit_cap) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 1
    log_lines = (tmp_path / ".gza" / "watch.log").read_text(encoding="utf-8").splitlines()
    warn_lines = [line for line in log_lines if " WARN " in line]
    assert len(warn_lines) == 1
    assert "requested batch=4" in warn_lines[0]
    assert "capped to 2" in warn_lines[0]
    assert "max_concurrent" in warn_lines[0]


def test_cmd_watch_keeps_explicit_max_concurrent_without_warning_when_batch_within_cap(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "max_concurrent: 4\nwatch:\n  batch: 2\n")

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=None,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
    )

    def assert_no_override(**kwargs) -> _CycleResult:
        assert kwargs["config"].max_concurrent == 4
        assert kwargs["batch"] == 2
        return _CycleResult(False, 0, 0)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=assert_no_override) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 1
    log_text = (tmp_path / ".gza" / "watch.log").read_text(encoding="utf-8")
    assert " WARN " not in log_text


def test_cmd_watch_uses_config_recovery_slots_zero_as_pending_only_mode(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  recovery_slots: 0\n")

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=2,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        dispatch_mode=None,
        recovery_slots=None,
        restart_failed_batch=None,
    )

    def assert_pending_only(**kwargs) -> _CycleResult:
        assert kwargs["recovery_mode"] == "pending_only"
        assert kwargs["recovery_slots"] == 0
        return _CycleResult(False, 0, 0)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=assert_pending_only) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 1


def test_cmd_watch_explicit_recovery_first_overrides_config_zero_recovery_slots(tmp_path: Path) -> None:
    setup_config(tmp_path)
    _append_watch_config(tmp_path, "watch:\n  recovery_slots: 0\n")

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=2,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        dispatch_mode="recovery_first_explicit",
        recovery_slots=None,
        restart_failed_batch=None,
    )

    def assert_recovery_first(**kwargs) -> _CycleResult:
        assert kwargs["recovery_mode"] == "recovery_first_explicit"
        assert kwargs["recovery_slots"] == 0
        return _CycleResult(False, 0, 0)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=assert_recovery_first) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 1


def test_cmd_watch_reexecs_on_drift_after_cycle_boundary(tmp_path: Path) -> None:
    """Watch should re-exec itself at the first cycle boundary where drift is detected."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=2,
        poll=5,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        resumed_reexec=False,
        tags=["release"],
        any_tag=False,
        recovery_mode="recovery-only",
        recovery_slots=1,
        max_resume_attempts=2,
        show_skipped=False,
        auto_restart_on_drift=True,
    )

    def run_cycle_with_drift(**kwargs) -> _CycleResult:
        drift_state = kwargs["installed_package_drift"]
        drift_state.pending_restart_fingerprint = "updated"
        return _CycleResult(False, 1, 1)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=run_cycle_with_drift) as run_cycle,
        patch("gza.cli.watch._task_snapshot", return_value={}),
        patch("gza.cli.watch._emit_transition_events"),
        patch("gza.cli.watch._collect_completed_transition_ids", return_value=[]),
        patch("gza.cli.watch._collect_unhandled_failures", return_value=[]),
        patch("gza.cli.watch._sleep_interruptibly"),
        patch("gza.cli.watch._installed_gza_package_fingerprint", return_value="startup"),
        patch("gza.cli.watch.os.execv", side_effect=SystemExit(0)) as execv,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        with pytest.raises(SystemExit) as excinfo:
            cmd_watch(args)

    assert excinfo.value.code == 0
    assert run_cycle.call_count == 1
    execv.assert_called_once()
    assert execv.call_args.args == (
        sys.executable,
        [
            sys.executable,
            "-m",
            "gza",
            "watch",
            "--project",
            str(tmp_path),
            "--batch",
            "2",
            "--poll",
            "5",
            "--max-iterations",
            "10",
            "--recovery-only",
            "--recovery-slots",
            "1",
            "--max-resume-attempts",
            "2",
            "--quiet",
            "--yes",
            "--resumed-reexec",
            "--tag",
            "release",
        ],
    )


def test_watch_restart_failed_skips_historical_prerequisite_unmerged_row_after_empty_reconciliation(
    tmp_path: Path,
) -> None:
    from gza.recovery_engine import _MergeContext

    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.branch = "feature/dependency"
    dependency.has_commits = True
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)
    store.set_merge_status(dependency.id, "merged")

    failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/prereq-empty"
    failed.has_commits = False
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    class _EmptyBranchGit:
        def resolve_fresh_merge_source(self, branch: str):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:
            if ref in {"main", "feature/prereq-empty"}:
                return "abc123"
            return None

        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            return False

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=_make_watch_git()),
        patch(
            "gza.recovery_engine._load_merge_context",
            lambda _project_dir=None: _MergeContext(git=_EmptyBranchGit(), default_branch="main"),
        ),
        patch(
            "gza.cli.watch._spawn_background_worker",
            side_effect=AssertionError("watch should not retry reconciled empty prerequisite failures"),
        ) as spawn_worker,
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("watch should not iterate reconciled empty prerequisite failures"),
        ) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=1,
        )

    assert result.work_done is False
    spawn_worker.assert_not_called()
    spawn_iterate.assert_not_called()
    assert store.get_based_on_children(failed.id) == []


def test_watch_restart_failed_skips_historical_prerequisite_unmerged_row_when_dependency_is_empty(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Empty dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.branch = "feature/dependency-empty-watch"
    dependency.has_commits = True
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)
    unit = store.create_merge_unit(
        source_branch=dependency.branch,
        target_branch="main",
        owner_task_id=dependency.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(dependency.id, unit.id, "owner")

    failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/downstream-watch-blocked"
    failed.has_commits = False
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.cli.watch._spawn_background_worker",
            side_effect=AssertionError("watch should not retry dependency-blocked prerequisite failures"),
        ) as spawn_worker,
        patch(
            "gza.cli.watch._spawn_background_iterate",
            side_effect=AssertionError("watch should not iterate dependency-blocked prerequisite failures"),
        ) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=1,
        )

    assert result.work_done is False
    spawn_worker.assert_not_called()
    spawn_iterate.assert_not_called()
    assert store.get_based_on_children(failed.id) == []


def test_watch_restart_failed_resumes_historical_prerequisite_unmerged_row_with_recorded_real_work(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.session_id = "sess-prereq-real-work"
    failed.output_content = "provider emitted output before the legacy prerequisite failure was stored"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("implement recovery should iterate")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=1,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    prepared_task_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert spawned_args.resume is False
    assert spawned_args.retry is False
    assert isinstance(prepared_task_id, str)
    assert spawned_task.id == prepared_task_id


def test_watch_restart_failed_retries_historical_prerequisite_unmerged_row_with_live_non_empty_branch(
    tmp_path: Path,
) -> None:
    from gza.recovery_engine import _MergeContext

    setup_config(tmp_path)
    store = make_store(tmp_path)

    dependency = store.add("Merged dependency", task_type="implement")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.merge_status = "merged"
    dependency.completed_at = datetime.now(UTC)
    store.update(dependency)

    failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "PREREQUISITE_UNMERGED"
    failed.branch = "feature/prereq-live-work-watch"
    failed.has_commits = False
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    class _NonEmptyBranchGit:
        def resolve_fresh_merge_source(self, branch: str):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def rev_parse_if_exists(self, ref: str) -> str | None:
            if ref == "main":
                return "target123"
            if ref == "feature/prereq-live-work-watch":
                return "source456"
            return None

        def branch_exists(self, branch: str) -> bool:
            return bool(branch)

        def is_merged(self, branch: str, into: str) -> bool:
            return False

        def count_commits_ahead(self, source_ref: str, base_ref: str) -> int:
            return 1

    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch(
            "gza.recovery_engine._load_merge_context",
            lambda _project_dir=None: _MergeContext(git=_NonEmptyBranchGit(), default_branch="main"),
        ),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("implement recovery should iterate")),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=1,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    prepared_task_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert spawned_args.resume is False
    assert spawned_task.id == prepared_task_id
    assert spawned_args.retry is False
    retry_children = store.get_based_on_children(failed.id)
    assert len(retry_children) == 1
    retry_child = retry_children[0]
    assert retry_child.id == prepared_task_id
    assert retry_child.recovery_origin == "retry"


def test_cmd_watch_reexecs_at_next_cycle_boundary_even_when_work_is_still_running(
    tmp_path: Path,
) -> None:
    """Detached workers should not block watch from restarting to pick up drifted code."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        resumed_reexec=False,
        tags=None,
        any_tag=False,
        restart_failed=False,
        restart_failed_batch=None,
        max_resume_attempts=None,
        show_skipped=False,
        auto_restart_on_drift=True,
    )

    def run_cycle_with_drift(**kwargs) -> _CycleResult:
        drift_state = kwargs["installed_package_drift"]
        drift_state.pending_restart_fingerprint = "updated"
        return _CycleResult(False, 1, 2)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=run_cycle_with_drift),
        patch("gza.cli.watch._task_snapshot", return_value={}),
        patch("gza.cli.watch._emit_transition_events"),
        patch("gza.cli.watch._collect_completed_transition_ids", return_value=[]),
        patch("gza.cli.watch._collect_unhandled_failures", return_value=[]),
        patch("gza.cli.watch._sleep_interruptibly") as sleep_interruptibly,
        patch("gza.cli.watch._installed_gza_package_fingerprint", return_value="startup"),
        patch("gza.cli.watch.os.execv", side_effect=SystemExit(0)) as execv,
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        with pytest.raises(SystemExit):
            cmd_watch(args)

    sleep_interruptibly.assert_not_called()
    execv.assert_called_once()


def test_reexec_recovery_keeps_live_in_progress_pid_and_counts_it_running(tmp_path: Path) -> None:
    """Live worker state should survive reconciliation and be rediscovered after watch re-exec."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    running = store.add("Running implement", task_type="implement")
    assert running.id is not None
    running.status = "in_progress"
    running.running_pid = os.getpid()
    store.update(running)

    config = Config.load(tmp_path)
    registry = WorkerRegistry(config.workers_path)
    registry.register(
        WorkerMetadata(
            worker_id="w-live-reexec",
            task_id=running.id,
            pid=os.getpid(),
            status="running",
        )
    )

    reconcile_in_progress_tasks(config)

    refreshed = store.get(running.id)
    assert refreshed is not None
    assert refreshed.status == "in_progress"
    assert refreshed.running_pid == os.getpid()
    assert refreshed.failure_reason is None

    live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)

    assert os.getpid() in live_pids
    assert running_task_ids == [running.id]
    assert anonymous_worker_count == 0


def test_cmd_watch_shutdown_signal_wins_over_pending_reexec(tmp_path: Path) -> None:
    """A shutdown signal in flight must suppress drift-triggered re-exec."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        resumed_reexec=False,
        tags=None,
        any_tag=False,
        restart_failed=False,
        restart_failed_batch=None,
        max_resume_attempts=None,
        show_skipped=False,
        auto_restart_on_drift=True,
    )

    handlers: dict[signal.Signals, object] = {}

    def fake_signal(sig: signal.Signals, handler: object) -> object:
        handlers[sig] = handler
        return object()

    def run_cycle_with_sigterm(**_kwargs) -> _CycleResult:
        handler = handlers[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)
        drift_state = _kwargs["installed_package_drift"]
        drift_state.pending_restart_fingerprint = "updated"
        return _CycleResult(False, 0, 0)

    with (
        patch("gza.cli.watch._run_cycle", side_effect=run_cycle_with_sigterm),
        patch("gza.cli.watch._task_snapshot", return_value={}),
        patch("gza.cli.watch._emit_transition_events"),
        patch("gza.cli.watch._collect_completed_transition_ids", return_value=[]),
        patch("gza.cli.watch._collect_unhandled_failures", return_value=[]),
        patch("gza.cli.watch._installed_gza_package_fingerprint", return_value="startup"),
        patch("gza.cli.watch.os.execv") as execv,
        patch("gza.cli.watch.signal.signal", side_effect=fake_signal),
    ):
        rc = cmd_watch(args)

    assert rc == 128 + signal.SIGTERM
    execv.assert_not_called()


def test_cmd_watch_dry_run_actionable_cycles_do_not_count_toward_max_idle(tmp_path: Path) -> None:
    """Dry-run cycles with actionable work should reset idle accounting."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        quiet=True,
    )

    cycle_results = [
        _CycleResult(True, 0, 1),
        _CycleResult(True, 0, 1),
        _CycleResult(False, 0, 1),
    ]

    with (
        patch("gza.cli.watch._run_cycle", side_effect=cycle_results) as run_cycle,
        patch("gza.cli.watch._sleep_interruptibly"),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 3


def test_cmd_watch_restart_failed_dry_run_restores_signal_handlers(tmp_path: Path) -> None:
    """The recovery dry-run fast path must restore original signal handlers before returning."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    original_sigint = object()
    original_sigterm = object()
    installs: list[tuple[signal.Signals, object]] = []

    def fake_signal(sig: signal.Signals, handler: object) -> object:
        installs.append((sig, handler))
        if sig == signal.SIGINT and len([call for call in installs if call[0] == signal.SIGINT]) == 1:
            return original_sigint
        if sig == signal.SIGTERM and len([call for call in installs if call[0] == signal.SIGTERM]) == 1:
            return original_sigterm
        return object()

    with patch("gza.cli.watch.signal.signal", side_effect=fake_signal):
        rc = cmd_watch(args)

    assert rc == 0
    assert installs[0][0] == signal.SIGINT
    assert installs[1][0] == signal.SIGTERM
    assert installs[-2] == (signal.SIGINT, original_sigint)
    assert installs[-1] == (signal.SIGTERM, original_sigterm)


def test_cmd_watch_restart_failed_dry_run_handles_mixed_naive_and_aware_completed_at(tmp_path: Path) -> None:
    """Recovery dry-run should tolerate legacy naive timestamps mixed with aware ones."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    legacy = store.add("Legacy failed implement", task_type="implement")
    assert legacy.id is not None
    legacy.status = "failed"
    legacy.failure_reason = "INFRASTRUCTURE_ERROR"
    legacy.completed_at = datetime(2026, 4, 28, 10, 0, 0)
    store.update(legacy)

    current = store.add("Current failed implement", task_type="implement")
    assert current.id is not None
    current.status = "failed"
    current.failure_reason = "INFRASTRUCTURE_ERROR"
    current.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(current)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0


def test_cmd_watch_restart_failed_dry_run_hides_skipped_by_default_and_sorts_oldest_created_first(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Recovery dry-run should print shared attention rows by default and hide ordinary skips."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    older = store.add("Older failed plan", task_type="plan")
    assert older.id is not None
    older.status = "failed"
    older.failure_reason = "INFRASTRUCTURE_ERROR"
    older.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(older)

    manual = store.add("Manual failed implement", task_type="implement")
    assert manual.id is not None
    manual.status = "failed"
    manual.failure_reason = "TEST_FAILURE"
    manual.completed_at = datetime(2026, 4, 28, 10, 30, 0, tzinfo=UTC)
    store.update(manual)

    exhausted_root = store.add("Failed resume root", task_type="implement")
    assert exhausted_root.id is not None
    exhausted_root.status = "failed"
    exhausted_root.failure_reason = "MAX_TURNS"
    exhausted_root.session_id = "sess-exhausted"
    exhausted_root.branch = "feature/exhausted"
    exhausted_root.completed_at = datetime(2026, 4, 28, 10, 45, 0, tzinfo=UTC)
    store.update(exhausted_root)

    exhausted_child = store.add("Failed resume attempt", task_type="implement", based_on=exhausted_root.id)
    assert exhausted_child.id is not None
    exhausted_child.status = "failed"
    exhausted_child.failure_reason = "MAX_TURNS"
    exhausted_child.session_id = exhausted_root.session_id
    exhausted_child.branch = exhausted_root.branch
    exhausted_child.completed_at = datetime(2026, 4, 28, 10, 50, 0, tzinfo=UTC)
    store.update(exhausted_child)

    exhausted_child_retry_limit = store.add("Failed resume attempt 2", task_type="implement", based_on=exhausted_root.id)
    assert exhausted_child_retry_limit.id is not None
    exhausted_child_retry_limit.status = "failed"
    exhausted_child_retry_limit.failure_reason = "MAX_TURNS"
    exhausted_child_retry_limit.session_id = exhausted_root.session_id
    exhausted_child_retry_limit.branch = exhausted_root.branch
    exhausted_child_retry_limit.completed_at = datetime(2026, 4, 28, 10, 52, 0, tzinfo=UTC)
    store.update(exhausted_child_retry_limit)

    hidden_skip = store.add("Pending retry already queued", task_type="implement")
    assert hidden_skip.id is not None
    hidden_skip.status = "failed"
    hidden_skip.failure_reason = "MAX_TURNS"
    hidden_skip.session_id = "sess-hidden"
    hidden_skip.branch = "feature/hidden"
    hidden_skip.completed_at = datetime(2026, 4, 28, 10, 55, 0, tzinfo=UTC)
    store.update(hidden_skip)

    pending_retry = store.add("Pending retry already queued", task_type="implement", based_on=hidden_skip.id)
    assert pending_retry.id is not None
    pending_retry.status = "pending"
    pending_retry.completed_at = None
    pending_retry.branch = "feature/hidden-retry"
    store.update(pending_retry)

    newer = store.add("Newer failed plan", task_type="plan")
    assert newer.id is not None
    newer.status = "failed"
    newer.failure_reason = "INFRASTRUCTURE_ERROR"
    newer.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(newer)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=False,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    normalized = " ".join(stdout.split())
    assert stdout.index(older.id) < stdout.index(newer.id)
    assert "Needs attention" in stdout
    assert hidden_skip.id not in stdout
    assert f'{manual.id} implement "Manual failed implement" reason=manual-failure-reason' in normalized
    assert "TEST_FAILURE requires manual intervention" in normalized
    assert f'{exhausted_root.id} implement "Failed resume root" reason=retry-limit-reached' in normalized
    assert "automatic recovery stops here; retry limit reached" in normalized
    assert f'{exhausted_child.id} implement "Failed resume attempt"' not in normalized
    assert f'{exhausted_child_retry_limit.id} implement "Failed resume attempt 2"' not in normalized
    assert "reason=newer-recovery-descendant-needs-attention" not in normalized
    assert "2 actionable (0 resume, 2 retry), 2 needs attention, 1 skipped hidden" in normalized


def test_cmd_watch_restart_failed_dry_run_show_skipped_includes_skipped_entries(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--show-skipped should include ordinary skipped recovery decisions that are hidden by default."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    actionable = store.add("Failed plan", task_type="plan")
    assert actionable.id is not None
    actionable.status = "failed"
    actionable.failure_reason = "INFRASTRUCTURE_ERROR"
    actionable.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(actionable)

    skipped = store.add("Failed implement", task_type="implement")
    assert skipped.id is not None
    skipped.status = "failed"
    skipped.failure_reason = "MAX_TURNS"
    skipped.session_id = "sess-skip"
    skipped.branch = "feature/skipped"
    skipped.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(skipped)

    pending_retry = store.add(skipped.prompt, task_type="implement", based_on=skipped.id, depends_on=skipped.depends_on)
    assert pending_retry.id is not None
    pending_retry.status = "pending"
    pending_retry.branch = "feature/skipped-retry"
    store.update(pending_retry)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert skipped.id in stdout
    assert stdout.index(actionable.id) < stdout.index(skipped.id)
    assert "reason=recovery_already_pending" in stdout
    assert "1 skipped" in stdout


def test_cmd_watch_restart_failed_dry_run_suppresses_fully_recovered_failed_ancestors(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Recovery dry-run should silently omit failed ancestors whose recovery chain already completed."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    actionable = store.add("Still actionable failed plan", task_type="plan")
    assert actionable.id is not None
    actionable.status = "failed"
    actionable.failure_reason = "INFRASTRUCTURE_ERROR"
    actionable.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(actionable)

    failed_root = store.add("Recovered failed implement", task_type="implement")
    assert failed_root.id is not None
    failed_root.status = "failed"
    failed_root.failure_reason = "MAX_TURNS"
    failed_root.session_id = "sess-root"
    failed_root.branch = "feature/root"
    failed_root.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(failed_root)

    failed_resume = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
    assert failed_resume.id is not None
    failed_resume.status = "failed"
    failed_resume.failure_reason = "MAX_TURNS"
    failed_resume.session_id = failed_root.session_id
    failed_resume.branch = failed_root.branch
    failed_resume.completed_at = datetime(2026, 4, 28, 10, 10, 0, tzinfo=UTC)
    store.update(failed_resume)

    completed_resume = store.add(failed_resume.prompt, task_type="implement", based_on=failed_resume.id)
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = failed_resume.session_id
    completed_resume.branch = failed_resume.branch
    completed_resume.completed_at = datetime(2026, 4, 28, 10, 15, 0, tzinfo=UTC)
    store.update(completed_resume)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert actionable.id in stdout
    assert failed_root.id not in stdout
    assert failed_resume.id not in stdout
    assert "recovery child already completed" not in stdout
    assert "recovery descendant already completed" not in stdout


def test_cmd_watch_restart_failed_dry_run_suppresses_failed_sidequests_with_merged_target_impl(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Merged implementation targets should silently suppress failed review/improve/rebase rows in watch recovery output."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    merged_impl = store.add("Merged implementation", task_type="implement")
    assert merged_impl.id is not None
    merged_impl.status = "completed"
    merged_impl.has_commits = True
    merged_impl.merge_status = "merged"
    merged_impl.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(merged_impl)

    visible_impl = store.add("Visible implementation", task_type="implement")
    assert visible_impl.id is not None
    visible_impl.status = "completed"
    visible_impl.has_commits = True
    visible_impl.merge_status = "unmerged"
    visible_impl.completed_at = datetime(2026, 4, 28, 10, 1, 0, tzinfo=UTC)
    store.update(visible_impl)

    failed_review = store.add("Failed review", task_type="review", depends_on=merged_impl.id, based_on=merged_impl.id)
    failed_review.status = "failed"
    failed_review.failure_reason = "MISSING_REPORT_ARTIFACT"
    failed_review.completed_at = datetime(2026, 4, 28, 10, 2, 0, tzinfo=UTC)
    store.update(failed_review)

    review_for_improve = store.add("Review for improve", task_type="review", depends_on=merged_impl.id, based_on=merged_impl.id)
    assert review_for_improve.id is not None
    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        depends_on=review_for_improve.id,
        based_on=merged_impl.id,
    )
    failed_improve.status = "failed"
    failed_improve.failure_reason = "GIT_ERROR"
    failed_improve.completed_at = datetime(2026, 4, 28, 10, 3, 0, tzinfo=UTC)
    store.update(failed_improve)

    failed_rebase = store.add("Failed rebase", task_type="rebase", based_on=merged_impl.id)
    failed_rebase.status = "failed"
    failed_rebase.failure_reason = "INTERRUPTED"
    failed_rebase.completed_at = datetime(2026, 4, 28, 10, 4, 0, tzinfo=UTC)
    store.update(failed_rebase)

    visible_failed = store.add("Visible failed review", task_type="review", depends_on=visible_impl.id, based_on=visible_impl.id)
    assert visible_failed.id is not None
    visible_failed.status = "failed"
    visible_failed.failure_reason = "MISSING_REPORT_ARTIFACT"
    visible_failed.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(visible_failed)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    stdout = capsys.readouterr().out
    assert rc == 0
    assert visible_failed.id in stdout
    assert failed_review.id not in stdout
    assert failed_improve.id not in stdout
    assert failed_rebase.id not in stdout
    assert "resolved_by_merged_target" not in stdout


def test_cmd_watch_restart_failed_dry_run_keeps_failed_descendant_visible_under_completed_non_recovery_ancestor(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A completed non-recovery ancestor must not hide a failed descendant in watch recovery output."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)

    root = store.add("Completed root implement", task_type="implement")
    assert root.id is not None
    root.status = "completed"
    root.session_id = "sess-root"
    root.branch = "feature/root"
    root.merge_status = "unmerged"
    root.has_commits = True
    root.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(root)

    manual_follow_up = store.add("Manual follow-up implement", task_type="implement", based_on=root.id)
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.merge_status = "merged"
    manual_follow_up.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(manual_follow_up)

    failed_descendant = store.add(manual_follow_up.prompt, task_type="implement", based_on=manual_follow_up.id)
    assert failed_descendant.id is not None
    failed_descendant.status = "failed"
    failed_descendant.failure_reason = "MAX_TURNS"
    failed_descendant.session_id = manual_follow_up.session_id
    failed_descendant.branch = manual_follow_up.branch
    failed_descendant.completed_at = datetime(2026, 4, 28, 10, 10, 0, tzinfo=UTC)
    store.update(failed_descendant)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert failed_descendant.id in stdout
    assert "Needs attention" in stdout


def test_cmd_watch_restart_failed_dry_run_keeps_failed_parent_visible_with_pending_manual_follow_up(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A pending manual based_on follow-up must not supersede the failed parent."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/failed"
    failed.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(failed)

    manual_follow_up = store.add("Fresh follow-up implement", task_type="implement", based_on=failed.id)
    assert manual_follow_up.id is not None
    manual_follow_up.status = "pending"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    store.update(manual_follow_up)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert failed.id in stdout
    assert f"resume {failed.id}" in stdout
    assert "reason=recovery_already_pending" not in stdout
    assert "recovery child already pending" not in stdout


def test_cmd_watch_restart_failed_dry_run_keeps_failed_parent_visible_with_failed_manual_follow_up(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed manual based_on follow-up must not supersede the failed parent."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)

    failed = store.add("Failed plan", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(failed)

    manual_follow_up = store.add("Fresh follow-up plan", task_type="plan", based_on=failed.id)
    assert manual_follow_up.id is not None
    manual_follow_up.status = "failed"
    manual_follow_up.failure_reason = "MAX_TURNS"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(manual_follow_up)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert failed.id in stdout
    assert manual_follow_up.id in stdout
    assert "retry  " in stdout
    assert "reason=recovery_has_newer_unresolved_descendant" not in stdout
    assert "newer recovery descendant" not in stdout


def test_cmd_watch_restart_failed_dry_run_keeps_failed_parent_visible_with_completed_same_payload_manual_follow_up(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A completed same-payload manual follow-up on a different session/branch must not resolve the failed parent."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="plan")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.completed_at = datetime(2026, 4, 28, 9, 55, 0, tzinfo=UTC)
    store.update(dependency)

    failed = store.add("Failed implement", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/failed"
    failed.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(failed)

    manual_follow_up = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        depends_on=failed.depends_on,
        recovery_origin="manual",
    )
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.merge_status = "unmerged"
    manual_follow_up.has_commits = True
    manual_follow_up.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(manual_follow_up)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert failed.id in stdout
    assert f"resume {failed.id}" in stdout
    assert "recovery child already completed" not in stdout


def test_cmd_watch_restart_failed_dry_run_keeps_failed_parent_visible_with_completed_same_payload_legacy_manual_follow_up(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A legacy same-payload manual follow-up on a different session/branch must not resolve the failed parent."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)

    dependency = store.add("Dependency", task_type="plan")
    assert dependency.id is not None
    dependency.status = "completed"
    dependency.completed_at = datetime(2026, 4, 28, 9, 55, 0, tzinfo=UTC)
    store.update(dependency)

    failed = store.add("Failed implement", task_type="implement", depends_on=dependency.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/failed"
    failed.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(failed)

    manual_follow_up = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        depends_on=failed.depends_on,
    )
    assert manual_follow_up.id is not None
    manual_follow_up.status = "completed"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.merge_status = "unmerged"
    manual_follow_up.has_commits = True
    manual_follow_up.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(manual_follow_up)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert failed.id in stdout
    assert f"resume {failed.id}" in stdout
    assert "recovery child already completed" not in stdout


def test_cmd_watch_restart_failed_dry_run_keeps_failed_root_visible_across_non_recovery_break_with_resolved_grandchild(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A resolved recovery grandchild below a manual break must not hide the original failed root."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)

    failed_root = store.add("Failed implement", task_type="implement")
    assert failed_root.id is not None
    failed_root.status = "failed"
    failed_root.failure_reason = "MAX_TURNS"
    failed_root.session_id = "sess-root"
    failed_root.branch = "feature/root"
    failed_root.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(failed_root)

    manual_follow_up = store.add(
        failed_root.prompt,
        task_type="implement",
        based_on=failed_root.id,
        recovery_origin="manual",
    )
    assert manual_follow_up.id is not None
    manual_follow_up.status = "failed"
    manual_follow_up.failure_reason = "MAX_TURNS"
    manual_follow_up.session_id = "sess-manual"
    manual_follow_up.branch = "feature/manual"
    manual_follow_up.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(manual_follow_up)

    completed_resume = store.add(manual_follow_up.prompt, task_type="implement", based_on=manual_follow_up.id)
    assert completed_resume.id is not None
    completed_resume.status = "completed"
    completed_resume.session_id = manual_follow_up.session_id
    completed_resume.branch = manual_follow_up.branch
    completed_resume.merge_status = "unmerged"
    completed_resume.has_commits = True
    completed_resume.completed_at = datetime(2026, 4, 28, 10, 10, 0, tzinfo=UTC)
    store.update(completed_resume)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert failed_root.id in stdout
    assert f"resume {failed_root.id}" in stdout
    assert "recovery child already completed" not in stdout
    assert "recovery descendant already completed" not in stdout


def test_cmd_watch_restart_failed_dry_run_keeps_failed_fix_visible_under_completed_implement_ancestor(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A cross-type completed ancestor must not hide an independent failed fix in watch recovery output."""
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "require_review_before_merge: false\n"
    )
    store = make_store(tmp_path)

    root = store.add("Completed root implement", task_type="implement")
    assert root.id is not None
    root.status = "completed"
    root.branch = "feature/root"
    root.merge_status = "unmerged"
    root.has_commits = True
    root.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(root)

    completed_fix = store.add("Completed fix", task_type="fix", based_on=root.id, same_branch=True)
    assert completed_fix.id is not None
    completed_fix.status = "completed"
    completed_fix.session_id = "sess-fix"
    completed_fix.branch = root.branch
    completed_fix.merge_status = "merged"
    completed_fix.completed_at = datetime(2026, 4, 28, 10, 5, 0, tzinfo=UTC)
    store.update(completed_fix)

    failed_fix = store.add(completed_fix.prompt, task_type="fix", based_on=completed_fix.id, same_branch=True)
    assert failed_fix.id is not None
    failed_fix.status = "failed"
    failed_fix.failure_reason = "MAX_TURNS"
    failed_fix.session_id = completed_fix.session_id
    failed_fix.branch = completed_fix.branch
    failed_fix.completed_at = datetime(2026, 4, 28, 10, 10, 0, tzinfo=UTC)
    store.update(failed_fix)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert failed_fix.id in stdout
    assert "Needs attention" in stdout


def test_cmd_watch_restart_failed_dry_run_saturates_retry_resume_attempt_display(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Exhausted retry->resume chains should display saturated attempt counters."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root = store.add("Root failed plan", task_type="plan")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "INFRASTRUCTURE_ERROR"
    root.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(root)

    retry_child = store.add(root.prompt, task_type="plan", based_on=root.id)
    assert retry_child.id is not None
    retry_child.status = "failed"
    retry_child.failure_reason = "MAX_TURNS"
    retry_child.session_id = "sess-retry"
    retry_child.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(retry_child)

    resumed_retry = store.add(retry_child.prompt, task_type="plan", based_on=retry_child.id)
    assert resumed_retry.id is not None
    resumed_retry.status = "failed"
    resumed_retry.failure_reason = "TIMEOUT"
    resumed_retry.session_id = retry_child.session_id
    resumed_retry.completed_at = datetime(2026, 4, 28, 12, 0, 0, tzinfo=UTC)
    store.update(resumed_retry)

    resumed_retry_retry_limit = store.add(retry_child.prompt, task_type="plan", based_on=retry_child.id)
    assert resumed_retry_retry_limit.id is not None
    resumed_retry_retry_limit.status = "failed"
    resumed_retry_retry_limit.failure_reason = "TIMEOUT"
    resumed_retry_retry_limit.session_id = retry_child.session_id
    resumed_retry_retry_limit.completed_at = datetime(2026, 4, 28, 12, 5, 0, tzinfo=UTC)
    store.update(resumed_retry_retry_limit)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=True,
        show_skipped=True,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert "attempt=3/2" not in stdout
    normalized = " ".join(stdout.split())
    assert f'{retry_child.id} plan "Root failed plan" reason=retry-limit-reached' in normalized
    assert "automatic recovery stops here; retry limit reached" in normalized


def test_collect_unhandled_failures_skips_actionable_recovery_failures(tmp_path: Path) -> None:
    """With restart-failed mode active, actionable failures are excluded from backoff accounting."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    resumable = store.add("Resumable implement", task_type="implement")
    assert resumable.id is not None
    resumable.status = "failed"
    resumable.failure_reason = "MAX_TURNS"
    resumable.session_id = "sess-123"
    resumable.completed_at = datetime.now(UTC)
    store.update(resumable)

    config = Config.load(tmp_path)
    old = {str(resumable.id): {"status": "in_progress"}}
    new = {
        str(resumable.id): {
            "status": "failed",
            "task_type": "implement",
            "failure_reason": "MAX_TURNS",
        }
    }

    failures = _collect_unhandled_failures(
        old,
        new,
        store=store,
        config=config,
        restart_failed_mode=True,
        max_recovery_attempts=config.max_resume_attempts,
    )
    assert failures == []


def test_collect_unhandled_failures_restart_failed_keeps_manual_failures_visible_with_pending_child(
    tmp_path: Path,
) -> None:
    """Manual-only failures must still count toward backoff even if a pending recovery child exists."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Broken plan", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "TEST_FAILURE"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    child = store.add("Manually queued retry", task_type="plan", based_on=failed.id)
    assert child.id is not None
    child.status = "pending"
    store.update(child)

    config = Config.load(tmp_path)
    old = {str(failed.id): {"status": "in_progress"}}
    new = {
        str(failed.id): {
            "status": "failed",
            "task_type": "plan",
            "failure_reason": "TEST_FAILURE",
        }
    }

    failures = _collect_unhandled_failures(
        old,
        new,
        store=store,
        config=config,
        restart_failed_mode=True,
        max_recovery_attempts=config.max_resume_attempts,
    )
    assert [(failure.task_id, failure.reason) for failure in failures] == [(str(failed.id), "TEST_FAILURE")]


def test_collect_unhandled_failures_restart_failed_counts_skipped_resumable_out_of_scope_failures(
    tmp_path: Path,
) -> None:
    """Restart-failed should suppress auto-recoverable failures from backoff accounting."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed review", task_type="review")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    old = {str(failed.id): {"status": "in_progress"}}
    new = {
        str(failed.id): {
            "status": "failed",
            "task_type": "review",
            "failure_reason": "MAX_TURNS",
        }
    }

    failures = _collect_unhandled_failures(
        old,
        new,
        store=store,
        config=config,
        restart_failed_mode=True,
        max_recovery_attempts=config.max_resume_attempts,
    )
    assert failures == []


def test_collect_unhandled_failures_restart_failed_suppresses_merged_target_sidequests(
    tmp_path: Path,
) -> None:
    """Restart-failed should hide merged-target sidequest failures from backoff accounting."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    merged_impl = store.add("Merged implementation", task_type="implement")
    assert merged_impl.id is not None
    merged_impl.status = "completed"
    merged_impl.has_commits = True
    merged_impl.merge_status = "merged"
    merged_impl.completed_at = datetime.now(UTC)
    store.update(merged_impl)

    visible_impl = store.add("Visible implementation", task_type="implement")
    assert visible_impl.id is not None
    visible_impl.status = "completed"
    visible_impl.has_commits = True
    visible_impl.merge_status = "unmerged"
    visible_impl.completed_at = datetime.now(UTC)
    store.update(visible_impl)

    hidden_review = store.add(
        "Hidden failed review",
        task_type="review",
        depends_on=merged_impl.id,
        based_on=merged_impl.id,
    )
    assert hidden_review.id is not None
    hidden_review.status = "failed"
    hidden_review.failure_reason = "MISSING_REPORT_ARTIFACT"
    hidden_review.completed_at = datetime.now(UTC)
    store.update(hidden_review)

    visible_review = store.add(
        "Visible failed review",
        task_type="review",
        depends_on=visible_impl.id,
        based_on=visible_impl.id,
    )
    assert visible_review.id is not None
    visible_review.status = "failed"
    visible_review.failure_reason = "MISSING_REPORT_ARTIFACT"
    visible_review.completed_at = datetime.now(UTC)
    store.update(visible_review)

    config = Config.load(tmp_path)
    old = {
        str(hidden_review.id): {"status": "in_progress"},
        str(visible_review.id): {"status": "in_progress"},
    }
    new = {
        str(hidden_review.id): {
            "status": "failed",
            "task_type": "review",
            "failure_reason": "MISSING_REPORT_ARTIFACT",
        },
        str(visible_review.id): {
            "status": "failed",
            "task_type": "review",
            "failure_reason": "MISSING_REPORT_ARTIFACT",
        },
    }

    failures = _collect_unhandled_failures(
        old,
        new,
        store=store,
        config=config,
        restart_failed_mode=True,
        max_recovery_attempts=config.max_resume_attempts,
    )
    assert [(failure.task_id, failure.reason) for failure in failures] == [
        (str(visible_review.id), "MISSING_REPORT_ARTIFACT")
    ]


@pytest.mark.parametrize("task_type", ["implement", "review", "improve", "rebase"])
def test_collect_unhandled_failures_default_mode_skips_auto_resumable_failures(
    tmp_path: Path, task_type: str
) -> None:
    """Default watch mode should keep backoff accounting limited to non-auto-resumable failures."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    resumable = store.add(f"Resumable {task_type}", task_type=task_type)
    assert resumable.id is not None
    resumable.status = "failed"
    resumable.failure_reason = "MAX_TURNS"
    resumable.session_id = "sess-123"
    resumable.completed_at = datetime.now(UTC)
    store.update(resumable)

    config = Config.load(tmp_path)
    old = {str(resumable.id): {"status": "in_progress"}}
    new = {
        str(resumable.id): {
            "status": "failed",
            "task_type": task_type,
            "failure_reason": "MAX_TURNS",
        }
    }

    failures = _collect_unhandled_failures(old, new, store=store, config=config, restart_failed_mode=False)
    assert failures == []


def test_collect_unhandled_failures_default_mode_counts_resumable_failure_after_attempt_cap(
    tmp_path: Path,
) -> None:
    """Default watch should count capped resumable failures toward backoff."""
    (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: 1\n")
    store = make_store(tmp_path)

    root = store.add("Failed root", task_type="implement")
    assert root.id is not None
    root.status = "failed"
    root.failure_reason = "MAX_TURNS"
    root.session_id = "sess-123"
    root.completed_at = datetime.now(UTC)
    store.update(root)

    failed = store.add("Failed capped child", task_type="implement", based_on=root.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    old = {str(failed.id): {"status": "in_progress"}}
    new = {
        str(failed.id): {
            "status": "failed",
            "task_type": "implement",
            "failure_reason": "MAX_TURNS",
        }
    }

    failures = _collect_unhandled_failures(
        old,
        new,
        store=store,
        config=config,
        max_recovery_attempts=config.max_resume_attempts,
        restart_failed_mode=False,
    )
    assert [(failure.task_id, failure.reason) for failure in failures] == [(str(failed.id), "MAX_TURNS")]


def test_collect_unhandled_failures_includes_unknown_failures(tmp_path: Path) -> None:
    """Unknown failures should contribute to watch backoff decisions."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Broken plan", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "UNKNOWN"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    old = {str(failed.id): {"status": "in_progress"}}
    new = {
        str(failed.id): {
            "status": "failed",
            "task_type": "plan",
            "failure_reason": "UNKNOWN",
        }
    }

    failures = _collect_unhandled_failures(old, new, store=store, config=config)
    assert [(failure.task_id, failure.reason) for failure in failures] == [(str(failed.id), "UNKNOWN")]


def test_watch_transition_collectors_apply_case_insensitive_tag_filters(tmp_path: Path) -> None:
    """Transition collectors should use canonical case-insensitive tag filtering."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    completed_tagged = store.add("Completed tagged", task_type="plan", tags=("release-1.2",))
    completed_other = store.add("Completed other", task_type="plan", tags=("backlog",))
    failed_tagged = store.add("Failed tagged", task_type="plan", tags=("release-1.2",))
    failed_other = store.add("Failed other", task_type="plan", tags=("backlog",))
    assert completed_tagged.id is not None
    assert completed_other.id is not None
    assert failed_tagged.id is not None
    assert failed_other.id is not None

    config = Config.load(tmp_path)
    old = {
        str(completed_tagged.id): {"status": "in_progress"},
        str(completed_other.id): {"status": "in_progress"},
        str(failed_tagged.id): {"status": "in_progress"},
        str(failed_other.id): {"status": "in_progress"},
    }
    new = {
        str(completed_tagged.id): {
            "status": "completed",
            "task_type": "plan",
            "failure_reason": None,
        },
        str(completed_other.id): {
            "status": "completed",
            "task_type": "plan",
            "failure_reason": None,
        },
        str(failed_tagged.id): {
            "status": "failed",
            "task_type": "plan",
            "failure_reason": "UNKNOWN",
        },
        str(failed_other.id): {
            "status": "failed",
            "task_type": "plan",
            "failure_reason": "UNKNOWN",
        },
    }

    completed_ids = _collect_completed_transition_ids(
        old,
        new,
        store=store,
        tags=("Release-1.2",),
    )
    failures = _collect_unhandled_failures(
        old,
        new,
        store=store,
        config=config,
        tags=("Release-1.2",),
    )

    assert completed_ids == [str(completed_tagged.id)]
    assert [(failure.task_id, failure.reason) for failure in failures] == [(str(failed_tagged.id), "UNKNOWN")]


def test_compute_failure_backoff_seconds_caps_at_max(tmp_path: Path) -> None:
    """Watch failure backoff should grow exponentially and clamp at the configured max."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 300\n"
    )
    config = Config.load(tmp_path)

    assert _compute_failure_backoff_seconds(config, 1) == 60
    assert _compute_failure_backoff_seconds(config, 2) == 120
    assert _compute_failure_backoff_seconds(config, 3) == 240
    assert _compute_failure_backoff_seconds(config, 4) == 300


def test_system_can_run_tasks_skips_docker_probe_when_not_required(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="test-project", use_docker=False)

    with patch("gza.cli.watch.wait_for_docker_ready") as mock_wait:
        assert _system_can_run_tasks(config) is True

    mock_wait.assert_not_called()


def test_system_can_run_tasks_waits_for_docker_when_required(tmp_path: Path) -> None:
    config = Config(
        project_dir=tmp_path,
        project_name="test-project",
        use_docker=True,
        docker_startup_timeout=17,
    )

    with patch("gza.cli.watch.wait_for_docker_ready", return_value=False) as mock_wait:
        assert _system_can_run_tasks(config) is False

    mock_wait.assert_called_once_with(17)


def test_cmd_watch_holds_until_docker_returns_then_resumes(tmp_path: Path) -> None:
    """Required-Docker watch should hold outside the normal pass and resume without backoff."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "use_docker: true\n"
        "docker_startup_timeout: 1\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 240\n"
        "  failure_halt_after: 10\n"
    )
    store = make_store(tmp_path)
    task = store.add("Pending plan", task_type="plan")
    assert task.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=10,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
    )

    signal_handlers: dict[signal.Signals, object] = {}
    sleeps: list[int] = []

    def register_signal(sig: signal.Signals, handler: object) -> object:
        signal_handlers[sig] = handler
        return object()

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 2:
            handler = signal_handlers[signal.SIGTERM]
            assert callable(handler)
            handler(signal.SIGTERM, None)

    with (
        patch("gza.cli.watch._system_can_run_tasks", side_effect=[False, True]),
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(False, 0, 1)) as run_cycle,
        patch("gza.cli.watch._emit_transition_events") as emit_transition_events,
        patch(
            "gza.cli.watch._task_snapshot",
            return_value={str(task.id): {"status": "pending", "task_type": "plan", "failure_reason": None}},
        ),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=register_signal),
    ):
        rc = cmd_watch(args)

    assert rc == 128 + signal.SIGTERM
    assert sleeps == [5, 5]
    assert run_cycle.call_count == 1
    assert emit_transition_events.call_count == 2
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "pending"
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "HOLD" in log_text
    assert "holding queue (1 pending)" in log_text
    assert "RESUME" in log_text
    assert "BACKOFF" not in log_text
    assert "failure halt threshold reached" not in log_text


def test_cmd_watch_no_docker_bypasses_system_probe(tmp_path: Path) -> None:
    """No-Docker watch should proceed without probing Docker readiness."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "use_docker: false\n"
    )
    store = make_store(tmp_path)
    task = store.add("Pending plan", task_type="plan")
    assert task.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=10,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
    )

    signal_handlers: dict[signal.Signals, object] = {}
    sleeps: list[int] = []

    def register_signal(sig: signal.Signals, handler: object) -> object:
        signal_handlers[sig] = handler
        return object()

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)
        handler = signal_handlers[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    with (
        patch(
            "gza.cli.watch.wait_for_docker_ready",
            side_effect=AssertionError("docker probe should be bypassed when use_docker=false"),
        ),
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(False, 0, 1)) as run_cycle,
        patch(
            "gza.cli.watch._task_snapshot",
            return_value={str(task.id): {"status": "pending", "task_type": "plan", "failure_reason": None}},
        ),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=register_signal),
    ):
        rc = cmd_watch(args)

    assert rc == 128 + signal.SIGTERM
    assert sleeps == [5]
    assert run_cycle.call_count == 1
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "HOLD" not in log_text


def test_cmd_watch_logs_and_sleeps_for_failure_backoff(tmp_path: Path) -> None:
    """watch should log explicit cooldowns after consecutive non-auto-resumable failures."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 240\n"
        "  failure_halt_after: 10\n"
    )
    store = make_store(tmp_path)
    task = store.add("Pending plan", task_type="plan")
    assert task.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
    )

    snapshots = [
        {str(task.id): {"status": "pending", "task_type": "plan", "failure_reason": None}},
        {str(task.id): {"status": "pending", "task_type": "plan", "failure_reason": None}},
        {str(task.id): {"status": "failed", "task_type": "plan", "failure_reason": "UNKNOWN"}},
        {str(task.id): {"status": "failed", "task_type": "plan", "failure_reason": "UNKNOWN"}},
        {str(task.id): {"status": "failed", "task_type": "plan", "failure_reason": "UNKNOWN"}},
    ]
    cycle_results = [
        _CycleResult(True, 0, 1),
        _CycleResult(False, 0, 1),
    ]
    sleeps: list[int] = []

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)

    with (
        patch("gza.cli.watch._task_snapshot", side_effect=snapshots),
        patch("gza.cli.watch._run_cycle", side_effect=cycle_results),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert sleeps == [60]
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "BACKOFF" in log_text
    assert "sleeping 60s before starting more work" in log_text


def test_cmd_watch_restart_failed_does_not_backoff_for_actionable_review_recovery(tmp_path: Path) -> None:
    """Restart-failed should not back off for failures that the shared recovery policy will handle."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 240\n"
        "  failure_halt_after: 10\n"
    )
    store = make_store(tmp_path)
    task = store.add("Failed review", task_type="review")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "MAX_TURNS"
    task.session_id = "sess-123"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    snapshots = [
        {str(task.id): {"status": "in_progress", "task_type": "review", "failure_reason": None}},
        {str(task.id): {"status": "in_progress", "task_type": "review", "failure_reason": None}},
        {str(task.id): {"status": "failed", "task_type": "review", "failure_reason": "MAX_TURNS"}},
        {str(task.id): {"status": "failed", "task_type": "review", "failure_reason": "MAX_TURNS"}},
        {str(task.id): {"status": "failed", "task_type": "review", "failure_reason": "MAX_TURNS"}},
    ]
    cycle_results = [
        _CycleResult(False, 0, 0),
        _CycleResult(False, 0, 0),
    ]
    sleeps: list[int] = []

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)

    with (
        patch("gza.cli.watch._task_snapshot", side_effect=snapshots),
        patch("gza.cli.watch._run_cycle", side_effect=cycle_results),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert sleeps == []
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "BACKOFF" not in log_text


def test_cmd_watch_restart_failed_suppresses_merged_target_sidequest_failure(tmp_path: Path) -> None:
    """Restart-failed should fully suppress merged-target sidequest failure transitions."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 240\n"
        "  failure_halt_after: 1\n"
    )
    store = make_store(tmp_path)

    merged_impl = store.add("Merged implementation", task_type="implement")
    assert merged_impl.id is not None
    merged_impl.status = "completed"
    merged_impl.has_commits = True
    merged_impl.merge_status = "merged"
    merged_impl.completed_at = datetime.now(UTC)
    store.update(merged_impl)

    failed_review = store.add(
        "Failed review",
        task_type="review",
        depends_on=merged_impl.id,
        based_on=merged_impl.id,
    )
    assert failed_review.id is not None
    failed_review.status = "failed"
    failed_review.failure_reason = "MISSING_REPORT_ARTIFACT"
    failed_review.completed_at = datetime.now(UTC)
    store.update(failed_review)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    snapshots = [
        {str(failed_review.id): {"status": "in_progress", "task_type": "review", "failure_reason": None}},
        {str(failed_review.id): {"status": "in_progress", "task_type": "review", "failure_reason": None}},
        {
            str(failed_review.id): {
                "status": "failed",
                "task_type": "review",
                "failure_reason": "MISSING_REPORT_ARTIFACT",
            }
        },
        {
            str(failed_review.id): {
                "status": "failed",
                "task_type": "review",
                "failure_reason": "MISSING_REPORT_ARTIFACT",
            }
        },
        {
            str(failed_review.id): {
                "status": "failed",
                "task_type": "review",
                "failure_reason": "MISSING_REPORT_ARTIFACT",
            }
        },
    ]
    cycle_results = [_CycleResult(False, 0, 0), _CycleResult(False, 0, 0)]
    sleeps: list[int] = []

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)

    with (
        patch("gza.cli.watch._task_snapshot", side_effect=snapshots),
        patch("gza.cli.watch._run_cycle", side_effect=cycle_results),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert sleeps == []
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "BACKOFF" not in log_text
    assert "failure halt threshold reached" not in log_text
    assert str(failed_review.id) not in log_text


def test_cmd_watch_restart_failed_backoffs_for_unmerged_target_sidequest_failure(tmp_path: Path) -> None:
    """Restart-failed should keep visible sidequest failures in backoff accounting when the target is unmerged."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 240\n"
        "  failure_halt_after: 1\n"
    )
    store = make_store(tmp_path)

    unmerged_impl = store.add("Unmerged implementation", task_type="implement")
    assert unmerged_impl.id is not None
    unmerged_impl.status = "completed"
    unmerged_impl.has_commits = True
    unmerged_impl.merge_status = "unmerged"
    unmerged_impl.completed_at = datetime.now(UTC)
    store.update(unmerged_impl)

    failed_review = store.add(
        "Failed review",
        task_type="review",
        depends_on=unmerged_impl.id,
        based_on=unmerged_impl.id,
    )
    assert failed_review.id is not None
    failed_review.status = "failed"
    failed_review.failure_reason = "MISSING_REPORT_ARTIFACT"
    failed_review.completed_at = datetime.now(UTC)
    store.update(failed_review)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=True,
        restart_failed_batch=None,
        max_resume_attempts=None,
    )

    snapshots = [
        {str(failed_review.id): {"status": "in_progress", "task_type": "review", "failure_reason": None}},
        {str(failed_review.id): {"status": "in_progress", "task_type": "review", "failure_reason": None}},
        {
            str(failed_review.id): {
                "status": "failed",
                "task_type": "review",
                "failure_reason": "MISSING_REPORT_ARTIFACT",
            }
        },
        {
            str(failed_review.id): {
                "status": "failed",
                "task_type": "review",
                "failure_reason": "MISSING_REPORT_ARTIFACT",
            }
        },
        {
            str(failed_review.id): {
                "status": "failed",
                "task_type": "review",
                "failure_reason": "MISSING_REPORT_ARTIFACT",
            }
        },
    ]
    cycle_results = [_CycleResult(False, 0, 0), _CycleResult(False, 0, 0)]
    sleeps: list[int] = []

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)

    with (
        patch("gza.cli.watch._task_snapshot", side_effect=snapshots),
        patch("gza.cli.watch._run_cycle", side_effect=cycle_results),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert sleeps == []
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "BACKOFF" in log_text
    assert "failure halt threshold reached" in log_text
    assert str(failed_review.id) in log_text


def test_cmd_watch_max_resume_attempts_zero_disables_default_auto_resume(tmp_path: Path) -> None:
    """--max-resume-attempts 0 should disable plain-watch auto-resume for that invocation."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 240\n"
        "  failure_halt_after: 10\n"
    )
    store = make_store(tmp_path)
    task = store.add("Failed implement", task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "MAX_TURNS"
    task.session_id = "sess-123"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=5,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
        restart_failed=False,
        restart_failed_batch=None,
        max_resume_attempts=0,
    )

    snapshots = [
        {str(task.id): {"status": "in_progress", "task_type": "implement", "failure_reason": None}},
        {str(task.id): {"status": "in_progress", "task_type": "implement", "failure_reason": None}},
        {str(task.id): {"status": "failed", "task_type": "implement", "failure_reason": "MAX_TURNS"}},
        {str(task.id): {"status": "failed", "task_type": "implement", "failure_reason": "MAX_TURNS"}},
        {str(task.id): {"status": "failed", "task_type": "implement", "failure_reason": "MAX_TURNS"}},
    ]
    cycle_results = [_CycleResult(False, 0, 0), _CycleResult(False, 0, 0)]
    sleeps: list[int] = []

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)

    with (
        patch("gza.cli.watch._task_snapshot", side_effect=snapshots),
        patch("gza.cli.watch._run_cycle", side_effect=cycle_results) as run_cycle,
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_args_list[0].kwargs["max_recovery_attempts"] == 0
    assert sleeps == [60]
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "BACKOFF" in log_text
    assert f"{task.id}=MAX_TURNS" in log_text


def test_cmd_watch_halts_after_configured_failure_streak(tmp_path: Path) -> None:
    """watch should stop and log when the configured failure streak threshold is reached."""
    worktree_dir = tmp_path / ".gza-test-worktrees"
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {worktree_dir}\n"
        "watch:\n"
        "  failure_backoff_initial: 60\n"
        "  failure_backoff_max: 3600\n"
        "  failure_halt_after: 2\n"
    )
    store = make_store(tmp_path)
    task1 = store.add("Pending plan 1", task_type="plan")
    task2 = store.add("Pending plan 2", task_type="plan")
    assert task1.id is not None
    assert task2.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=5,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        group=None,
    )

    snapshots = [
        {
            str(task1.id): {"status": "pending", "task_type": "plan", "failure_reason": None},
            str(task2.id): {"status": "pending", "task_type": "plan", "failure_reason": None},
        },
        {
            str(task1.id): {"status": "pending", "task_type": "plan", "failure_reason": None},
            str(task2.id): {"status": "pending", "task_type": "plan", "failure_reason": None},
        },
        {
            str(task1.id): {"status": "failed", "task_type": "plan", "failure_reason": "UNKNOWN"},
            str(task2.id): {"status": "pending", "task_type": "plan", "failure_reason": None},
        },
        {
            str(task1.id): {"status": "failed", "task_type": "plan", "failure_reason": "UNKNOWN"},
            str(task2.id): {"status": "pending", "task_type": "plan", "failure_reason": None},
        },
        {
            str(task1.id): {"status": "failed", "task_type": "plan", "failure_reason": "UNKNOWN"},
            str(task2.id): {"status": "failed", "task_type": "plan", "failure_reason": "UNKNOWN"},
        },
    ]
    cycle_results = [
        _CycleResult(True, 0, 2),
        _CycleResult(True, 0, 1),
    ]
    sleeps: list[int] = []

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleeps.append(seconds)

    with (
        patch("gza.cli.watch._task_snapshot", side_effect=snapshots),
        patch("gza.cli.watch._run_cycle", side_effect=cycle_results),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert sleeps == [60]
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "failure halt threshold reached" in log_text


def test_cmd_watch_quiet_suppresses_worker_stdout_and_still_logs_events(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """`watch --quiet` should suppress helper stdout while still writing watch.log events."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    plan = store.add("Plan follow-up", task_type="plan")
    assert impl.id is not None
    assert plan.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=2,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
    )

    def fake_spawn_iterate(_args, _config, impl_task, *, quiet=False, **_kwargs):
        if not quiet:
            print("Started iterate worker noisy output")
        impl_task.status = "in_progress"
        store.update(impl_task)
        return 0

    def fake_spawn_worker(_args, _config, task_id=None, quiet=False, **_kwargs):
        if not quiet:
            print("Started worker noisy output")
        assert task_id is not None
        task = store.get(task_id)
        assert task is not None
        task.status = "in_progress"
        store.update(task)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=fake_spawn_iterate),
        patch("gza.cli.watch._spawn_background_worker", side_effect=fake_spawn_worker),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
        patch("gza.cli.watch.time.sleep"),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert "Started worker noisy output" not in stdout
    assert "Started iterate worker noisy output" not in stdout
    assert "Use 'gza ps' to view running workers" not in stdout

    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{impl.id} implement" in line
        for line in log_text.splitlines()
    )
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{plan.id} plan" in line
        for line in log_text.splitlines()
    )


def test_cmd_watch_uses_startup_quiet_and_emits_sleep_for_productive_and_idle_cycles(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Watch should suppress helper startup blocks even when not quiet, and log SLEEP every pass."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    plan = store.add("Plan follow-up", task_type="plan")
    assert impl.id is not None
    assert plan.id is not None

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=2,
        poll=1,
        max_idle=1,
        max_iterations=10,
        dry_run=False,
        quiet=False,
        yes=True,
    )

    def fake_spawn_iterate(_args, _config, impl_task, *, startup_quiet=False, **_kwargs):
        if not startup_quiet:
            print("Started iterate worker noisy output")
        impl_task.status = "in_progress"
        store.update(impl_task)
        return 0

    def fake_spawn_worker(_args, _config, task_id=None, *, startup_quiet=False, **_kwargs):
        if not startup_quiet:
            print("Started worker noisy output")
        assert task_id is not None
        task = store.get(task_id)
        assert task is not None
        task.status = "in_progress"
        store.update(task)
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=fake_spawn_iterate),
        patch("gza.cli.watch._spawn_background_worker", side_effect=fake_spawn_worker),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()),
        patch("gza.cli.watch._sleep_interruptibly"),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    stdout = capsys.readouterr().out
    assert "Started worker noisy output" not in stdout
    assert "Started iterate worker noisy output" not in stdout

    log_lines = (tmp_path / ".gza" / "watch.log").read_text().splitlines()
    sleep_lines = [
        line
        for line in log_lines
        if line.strip() and line.split(maxsplit=2)[1] == "SLEEP"
    ]
    assert len(sleep_lines) == 2
    assert all("sleeping 1s (0 pending, 0 running)" in line for line in sleep_lines)


def test_watch_cycle_quiet_logs_start_failed_when_iterate_spawn_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Quiet mode should suppress iterate helper stdout and emit START_FAILED."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    def noisy_iterate_fail(*_args, **_kwargs):
        print("Error spawning background iterate worker: boom")
        return 1

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=noisy_iterate_fail),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    stdout = capsys.readouterr().out
    assert "Error spawning background iterate worker" not in stdout
    log_text = log_path.read_text()
    assert "START_FAILED" in log_text
    assert f"{impl.id} implement: iterate worker spawn failed" in log_text


def test_watch_cycle_quiet_logs_start_failed_when_worker_spawn_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Quiet mode should suppress plain worker stdout and emit START_FAILED."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Plan follow-up", task_type="plan")
    assert plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    def noisy_worker_fail(*_args, **_kwargs):
        print("Error spawning background worker: boom")
        return 1

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=noisy_worker_fail),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
        )

    stdout = capsys.readouterr().out
    assert "Error spawning background worker" not in stdout
    log_text = log_path.read_text()
    assert "START_FAILED" in log_text
    assert f"{plan.id} plan: worker spawn failed" in log_text


def test_watch_cycle_quiet_logs_start_failed_when_recovery_iterate_spawn_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Quiet mode should suppress recovery iterate worker stdout and emit START_FAILED."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    def noisy_iterate_fail(*_args, **_kwargs):
        print("Error spawning background worker: boom")
        return 1

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume,
        patch("gza.cli.watch._spawn_background_iterate", side_effect=noisy_iterate_fail) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            quiet=True,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    stdout = capsys.readouterr().out
    assert "Error spawning background worker" not in stdout
    assert spawn_resume.call_count == 0
    assert spawn_iterate.call_count == 1
    children = store.get_based_on_children(failed.id)
    assert len(children) == 1
    child_id = children[0].id
    log_text = log_path.read_text()
    assert "START_FAILED" in log_text
    assert f"{failed.id} -> {child_id}: iterate worker spawn failed" in log_text


def test_watch_cycle_queue_events_use_queue_label(tmp_path: Path) -> None:
    """Pending queue transitions should use QUEUE, not PHASE."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Plan follow-up", task_type="plan")
    assert plan.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    log_lines = log_path.read_text().splitlines()
    queue_lines = [line for line in log_lines if line.split(maxsplit=2)[1] == "QUEUE"]
    assert any("pending queue active" in line for line in queue_lines)
    assert not any(line.split(maxsplit=2)[1] == "PHASE" for line in log_lines)


def test_cmd_watch_interrupts_sleep_promptly_on_sigint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """SIGINT should stop watch promptly, emit a clean shutdown line, and return 130."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=300,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
    )

    handlers: dict[int, object] = {}

    def fake_signal(sig, handler):
        previous = handlers.get(sig, signal.SIG_DFL)
        handlers[sig] = handler
        return previous

    sleep_calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) == 1:
            handler = handlers[signal.SIGINT]
            assert callable(handler)
            handler(signal.SIGINT, None)

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(True, 0, 0)) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=fake_signal),
        patch("gza.cli.watch.time.sleep", side_effect=fake_sleep),
    ):
        rc = cmd_watch(args)

    captured = capsys.readouterr()
    assert rc == 130
    assert run_cycle.call_count == 1
    assert sleep_calls
    assert max(sleep_calls) < args.poll
    assert captured.out == ""
    assert captured.err == "shutting down (workers left running)\n"


def test_cmd_watch_does_not_swallow_keyboard_interrupt_during_confirmation(tmp_path: Path) -> None:
    """Ctrl-C during the confirmation prompt should escape for top-level SIGINT handling."""
    setup_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=300,
        max_idle=None,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=False,
        resumed_reexec=False,
    )

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(True, 0, 0)),
        patch("builtins.input", side_effect=KeyboardInterrupt),
        patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: signal.SIG_DFL),
    ):
        with pytest.raises(KeyboardInterrupt):
            cmd_watch(args)


def test_watch_cycle_logs_create_review_validation_skip(tmp_path: Path) -> None:
    """Watch should bypass local create_review validation and route through iterate."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/create-review-validation"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "create_review"}),
        patch("gza.cli.git_ops._create_review_task", side_effect=AssertionError("local review creation should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    text = log_path.read_text()
    assert spawn_iterate.call_count == 1
    assert f"{impl.id} iterate" in text


def test_watch_cycle_run_review_spawn_failure_not_retried_in_step3(tmp_path: Path) -> None:
    """A routed run_review iterate spawn failure must not let the pending review relaunch from step 3."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/run-review-fail"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Pending review", task_type="review", depends_on=impl.id)
    assert review.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    action: dict[str, object] = {"type": "run_review", "review_task": review}

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value=action),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        # First call fails (iterate in step 1), second would be a bad step-3 plain pickup
        patch("gza.cli.watch._spawn_background_iterate", side_effect=[1, 0]) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    # Spawn should only be attempted once (step 1), not retried in step 3
    assert spawn_iterate.call_count == 1
    assert result.work_done is False

    log_lines = log_path.read_text().splitlines()
    impl_id = str(impl.id)
    assert any("START_FAILED" in line and impl_id in line for line in log_lines)
    assert not any(line.split(maxsplit=2)[1] == "START" and f"{review.id} review" in line for line in log_lines)


def test_watch_cycle_run_improve_spawn_failure_not_retried_in_step3(tmp_path: Path) -> None:
    """A routed run_improve iterate spawn failure must not let the pending improve relaunch from step 3."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/run-improve-fail"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    improve = store.add(
        "Improve feature",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = True

    action: dict[str, object] = {"type": "run_improve", "improve_task": improve}

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value=action),
        patch("gza.cli.git_ops.get_review_verdict", return_value="CHANGES_REQUESTED"),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        # First call fails (iterate in step 1), second would be a bad step-3 plain pickup
        patch("gza.cli.watch._spawn_background_iterate", side_effect=[1, 0]) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    # Spawn should only be attempted once (step 1), not retried in step 3
    assert spawn_iterate.call_count == 1
    assert result.work_done is False

    log_lines = log_path.read_text().splitlines()
    impl_id = str(impl.id)
    assert any("START_FAILED" in line and impl_id in line for line in log_lines)
    assert not any(line.split(maxsplit=2)[1] == "START" and f"{improve.id} improve" in line for line in log_lines)


def test_watch_cycle_run_improve_routes_retry_chain_through_root_impl_iterate(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/run-improve-iterate"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    failed_improve = store.add(
        "Failed improve",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed_improve.id is not None
    failed_improve.status = "failed"
    failed_improve.failure_reason = "MAX_TURNS"
    failed_improve.session_id = "sess-improve"
    failed_improve.completed_at = datetime.now(UTC)
    store.update(failed_improve)

    retry_improve = store.add(
        "Pending retry improve",
        task_type="improve",
        depends_on=review.id,
        based_on=failed_improve.id,
        same_branch=True,
    )
    assert retry_improve.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"
    git.can_merge.return_value = True

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "run_improve", "improve_task": retry_improve}),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=0) as spawn_iterate,
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert any(
        line.split(maxsplit=2)[1] == "START" and f"{impl.id} iterate" in line
        for line in log_path.read_text().splitlines()
    )


def test_watch_iterate_helper_skips_duplicate_impl_worker_for_pending_child(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/duplicate-iterate"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    improve = store.add(
        "Pending improve",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert improve.id is not None

    result = _watch_iterate_impl_target(
        store=store,
        git=_make_watch_git(),
        task=impl,
        action={"type": "run_improve", "improve_task": improve},
        running_task_ids={impl.id},
        target_branch="main",
        max_recovery_attempts=1,
    )

    assert result is not None
    assert result.status == "skip"
    assert result.message == f"{impl.id}: iterate already running for implementation chain"
    assert result.worker_label == "iterate"
    assert result.guarded_pending_task_id == improve.id


def _make_preseeded_watch_git(tmp_path: Path) -> "Git":
    """Create a Git subclass instance that satisfies isinstance(git, Git).

    _run_cycle constructs git = Git(config.project_dir) and lineage_query seeds
    read_context.merge_context only when isinstance(git, Git) is True.  A plain
    MagicMock does not pass that check, so a real subclass is required to prove
    the ambient _load_merge_context path is eliminated.
    """

    class _PreseedWatchGit(Git):
        def __init__(self, repo_dir: Path) -> None:
            self.repo_dir = repo_dir
            self._cache = None

        def _unexpected_git_subprocess(self, method_name: str) -> AssertionError:
            return AssertionError(
                f"{method_name} should not be reached in unit tests; "
                "patch the watch git seam instead of spawning real git subprocesses"
            )

        def default_branch(self) -> str:
            return "main"

        def current_branch(self) -> str:
            return "main"

        def local_branch_names(self) -> frozenset[str]:  # type: ignore[override]
            return frozenset()

        def branch_exists(self, branch: str) -> bool:
            return False

        def ref_exists(self, ref: str) -> bool:
            return False

        def branches_exist(self, branches: object) -> dict[str, bool]:
            if not isinstance(branches, (tuple, list, set, frozenset)):
                return {}
            return {str(branch): False for branch in branches}

        def resolve_refs(self, refs: object, *, peel: str = "commit") -> dict[str, str | None]:
            if not isinstance(refs, (tuple, list, set, frozenset)):
                return {}
            return {str(ref): None for ref in refs}

        def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:  # type: ignore[override]
            return False

        def _run_readonly_cached(self, *args: object, **kwargs: object) -> object:  # type: ignore[override]
            raise self._unexpected_git_subprocess("_run_readonly_cached")

        def _run_readonly_success_cached(self, *args: object, **kwargs: object) -> object:  # type: ignore[override]
            raise self._unexpected_git_subprocess("_run_readonly_success_cached")

    return _PreseedWatchGit(tmp_path)


def _make_empty_merged_watch_git(tmp_path: Path, *, empty_branch: str) -> "Git":
    class _EmptyMergedWatchGit(Git):
        def __init__(self, repo_dir: Path) -> None:
            self.repo_dir = repo_dir
            self._cache = None

        def default_branch(self) -> str:
            return "main"

        def current_branch(self) -> str:
            return "main"

        def local_branch_names(self) -> frozenset[str]:  # type: ignore[override]
            return frozenset({empty_branch})

        def branch_exists(self, branch: str) -> bool:
            return branch == empty_branch

        def branches_exist(self, branches: object) -> dict[str, bool]:
            if not isinstance(branches, (tuple, list, set, frozenset)):
                return {}
            return {str(branch): str(branch) == empty_branch for branch in branches}

        def ref_exists(self, ref: str) -> bool:
            return ref in {empty_branch, "main"}

        def resolve_fresh_merge_source(self, branch: str, **_kwargs: object):
            from gza.git import ResolvedMergeSourceRef

            return ResolvedMergeSourceRef(branch)

        def resolve_refs(self, refs: object, *, peel: str = "commit") -> dict[str, str | None]:
            if not isinstance(refs, (tuple, list, set, frozenset)):
                return {}
            return {
                str(ref): ("shared-tip" if str(ref) in {empty_branch, "main"} else None)
                for ref in refs
            }

        def rev_parse_if_exists(self, ref: str) -> str | None:  # type: ignore[override]
            if ref in {empty_branch, "main"}:
                return "shared-tip"
            return None

        def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:  # type: ignore[override]
            return branch == empty_branch and into == "main"

        # Close the merge-state seam fully so the unit test never falls back to
        # inherited Git subprocess helpers when classification probes ancestry.
        def merge_base(self, ref1: str, ref2: str) -> str:  # type: ignore[override]
            if {ref1, ref2} == {empty_branch, "main"}:
                return "shared-tip"
            return "synthetic-merge-base"

        def count_commits_ahead(self, source_ref: str, target_ref: str) -> int:  # type: ignore[override]
            checked = self.count_commits_ahead_checked(source_ref, target_ref)
            if checked is None:
                raise RuntimeError("ahead-count unavailable")
            return checked

        def count_commits_ahead_checked(self, source_ref: str, target_ref: str) -> int | None:  # type: ignore[override]
            if source_ref == empty_branch and target_ref == "main":
                return 0
            if source_ref == empty_branch and target_ref == "shared-tip":
                return 0
            return 1

        def get_diff_name_status(
            self,
            revision_range: str,
            paths: tuple[str, ...] | list[str] = (),
            *,
            check: bool = False,
        ) -> str:  # type: ignore[override]
            if revision_range in {f"main...{empty_branch}", f"shared-tip...{empty_branch}"}:
                return ""
            return "M\tfeature.txt\n"

        def is_on_first_parent_history(self, commit: str, target: str) -> bool:
            return commit == "shared-tip" and target == "main"

    return _EmptyMergedWatchGit(tmp_path)


def test_watch_run_does_not_call_load_merge_context_when_git_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_run_cycle must not invoke _load_merge_context when it holds a live git/target_branch.

    _run_cycle constructs git = Git(config.project_dir) and passes it to
    _query_owner_rows_with_context, which seeds read_context.merge_context via
    build_merge_context_from_git before list_failed_tasks_for_recovery runs.
    When seeding works, _load_merge_context is never called — the pre-seeded
    context from the run's own git is used instead of the ambient discover=True path.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    def _must_not_be_called(_project_dir: object = None) -> object:
        raise AssertionError(
            "_load_merge_context was called despite holding a live git; "
            "the ambient discover=True load was not eliminated by the pre-seeded merge context"
        )

    monkeypatch.setattr(recovery_engine, "_load_merge_context", _must_not_be_called)

    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    preseeded_git = _make_preseeded_watch_git(tmp_path)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=preseeded_git),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=1,
            dry_run=True,
            log=log,
        )

    # The run completed without triggering the ambient _load_merge_context path.
    assert result is not None


def test_watch_cycle_uses_git_cache_for_analysis_only_before_lifecycle_execution(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    review_owner = store.add("Needs review before merge", task_type="implement")
    assert review_owner.id is not None
    review_owner.status = "completed"
    review_owner.completed_at = datetime.now(UTC)
    review_owner.branch = "feature/watch-analysis-cache"
    review_owner.has_commits = True
    review_owner.merge_status = "unmerged"
    store.update(review_owner)

    row = LineageOwnerRow(
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
    )
    git = _make_watch_git()
    cache_state = {"active": False}
    phases: list[str] = []
    pending_reads = {"count": 0}
    pending_reads = {"count": 0}

    @contextlib.contextmanager
    def _tracked_cached():
        assert cache_state["active"] is False
        cache_state["active"] = True
        git._cache = {}
        phases.append("cache-enter")
        try:
            yield git
        finally:
            phases.append("cache-exit")
            cache_state["active"] = False
            git._cache = None

    git.cached = _tracked_cached  # type: ignore[method-assign]

    def _assert_cached_read(*_args, **_kwargs):
        assert cache_state["active"] is True
        return []

    def _fake_determine(_config, _store, _git, task, _target_branch, **_kwargs):
        assert cache_state["active"] is True
        assert task.id == review_owner.id
        phases.append("determine-next-action")
        return {"type": "create_review", "description": "Create review before merge"}

    def _fake_execute_advance_action(*, task, action, context):
        assert cache_state["active"] is False
        assert task.id == review_owner.id
        assert action["type"] == "create_review"
        phases.append("execute-advance-action")
        return AdvanceActionExecutionResult(
            action_type="create_review",
            status="dry_run",
            message="Would create_review",
            worker_label="worker",
            worker_consuming=True,
        )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", side_effect=_assert_cached_read),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", side_effect=_assert_cached_read),
        patch("gza.cli.watch._pending_runnable_tasks", return_value=[]),
        patch("gza.cli.watch.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.watch.execute_advance_action", side_effect=_fake_execute_advance_action),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=True,
            log=_WatchLog(tmp_path / ".gza" / "watch.log", quiet=True),
        )

    assert phases == [
        "cache-enter",
        "determine-next-action",
        "cache-exit",
        "execute-advance-action",
    ]


def test_watch_cycle_cleans_stale_recovery_no_progress_park_only_after_cached_analysis_exits(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    failed = store.add("Failed implement for stale park cleanup", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    recovery_child = store.add(
        failed.prompt,
        task_type="implement",
        based_on=failed.id,
        recovery_origin="retry",
    )
    assert recovery_child.id is not None
    recovery_child.status = "pending"
    store.update(recovery_child)

    decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=config.max_resume_attempts)
    recovery_action = failed_recovery_decision_to_action(failed, decision)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=failed,
        action=recovery_action,
        action_task=recovery_child,
        failed_task=failed,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            streak=2,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    git = _make_watch_git()
    cache_state = {"active": False}
    phases: list[str] = []
    real_delete_watch_progress_subject = store.delete_watch_progress_subject

    @contextlib.contextmanager
    def _tracked_cached():
        assert cache_state["active"] is False
        cache_state["active"] = True
        git._cache = {}
        phases.append("cache-enter")
        try:
            yield git
        finally:
            phases.append("cache-exit")
            cache_state["active"] = False
            git._cache = None

    def _tracked_delete_watch_progress_subject(*args, **kwargs):
        phases.append("delete-stale-observation")
        assert cache_state["active"] is False
        return real_delete_watch_progress_subject(*args, **kwargs)

    git.cached = _tracked_cached  # type: ignore[method-assign]
    store.delete_watch_progress_subject = _tracked_delete_watch_progress_subject  # type: ignore[method-assign]

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.reconcile_stale_watch_no_progress_parks"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._spawn_background_worker", side_effect=AssertionError("plain worker should not run")),
        patch("gza.cli.watch._spawn_background_resume_worker", side_effect=AssertionError("resume worker should not run")),
        patch("gza.cli.watch._spawn_background_iterate", return_value=1),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(tmp_path / ".gza" / "watch.log", quiet=True),
            restart_failed=True,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert phases == ["cache-enter", "cache-exit", "delete-stale-observation"]
    assert store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    ) is None


def test_watch_cycle_recomputes_pending_tasks_after_lifecycle_execution_outside_git_cache(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    config = Config.load(tmp_path)

    pending = store.add("Pending implement unblocked by lifecycle work", task_type="implement")
    assert pending.id is not None

    review_owner = store.add("Lifecycle work that unblocks pending", task_type="implement")
    assert review_owner.id is not None
    review_owner.status = "completed"
    review_owner.completed_at = datetime.now(UTC)
    review_owner.branch = "feature/watch-pending-refresh"
    review_owner.has_commits = True
    review_owner.merge_status = "unmerged"
    store.update(review_owner)

    row = LineageOwnerRow(
        owner_task=review_owner,
        members=(review_owner,),
        tree=None,
        lineage_status="actionable",
        next_action=None,
        next_action_reason="reconcile_branch_divergence",
        unresolved_tasks=(review_owner,),
        unresolved_leaf_summary=(),
        lifecycle_action_task=review_owner,
        recovery_action_task=None,
        recovery_leaf_task=None,
    )

    git = _make_watch_git()
    cache_state = {"active": False}
    phases: list[str] = []
    pending_reads = {"count": 0}
    lifecycle_executed = {"done": False}

    @contextlib.contextmanager
    def _tracked_cached():
        assert cache_state["active"] is False
        cache_state["active"] = True
        git._cache = {}
        phases.append("cache-enter")
        try:
            yield git
        finally:
            phases.append("cache-exit")
            cache_state["active"] = False
            git._cache = None

    git.cached = _tracked_cached  # type: ignore[method-assign]

    def _fake_pending_tasks(*_args, **_kwargs):
        pending_reads["count"] += 1
        phases.append("read-pending-prewake")
        return []

    def _fake_build_dispatch_preview(*args, **kwargs):
        assert cache_state["active"] is False
        assert lifecycle_executed["done"] is True
        phases.append("build-dispatch-preview-post-lifecycle")
        preview = build_dispatch_preview(*args, **kwargs)
        assert [entry.task.id for entry in preview.pending_entries] == [pending.id]
        return preview

    def _fake_determine(_config, _store, _git, task, _target_branch, **_kwargs):
        assert cache_state["active"] is True
        assert task.id == review_owner.id
        phases.append("determine-next-action")
        return {
            "type": "reconcile_branch_divergence",
            "description": "Reconcile branch publication before queue pickup",
        }

    def _fake_execute_advance_action(*, task, action, context):
        assert cache_state["active"] is False
        assert task.id == review_owner.id
        assert action["type"] == "reconcile_branch_divergence"
        lifecycle_executed["done"] = True
        phases.append("execute-advance-action")
        return AdvanceActionExecutionResult(
            action_type="reconcile_branch_divergence",
            status="success",
            message="Reconciled branch publication",
            success_message="Reconciled branch publication",
            worker_label="worker",
            worker_consuming=False,
        )

    def _fake_prepare(_config, task, **_kwargs):
        assert cache_state["active"] is False
        assert task.id == pending.id
        assert lifecycle_executed["done"] is True
        phases.append("prepare-pending")
        return task

    def _fake_spawn_iterate(*_args, **_kwargs):
        assert cache_state["active"] is False
        phases.append("spawn-iterate")
        return 0

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch.collect_scoped_tag_scope_gaps", return_value=[]),
        patch("gza.cli.watch._query_owner_rows_with_context", return_value=([row], RecoveryReadContext())),
        patch("gza.cli.watch.collect_recovery_lane_entries", return_value=[]),
        patch("gza.cli.watch._pending_runnable_tasks", side_effect=_fake_pending_tasks),
        patch("gza.cli.watch.build_dispatch_preview", side_effect=_fake_build_dispatch_preview),
        patch("gza.cli.watch.determine_next_action", side_effect=_fake_determine),
        patch("gza.cli.watch.execute_advance_action", side_effect=_fake_execute_advance_action),
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=_fake_prepare),
        patch("gza.cli.watch._spawn_background_iterate", side_effect=_fake_spawn_iterate),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=_WatchLog(tmp_path / ".gza" / "watch.log", quiet=True),
        )

    assert result.work_done is True
    assert pending_reads["count"] >= 1
    assert phases[:5] == [
        "read-pending-prewake",
        "cache-enter",
        "determine-next-action",
        "cache-exit",
        "execute-advance-action",
    ]
    assert phases[5:8] == [
        "build-dispatch-preview-post-lifecycle",
        "prepare-pending",
        "spawn-iterate",
    ]
