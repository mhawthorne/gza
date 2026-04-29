"""Tests for `gza watch` scheduler behavior."""

import argparse
import signal
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gza.cli.git_ops import _execute_merge_action, ensure_watch_main_checkout
from gza.cli.watch import (
    _collect_completed_transition_ids,
    _collect_live_running_state,
    _collect_unhandled_failures,
    _compute_failure_backoff_seconds,
    _count_live_workers,
    _CycleResult,
    _emit_transition_events,
    _format_wake_message,
    _run_cycle,
    _task_snapshot,
    _WatchLog,
    cmd_watch,
)
from gza.config import Config
from gza.git import Git, GitError
from gza.workers import WorkerMetadata, WorkerRegistry

from .conftest import make_store, run_gza, setup_config, setup_git_repo_with_task_branch


def _task_count(store) -> int:
    with store._connect() as conn:  # noqa: SLF001 - test helper
        row = conn.execute("SELECT COUNT(*) AS c FROM tasks").fetchone()
    assert row is not None
    return int(row["c"])


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
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_iterate.call_args.args[2].id == impl.id
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == plan.id


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
    assert spawn_worker.call_args.kwargs["task_id"] == bumped.id


def test_watch_cycle_group_filters_pending_pickup(tmp_path: Path) -> None:
    """Group-scoped watch should only start pending tasks from the selected group."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    release_task = store.add("Release plan", task_type="plan", group="release-1")
    backlog_task = store.add("Backlog plan", task_type="plan", group="backlog")
    assert release_task.id is not None
    assert backlog_task.id is not None

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
            group="release-1",
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == release_task.id


def test_watch_cycle_group_prefers_explicit_queue_order(tmp_path: Path) -> None:
    """Group-scoped watch should respect explicit queue positions before urgent/FIFO fallback."""
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
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            group="release",
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
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    assert spawned_args.resume is True
    assert spawned_args.retry is False
    assert spawned_task.id == failed.id


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
    assert spawn_resume.call_count == 1
    recovered = max(
        [task for task in store.get_all() if task.based_on in {older.id, newer.id}],
        key=lambda task: int(str(task.id).split("-")[-1]),
    )
    assert recovered.based_on == older.id
    assert spawn_resume.call_args.args[2] == recovered.id


@pytest.mark.parametrize("task_type", ["implement", "review", "improve", "rebase"])
def test_watch_cycle_default_mode_auto_resumes_resumable_failed_task(
    tmp_path: Path, task_type: str
) -> None:
    """Plain watch should preserve legacy resume-worker behavior for resumable failures."""
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
    assert spawn_iterate.call_count == 0
    assert spawn_resume.call_count == 1
    spawned_task = store.get(spawn_resume.call_args.args[2])
    assert spawned_task is not None
    assert spawned_task.based_on == failed.id
    assert spawned_task.id != pending_impl.id
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert f'START  {spawned_task.id} {task_type} "{spawned_task.prompt}"' in log_text
    assert f"RECOVR {failed.id}" not in log_text


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
    assert result.work_done is True
    assert spawn_resume.call_count == 1
    assert spawn_resume.call_args.args[2] == resume_child.id
    assert len(children) == 1
    assert children[0].id == resume_child.id


def test_watch_cycle_default_mode_starts_queued_retry_child_as_pending_work(tmp_path: Path) -> None:
    """Plain watch should leave a queued retry child on the normal pending queue."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-123"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    retry_child = store.add("Pending retry child", task_type="implement", based_on=failed.id)
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

    assert result.work_done is False
    assert spawn_resume.call_count == 0
    assert [task.id for task in store.get_based_on_children(failed.id)] == [unrelated_child.id]


def test_watch_cycle_default_mode_spawn_failure_reuses_pending_resume_child_next_cycle(tmp_path: Path) -> None:
    """Plain watch should retry the same pending resume child after spawn failure, not create duplicates."""
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
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=1) as spawn_resume,
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
    assert spawn_resume.call_count == 2
    assert spawn_resume.call_args_list[0].args[2] == spawn_resume.call_args_list[1].args[2]
    assert len(children) == 1
    assert len(pending_children) == 1


def test_watch_cycle_default_mode_attempt_cap_skips_failed_resume_and_starts_pending(tmp_path: Path) -> None:
    """Plain watch should leave capped failed chains alone and move on to pending work."""
    (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: 1\n")
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
    assert spawned_task.based_on == failed.id
    log_text = log_path.read_text()
    assert f"RECOVR {failed.id} retry via iterate -> {spawned_task.id}" in log_text
    assert f"RECOVR {failed.id} resume via iterate -> {spawned_task.id}" not in log_text


def test_watch_cycle_restart_failed_reuses_existing_deep_recovery_chain_without_creating_sibling(
    tmp_path: Path,
) -> None:
    """Restart-failed should continue the newest recovery branch instead of forking from an older failed ancestor."""
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
    assert spawned_args.resume is True
    assert spawned_task.id == failed_retry.id
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
    assert f"RECOVR {failed.id} resume via iterate -> (new task)" in log_text
    assert len(store.get_based_on_children(failed.id)) == 0


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
    """Implement recovery should retry the failed root iterate launch without creating local resume children."""
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
    assert first_task.id == failed.id
    assert second_args.resume is True
    assert second_args.retry is False
    assert children == []


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
    assert spawned_task.id == failed.id
    assert spawned_args.resume is True
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
    assert spawned_task.id == failed.id
    assert spawned_args.resume is True
    assert spawned_args.retry is False
    assert len(store.get_based_on_children(failed.id)) == 1


def test_watch_cycle_restart_failed_drains_failed_queue_before_pending_queue(tmp_path: Path) -> None:
    """Pending work must not start while actionable failed tasks remain beyond restart_failed_batch."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    for idx in range(2):
        failed = store.add(f"Failed implement {idx}", task_type="implement")
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
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_worker.call_count == 0
    assert store.get(pending_plan.id).status == "pending"


def test_watch_cycle_pending_queue_starts_only_after_recovery_exhaustion(tmp_path: Path) -> None:
    """Pending work should begin on a later cycle only after recovery work is fully exhausted."""
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
        result_first = _run_cycle(
            config=config,
            store=store,
            batch=4,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )
        recovery_child = store.get_based_on_children(failed.id)[0]
        recovery_child.status = "completed"
        store.update(recovery_child)
        result_second = _run_cycle(
            config=config,
            store=store,
            batch=4,
            max_iterations=10,
            dry_run=False,
            log=log,
            restart_failed=True,
            restart_failed_batch=1,
            max_recovery_attempts=config.max_resume_attempts,
        )

    assert result_first.work_done is True
    assert result_second.work_done is True
    assert spawn_iterate.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == pending_plan.id


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
        patch("gza.cli.watch._collect_live_running_state", return_value=(set(), set())),
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
    text = log_path.read_text()
    assert "recovery-skip" not in text
    assert f"{failed.id} failed {failed.task_type}" not in text


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
            show_skipped=True,
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    text = log_path.read_text()
    assert "SKIP" in text
    assert f"{failed.id} failed {failed.task_type}: task_type_out_of_scope" in text


def test_watch_cycle_restart_failed_out_of_scope_failure_child_does_not_block_pending_queue(tmp_path: Path) -> None:
    """Out-of-scope failed chains should not keep restart-failed sessions stuck in recovery phase."""
    setup_config(tmp_path)
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
        patch("gza.cli.watch._collect_live_running_state", return_value=(set(), set())),
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
    """Task-creating advance children should not be retried via generic pickup in same cycle."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    root_type = "plan" if action_type == "create_implement" else "implement"
    root = store.add("Root task", task_type=root_type)
    assert root.id is not None
    root.status = "completed"
    root.completed_at = datetime.now(UTC)
    if action_type != "create_implement":
        root.branch = "feature/same-cycle-no-retry"
    store.update(root)
    if action_type != "create_implement":
        store.set_merge_status(root.id, "unmerged")

    review_task = None
    if action_type == "create_review":
        review_task = store.add("Pending review", task_type="review", depends_on=root.id)
        assert review_task.id is not None

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
    if action_type == "create_implement":
        assert spawn_iterate.call_count == 1
        assert spawn_worker.call_count == 0
        created_children = [
            task for task in store.get_all() if task.task_type == "implement" and task.depends_on == root.id
        ]
        assert len(created_children) == 1
        child_id = str(created_children[0].id)
    else:
        assert spawn_worker.call_count == 1
        if action_type == "create_review":
            assert create_review.call_count == 1
            assert review_task is not None
            child_id = str(review_task.id)
        elif action_type == "needs_rebase":
            assert create_rebase.call_count == 1
            assert rebase_task is not None
            child_id = str(rebase_task.id)
        else:
            improved_children = [
                task for task in store.get_based_on_children(root.id) if task.task_type == "improve"
            ]
            assert len(improved_children) == 1
            child_id = str(improved_children[0].id)

    log_lines = log_path.read_text().splitlines()
    assert any("START_FAILED" in line and child_id in line for line in log_lines)
    assert not any(f"START  {child_id} {child_type}" in line for line in log_lines)


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
        patch("gza.cli.watch.WorkerRegistry", return_value=registry),
        patch("gza.cli.watch._pid_alive", return_value=True),
    ):
        assert _count_live_workers(config, store) == 1


def test_count_live_workers_ignores_shutting_down_worker_for_terminal_task(tmp_path: Path) -> None:
    """Terminal tasks should not keep a slot occupied just because the worker PID is still alive."""
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
        patch("gza.cli.watch.WorkerRegistry", return_value=registry),
        patch("gza.cli.watch._pid_alive", return_value=True),
    ):
        assert _count_live_workers(config, store) == 0


def test_collect_live_running_state_tracks_worker_and_pid_only_tasks(tmp_path: Path) -> None:
    """WAKE task summaries should include both worker-backed and pid-only in-progress tasks."""
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
        patch("gza.cli.watch.WorkerRegistry", return_value=registry),
        patch("gza.cli.watch._pid_alive", side_effect=lambda pid: pid == 5252),
    ):
        live_pids, running_task_ids = _collect_live_running_state(config, store)

    assert live_pids == {4242, 5252}
    assert running_task_ids == [worker_task.id, pid_only_task.id]


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

    with patch("gza.cli.watch.WorkerRegistry", return_value=registry):
        live_pids, running_task_ids = _collect_live_running_state(config, store)

    assert live_pids == {4242}
    assert running_task_ids == [pending_task.id]


def test_format_wake_message_includes_running_task_ids() -> None:
    """WAKE line should append task IDs when tasks are actively running."""
    assert _format_wake_message(running=1, pending=3, slots=0, running_task_ids=["gza-42"]) == (
        "checking... (1 running, 3 pending, 0 slots) tasks: gza-42"
    )
    assert _format_wake_message(running=0, pending=2, slots=2, running_task_ids=[]) == (
        "checking... (0 running, 2 pending, 2 slots)"
    )


def test_watch_cycle_logs_group_scoped_pending_count_in_wake_line(tmp_path: Path) -> None:
    """WAKE line should report runnable pending tasks using the selected group filter."""
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
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            group="release-1",
        )

    assert "WAKE   checking... (0 running, 2 pending, 1 slots)" in log_path.read_text()


def test_watch_cycle_logs_tag_scope_with_all_mode(tmp_path: Path) -> None:
    """Tag-scoped watch should log normalized filter scope with all-tag semantics."""
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
        )

    assert "INFO   scope: tags=backend,release-1.2 mode=all" in log_path.read_text()


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

    assert "INFO   scope: tags=backend,release-1.2 mode=any" in log_path.read_text()


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
        patch("gza.cli.watch.WorkerRegistry", return_value=registry),
        patch("gza.cli.watch._pid_alive", return_value=True),
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

    task = store.add("Completed task", task_type="implement")
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

    task = store.add("Completed task", task_type="implement")
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

    task = store.add("Completed task", task_type="implement")
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
            return_value=SimpleNamespace(rc=0, created_followups=[], reused_followups=[]),
        ) as execute_merge,
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
    ensure_isolated.assert_called_once_with(config, repo_git, "main")
    require_default_branch.assert_not_called()
    assert execute_merge.call_count == 1
    assert execute_merge.call_args.kwargs["merge_git"] is isolated_git
    assert execute_merge.call_args.kwargs["merge_current_branch"] == "main"


def test_watch_cycle_with_isolation_enabled_rebuilds_checkout_after_preflight_failure_and_merges(
    tmp_path: Path,
) -> None:
    """A stale isolated checkout at cycle start should rebuild once and still allow same-cycle merges."""
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
            return_value=SimpleNamespace(rc=0, created_followups=[], reused_followups=[]),
        ) as execute_merge,
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
    assert ensure_isolated.call_count == 2
    assert ensure_isolated.call_args_list[0].args == (config, repo_git, "main")
    assert ensure_isolated.call_args_list[1].args == (config, repo_git, "main")
    assert ensure_isolated.call_args_list[1].kwargs["rebuild"] is True
    require_default_branch.assert_not_called()
    assert execute_merge.call_count == 1
    assert execute_merge.call_args.kwargs["merge_git"] is rebuilt_git
    assert execute_merge.call_args.kwargs["merge_current_branch"] == "main"
    log_text = log_path.read_text()
    assert "isolated merge checkout refresh failed; rebuilding: stale checkout" in log_text
    assert "isolated merge checkout rebuilt" in log_text
    assert f"MERGE  {task.id} -> main" in log_text


def test_watch_cycle_with_isolation_enabled_dry_run_does_not_mutate_checkout(tmp_path: Path) -> None:
    """Isolation dry-run should preview merges without reconciling isolated checkout."""
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
    assert f"MERGE  {task.id} -> main [dry-run]" in log_path.read_text()


def test_ensure_watch_main_checkout_detaches_existing_shared_default_branch_worktree(tmp_path: Path) -> None:
    """Isolation helper should not leave the integration worktree attached to the shared default-branch ref."""
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "file.txt").write_text("initial\n")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Initial commit")

    checkout_path = config.main_checkout_integration_path
    checkout_path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", "--force", str(checkout_path), "main")

    isolated_git = ensure_watch_main_checkout(config, git, "main")

    assert isolated_git.current_branch() == "HEAD"
    entry = next(
        item
        for item in git.worktree_list()
        if Path(str(item["path"])).resolve() == checkout_path.resolve()
    )
    assert entry.get("detached") is True
    assert entry.get("branch") != "refs/heads/main"
    assert git.has_changes(include_untracked=False) is False


def test_isolated_watch_merge_advances_primary_main_checkout_cleanly(tmp_path: Path) -> None:
    """A successful isolated watch merge must land on main and keep the attached checkout clean."""
    store, git, task, _wt = setup_git_repo_with_task_branch(
        tmp_path,
        "Successful isolated merge",
        "feature/watch-isolated-success",
    )
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)

    assert task.id is not None
    assert git.current_branch() == "main"

    isolated_git = ensure_watch_main_checkout(config, git, "main")
    merge_result = _execute_merge_action(
        config,
        store,
        git,
        task,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        merge_git=isolated_git,
        merge_current_branch="main",
    )

    assert merge_result.rc == 0
    assert isolated_git.current_branch() == "HEAD"
    assert isolated_git.has_changes(include_untracked=True) is False
    assert (config.main_checkout_integration_path / "feature.txt").exists()
    assert git.current_branch() == "main"
    assert git.has_changes(include_untracked=False) is False
    assert (tmp_path / "feature.txt").exists()
    assert git.is_merged(task.branch, "main") is True
    assert git.rev_parse("main") == isolated_git.rev_parse("HEAD")
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "merged"


def test_isolated_watch_merge_promotes_real_main_before_marking_sequential_merges(tmp_path: Path) -> None:
    """Sequential isolated merges must advance the real main ref before merge_status flips."""
    store, git, task1, _wt = setup_git_repo_with_task_branch(
        tmp_path,
        "First isolated merge",
        "feature/watch-isolated-first",
    )
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)

    assert task1.id is not None
    store.set_merge_status(task1.id, "unmerged")

    task2 = store.add("Second isolated merge", task_type="implement")
    assert task2.id is not None
    task2.status = "completed"
    task2.completed_at = datetime.now(UTC)
    task2.branch = "feature/watch-isolated-second"
    store.update(task2)
    store.set_merge_status(task2.id, "unmerged")

    git._run("checkout", "-b", task2.branch)
    (tmp_path / "second.txt").write_text("second content")
    git._run("add", "second.txt")
    git._run("commit", "-m", "Add second isolated feature")
    git._run("checkout", "main")

    isolated_git = ensure_watch_main_checkout(config, git, "main")

    first_result = _execute_merge_action(
        config,
        store,
        git,
        task1,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        merge_git=isolated_git,
        merge_current_branch="main",
    )

    assert first_result.rc == 0
    assert git.is_merged(task1.branch, "main") is True
    assert store.get(task1.id).merge_status == "merged"
    first_main_oid = git.rev_parse("main")

    second_result = _execute_merge_action(
        config,
        store,
        git,
        task2,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        merge_git=isolated_git,
        merge_current_branch="main",
    )

    assert second_result.rc == 0
    assert git.rev_parse("main") != first_main_oid
    assert git.is_merged(task2.branch, "main") is True
    assert store.get(task2.id).merge_status == "merged"
    assert (tmp_path / "feature.txt").exists()
    assert (tmp_path / "second.txt").exists()
    assert git.has_changes(include_untracked=False) is False
    assert isolated_git.current_branch() == "HEAD"
    assert isolated_git.has_changes(include_untracked=True) is False
    assert isolated_git.rev_parse("HEAD") == git.rev_parse("main")


def test_watch_cycle_with_isolation_enabled_rebuilds_after_cleanup_failure_and_continues_merging(tmp_path: Path) -> None:
    """Cleanup failures in isolated mode should rebuild checkout and continue later merges in-cycle."""
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

    def choose_action(_cfg, _store, _git, task, _target, *, impl_based_on_ids):  # noqa: ARG001
        if task.id == task_a.id:
            return {"type": "merge"}
        if task.id == task_b.id:
            return {"type": "merge"}
        return {"type": "skip"}

    merge_results = [
        SimpleNamespace(rc=1, created_followups=[], reused_followups=[]),
        SimpleNamespace(rc=0, created_followups=[], reused_followups=[]),
    ]

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=repo_git),
        patch("gza.cli.watch.ensure_watch_main_checkout", side_effect=[isolated_git, rebuilt_git]) as ensure_isolated,
        patch("gza.cli.determine_next_action", side_effect=choose_action),
        patch("gza.cli.watch._execute_merge_action", side_effect=merge_results) as execute_merge,
        patch("gza.cli.watch.cleanup_failed_merge_checkout", side_effect=GitError("cleanup failed")),
        patch("gza.cli.watch._create_rebase_task", return_value=rebase_task),
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
    assert ensure_isolated.call_count == 2
    assert ensure_isolated.call_args_list[1].kwargs["rebuild"] is True
    assert execute_merge.call_count == 2
    assert execute_merge.call_args_list[0].kwargs["merge_git"] is isolated_git
    assert execute_merge.call_args_list[1].kwargs["merge_git"] is rebuilt_git
    assert spawn_worker.call_count == 1
    log_text = log_path.read_text()
    assert "isolated merge checkout rebuilt" in log_text
    assert "merge conflict routed to rebase" in log_text
    assert " MERGE " in log_text


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

    def choose_action(_cfg, _store, _git, task, _target, *, impl_based_on_ids):  # noqa: ARG001
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
    """Already-merged branches must not create isolated rebase work on merge failure."""
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
    isolated_git.can_merge.assert_not_called()
    log_text = log_path.read_text()
    assert "merge conflict routed to rebase" not in log_text
    assert f"{task.id}: merge failed (branch already merged); not routing to rebase" in log_text


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

        captured["merge_args"] = _build_auto_merge_args(config_arg, git_arg, task_arg, target_branch)
        return SimpleNamespace(rc=0, created_followups=[], reused_followups=[])

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._execute_merge_action", side_effect=fake_execute_merge_action),
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )
        watch_merge_args = captured["merge_args"]
        from gza.cli.git_ops import _build_auto_merge_args
        advance_merge_args = _build_auto_merge_args(config, git, task, "main")

    assert watch_merge_args.squash is True
    assert advance_merge_args.squash is True


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
            side_effect=lambda *_args, **_kwargs: SimpleNamespace(
                rc=noisy_merge(),
                created_followups=[],
                reused_followups=[],
            ),
        ),
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
    assert "Merging 'feature/watch-quiet-merge' into 'main'..." not in stdout
    assert " MERGE " in log_path.read_text()
    assert f"MERGE  {task.id} -> main" in log_path.read_text()


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
                created_followups=[created_followup],
                reused_followups=[],
            ),
        ) as execute_merge,
    ):
        result = _run_cycle(
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
    assert f"MERGE  {task.id} -> main" in log_path.read_text()
    assert "FOLLOW gza-999 created from" in log_path.read_text()


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
    assert f"MERGE  {task.id} -> main [dry-run]" in log_path.read_text()


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

    assert (
        f"REVIEW {review.id} for {impl.id}: APPROVED_WITH_FOLLOWUPS [follow-ups: F1]"
        in log_path.read_text()
    )


def test_cmd_watch_logs_completed_review_before_same_cycle_merge(tmp_path: Path) -> None:
    """Pre-cycle transitions should land before merge logs from the same watch pass."""
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
        {review_id: {"status": "in_progress", "task_type": "review", "started_at": None, "completed_at": None, "failure_reason": None, "depends_on": impl_id}},
        {review_id: {"status": "completed", "task_type": "review", "started_at": None, "completed_at": datetime.now(UTC).isoformat(), "failure_reason": None, "depends_on": impl_id}},
        {review_id: {"status": "completed", "task_type": "review", "started_at": None, "completed_at": datetime.now(UTC).isoformat(), "failure_reason": None, "depends_on": impl_id}},
        {review_id: {"status": "completed", "task_type": "review", "started_at": None, "completed_at": datetime.now(UTC).isoformat(), "failure_reason": None, "depends_on": impl_id}},
        {review_id: {"status": "completed", "task_type": "review", "started_at": None, "completed_at": datetime.now(UTC).isoformat(), "failure_reason": None, "depends_on": impl_id}},
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

    def fake_run_cycle(**kwargs):
        log = kwargs["log"]
        if not hasattr(fake_run_cycle, "seen"):
            fake_run_cycle.seen = True  # type: ignore[attr-defined]
            log.emit("MERGE", f"{impl_id} -> main")
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
    assert "SKIP   merge actions skipped: not on default branch" in log_path.read_text()


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
    """Completed unmerged implement with no review should queue and run review."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/create-review"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = MagicMock()
    review.id = "test-review-id"

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
        patch(
            "gza.cli.watch._prepare_create_review_action",
            return_value=SimpleNamespace(
                status="created",
                review_task=review,
                message=f"✓ Created review task {review.id}",
            ),
        ) as create_review,
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
    assert create_review.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == review.id


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
    assert spawn_worker.call_count == 0
    assert spawn_iterate.call_count == 1
    iterate_args = spawn_iterate.call_args.args[0]
    created_impl = spawn_iterate.call_args.args[2]
    assert iterate_args.max_iterations == 7
    assert created_impl.task_type == "implement"
    assert created_impl.depends_on == plan.id


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
    """Completed task with pending improve child should run improve worker."""
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
        patch("gza.cli.git_ops.get_review_verdict", return_value="CHANGES_REQUESTED"),
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
    assert spawn_worker.call_args.kwargs["task_id"] == improve.id


def test_watch_cycle_max_resume_attempts_zero_skips_failed_improve_recovery(tmp_path: Path) -> None:
    """Watch should honor the per-run attempt cap for advance-driven improve recovery."""
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
        patch("gza.cli.watch._spawn_background_worker", return_value=0) as spawn_worker,
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume_worker,
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

    assert result.work_done is False
    assert spawn_worker.call_count == 0
    assert spawn_resume_worker.call_count == 0
    log_text = log_path.read_text()
    assert "max improve attempts (0) reached" in log_text
    assert str(failed_improve.id) in log_text


def test_watch_cycle_improve_creation_includes_unresolved_comments_in_prompt(tmp_path: Path) -> None:
    """Watch-created improve prompts should include unresolved comments when present."""
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
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_worker", return_value=0),
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
    improves = store.get_improve_tasks_for(impl.id, review.id)
    assert len(improves) == 1
    assert "unresolved comments" in improves[0].prompt


def test_watch_cycle_improve_action_resumes_failed_improve_chain(tmp_path: Path) -> None:
    """Improve advance action should resume a resumable failed improve instead of creating a new sibling."""
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
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume_worker,
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

    improves = [t for t in store.get_all() if t.task_type == "improve"]
    resumed = [t for t in improves if t.id != failed_improve.id]
    assert len(resumed) == 1
    assert resumed[0].based_on == failed_improve.id
    assert result.work_done is True
    assert spawn_resume_worker.call_count == 1
    assert spawn_worker.call_count == 0

    direct_siblings = [t for t in improves if t.based_on == impl.id]
    assert len(direct_siblings) == 1


def test_watch_cycle_improve_action_retries_non_resumable_failed_improve_chain(tmp_path: Path) -> None:
    """Improve advance action should create retry improve based on failed improve when not resumable."""
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
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    git = MagicMock()
    git.current_branch.return_value = "main"
    git.default_branch.return_value = "main"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "improve", "review_task": review}),
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume_worker,
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

    improves = [t for t in store.get_all() if t.task_type == "improve"]
    retried = [t for t in improves if t.id != failed_improve.id]
    assert len(retried) == 1
    assert retried[0].based_on == failed_improve.id
    assert retried[0].depends_on == review.id
    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_resume_worker.call_count == 0

    direct_siblings = [t for t in improves if t.based_on == impl.id]
    assert len(direct_siblings) == 1


def test_watch_cycle_improve_action_respects_max_improve_attempts(tmp_path: Path) -> None:
    """When improve attempts hit cap, watch logs skip and does not create another improve."""
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
            f"Improve attempt {attempt}",
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
        patch("gza.cli.watch._spawn_background_resume_worker", return_value=0) as spawn_resume_worker,
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

    assert result.work_done is False
    assert _task_count(store) == before_count
    assert spawn_worker.call_count == 0
    assert spawn_resume_worker.call_count == 0
    text = log_path.read_text()
    assert "max improve attempts" in text
    assert f"Run uv run gza fix {impl.id}" in text


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

    rebase_task = MagicMock()
    rebase_task.id = "test-rebase-id"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
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
    assert any(f"START  {rebase_task.id} rebase" in line for line in lines)
    assert not any(" REBASE " in line for line in lines)


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

    rebase_task = MagicMock()
    rebase_task.id = "test-rebase-off-default"

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.determine_next_action", return_value={"type": "needs_rebase"}),
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
    assert any("[dry-run]" in line and expected_fragment in line for line in log_lines)
    if action_type == "create_implement":
        assert any(f"(new) implement for {root.id} [dry-run]" in line for line in log_lines)


def test_watch_dry_run_command_does_not_reconcile_or_prune_dead_in_progress_task(tmp_path: Path) -> None:
    """watch --dry-run must not mutate dead in-progress tasks or worker registry rows."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    proc = subprocess.Popen(["true"])
    proc.wait()
    dead_pid = proc.pid

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

    result = run_gza(
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

    proc = subprocess.Popen(["true"])
    proc.wait()
    dead_pid = proc.pid

    task = store.add("Dead worker in progress", task_type="implement")
    assert task.id is not None
    task.status = "in_progress"
    task.started_at = datetime.now(UTC)
    task.running_pid = dead_pid
    store.update(task)

    registry = WorkerRegistry(tmp_path / ".gza" / "workers")
    registry.register(
        WorkerMetadata(
            worker_id="w-watch-dry-run-cycle",
            task_id=task.id,
            pid=dead_pid,
            status="running",
            started_at=datetime.now(UTC).isoformat(),
        )
    )

    before_row = store.get(task.id)
    assert before_row is not None
    before_worker = registry.get("w-watch-dry-run-cycle")
    assert before_worker is not None

    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
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
    after_worker = registry.get("w-watch-dry-run-cycle")
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
        ("needs_discussion", "SKIP: review verdict is NEEDS_DISCUSSION, needs manual attention"),
        ("max_cycles_reached", "SKIP: max review cycles (2) reached, needs manual intervention"),
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


def test_watch_cycle_off_default_branch_still_runs_non_merge_advance_actions(tmp_path: Path, capsys) -> None:
    """Off default branch should only block merge actions, not worker-spawning actions."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/off-default-review"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = MagicMock()
    review.id = "test-review-id"

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
        patch(
            "gza.cli.watch._prepare_create_review_action",
            return_value=SimpleNamespace(
                status="created",
                review_task=review,
                message=f"✓ Created review task {review.id}",
            ),
        ) as create_review,
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

    output = capsys.readouterr().out
    assert result.work_done is True
    assert create_review.call_count == 1
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == review.id
    assert "must be run from the default branch" not in output
    assert "merge actions skipped: not on default branch" not in log_path.read_text()


def test_watch_review_spawn_logs_start_and_review_transition_logs_verdict(tmp_path: Path) -> None:
    """Review workers should log START; REVIEW is only for completed verdict transitions."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/review-events"
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
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
    assert any(f"START  {review.id} review" in line for line in log_lines)
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
    assert f"REVIEW {review.id} for {impl.id}: APPROVED" in review_lines[0]


def test_watch_cycle_dedupes_merge_not_default_skip_across_cycles(tmp_path: Path) -> None:
    """Persistent 'not on default branch' skip should not spam every cycle."""
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
    """Persistent attempt-cap skip should only log once while condition is unchanged."""
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

    assert log_path.read_text().count(f"{failed.id} failed implement: attempt_cap_reached") == 1


def test_watch_log_inserts_blank_line_between_cycles(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Each watch cycle should be visually separated in stdout and watch.log."""
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
        "18:08:47 WAKE   checking... (0 running, 2 pending, 1 slots)\n"
        "\n"
        "18:13:47 WAKE   checking... (1 running, 0 pending, 0 slots)\n"
    )
    assert capsys.readouterr().out == (
        "18:08:47 WAKE   checking... (0 running, 2 pending, 1 slots)\n"
        "\n"
        "18:13:47 WAKE   checking... (1 running, 0 pending, 0 slots)\n"
    )


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
    """Recovery dry-run should show only actionable entries by default, oldest created first."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    older = store.add("Older failed plan", task_type="plan")
    assert older.id is not None
    older.status = "failed"
    older.failure_reason = "INFRASTRUCTURE_ERROR"
    older.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(older)

    skipped = store.add("Skipped failed review", task_type="review")
    assert skipped.id is not None
    skipped.status = "failed"
    skipped.failure_reason = "MAX_TURNS"
    skipped.session_id = "sess-skip"
    skipped.completed_at = datetime(2026, 4, 28, 12, 0, 0, tzinfo=UTC)
    store.update(skipped)

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
    assert skipped.id not in stdout
    assert stdout.index(older.id) < stdout.index(newer.id)
    assert "1 skipped hidden" in stdout


def test_cmd_watch_restart_failed_dry_run_show_skipped_includes_skipped_entries(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--show-skipped should include skipped recovery decisions in the dry-run report."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    actionable = store.add("Failed plan", task_type="plan")
    assert actionable.id is not None
    actionable.status = "failed"
    actionable.failure_reason = "INFRASTRUCTURE_ERROR"
    actionable.completed_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)
    store.update(actionable)

    skipped = store.add("Failed review", task_type="review")
    assert skipped.id is not None
    skipped.status = "failed"
    skipped.failure_reason = "MAX_TURNS"
    skipped.session_id = "sess-skip"
    skipped.completed_at = datetime(2026, 4, 28, 11, 0, 0, tzinfo=UTC)
    store.update(skipped)

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
    assert "1 skipped" in stdout


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
    """Restart-failed should keep resumable-but-skipped failures visible to backoff accounting."""
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
    assert [(failure.task_id, failure.reason) for failure in failures] == [(str(failed.id), "MAX_TURNS")]


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


def test_cmd_watch_restart_failed_logs_backoff_for_skipped_resumable_review_failure(tmp_path: Path) -> None:
    """Restart-failed should still back off when a resumable review failure is out of recovery scope."""
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
    assert sleeps == [60]
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert "BACKOFF" in log_text
    assert f"{task.id}=MAX_TURNS" in log_text
    assert "streak 1" in log_text


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

    def fake_spawn_iterate(_args, _config, impl_task, *, quiet=False):
        if not quiet:
            print("Started iterate worker noisy output")
        impl_task.status = "in_progress"
        store.update(impl_task)
        return 0

    def fake_spawn_worker(_args, _config, task_id=None, quiet=False):
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
    assert f"START  {impl.id} implement" in log_text
    assert f"START  {plan.id} plan" in log_text


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
    assert store.get_based_on_children(failed.id) == []
    log_text = log_path.read_text()
    assert "START_FAILED" in log_text
    assert f"{failed.id} -> {failed.id}: iterate worker spawn failed" in log_text


def test_cmd_watch_interrupts_sleep_promptly_on_signal(tmp_path: Path) -> None:
    """Signal-triggered shutdown should interrupt poll waiting without sleeping the full interval."""
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
            handler = handlers[signal.SIGTERM]
            assert callable(handler)
            handler(signal.SIGTERM, None)

    with (
        patch("gza.cli.watch._run_cycle", return_value=_CycleResult(True, 0, 0)) as run_cycle,
        patch("gza.cli.watch.signal.signal", side_effect=fake_signal),
        patch("gza.cli.watch.time.sleep", side_effect=fake_sleep),
    ):
        rc = cmd_watch(args)

    assert rc == 0
    assert run_cycle.call_count == 1
    assert sleep_calls
    assert max(sleep_calls) < args.poll


def test_watch_cycle_logs_create_review_validation_skip(tmp_path: Path) -> None:
    """watch should log create_review validation failures instead of silently continuing."""
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
        patch("gza.cli.git_ops._create_review_task", side_effect=ValueError("review blocked by validation")),
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
    assert "SKIP" in text
    assert "review blocked by validation" in text


def test_watch_cycle_run_review_spawn_failure_not_retried_in_step3(tmp_path: Path) -> None:
    """A run_review spawn failure must not let the same task be relaunched from step 3."""
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
        # First call fails (run_review in step 1), second would be step 3
        patch("gza.cli.watch._spawn_background_worker", side_effect=[1, 0]) as spawn_worker,
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
    assert spawn_worker.call_count == 1
    assert result.work_done is False

    log_lines = log_path.read_text().splitlines()
    review_id = str(review.id)
    assert any("START_FAILED" in line and review_id in line for line in log_lines)
    assert not any(f"START  {review_id}" in line for line in log_lines)


def test_watch_cycle_run_improve_spawn_failure_not_retried_in_step3(tmp_path: Path) -> None:
    """A run_improve spawn failure must not let the same task be relaunched from step 3."""
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
        # First call fails (run_improve in step 1), second would be step 3
        patch("gza.cli.watch._spawn_background_worker", side_effect=[1, 0]) as spawn_worker,
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
    assert spawn_worker.call_count == 1
    assert result.work_done is False

    log_lines = log_path.read_text().splitlines()
    improve_id = str(improve.id)
    assert any("START_FAILED" in line and improve_id in line for line in log_lines)
    assert not any(f"START  {improve_id}" in line for line in log_lines)
