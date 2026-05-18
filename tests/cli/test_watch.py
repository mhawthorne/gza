"""Tests for `gza watch` scheduler behavior."""

import argparse
import signal
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from gza.cli.git_ops import (
    _execute_merge_action,
    _MergeSingleTaskResult,
    _PendingSquashBranchReconcile,
    _ResolvedMergeSubject,
    ensure_watch_main_checkout,
)
from gza.cli.watch import (
    _collect_advance_completed_tasks,
    _collect_completed_transition_ids,
    _collect_live_running_state,
    _collect_unhandled_failures,
    _compute_failure_backoff_seconds,
    _count_live_workers,
    _CycleResult,
    _emit_transition_events,
    _format_elapsed,
    _format_wake_message,
    _installed_gza_package_fingerprint,
    _InstalledPackageDriftState,
    _query_owner_rows,
    _resolve_watch_attention_display_task,
    _run_cycle,
    _task_snapshot,
    _watch_needs_attention_message,
    _warn_if_installed_gza_changed,
    _watch_iterate_impl_target,
    _WatchLog,
    cmd_watch,
)
from gza.config import Config
from gza.git import GitError
from gza.recovery_engine import decide_failed_task_recovery
from gza.workers import WorkerMetadata, WorkerRegistry

from .conftest import make_store, run_gza, setup_config


def _task_count(store) -> int:
    with store._connect() as conn:  # noqa: SLF001 - test helper
        row = conn.execute("SELECT COUNT(*) AS c FROM tasks").fetchone()
    assert row is not None
    return int(row["c"])


def _make_watch_git() -> MagicMock:
    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.ref_exists.return_value = False
    git.can_merge.return_value = True
    git.is_merged.return_value = False
    git.count_commits_ahead.return_value = 1
    git.get_diff_stat_parsed.return_value = (1, 1, 0)
    git.get_diff_numstat.return_value = "1\t0\tfeature.txt\n"
    return git


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
    rows = _query_owner_rows(
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

    subject_task = _resolve_watch_attention_display_task(store, row)
    assert subject_task.id == plan.id

    message = _watch_needs_attention_message(subject_task, row.next_action)
    assert plan.id in message
    assert impl.id not in message


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
    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.ref_exists.return_value = False
    git.can_merge.return_value = True
    git.is_merged.return_value = False
    git.count_commits_ahead.return_value = 1
    git.get_diff_stat_parsed.return_value = (1, 1, 0)
    git.get_diff_numstat.return_value = "1\t0\tfeature.txt\n"

    rows = _query_owner_rows(
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

    owner_rows = _query_owner_rows(
        store=store,
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


def test_watch_cycle_tag_filters_pending_pickup(tmp_path: Path) -> None:
    """Tag-scoped watch should only start pending tasks from the selected tag."""
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
        result = _run_cycle_and_emit_transition_events(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            tags=("release-1",),
        )

    assert result.work_done is True
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == release_task.id


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
    assert spawned_args.resume is True
    assert spawned_task.id == older.id


@pytest.mark.parametrize("task_type", ["implement", "review", "improve", "rebase"])
def test_watch_cycle_plain_mode_prioritizes_pending_over_actionable_failed_recovery(
    tmp_path: Path, task_type: str
) -> None:
    """Plain watch should launch pending work first when slots are saturated."""
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
    assert spawn_resume.call_count == 0
    assert spawn_iterate.call_count == 1
    spawned_args = spawn_iterate.call_args.args[0]
    spawned_task = spawn_iterate.call_args.args[2]
    assert spawned_args.resume is False
    assert spawned_task.id == pending_impl.id
    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert not any("RECOVR" in line and f"{failed.id} resume via" in line for line in log_text.splitlines())


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
    assert spawn_iterate.call_args.args[2].id == failed.id
    assert spawn_iterate.call_args.args[0].resume is True
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
    assert spawn_iterate.call_args.args[2].id == failed.id
    assert spawn_iterate.call_args.args[0].resume is True
    prepared_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert isinstance(prepared_child_id, str)
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


def test_watch_cycle_skipped_failed_descendant_does_not_emit_attention_without_owner_plan_attention(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store, _, failed_rebase = _setup_watch_owner_with_failed_rebase(tmp_path, failure_reason="MERGE_CONFLICT")

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
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert failed_rebase.id not in text


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
    assert len(attention_lines) == 1
    assert impl.id in attention_lines[0]
    assert failed_rebase.id not in attention_lines[0]


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


def test_watch_cycle_show_skipped_emits_skip_for_failed_descendant_without_attention(tmp_path: Path) -> None:
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
    assert "ATTENTION" not in text
    assert any(
        "SKIP" in line and f"{impl.id} failed rebase: manual_failure_reason" in line
        for line in text.splitlines()
    )
    assert failed_rebase.id not in text


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
    assert spawned_task.id == failed.id
    assert spawn_iterate.call_args.kwargs["prepared_resume"] is False
    assert spawn_iterate.call_args.kwargs["prepared_phase"] == "preloop"
    assert spawn_iterate.call_args.kwargs["startup_quiet"] is True
    spawned_child_id = spawn_iterate.call_args.kwargs["prepared_task_id"]
    assert isinstance(spawned_child_id, str)
    log_text = log_path.read_text()
    assert any("RECOVR" in line and f"{failed.id} retry via iterate -> {spawned_child_id}" in line for line in log_text.splitlines())
    assert not any("RECOVR" in line and f"{failed.id} resume via iterate -> {spawned_child_id}" in line for line in log_text.splitlines())


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
    assert first_task.id == failed.id
    first_prepared_child = spawn_iterate.call_args_list[0].kwargs["prepared_task_id"]
    second_prepared_child = spawn_iterate.call_args_list[1].kwargs["prepared_task_id"]
    assert first_prepared_child == second_prepared_child
    assert second_args.resume is True
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
    """Pending work should begin on a later watch pass only after recovery work is fully exhausted."""
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


def test_watch_cycle_restart_failed_starts_manually_queued_child_after_recovery_exhaustion(tmp_path: Path) -> None:
    """--restart-failed should start manual pending children after actionable recovery work is drained."""
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

    assert first_result.work_done is True
    assert second_result.work_done is True
    assert spawn_worker.call_count == 2
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


def test_watch_cycle_restart_failed_in_progress_recovery_child_blocks_pending_queue(tmp_path: Path) -> None:
    """Restart-failed should wait while an existing recovery child is still in progress."""
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
        patch("gza.cli.watch.WorkerRegistry", return_value=registry),
        patch("gza.cli.watch._pid_alive", return_value=True),
    ):
        assert _count_live_workers(config, store) == 1


def test_count_live_workers_ignores_shutting_down_worker_for_terminal_task(tmp_path: Path) -> None:
    """A stale terminal-task registry entry must not consume a slot."""
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


def test_collect_live_running_state_ignores_terminal_task_worker_entry(tmp_path: Path) -> None:
    """A stale terminal-task worker must not count as running or anonymous."""
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
        live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)

    assert live_pids == {4242, 5252}
    assert running_task_ids == [worker_task.id, pid_only_task.id]
    assert anonymous_worker_count == 0


def test_collect_live_running_state_returns_zero_for_stale_terminal_task_worker(
    tmp_path: Path,
) -> None:
    """A stale terminal-task worker entry alone must not count as live or anonymous."""
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
    registry.is_running.return_value = True

    with patch("gza.cli.watch.WorkerRegistry", return_value=registry):
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

    with patch("gza.cli.watch.WorkerRegistry", return_value=registry):
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

    with patch("gza.cli.watch.WorkerRegistry", return_value=registry):
        live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)

    assert live_pids == {4242}
    assert running_task_ids == []
    assert anonymous_worker_count == 1


def test_watch_cycle_starts_pending_work_when_terminal_task_worker_is_still_alive(
    tmp_path: Path,
) -> None:
    """A stale terminal-task worker must not block new starts when batch=1."""
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
        patch("gza.cli.watch.WorkerRegistry", return_value=registry),
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
    assert spawn_iterate.call_args.args[2].id == pending_task.id


def test_format_wake_message_includes_running_task_ids() -> None:
    """WAKE line should append task IDs when tasks are actively running."""
    assert _format_wake_message(running=1, pending=3, slots=0, running_task_ids=["gza-42"]) == (
        "checking... (1 running, 3 pending, 0 slots)\n"
        "live workers:\n"
        "- gza-42"
    )
    assert _format_wake_message(running=0, pending=2, slots=2, running_task_ids=[]) == (
        "checking... (0 running, 2 pending, 2 slots)"
    )
    assert _format_wake_message(
        running=2,
        pending=3,
        slots=0,
        running_task_ids=["gza-42"],
        anonymous_worker_count=1,
    ) == (
        "checking... (2 running, 3 pending, 0 slots)\n"
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

    assert "WAKE      checking... (0 running, 2 pending, 1 slots)" in log_path.read_text()


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
    assert f"MERGE     {task.id} -> main [dry-run]" in log_path.read_text()


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
    assert workspace_git._run.call_args_list == [
        call("checkout", "--detach", "main"),
        call("reset", "--hard", "main"),
        call("clean", "-fd"),
    ]
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
    print_reconcile.assert_called_once_with(reconcile_result)
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

    def choose_action(_cfg, _store, _git, task, _target, *, impl_based_on_ids):  # noqa: ARG001
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

    task = store.add("Completed task", task_type="implement")
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
    assert log_path.read_text().count(f"MERGE     {task.id} -> main") == 1
    assert any(
        line.split(maxsplit=2)[1] == "FOLLOW" and "gza-999 created from" in line
        for line in log_path.read_text().splitlines()
    )


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


def test_watch_cycle_attempt_capped_improve_chain_routes_to_iterate(tmp_path: Path) -> None:
    """Watch should hand attempt-capped improve chains to iterate instead of stopping locally."""
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
    text = log_path.read_text()
    assert "ATTENTION" not in text
    assert f"{impl.id} iterate" in text


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
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
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
    assert "implementation chain already merged; not starting iterate" in log_path.read_text()


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
        patch("gza.cli.watch._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
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
    assert text.count("ATTENTION") == 2
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
    rows = _query_owner_rows(
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
    rows = _query_owner_rows(
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
            "needs_attention_reason": "manual-review-required",
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
    assert text.count("ATTENTION") == 0
    assert text.count("SKIP") == 1
    assert f"{failed.id} failed implement: automatic_recovery_disabled" in text


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
    """Watch should emit one WARNING per newly observed installed-package fingerprint drift."""
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    drift_state = _InstalledPackageDriftState(startup_fingerprint="startup")

    with patch(
        "gza.cli.watch._installed_gza_package_fingerprint",
        side_effect=["startup", "changed-1", "changed-1", "changed-2"],
    ):
        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

        log.begin_cycle()
        _warn_if_installed_gza_changed(log, drift_state)
        log.emit("WAKE", "checking... (0 running, 0 pending, 1 slots)")
        log.end_cycle()

    warning_lines = [line for line in log_path.read_text().splitlines() if "WARNING" in line]
    assert len(warning_lines) == 2
    assert all(
        line.endswith(
            "WARNING   installed gza changed since watch started -- restart watch to pick up new code"
        )
        for line in warning_lines
    )
    assert drift_state.warned_fingerprint == "changed-2"


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
    assert f'{exhausted_child.id} implement "Failed resume attempt" reason=retry-limit-reached' in normalized
    assert "automatic recovery stops here; retry limit reached" in normalized
    assert f'{exhausted_root.id} implement "Failed resume root"' not in normalized
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
        "advance_requires_review: false\n"
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
        "advance_requires_review: false\n"
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
        "advance_requires_review: false\n"
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
        "advance_requires_review: false\n"
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
        "advance_requires_review: false\n"
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
        "advance_requires_review: false\n"
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
        "advance_requires_review: false\n"
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
    assert f'{resumed_retry.id} plan "Root failed plan" reason=retry-limit-reached' in normalized
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


def test_watch_cycle_restart_failed_queue_events_use_queue_label(tmp_path: Path) -> None:
    """Restart-failed queue transitions should use QUEUE, not PHASE."""
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
    assert any("recovery queue enabled (--restart-failed)" in line for line in queue_lines)
    assert any("recovery queue exhausted; switching to pending queue" in line for line in queue_lines)
    assert any("pending queue active" in line for line in queue_lines)
    assert not any(line.split(maxsplit=2)[1] == "PHASE" for line in log_lines)


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
    )

    assert result is not None
    assert result.status == "skip"
    assert result.message == f"{impl.id}: iterate already running for implementation chain"
    assert result.worker_label == "iterate"
    assert result.guarded_pending_task_id == improve.id
