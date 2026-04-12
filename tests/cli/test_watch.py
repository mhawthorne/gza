"""Tests for `gza watch` scheduler behavior."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from gza.config import Config
from gza.cli.watch import _WatchLog, _run_cycle

from .conftest import make_store, setup_config


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


def test_watch_cycle_resumes_failed_task_before_starting_new_pending(tmp_path: Path) -> None:
    """Resume-eligible failed tasks consume slots before new pending tasks."""
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
        )

    assert result.work_done is True
    assert spawn_resume.call_count == 1
    assert spawn_iterate.call_count == 0
