"""Tests for `gza watch` scheduler behavior."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from gza.config import Config
from gza.cli.watch import _WatchLog, _count_live_workers, _run_cycle
from gza.workers import WorkerMetadata

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
        patch("gza.cli.watch._determine_advance_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._merge_single_task", return_value=0) as merge_single,
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
    assert merge_single.call_count == 0
    assert " MERGE " not in log_path.read_text()


def test_watch_cycle_uses_auto_squash_merge_args_from_shared_logic(tmp_path: Path) -> None:
    """Watch merge execution should honor merge_squash_threshold auto-squash."""
    (tmp_path / "gza.yaml").write_text("project_name: test-project\nmerge_squash_threshold: 2\n")
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

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
        patch("gza.cli.watch._determine_advance_action", return_value={"type": "merge"}),
        patch("gza.cli.watch._merge_single_task", return_value=0) as merge_single,
    ):
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )
        watch_merge_args = merge_single.call_args.args[4]
        from gza.cli.git_ops import _build_auto_merge_args
        advance_merge_args = _build_auto_merge_args(config, git, task, "main")

    assert watch_merge_args.squash is True
    assert advance_merge_args.squash is True


def test_watch_cycle_suppresses_repeated_unsupported_type_skip_logs(tmp_path: Path) -> None:
    """Unsupported pending type should log SKIP once per task while state is unchanged."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    unsupported = store.add("Unsupported", task_type="review")
    assert unsupported.id is not None

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)
    seen: set[str] = set()

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
            seen_unsupported_types=seen,
        )
        _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
            seen_unsupported_types=seen,
        )

    skip_lines = [line for line in log_path.read_text().splitlines() if "SKIP" in line]
    assert len(skip_lines) == 1
