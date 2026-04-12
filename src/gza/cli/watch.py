"""Continuous watch loop and queue management commands."""

import argparse
import os
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ..config import Config
from ..console import truncate
from ..db import SqliteTaskStore, Task as DbTask
from ..git import Git
from ..workers import WorkerRegistry
from ._common import (
    _create_resume_task,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    get_review_verdict,
    get_store,
    resolve_id,
)
from .execution import _spawn_background_iterate
from .git_ops import _determine_advance_action, _merge_single_task

RESUMABLE_FAILURE_REASONS = {"MAX_STEPS", "MAX_TURNS", "TEST_FAILURE"}
SUPPORTED_PLAIN_QUEUE_TYPES = {"plan", "explore"}


def _short_prompt(prompt: str) -> str:
    return truncate(prompt.replace("\n", " "), 56)


def _format_hms() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _format_elapsed(started_at: str | None, completed_at: str | None) -> str | None:
    start_dt = _parse_dt(started_at)
    end_dt = _parse_dt(completed_at)
    if start_dt is None or end_dt is None:
        return None
    elapsed = max(0.0, (end_dt - start_dt).total_seconds())
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    if mins > 0:
        return f"{mins}m{secs:02d}s"
    return f"{secs}s"


def _pid_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _task_snapshot(store: SqliteTaskStore) -> dict[str, dict[str, str | None]]:
    snap: dict[str, dict[str, str | None]] = {}
    with store._connect() as conn:  # noqa: SLF001 - CLI internal polling helper
        cur = conn.execute(
            """
            SELECT id, status, task_type, started_at, completed_at, failure_reason, depends_on
            FROM tasks
            """
        )
        for row in cur.fetchall():
            task_id = str(row["id"])
            snap[task_id] = {
                "status": row["status"],
                "task_type": row["task_type"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "failure_reason": row["failure_reason"],
                "depends_on": row["depends_on"],
            }
    return snap


class _WatchLog:
    def __init__(self, path: Path, *, quiet: bool = False) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.quiet = quiet

    def emit(self, event: str, message: str) -> None:
        line = f"{_format_hms()} {event:<6} {message}".rstrip()
        with open(self.path, "a") as f:
            f.write(line + "\n")
        if not self.quiet:
            print(line)


def _emit_transition_events(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    config: Config,
    log: _WatchLog,
) -> None:
    for task_id in sorted(new.keys()):
        old_status = (old.get(task_id) or {}).get("status")
        new_row = new[task_id]
        new_status = new_row.get("status")
        if old_status == new_status:
            continue

        task_type = new_row.get("task_type") or "implement"
        elapsed = _format_elapsed(new_row.get("started_at"), new_row.get("completed_at"))
        elapsed_suffix = f" ({elapsed})" if elapsed else ""
        if new_status == "completed":
            if task_type == "review":
                task = store.get(task_id)
                impl_id = new_row.get("depends_on") or "unknown"
                verdict = get_review_verdict(config, task) if task is not None else None
                log.emit("REVIEW", f"{task_id} for {impl_id}: {verdict or 'UNKNOWN'}")
            else:
                log.emit("DONE", f"{task_id} {task_type}{elapsed_suffix}")
        elif new_status == "failed":
            reason = new_row.get("failure_reason") or "UNKNOWN"
            log.emit("FAIL", f"{task_id} {task_type}: {reason}{elapsed_suffix}")


def _count_live_workers(config: Config, store: SqliteTaskStore) -> int:
    registry = WorkerRegistry(config.workers_path)
    live: set[str] = set()

    for worker in registry.list_all(include_completed=False):
        if worker.status != "running":
            continue
        if not registry.is_running(worker.worker_id):
            continue
        key = f"task:{worker.task_id}" if worker.task_id else f"worker:{worker.worker_id}"
        live.add(key)

    for task in store.get_in_progress():
        assert task.id is not None
        if not _pid_alive(task.running_pid):
            continue
        live.add(f"task:{task.id}")

    return len(live)


def _pending_runnable_tasks(store: SqliteTaskStore) -> list[DbTask]:
    runnable: list[DbTask] = []
    for task in store.get_pending():
        if task.task_type == "internal":
            continue
        blocked, _, _ = store.is_task_blocked(task)
        if blocked:
            continue
        runnable.append(task)
    return runnable


@dataclass
class _CycleResult:
    work_done: bool
    running: int
    pending: int


def _run_cycle(
    *,
    config: Config,
    store: SqliteTaskStore,
    batch: int,
    max_iterations: int,
    dry_run: bool,
    log: _WatchLog,
) -> _CycleResult:
    from ._common import prune_terminal_dead_workers, reconcile_in_progress_tasks

    reconcile_in_progress_tasks(config)
    prune_terminal_dead_workers(config)

    running = _count_live_workers(config, store)
    slots = max(0, batch - running)
    work_done = False

    log.emit("WAKE", f"checking... ({running} running, {slots} slots)")

    # 1) Merge completed tasks that are ready (does not consume slots)
    merge_candidates = [task for task in store.get_unmerged() if task.status == "completed"]
    if merge_candidates:
        git = Git(config.project_dir)
        target_branch = git.current_branch()
        impl_based_on_ids = store.get_impl_based_on_ids()
        for task in merge_candidates:
            action = _determine_advance_action(
                config,
                store,
                git,
                task,
                target_branch,
                impl_based_on_ids=impl_based_on_ids,
            )
            if action.get("type") != "merge":
                continue
            if dry_run:
                log.emit("MERGE", f"{task.id} -> {target_branch} [dry-run]")
                work_done = True
                continue
            merge_args = argparse.Namespace(
                rebase=False,
                squash=False,
                delete=False,
                mark_only=False,
                remote=False,
                resolve=False,
            )
            rc = _merge_single_task(str(task.id), config, store, git, merge_args, target_branch)
            if rc == 0:
                log.emit("MERGE", f"{task.id} -> {target_branch}")
                work_done = True

    # 2) Resume failed resumable tasks (consumes slots)
    if slots > 0:
        failed_tasks = store.get_resumable_failed_tasks()
        for failed in failed_tasks:
            if slots <= 0:
                break
            if failed.failure_reason not in RESUMABLE_FAILURE_REASONS or not failed.session_id:
                continue
            assert failed.id is not None
            if store.get_based_on_children(failed.id):
                continue
            depth = store.count_resume_chain_depth(failed.id)
            attempt = depth + 1
            if depth >= config.max_resume_attempts:
                log.emit("SKIP", f"{failed.id}: max_resume_attempts reached")
                continue
            if dry_run:
                log.emit(
                    "RESUME",
                    f"{failed.id} -> (new task) (attempt {attempt}/{config.max_resume_attempts}) [dry-run]",
                )
                slots -= 1
                work_done = True
                continue
            resume_task = _create_resume_task(store, failed)
            assert resume_task.id is not None
            worker_args = argparse.Namespace(no_docker=False, max_turns=None)
            rc = _spawn_background_resume_worker(worker_args, config, resume_task.id, quiet=True)
            if rc != 0:
                continue
            slots -= 1
            work_done = True
            log.emit(
                "RESUME",
                f"{failed.id} -> {resume_task.id} (attempt {attempt}/{config.max_resume_attempts})",
            )

    # 3) Start new queued tasks (consumes slots)
    if slots > 0:
        for task in _pending_runnable_tasks(store):
            if slots <= 0:
                break
            assert task.id is not None
            task_type = task.task_type or "implement"
            if task_type == "implement":
                if dry_run:
                    log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\" [dry-run]")
                    slots -= 1
                    work_done = True
                    continue
                iterate_args = argparse.Namespace(
                    max_iterations=max_iterations,
                    no_docker=False,
                    resume=False,
                    retry=False,
                )
                rc = _spawn_background_iterate(iterate_args, config, task)
                if rc != 0:
                    continue
                slots -= 1
                work_done = True
                log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\"")
                continue

            if task_type in SUPPORTED_PLAIN_QUEUE_TYPES:
                if dry_run:
                    log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\" [dry-run]")
                    slots -= 1
                    work_done = True
                    continue
                worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
                rc = _spawn_background_worker(worker_args, config, task_id=task.id, quiet=True)
                if rc != 0:
                    continue
                slots -= 1
                work_done = True
                log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\"")
                continue

            log.emit("SKIP", f"{task.id}: unsupported pending type '{task_type}' for watch")

    return _CycleResult(
        work_done=work_done,
        running=_count_live_workers(config, store),
        pending=len(_pending_runnable_tasks(store)),
    )


def cmd_watch(args: argparse.Namespace) -> int:
    """Run continuous scheduler loop that maintains N concurrent workers."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    batch = args.batch if args.batch is not None else config.watch.batch
    poll = args.poll if args.poll is not None else config.watch.poll
    max_idle = args.max_idle if args.max_idle is not None else config.watch.max_idle
    max_iterations = (
        args.max_iterations if args.max_iterations is not None else config.watch.max_iterations
    )
    dry_run = bool(getattr(args, "dry_run", False))
    quiet = bool(getattr(args, "quiet", False))

    if batch < 1:
        print("Error: --batch must be a positive integer")
        return 1
    if poll < 1:
        print("Error: --poll must be a positive integer")
        return 1
    if max_idle is not None and max_idle < 1:
        print("Error: --max-idle must be a positive integer")
        return 1
    if max_iterations < 1:
        print("Error: --max-iterations must be a positive integer")
        return 1

    log = _WatchLog(config.project_dir / ".gza" / "watch.log", quiet=quiet)
    stop_requested = False

    def _handle_shutdown(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True
        log.emit("INFO", "shutting down (workers left running)")

    old_sigint = signal.signal(signal.SIGINT, _handle_shutdown)
    old_sigterm = signal.signal(signal.SIGTERM, _handle_shutdown)

    idle_seconds = 0
    previous_snapshot = _task_snapshot(store)

    try:
        while True:
            if stop_requested:
                break

            cycle_result = _run_cycle(
                config=config,
                store=store,
                batch=batch,
                max_iterations=max_iterations,
                dry_run=dry_run,
                log=log,
            )

            current_snapshot = _task_snapshot(store)
            _emit_transition_events(
                previous_snapshot,
                current_snapshot,
                store=store,
                config=config,
                log=log,
            )
            previous_snapshot = current_snapshot

            if cycle_result.work_done:
                idle_seconds = 0
            else:
                idle_seconds += poll
                log.emit(
                    "IDLE",
                    f"sleeping {poll}s ({cycle_result.pending} pending, {cycle_result.running} running)",
                )
                if max_idle is not None and idle_seconds > max_idle:
                    log.emit("INFO", f"max idle time reached ({max_idle}s), exiting")
                    break

            if stop_requested:
                break
            time.sleep(poll)
    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    """Inspect and adjust pending queue urgency."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    action = getattr(args, "queue_action", None)

    if action in {"bump", "unbump"}:
        task_id = resolve_id(config, args.task_id)
        task = store.get(task_id)
        if task is None:
            print(f"Error: Task {task_id} not found")
            return 1
        if task.status != "pending":
            print(f"Error: Task {task_id} is not pending (status: {task.status})")
            return 1
        new_urgent = action == "bump"
        store.set_urgent(task_id, new_urgent)
        if new_urgent:
            print(f"✓ Bumped task {task_id} to urgent queue")
        else:
            print(f"✓ Removed task {task_id} from urgent queue")
        return 0

    pending = store.get_pending()
    if not pending:
        print("No pending tasks")
        return 0

    for index, task in enumerate(pending, start=1):
        lane = "urgent" if task.urgent else "normal"
        print(f"{index:>3}  {task.id}  [{lane}] [{task.task_type}] {_short_prompt(task.prompt)}")

    return 0
