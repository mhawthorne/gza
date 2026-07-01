"""Shared task-launch concurrency controls."""

from __future__ import annotations

import contextlib
import fcntl
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .config import Config
from .db import SqliteTaskStore, task_id_numeric_key
from .workers import WorkerRegistry


@dataclass
class _ProcessLockState:
    lock_file: BinaryIO | None = None
    owner_thread_id: int | None = None
    depth: int = 0


_PROCESS_LOCKS: dict[str, _ProcessLockState] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()
_RESERVED_LAUNCH_PERMITS: dict[str, LaunchPermit] = {}
_RESERVED_LAUNCH_PERMITS_GUARD = threading.Lock()


@dataclass(frozen=True)
class ConcurrencySnapshot:
    """Current project-wide task execution occupancy."""

    limit: int
    running: int
    available: int
    live_pids: frozenset[int]
    running_task_ids: tuple[str, ...]
    anonymous_worker_count: int
    current_pid_counted: bool
    starting_worker_count: int = 0


@dataclass(frozen=True)
class _LiveRunningState:
    live_pids: frozenset[int]
    live_active_task_pids: frozenset[int]
    running_task_ids: tuple[str, ...]
    anonymous_worker_count: int
    starting_worker_count: int = 0


class MaxConcurrentTasksError(RuntimeError):
    """Raised when a launch would exceed the project-wide concurrency ceiling."""


@dataclass
class LaunchPermit:
    """Lock-backed launch permit that can be released once the launch is visible."""

    _lock_file: BinaryIO
    _lock_key: str
    snapshot: ConcurrencySnapshot
    _released: bool = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        lock_file: BinaryIO | None = None
        with _PROCESS_LOCKS_GUARD:
            state = _PROCESS_LOCKS.get(self._lock_key)
            if state is None:
                return
            if state.depth > 1:
                state.depth -= 1
                return
            lock_file = state.lock_file
            state.lock_file = None
            state.owner_thread_id = None
            state.depth = 0
            del _PROCESS_LOCKS[self._lock_key]
        if lock_file is None:
            return
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()

    def __enter__(self) -> LaunchPermit:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def format_max_concurrent_message(*, running: int, limit: int) -> str:
    return f"already at max concurrent tasks: {running} running, limit is {limit}"


def _lock_path(config: Config) -> Path:
    return config.project_dir / ".gza" / "max-concurrent.lock"


def _pid_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _collect_live_running_state_details(config: Config, store: SqliteTaskStore) -> _LiveRunningState:
    registry = WorkerRegistry(config.workers_path)
    live_pids: set[int] = set()
    live_task_ids: set[str] = set()
    live_active_task_pids: set[int] = set()
    live_starting_task_pids: set[int] = set()
    active_task_statuses = {
        str(task.id): task.status
        for task in store.get_in_progress()
        if task.id is not None
    }

    for worker in registry.list_all(include_completed=False):
        if worker.status != "running" or not registry.is_running(worker.worker_id):
            continue
        if worker.pid > 0:
            live_pids.add(worker.pid)
        if worker.task_id is not None:
            task_id = str(worker.task_id)
            task_status = active_task_statuses.get(task_id)
            if task_status is None:
                task = store.get(task_id)
                task_status = task.status if task is not None else None
                if task_status is not None:
                    active_task_statuses[task_id] = task_status
            if task_status == "in_progress":
                if worker.pid > 0:
                    live_active_task_pids.add(worker.pid)
                live_task_ids.add(task_id)
                continue
            if task_status == "pending":
                if worker.pid > 0:
                    live_starting_task_pids.add(worker.pid)
                continue
            if task_status is not None:
                continue

    for task in store.get_in_progress():
        pid = task.running_pid
        if not _pid_alive(pid):
            continue
        assert pid is not None
        live_pids.add(pid)
        live_active_task_pids.add(pid)
        if task.id is not None:
            live_task_ids.add(str(task.id))

    running_task_ids = tuple(sorted(live_task_ids, key=lambda task_id: task_id_numeric_key(task_id)))
    starting_worker_count = len(live_starting_task_pids - live_active_task_pids)
    anonymous_worker_count = len(live_pids - live_active_task_pids - live_starting_task_pids)
    return _LiveRunningState(
        live_pids=frozenset(live_pids),
        live_active_task_pids=frozenset(live_active_task_pids),
        running_task_ids=running_task_ids,
        anonymous_worker_count=anonymous_worker_count,
        starting_worker_count=starting_worker_count,
    )


def _collect_live_running_state(config: Config, store: SqliteTaskStore) -> tuple[set[int], tuple[str, ...], int, int]:
    details = _collect_live_running_state_details(config, store)
    return (
        set(details.live_pids),
        details.running_task_ids,
        details.anonymous_worker_count,
        details.starting_worker_count,
    )


def _best_effort_stale_cleanup(config: Config) -> None:
    # Import lazily to avoid a module dependency loop with cli._common importing us.
    from .cli._common import prune_terminal_dead_workers, reconcile_in_progress_tasks

    reconcile_in_progress_tasks(config)
    prune_terminal_dead_workers(config)


def get_concurrency_snapshot(
    config: Config,
    store: SqliteTaskStore,
    *,
    current_pid: int | None = None,
    cleanup_stale: bool = True,
) -> ConcurrencySnapshot:
    if cleanup_stale:
        _best_effort_stale_cleanup(config)
    live_state = _collect_live_running_state_details(config, store)
    limit = config.max_concurrent
    running = len(live_state.live_active_task_pids)
    available = max(0, limit - running)
    counted = bool(current_pid and current_pid in live_state.live_active_task_pids)
    return ConcurrencySnapshot(
        limit=limit,
        running=running,
        available=available,
        live_pids=live_state.live_pids,
        running_task_ids=live_state.running_task_ids,
        anonymous_worker_count=live_state.anonymous_worker_count,
        starting_worker_count=live_state.starting_worker_count,
        current_pid_counted=counted,
    )


def launch_permit(
    config: Config,
    store: SqliteTaskStore,
    *,
    current_pid: int | None = None,
) -> LaunchPermit:
    lock_path = _lock_path(config)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_key = str(lock_path.resolve())
    thread_id = threading.get_ident()
    created_state = False
    lock_file: BinaryIO | None = None
    owns_flock = False
    try:
        while True:
            with _PROCESS_LOCKS_GUARD:
                state = _PROCESS_LOCKS.get(lock_key)
                if state is None:
                    state = _ProcessLockState()
                    _PROCESS_LOCKS[lock_key] = state
                    created_state = True
                if state.owner_thread_id is None:
                    lock_file = lock_path.open("a+b")
                    state.lock_file = lock_file
                    state.owner_thread_id = thread_id
                    state.depth = 1
                    owns_flock = True
                    break
                if state.owner_thread_id == thread_id:
                    assert state.lock_file is not None
                    lock_file = state.lock_file
                    state.depth += 1
                    break
            threading.Event().wait(0.001)
        if owns_flock:
            assert lock_file is not None
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        _best_effort_stale_cleanup(config)
        snapshot = get_concurrency_snapshot(
            config,
            store,
            current_pid=current_pid,
            cleanup_stale=False,
        )
        if snapshot.running >= snapshot.limit and not snapshot.current_pid_counted:
            raise MaxConcurrentTasksError(
                format_max_concurrent_message(running=snapshot.running, limit=snapshot.limit)
            )
        assert lock_file is not None
        return LaunchPermit(lock_file, lock_key, snapshot)
    except Exception:
        held_file: BinaryIO | None = None
        with _PROCESS_LOCKS_GUARD:
            state = _PROCESS_LOCKS.get(lock_key)
            if state is not None and state.owner_thread_id == thread_id:
                if state.depth > 1:
                    state.depth -= 1
                else:
                    held_file = state.lock_file
                    del _PROCESS_LOCKS[lock_key]
            elif created_state:
                _PROCESS_LOCKS.pop(lock_key, None)
        if held_file is not None:
            with contextlib.suppress(Exception):
                fcntl.flock(held_file.fileno(), fcntl.LOCK_UN)
            held_file.close()
        elif owns_flock and lock_file is not None:
            lock_file.close()
        raise


def _clone_launch_permit(permit: LaunchPermit) -> LaunchPermit:
    lock_key = permit._lock_key
    thread_id = threading.get_ident()
    with _PROCESS_LOCKS_GUARD:
        state = _PROCESS_LOCKS.get(lock_key)
        if state is None or state.owner_thread_id != thread_id or state.lock_file is None:
            raise RuntimeError("launch permit is not owned by the current thread")
        state.depth += 1
    return LaunchPermit(state.lock_file, lock_key, permit.snapshot)


def launch_permits(
    config: Config,
    store: SqliteTaskStore,
    *,
    count: int,
    current_pid: int | None = None,
) -> list[LaunchPermit]:
    if count <= 0:
        return []

    first_permit = launch_permit(config, store, current_pid=current_pid)
    required_new_slots = count - (1 if first_permit.snapshot.current_pid_counted else 0)
    if first_permit.snapshot.available < required_new_slots:
        snapshot = first_permit.snapshot
        first_permit.release()
        raise MaxConcurrentTasksError(
            format_max_concurrent_message(running=snapshot.running, limit=snapshot.limit)
        )

    permits = [first_permit]
    for _ in range(1, count):
        permits.append(_clone_launch_permit(first_permit))
    return permits


def reserve_task_launch_permit(task_id: str, permit: LaunchPermit) -> None:
    with _RESERVED_LAUNCH_PERMITS_GUARD:
        _RESERVED_LAUNCH_PERMITS[task_id] = permit


def take_task_launch_permit(task_id: str | None) -> LaunchPermit | None:
    if task_id is None:
        return None
    with _RESERVED_LAUNCH_PERMITS_GUARD:
        return _RESERVED_LAUNCH_PERMITS.pop(task_id, None)


def release_task_launch_permit(task_id: str | None) -> None:
    permit = take_task_launch_permit(task_id)
    if permit is not None:
        permit.release()
