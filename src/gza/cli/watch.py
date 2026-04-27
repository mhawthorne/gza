"""Continuous watch loop and queue management commands."""

import argparse
import contextlib
import io
import os
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

from ..config import Config
from ..console import truncate
from ..db import SqliteTaskStore, Task as DbTask, task_id_numeric_key
from ..failure_policy import is_resumable_failure_reason
from ..git import Git
from ..pickup import get_runnable_pending_tasks, is_worker_consuming_advance_action
from ..task_query import (
    TaskQueryPresets,
    TaskQueryService,
    TaskRow,
    normalize_tag_filters,
    task_matches_tag_filters,
)
from ..workers import WorkerRegistry
from ._common import (
    _create_rebase_task,
    _create_resume_task,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    clear_task_queue_position,
    format_review_outcome,
    get_store,
    resolve_id,
    set_task_queue_position,
    set_task_urgency,
)
from .advance_executor import AdvanceActionExecutionContext, execute_advance_action
from .execution import _spawn_background_iterate
from .git_ops import (
    _collect_advance_completed_tasks,
    _determine_advance_action,
    _execute_merge_action,
    _merge_single_task as _git_ops_merge_single_task,
    _prepare_create_review_action,
    _require_default_branch,
    _unimplemented_implement_prompt,
)

_WATCH_ADVANCE_ACTION_ORDER: dict[str, int] = {"merge": 0}
T = TypeVar("T")


def _merge_single_task(
    task_id: str,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    args: argparse.Namespace,
    current_branch: str,
) -> int:
    """Compatibility shim for tests patching watch-local merge execution."""
    return _git_ops_merge_single_task(task_id, config, store, git, args, current_branch)


def _watch_skip_message(task: DbTask, action: dict) -> str:
    """Build a stable skip message for non-executed advance actions."""
    action_type = str(action.get("type", "skip"))
    description = str(action.get("description", "")).strip()
    if description.startswith("SKIP: "):
        description = description[len("SKIP: ") :]
    if not description:
        description = action_type.replace("_", " ")
    return f"{task.id}: {description}"


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


def _sleep_interruptibly(seconds: int, stop_requested: Callable[[], bool], *, quantum: float = 1.0) -> None:
    """Sleep for up to `seconds`, exiting early if stop was requested."""
    remaining = float(seconds)
    while remaining > 0:
        if stop_requested():
            return
        step = min(quantum, remaining)
        time.sleep(step)
        remaining -= step


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
        self._has_emitted_cycle = False
        self._skip_keys_prev_cycle: set[str] = set()
        self._skip_keys_this_cycle: set[str] = set()

    def begin_cycle(self) -> None:
        if self._has_emitted_cycle:
            with open(self.path, "a") as f:
                f.write("\n")
            if not self.quiet:
                print()
        self._skip_keys_this_cycle.clear()
        self._has_emitted_cycle = True

    def end_cycle(self) -> None:
        self._skip_keys_prev_cycle = set(self._skip_keys_this_cycle)

    def emit(self, event: str, message: str, *, dedupe_key: str | None = None) -> None:
        if event == "SKIP" and dedupe_key is not None:
            self._skip_keys_this_cycle.add(dedupe_key)
            if dedupe_key in self._skip_keys_prev_cycle:
                return
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
                verdict = (
                    format_review_outcome(config, task)
                    if task is not None
                    else "UNKNOWN"
                )
                log.emit("REVIEW", f"{task_id} for {impl_id}: {verdict}")
            else:
                log.emit("DONE", f"{task_id} {task_type}{elapsed_suffix}")
        elif new_status == "failed":
            reason = new_row.get("failure_reason") or "UNKNOWN"
            log.emit("FAIL", f"{task_id} {task_type}: {reason}{elapsed_suffix}")


def _count_live_workers(config: Config, store: SqliteTaskStore) -> int:
    live_pids, _ = _collect_live_running_state(config, store)
    return len(live_pids)


def _collect_live_running_state(config: Config, store: SqliteTaskStore) -> tuple[set[int], list[str]]:
    registry = WorkerRegistry(config.workers_path)
    live_pids: set[int] = set()
    live_task_ids: set[str] = set()
    active_task_statuses = {
        str(task.id): task.status
        for task in store.get_in_progress()
        if task.id is not None
    }

    for worker in registry.list_all(include_completed=False):
        if worker.status != "running":
            continue
        if worker.task_id is not None and active_task_statuses.get(str(worker.task_id)) not in {"pending", "in_progress"}:
            continue
        if not registry.is_running(worker.worker_id):
            continue
        if worker.pid > 0:
            live_pids.add(worker.pid)
        if worker.task_id is not None:
            live_task_ids.add(str(worker.task_id))

    for task in store.get_in_progress():
        pid = task.running_pid
        if not _pid_alive(pid):
            continue
        assert pid is not None
        live_pids.add(pid)
        if task.id is not None:
            live_task_ids.add(str(task.id))

    return live_pids, sorted(live_task_ids, key=lambda task_id: task_id_numeric_key(task_id))


def _format_wake_message(*, running: int, pending: int, slots: int, running_task_ids: list[str]) -> str:
    message = f"checking... ({running} running, {pending} pending, {slots} slots)"
    if running_task_ids:
        message += f" tasks: {', '.join(running_task_ids)}"
    return message


def _pending_runnable_tasks(
    store: SqliteTaskStore,
    *,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> list[DbTask]:
    return get_runnable_pending_tasks(store, tags=tags, any_tag=any_tag)


def _run_with_optional_stdout_suppressed(quiet: bool, fn: Callable[[], T]) -> T:
    if not quiet:
        return fn()
    with contextlib.redirect_stdout(io.StringIO()):
        return fn()


def _spawn_worker_with_failure_log(
    *,
    quiet: bool,
    log: _WatchLog,
    failure_message: str,
    spawn_fn: Callable[[], int],
    dedupe_key: str,
) -> int:
    rc = _run_with_optional_stdout_suppressed(quiet, spawn_fn)
    if rc != 0:
        log.emit("START_FAILED", failure_message, dedupe_key=dedupe_key)
    return rc


@dataclass
class _CycleResult:
    work_done: bool
    running: int
    pending: int


@dataclass(frozen=True)
class _ObservedFailure:
    task_id: str
    task_type: str
    reason: str


def _iter_status_transitions(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
) -> list[tuple[str, str | None, dict[str, str | None]]]:
    transitions: list[tuple[str, str | None, dict[str, str | None]]] = []
    for task_id in sorted(new.keys()):
        old_status = (old.get(task_id) or {}).get("status")
        new_row = new[task_id]
        new_status = new_row.get("status")
        if old_status == new_status:
            continue
        transitions.append((task_id, old_status, new_row))
    return transitions


def _task_matches_tags(
    store: SqliteTaskStore,
    task_id: str,
    tags: tuple[str, ...] | None,
    any_tag: bool,
) -> bool:
    normalized_tags = normalize_tag_filters(tags)
    if not normalized_tags:
        return True
    task = store.get(task_id)
    if task is None:
        return False
    return task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized_tags, any_tag=any_tag)


def _failure_is_auto_resumable_by_watch(
    *,
    task_id: str,
    store: SqliteTaskStore,
    config: Config,
    reason: str | None,
) -> bool:
    task = store.get(task_id)
    if task is None or task.id is None:
        return False
    if not is_resumable_failure_reason(reason):
        return False
    if not task.session_id:
        return False
    return store.count_resume_chain_depth(task_id) < config.max_resume_attempts


def _collect_completed_transition_ids(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> list[str]:
    completed_ids: list[str] = []
    for task_id, _old_status, new_row in _iter_status_transitions(old, new):
        if new_row.get("status") != "completed":
            continue
        if not _task_matches_tags(store, task_id, tags, any_tag):
            continue
        completed_ids.append(task_id)
    return completed_ids


def _collect_unhandled_failures(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    config: Config,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> list[_ObservedFailure]:
    failures: list[_ObservedFailure] = []
    for task_id, _old_status, new_row in _iter_status_transitions(old, new):
        if new_row.get("status") != "failed":
            continue
        if not _task_matches_tags(store, task_id, tags, any_tag):
            continue
        reason = new_row.get("failure_reason") or "UNKNOWN"
        if _failure_is_auto_resumable_by_watch(task_id=task_id, store=store, config=config, reason=reason):
            continue
        failures.append(
            _ObservedFailure(
                task_id=task_id,
                task_type=new_row.get("task_type") or "implement",
                reason=reason,
            )
        )
    return failures


def _compute_failure_backoff_seconds(config: Config, streak: int) -> int:
    if streak <= 0:
        return 0
    initial = config.watch.failure_backoff_initial
    maximum = config.watch.failure_backoff_max
    return min(initial * (2 ** (streak - 1)), maximum)


def _run_cycle(
    *,
    config: Config,
    store: SqliteTaskStore,
    batch: int,
    max_iterations: int,
    dry_run: bool,
    log: _WatchLog,
    tags: tuple[str, ...] | None = None,
    group: str | None = None,
    any_tag: bool = False,
    quiet: bool = False,
) -> _CycleResult:
    from ._common import prune_terminal_dead_workers, reconcile_in_progress_tasks

    if group:
        merged_tags = list(tags or ())
        merged_tags.append(group)
        tags = tuple(merged_tags)
    tags = normalize_tag_filters(tags)

    log.begin_cycle()
    if not dry_run:
        reconcile_in_progress_tasks(config)
        prune_terminal_dead_workers(config)

    live_pids, running_task_ids = _collect_live_running_state(config, store)
    pending_count = len(_pending_runnable_tasks(store, tags=tags, any_tag=any_tag))
    running = len(live_pids)
    slots = max(0, batch - running)
    work_done = False
    started_task_ids: set[str] = set()
    step1_handled_child_task_ids: set[str] = set()

    log.emit(
        "WAKE",
        _format_wake_message(
            running=running,
            pending=pending_count,
            slots=slots,
            running_task_ids=running_task_ids,
        ),
    )

    # 1) Execute advance actions for completed tasks (includes completed plans
    # with no implement child, aligned with gza advance).
    # Merges run first; worker-spawning actions consume available slots.
    merge_candidates, impl_based_on_ids = _collect_advance_completed_tasks(store)
    if tags:
        merge_candidates = [
            task
            for task in merge_candidates
            if task_matches_tag_filters(task_tags=task.tags, tag_filters=tags, any_tag=any_tag)
        ]
    if merge_candidates:
        git = Git(config.project_dir)
        current_branch = git.current_branch()
        target_branch = git.default_branch()
        action_plan: list[tuple[DbTask, dict]] = []
        for task in merge_candidates:
            action_plan.append(
                (
                    task,
                    _determine_advance_action(
                        config,
                        store,
                        git,
                        task,
                        target_branch,
                        impl_based_on_ids=impl_based_on_ids,
                    ),
                )
            )
        action_plan.sort(key=lambda item: _WATCH_ADVANCE_ACTION_ORDER.get(item[1].get("type", ""), 1))
        has_merge_action = any(action.get("type") in {"merge", "merge_with_followups"} for _, action in action_plan)
        can_merge = True
        if has_merge_action:
            can_merge = _run_with_optional_stdout_suppressed(
                quiet,
                lambda: _require_default_branch(git, current_branch, "merge"),
            )

        worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)

        def _watch_spawn_worker(task_id: str, task_kind: str) -> int:
            return _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task_id} {task_kind}: worker spawn failed",
                dedupe_key=f"spawn-worker-failed:{task_id}",
                spawn_fn=lambda: _spawn_background_worker(worker_args, config, task_id=task_id, quiet=quiet),
            )

        def _watch_spawn_resume_worker(task_id: str, task_kind: str) -> int:
            return _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task_id} {task_kind}: resume worker spawn failed",
                dedupe_key=f"spawn-resume-failed:{task_id}",
                spawn_fn=lambda: _spawn_background_resume_worker(worker_args, config, new_task_id=task_id, quiet=quiet),
            )

        def _watch_spawn_iterate(task_obj: DbTask, task_kind: str) -> int:
            iterate_args = argparse.Namespace(
                max_iterations=max_iterations,
                no_docker=False,
                resume=False,
                retry=False,
            )
            return _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task_obj.id} {task_kind}: iterate worker spawn failed",
                dedupe_key=f"spawn-iterate-failed:{task_obj.id}",
                spawn_fn=lambda: _spawn_background_iterate(iterate_args, config, task_obj),
            )

        def _create_rebase_from_task(parent_task: DbTask) -> DbTask:
            assert parent_task.id is not None
            assert parent_task.branch is not None
            return _create_rebase_task(store, parent_task.id, parent_task.branch, target_branch)

        def _create_implement_from_task(parent_task: DbTask) -> DbTask:
            assert parent_task.id is not None
            return store.add(
                prompt=_unimplemented_implement_prompt(parent_task),
                task_type="implement",
                depends_on=parent_task.id,
                tags=parent_task.tags,
            )

        executor_context = AdvanceActionExecutionContext(
            store=store,
            dry_run=dry_run,
            max_resume_attempts=config.max_resume_attempts,
            use_iterate_for_create_implement=True,
            use_iterate_for_needs_rebase=False,
            prepare_create_review=lambda t: _prepare_create_review_action(store, t),
            create_resume_task=lambda t: _create_resume_task(store, t),
            create_rebase_task=_create_rebase_from_task,
            create_implement_task=_create_implement_from_task,
            spawn_worker=_watch_spawn_worker,
            spawn_resume_worker=_watch_spawn_resume_worker,
            spawn_iterate_worker=_watch_spawn_iterate,
        )

        for task, action in action_plan:
            action_type = action.get("type")
            if action_type in {
                "skip",
                "wait_review",
                "wait_improve",
                "needs_discussion",
                "max_cycles_reached",
                "max_improve_attempts",
            }:
                log.emit(
                    "SKIP",
                    _watch_skip_message(task, action),
                    dedupe_key=f"advance-skip:{action_type}:{task.id}",
                )
                continue

            if action_type in {"merge", "merge_with_followups"}:
                if not can_merge:
                    log.emit(
                        "SKIP",
                        "merge actions skipped: not on default branch",
                        dedupe_key="merge-not-default-branch",
                    )
                    continue
                if dry_run:
                    log.emit("MERGE", f"{task.id} -> {target_branch} [dry-run]")
                    work_done = True
                    continue
                merge_result = _run_with_optional_stdout_suppressed(
                    quiet,
                    lambda: _execute_merge_action(
                        config,
                        store,
                        git,
                        task,
                        action,
                        target_branch=target_branch,
                        current_branch=current_branch,
                    ),
                )
                rc = merge_result.rc
                for followup_task in merge_result.created_followups:
                    log.emit("FOLLOW", f"{followup_task.id} created from {task.id}")
                for followup_task in merge_result.reused_followups:
                    log.emit("FOLLOW", f"{followup_task.id} reused from {task.id}")
                if rc == 0:
                    log.emit("MERGE", f"{task.id} -> {target_branch}")
                    work_done = True
                else:
                    log.emit(
                        "SKIP",
                        f"{task.id}: merge failed",
                        dedupe_key=f"merge-failed:{task.id}",
                    )
                continue

            if not is_worker_consuming_advance_action(str(action_type)) or action_type == "resume":
                continue
            if slots <= 0:
                continue

            exec_result = execute_advance_action(task=task, action=action, context=executor_context)
            child_id = exec_result.handled_task_id

            if exec_result.status == "skip":
                message = exec_result.message
                if action_type == "improve" and task.id is not None:
                    message = f"{task.id}: {message}"
                log.emit(
                    "SKIP",
                    message,
                    dedupe_key=f"advance-worker-skip:{action_type}:{task.id}:{message}",
                )
                continue

            if exec_result.status == "error":
                if not exec_result.attempted_spawn and task.id is not None:
                    log.emit(
                        "ERROR",
                        f"{task.id}: {exec_result.message}",
                        dedupe_key=f"advance-worker-error:{action_type}:{task.id}:{exec_result.message}",
                    )
                if child_id is not None and action_type in {
                    "create_review",
                    "improve",
                    "create_implement",
                    "needs_rebase",
                    "run_review",
                    "run_improve",
                }:
                    step1_handled_child_task_ids.add(str(child_id))
                continue

            if exec_result.status == "dry_run":
                if action_type == "create_review" and task.id is not None:
                    log.emit("START", f"(new) review for {task.id} [dry-run]")
                elif action_type == "run_review" and child_id is not None:
                    log.emit("START", f"{child_id} review [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "improve":
                    failed_id = exec_result.failed_improve.id if exec_result.failed_improve is not None else None
                    if exec_result.improve_mode == "resume" and failed_id is not None:
                        log.emit("START", f"(resume) improve for {failed_id} [dry-run]")
                    elif exec_result.improve_mode == "retry" and failed_id is not None:
                        log.emit("START", f"(retry) improve for {failed_id} [dry-run]")
                    elif task.id is not None:
                        log.emit("START", f"(new) improve for {task.id} [dry-run]")
                elif action_type == "run_improve" and child_id is not None:
                    log.emit("START", f"{child_id} improve [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "create_implement" and task.id is not None:
                    log.emit("START", f"(new) implement for {task.id} [dry-run]")
                elif action_type == "needs_rebase" and task.id is not None:
                    log.emit("START", f"(new) rebase for {task.id} [dry-run]")
                slots -= 1
                work_done = True
                continue

            if child_id is not None and action_type in {"create_review", "improve", "create_implement", "needs_rebase"}:
                step1_handled_child_task_ids.add(str(child_id))

            if exec_result.status == "success" and child_id is not None:
                if action_type in {"create_review", "run_review"}:
                    log.emit("START", f"{child_id} review")
                elif action_type in {"improve", "run_improve"}:
                    log.emit("START", f"{child_id} improve")
                elif action_type == "create_implement":
                    log.emit("START", f"{child_id} implement")
                elif action_type == "needs_rebase":
                    log.emit("START", f"{child_id} rebase")
                started_task_ids.add(str(child_id))
                slots -= 1
                work_done = True

    # 2) Resume failed resumable tasks (consumes slots)
    pending_resume_task_ids: set[str] = set()
    if slots > 0:
        failed_tasks = store.get_resumable_failed_tasks()
        if tags:
            failed_tasks = [
                task
                for task in failed_tasks
                if task_matches_tag_filters(task_tags=task.tags, tag_filters=tags, any_tag=any_tag)
            ]
        for failed in failed_tasks:
            if slots <= 0:
                break
            if not is_resumable_failure_reason(failed.failure_reason) or not failed.session_id:
                continue
            assert failed.id is not None

            children = store.get_based_on_children(failed.id)
            resume_task: DbTask | None = None
            for child in children:
                if (
                    child.status == "pending"
                    and child.task_type == failed.task_type
                    and child.session_id == failed.session_id
                    and child.id is not None
                ):
                    resume_task = child
                    break
            if resume_task is None and children:
                continue

            depth = store.count_resume_chain_depth(failed.id)
            attempt = depth + 1
            if depth >= config.max_resume_attempts:
                log.emit(
                    "SKIP",
                    f"{failed.id}: max_resume_attempts reached",
                    dedupe_key=f"max-resume:{failed.id}",
                )
                continue
            if dry_run:
                resume_target = str(resume_task.id) if resume_task is not None and resume_task.id is not None else "(new task)"
                log.emit(
                    "RESUME",
                    f"{failed.id} -> {resume_target} (attempt {attempt}/{config.max_resume_attempts}) [dry-run]",
                )
                slots -= 1
                work_done = True
                continue
            if resume_task is None:
                resume_task = _create_resume_task(store, failed)
            assert resume_task.id is not None
            resume_task_id = str(resume_task.id)
            pending_resume_task_ids.add(resume_task_id)
            worker_args = argparse.Namespace(no_docker=False, max_turns=None)
            rc = _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{failed.id} -> {resume_task_id}: resume worker spawn failed",
                dedupe_key=f"spawn-resume-failed:{failed.id}:{resume_task_id}",
                spawn_fn=lambda: _spawn_background_resume_worker(worker_args, config, resume_task_id, quiet=quiet),
            )
            if rc != 0:
                continue
            slots -= 1
            work_done = True
            started_task_ids.add(resume_task_id)
            log.emit(
                "RESUME",
                f"{failed.id} -> {resume_task_id} (attempt {attempt}/{config.max_resume_attempts})",
            )

    # 3) Start new queued tasks (consumes slots)
    pending_tasks = _pending_runnable_tasks(store, tags=tags, any_tag=any_tag)
    if slots > 0:
        for task in pending_tasks:
            if slots <= 0:
                break
            assert task.id is not None
            if str(task.id) in started_task_ids:
                continue
            if str(task.id) in pending_resume_task_ids:
                continue
            if str(task.id) in step1_handled_child_task_ids:
                continue
            task_type = task.task_type or "implement"
            if task_type == "implement":
                if dry_run:
                    log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\" [dry-run]")
                    started_task_ids.add(str(task.id))
                    slots -= 1
                    work_done = True
                    continue
                iterate_args = argparse.Namespace(
                    max_iterations=max_iterations,
                    no_docker=False,
                    resume=False,
                    retry=False,
                )
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{task.id} {task_type}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{task.id}",
                    spawn_fn=lambda: _spawn_background_iterate(iterate_args, config, task),
                )
                if rc != 0:
                    continue
                slots -= 1
                work_done = True
                started_task_ids.add(str(task.id))
                log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\"")
                continue

            if dry_run:
                log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\" [dry-run]")
                started_task_ids.add(str(task.id))
                slots -= 1
                work_done = True
                continue
            worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
            rc = _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task.id} {task_type}: worker spawn failed",
                dedupe_key=f"spawn-worker-failed:{task.id}",
                spawn_fn=lambda: _spawn_background_worker(worker_args, config, task_id=task.id, quiet=quiet),
            )
            if rc != 0:
                continue
            slots -= 1
            work_done = True
            started_task_ids.add(str(task.id))
            log.emit("START", f"{task.id} {task_type} \"{_short_prompt(task.prompt)}\"")

    pending_count = len(_pending_runnable_tasks(store, tags=tags, any_tag=any_tag))
    log.end_cycle()
    return _CycleResult(
        work_done=work_done,
        running=_count_live_workers(config, store),
        pending=pending_count,
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
    tags = list(getattr(args, "tags", None) or [])
    group = getattr(args, "group", None)
    if group:
        print("Warning: --group is deprecated; use --tag instead.", file=sys.stderr)
        tags.append(group)
    tag_filters = tuple(tags) if tags else None
    any_tag = bool(getattr(args, "any_tag", False))

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
    if config.watch.failure_backoff_initial < 1:
        print("Error: watch.failure_backoff_initial must be a positive integer")
        return 1
    if config.watch.failure_backoff_max < config.watch.failure_backoff_initial:
        print("Error: watch.failure_backoff_max must be >= watch.failure_backoff_initial")
        return 1
    if config.watch.failure_halt_after is not None and config.watch.failure_halt_after < 1:
        print("Error: watch.failure_halt_after must be null or a positive integer")
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
    failure_streak = 0
    previous_snapshot = _task_snapshot(store)

    # Preview first cycle and ask for confirmation before executing
    skip_confirm = dry_run or bool(getattr(args, "yes", False))
    if not skip_confirm:
        preview_result = _run_cycle(
            config=config,
            store=store,
            batch=batch,
            max_iterations=max_iterations,
            dry_run=True,
            quiet=False,
            log=log,
            tags=tag_filters,
            any_tag=any_tag,
        )
        if preview_result.work_done:
            try:
                answer = input("\nProceed? [y/N] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ""
            if answer not in ("y", "yes"):
                print("Aborted.")
                return 0

    try:
        while True:
            if stop_requested:
                break

            pre_cycle_snapshot = _task_snapshot(store)
            _emit_transition_events(
                previous_snapshot,
                pre_cycle_snapshot,
                store=store,
                config=config,
                log=log,
            )
            previous_snapshot = pre_cycle_snapshot

            cycle_result = _run_cycle(
                config=config,
                store=store,
                batch=batch,
                max_iterations=max_iterations,
                dry_run=dry_run,
                quiet=quiet,
                log=log,
                tags=tag_filters,
                any_tag=any_tag,
            )

            current_snapshot = _task_snapshot(store)
            _emit_transition_events(
                previous_snapshot,
                current_snapshot,
                store=store,
                config=config,
                log=log,
            )
            completed_ids = _collect_completed_transition_ids(
                previous_snapshot,
                current_snapshot,
                store=store,
                tags=tag_filters,
                any_tag=any_tag,
            )
            if completed_ids and failure_streak > 0:
                failure_streak = 0
                log.emit(
                    "INFO",
                    f"failure backoff reset after completion(s): {', '.join(completed_ids[:5])}",
                )
            unhandled_failures = _collect_unhandled_failures(
                previous_snapshot,
                current_snapshot,
                store=store,
                config=config,
                tags=tag_filters,
                any_tag=any_tag,
            )
            previous_snapshot = current_snapshot

            if unhandled_failures:
                failure_streak += len(unhandled_failures)
                backoff_seconds = _compute_failure_backoff_seconds(config, failure_streak)
                summary = ", ".join(
                    f"{failure.task_id}={failure.reason}" for failure in unhandled_failures[:3]
                )
                if len(unhandled_failures) > 3:
                    summary += ", ..."
                log.emit(
                    "BACKOFF",
                    (
                        f"{len(unhandled_failures)} non-auto-resumable failure(s); "
                        f"sleeping {backoff_seconds}s before starting more work "
                        f"(streak {failure_streak}"
                        + (f"; latest: {summary}" if summary else "")
                        + ")"
                    ),
                )
                halt_after = config.watch.failure_halt_after
                if halt_after is not None and failure_streak >= halt_after:
                    log.emit(
                        "INFO",
                        (
                            "failure halt threshold reached "
                            f"({failure_streak} consecutive non-auto-resumable failures >= {halt_after}); "
                            "stopping watch for human intervention"
                        ),
                    )
                    break
                if stop_requested:
                    break
                _sleep_interruptibly(backoff_seconds, lambda: stop_requested)
                continue

            if cycle_result.work_done:
                idle_seconds = 0
            else:
                idle_seconds += poll
                log.emit(
                    "IDLE",
                    f"sleeping {poll}s ({cycle_result.pending} pending, {cycle_result.running} running)",
                )
                if max_idle is not None and idle_seconds >= max_idle:
                    log.emit("INFO", f"max idle time reached ({max_idle}s), exiting")
                    break

            if stop_requested:
                break
            _sleep_interruptibly(poll, lambda: stop_requested)
    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    """Inspect and adjust pending queue urgency."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    service = TaskQueryService(store)
    action = getattr(args, "queue_action", None)
    tags = list(getattr(args, "tags", None) or [])
    group = getattr(args, "group", None)
    if group:
        print("Warning: --group is deprecated; use --tag instead.", file=sys.stderr)
        tags.append(group)
    tag_filters = tuple(tags) if tags else None
    any_tag = bool(getattr(args, "any_tag", False))

    if action in {"bump", "unbump", "move", "next", "clear"}:
        task_id = resolve_id(config, args.task_id)
        task = store.get(task_id)
        if task is None:
            print(f"Error: Task {task_id} not found")
            return 1
        if task.status != "pending":
            print(f"Error: Task {task_id} is not pending (status: {task.status})")
            return 1
        if task.task_type == "internal":
            print(f"Error: Task {task_id} is internal and not part of the runnable queue")
            return 1

        runnable_pending_ids = {
            str(row.task.id)
            for row in service.run(TaskQueryPresets.queue(limit=None, tags=tag_filters, any_tag=any_tag)).rows
            if isinstance(row, TaskRow) and row.task.id is not None
        }
        is_currently_runnable = str(task_id) in runnable_pending_ids

        if action in {"bump", "unbump"}:
            new_urgent = action == "bump"
            set_task_urgency(store, task_id, urgent=new_urgent)
            if new_urgent:
                if is_currently_runnable:
                    print(f"✓ Bumped task {task_id} to urgent queue")
                else:
                    print(f"✓ Bumped task {task_id} (not currently runnable; urgency will apply once runnable)")
            else:
                if is_currently_runnable:
                    print(f"✓ Removed task {task_id} from urgent queue")
                else:
                    print(f"✓ Removed urgent flag from task {task_id} (task is not currently runnable)")
            return 0

        if action == "clear":
            clear_task_queue_position(store, task_id)
            if is_currently_runnable:
                print(f"✓ Cleared explicit queue order for task {task_id}")
            else:
                print(f"✓ Cleared explicit queue order for task {task_id} (task is not currently runnable)")
            return 0

        position = 1 if action == "next" else int(args.position)
        if position < 1:
            print("Error: queue position must be >= 1")
            return 1
        set_task_queue_position(store, task_id, position=position)
        if position == 1:
            message = f"✓ Moved task {task_id} to queue position 1"
        else:
            message = f"✓ Moved task {task_id} to queue position {position}"
        if is_currently_runnable:
            print(message)
        else:
            print(f"{message} (task is not currently runnable; ordering will apply once runnable)")
        return 0

    pending = [
        row.task
        for row in service.run(TaskQueryPresets.queue(limit=None, tags=tag_filters, any_tag=any_tag)).rows
        if isinstance(row, TaskRow)
    ]
    if not pending:
        if tag_filters:
            print(f"No runnable tasks matching tags: {', '.join(tag_filters)}")
        else:
            print("No runnable tasks")
        return 0

    limit_arg = getattr(args, "limit", 10)
    show_all = bool(getattr(args, "all", False)) or limit_arg in {0, -1}
    display_limit = None if show_all else max(1, int(limit_arg))
    visible_pending = pending if display_limit is None else pending[:display_limit]

    for index, task in enumerate(visible_pending, start=1):
        lane = "urgent" if task.urgent else "normal"
        position_label = f"[#{task.queue_position}] " if task.queue_position is not None else ""
        print(f"{index:>3}  {task.id}  {position_label}[{lane}] [{task.task_type}] {_short_prompt(task.prompt)}")

    if display_limit is not None and len(pending) > display_limit:
        remaining = len(pending) - display_limit
        plural = "tasks" if remaining != 1 else "task"
        print(f"({remaining} more runnable {plural}; use -n 0, -n -1, or --all to show everything)")

    return 0
