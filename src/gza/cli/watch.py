"""Continuous watch loop and queue management commands."""

import argparse
import contextlib
import io
import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

from .. import lineage
from ..config import Config
from ..console import prompt_available_width, shorten_prompt
from ..db import SqliteTaskStore, Task as DbTask, task_id_numeric_key
from ..git import Git, GitError
from ..lineage_query import LineageOwnerQuery, LineageOwnerRow, query_lineage_owner_rows
from ..merge_state import resolve_task_merge_state_for_target
from ..pickup import get_runnable_pending_tasks, is_worker_consuming_advance_action
from ..recovery_engine import (
    FailedRecoveryDecision,
    decide_failed_task_recovery,
    should_hide_failed_recovery_decision,
)
from ..source_followup import collect_non_dropped_implement_source_ids
from ..sync_ops import reconcile_task_branch_merge_truth
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
    _create_retry_task,
    _prepare_task_for_immediate_execution,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    clear_task_queue_position_scoped,
    format_review_outcome,
    get_store,
    parse_cli_tag_filters,
    resolve_id,
    set_task_queue_position_scoped,
    set_task_urgency,
)
from .advance_engine import (
    NEEDS_ATTENTION_LABEL,
    classify_advance_action,
    determine_next_action,
    failed_recovery_decision_to_attention_action,
    format_needs_attention_entry_for_display,
)
from .advance_executor import (
    AdvanceActionExecutionContext,
    AdvanceActionExecutionResult,
    execute_advance_action,
    resolve_execution_needs_attention,
)
from .execution import _spawn_background_iterate
from .git_ops import (
    _collect_advance_completed_tasks as _git_ops_collect_advance_completed_tasks,
    _execute_merge_action,
    _merge_single_task as _git_ops_merge_single_task,
    _prepare_create_review_action,
    _require_default_branch,
    _unimplemented_implement_prompt,
    cleanup_failed_merge_checkout,
    ensure_watch_main_checkout,
)
from .query import _resolve_incomplete_owner_task

_WATCH_ADVANCE_ACTION_ORDER: dict[str, int] = {"merge": 0}
_WATCH_EVENT_LABEL_WIDTH = len("ATTENTION")
_WATCH_ITERATE_ROUTED_ACTIONS = frozenset({"create_review", "run_review", "improve", "run_improve"})
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


def _collect_advance_completed_tasks(
    store: SqliteTaskStore,
    *,
    advance_type: str | None = None,
    target_branch: str | None = None,
) -> tuple[list[DbTask], set[str]]:
    """Compatibility shim for tests patching watch-local task collection."""
    return _git_ops_collect_advance_completed_tasks(
        store,
        advance_type=advance_type,
        target_branch=target_branch,
    )


def _watch_skip_message(task: DbTask, action: dict) -> str:
    """Build a stable skip message for non-executed advance actions."""
    action_type = str(action.get("type", "skip"))
    description = str(action.get("description", "")).strip()
    if description.startswith("SKIP: "):
        description = description[len("SKIP: ") :]
    if not description:
        description = action_type.replace("_", " ")
    return f"{task.id}: {description}"


def _watch_needs_attention_message(task: DbTask, action: dict) -> str:
    return format_needs_attention_entry_for_display(task, action=action)


def _resolve_watch_attention_display_task(store: SqliteTaskStore, row: LineageOwnerRow) -> DbTask:
    """Match watch ATTENTION row identity to the same row-level owner selection as incomplete."""
    return _resolve_incomplete_owner_task(store, cast(Any, row))


def _failed_recovery_attention_action(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    decision: FailedRecoveryDecision,
    max_recovery_attempts: int,
) -> dict[str, object] | None:
    return failed_recovery_decision_to_attention_action(
        store,
        task,
        decision,
        max_recovery_attempts=max_recovery_attempts,
    )


def _format_recovery_report_subject(row: LineageOwnerRow, task: DbTask) -> str:
    owner_id = row.owner_task.id or "unknown"
    task_id = task.id or "unknown"
    subject_ids: list[str] = [owner_id]
    if task_id != owner_id:
        subject_ids.append(task_id)
    subject_ids.extend(
        failed_task.id
        for failed_task in row.unresolved_tasks
        if failed_task.id is not None and failed_task.status == "failed" and failed_task.id not in set(subject_ids)
    )
    return " ".join(subject_ids)


def _query_owner_rows(
    *,
    store: SqliteTaskStore,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    max_recovery_attempts: int,
    include_skipped: bool,
) -> list[LineageOwnerRow]:
    return list(
        query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                tags=tags,
                any_tag=any_tag,
                include_skipped=include_skipped,
                exclude_dropped_from_planning=True,
                max_recovery_attempts=max_recovery_attempts,
            ),
            config=config,
            git=git,
            target_branch=target_branch,
        )
    )


def _watch_iterate_result(
    *,
    action_type: str,
    status: Literal["skip", "error"],
    message: str,
    guarded_pending_task_id: str | None = None,
) -> AdvanceActionExecutionResult:
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status=status,
        message=message,
        guarded_pending_task_id=guarded_pending_task_id,
        worker_label="iterate",
    )


def _watch_iterate_impl_target(
    *,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    action: dict[str, object],
    running_task_ids: set[str],
    target_branch: str,
) -> DbTask | AdvanceActionExecutionResult | None:
    action_type = str(action.get("type", "skip"))
    if action_type not in _WATCH_ITERATE_ROUTED_ACTIONS:
        return None

    guarded_pending_task_id: str | None = None
    impl_task: DbTask | None = None

    if action_type in {"create_review", "improve"}:
        if task.task_type != "implement" or task.id is None:
            return None
        impl_task = task
    elif action_type == "run_review":
        review_task = action.get("review_task")
        if not isinstance(review_task, DbTask) or review_task.id is None:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message="missing review task",
            )
        guarded_pending_task_id = review_task.id
        if review_task.depends_on is None:
            return None
        impl_task = store.get(review_task.depends_on)
        if impl_task is None or impl_task.id is None:
            return _watch_iterate_result(
                action_type=action_type,
                status="error",
                message=f"review task {review_task.id} points to missing implementation {review_task.depends_on}",
                guarded_pending_task_id=guarded_pending_task_id,
            )
        if impl_task.task_type != "implement":
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=(
                    f"review task {review_task.id} points to non-implementation task "
                    f"{review_task.depends_on}"
                ),
                guarded_pending_task_id=guarded_pending_task_id,
            )
        if task.id is not None and impl_task.id != task.id:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=(
                    f"review task {review_task.id} resolves to {impl_task.id}, "
                    f"not completed task {task.id}"
                ),
                guarded_pending_task_id=guarded_pending_task_id,
            )
    else:
        improve_task = action.get("improve_task")
        if not isinstance(improve_task, DbTask) or improve_task.id is None:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message="missing improve task",
            )
        guarded_pending_task_id = improve_task.id
        impl_task, resolve_error = lineage.resolve_impl_task(store, improve_task.id)
        if impl_task is None:
            if "has no based_on implementation task" in str(resolve_error):
                return None
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=resolve_error or f"unable to resolve implementation for {improve_task.id}",
                guarded_pending_task_id=guarded_pending_task_id,
            )
        if task.id is not None and impl_task.id != task.id:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=(
                    f"improve task {improve_task.id} resolves to {impl_task.id}, "
                    f"not completed task {task.id}"
                ),
                guarded_pending_task_id=guarded_pending_task_id,
            )

    if impl_task is None or impl_task.id is None:
        return None
    if impl_task.task_type != "implement":
        return None
    if impl_task.status not in {"completed", "pending"}:
        return _watch_iterate_result(
            action_type=action_type,
            status="skip",
            message=(
                f"{impl_task.id}: iterate routing requires implementation status "
                f"completed or pending (found {impl_task.status})"
            ),
            guarded_pending_task_id=guarded_pending_task_id,
        )
    if (
        resolve_task_merge_state_for_target(
            store=store,
            task=impl_task,
            git=git,
            target_branch=target_branch,
        )
        == "merged"
    ):
        return _watch_iterate_result(
            action_type=action_type,
            status="skip",
            message=f"{impl_task.id}: implementation chain already merged; not starting iterate",
            guarded_pending_task_id=guarded_pending_task_id,
        )
    if impl_task.id in running_task_ids:
        return _watch_iterate_result(
            action_type=action_type,
            status="skip",
            message=f"{impl_task.id}: iterate already running for implementation chain",
            guarded_pending_task_id=guarded_pending_task_id,
        )
    return impl_task


@dataclass(frozen=True)
class _IsolatedMergeFailureAssessment:
    is_conflict: bool
    reason: str | None = None


def _assess_isolated_merge_failure(
    merge_git: Git,
    branch: str,
    target_branch: str,
) -> _IsolatedMergeFailureAssessment:
    """Classify whether an isolated merge failure is a real merge conflict."""
    if not merge_git.branch_exists(branch):
        return _IsolatedMergeFailureAssessment(False, "branch missing")
    if merge_git.is_merged(branch, target_branch):
        return _IsolatedMergeFailureAssessment(False, "branch already merged")
    if merge_git.can_merge(branch, target_branch):
        return _IsolatedMergeFailureAssessment(False, "no merge conflict detected")
    return _IsolatedMergeFailureAssessment(True)


def _format_prompt_for_width(prompt: str, *, prefix: int = 0, suffix: int = 0) -> str:
    available = prompt_available_width(prefix=prefix, suffix=suffix)
    return shorten_prompt(prompt, available)


def _format_hms() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


def _format_scope_message(tags: tuple[str, ...] | None, *, any_tag: bool) -> str | None:
    """Return a stable watch-scope message when tag filtering is active."""
    if not tags:
        return None
    mode = "any" if any_tag else "all"
    return f"scope: tags={','.join(tags)} mode={mode}"


def _format_queue_scope_error(task_id: str, tags: tuple[str, ...], *, any_tag: bool) -> str:
    """Return consistent fail-closed messaging for queue ordering scope mismatch."""
    mode = "any" if any_tag else "all"
    return (
        f"Error: Task {task_id} does not match tag scope ({mode}: {', '.join(tags)}); "
        "queue ordering was not changed"
    )


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
            SELECT id, status, task_type, started_at, completed_at, failure_reason, completion_reason, depends_on
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
                "completion_reason": row["completion_reason"],
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
        self._sticky_attention_prev_cycle: dict[str, str] = {}
        self._sticky_attention_this_cycle: dict[str, str] = {}

    def begin_cycle(self) -> None:
        if self._has_emitted_cycle:
            with open(self.path, "a") as f:
                f.write("\n")
            if not self.quiet:
                print(flush=True)
        self._skip_keys_this_cycle.clear()
        self._sticky_attention_this_cycle.clear()
        self._has_emitted_cycle = True

    def end_cycle(self) -> None:
        self._skip_keys_prev_cycle = set(self._skip_keys_this_cycle)
        self._sticky_attention_prev_cycle = dict(self._sticky_attention_this_cycle)

    def emit_attention(self, *, attention_key: str, message: str) -> None:
        previous_message = self._sticky_attention_this_cycle.get(attention_key)
        if previous_message == message:
            return
        self._sticky_attention_this_cycle[attention_key] = message
        self.emit("ATTENTION", message)

    def emit(self, event: str, message: str, *, dedupe_key: str | None = None) -> None:
        if event == "SKIP" and dedupe_key is not None:
            self._skip_keys_this_cycle.add(dedupe_key)
            if dedupe_key in self._skip_keys_prev_cycle:
                return
        prefix = f"{_format_hms()} {event:<{_WATCH_EVENT_LABEL_WIDTH}} "
        continuation_prefix = " " * len(prefix)
        parts = message.splitlines() or [""]
        line = "\n".join(
            (prefix if idx == 0 else continuation_prefix) + part.rstrip()
            for idx, part in enumerate(parts)
        ).rstrip()
        with open(self.path, "a") as f:
            f.write(line + "\n")
        if not self.quiet:
            print(line, flush=True)


def _emit_transition_events(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    config: Config,
    log: _WatchLog,
    restart_failed_mode: bool = False,
    max_recovery_attempts: int = 1,
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
            completion_reason = new_row.get("completion_reason")
            reason_suffix = f": {completion_reason}" if completion_reason else ""
            if task_type == "review":
                task = store.get(task_id)
                impl_id = new_row.get("depends_on") or "unknown"
                verdict = (
                    format_review_outcome(config, task)
                    if task is not None
                    else "UNKNOWN"
                )
                log.emit("REVIEW", f"{task_id} for {impl_id}: {verdict}{reason_suffix}")
            else:
                log.emit("DONE", f"{task_id} {task_type}{reason_suffix}{elapsed_suffix}")
        elif new_status == "failed":
            reason = new_row.get("failure_reason") or "UNKNOWN"
            task = store.get(task_id)
            if restart_failed_mode and task is not None:
                decision = decide_failed_task_recovery(
                    store,
                    task,
                    max_recovery_attempts=max_recovery_attempts,
                )
                if should_hide_failed_recovery_decision(decision):
                    continue
            log.emit("FAIL", f"{task_id} {task_type}: {reason}{elapsed_suffix}")


def _count_live_workers(config: Config, store: SqliteTaskStore) -> int:
    live_pids, _, _ = _collect_live_running_state(config, store)
    return len(live_pids)


def _collect_live_running_state(config: Config, store: SqliteTaskStore) -> tuple[set[int], list[str], int]:
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
        if not registry.is_running(worker.worker_id):
            continue
        if worker.task_id is not None:
            task_id = str(worker.task_id)
            task_status = active_task_statuses.get(task_id)
            if task_status is None:
                task = store.get(task_id)
                task_status = task.status if task is not None else None
                if task_status is not None:
                    active_task_statuses[task_id] = task_status
            if task_status not in {"pending", "in_progress"}:
                continue
            if worker.pid > 0:
                live_pids.add(worker.pid)
        elif worker.pid > 0:
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

    running_task_ids = sorted(live_task_ids, key=lambda task_id: task_id_numeric_key(task_id))
    anonymous_worker_count = max(0, len(live_pids) - len(running_task_ids))
    return live_pids, running_task_ids, anonymous_worker_count


def _format_wake_message(
    *,
    running: int,
    pending: int,
    slots: int,
    running_task_ids: list[str],
    anonymous_worker_count: int = 0,
) -> str:
    message = f"checking... ({running} running, {pending} pending, {slots} slots)"
    if running_task_ids or anonymous_worker_count > 0:
        worker_lines = ["live workers:"]
        worker_lines.extend(f"- {task_id}" for task_id in running_task_ids)
        if anonymous_worker_count == 1:
            worker_lines.append("- 1 worker without an active task id")
        elif anonymous_worker_count > 1:
            worker_lines.append(f"- {anonymous_worker_count} workers without active task ids")
        message += "\n" + "\n".join(worker_lines)
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


@dataclass
class _RecoveryReport:
    actionable_count: int
    resume_count: int
    retry_count: int


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
    config: Config | None = None,
    max_recovery_attempts: int = 1,
    restart_failed_mode: bool = False,
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
        task = store.get(task_id)
        if task is not None:
            decision = decide_failed_task_recovery(
                store,
                task,
                max_recovery_attempts=max_recovery_attempts,
            )
            if decision.action in {"resume", "retry"} or should_hide_failed_recovery_decision(decision):
                continue
        failures.append(
            _ObservedFailure(
                task_id=task_id,
                task_type=new_row.get("task_type") or "implement",
                reason=reason,
            )
        )
    return failures


def _emit_recovery_dry_run_report(
    *,
    store: SqliteTaskStore,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    max_recovery_attempts: int,
    show_skipped: bool = False,
) -> _RecoveryReport:
    failed_rows = [
        row
        for row in _query_owner_rows(
            store=store,
            tags=tags,
            any_tag=any_tag,
            max_recovery_attempts=max_recovery_attempts,
            include_skipped=True,
        )
        if (
            row.recovery_leaf_task is not None
            and row.recovery_action_task is not None
            and row.recovery_action_task.id == row.recovery_leaf_task.id
        )
    ]
    failed_rows.sort(
        key=lambda row: (
            row.recovery_leaf_task.created_at if row.recovery_leaf_task and row.recovery_leaf_task.created_at else datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(row.owner_task.id),
        )
    )
    scope = ",".join(tags) if tags else "*"
    print(f"Failed recovery plan (tags={scope}, mode=restart-failed)")
    print()
    actionable = resume = retry = 0
    attention_rows: list[tuple[DbTask, dict[str, object]]] = []
    skipped = 0
    hidden_skipped = 0
    for row in failed_rows:
        task = row.recovery_leaf_task
        assert task is not None
        if task.id is None:
            continue
        decision = decide_failed_task_recovery(store, task, max_recovery_attempts=max_recovery_attempts)
        if decision.action in {"resume", "retry"}:
            launch = decision.launch_mode
            print(
                f"{decision.action:<6} {_format_recovery_report_subject(row, task)} {task.task_type:<9} via {launch:<7} reason={decision.reason_code} "
                f"attempt={decision.attempt_index}/{decision.attempt_limit}"
            )
            actionable += 1
            if decision.action == "resume":
                resume += 1
            if decision.action == "retry":
                retry += 1
            continue
        attention_action = _failed_recovery_attention_action(
            store=store,
            task=task,
            decision=decision,
            max_recovery_attempts=max_recovery_attempts,
        )
        if attention_action is not None:
            attention_rows.append((task, attention_action))
            continue
        skipped += 1
        if show_skipped:
            launch = decision.launch_mode
            print(
                f"{decision.action:<6} {_format_recovery_report_subject(row, task)} {task.task_type:<9} via {launch:<7} reason={decision.reason_code} "
                f"attempt={decision.attempt_index}/{decision.attempt_limit}"
            )
        else:
            hidden_skipped += 1
    if attention_rows:
        print()
        print(f"{NEEDS_ATTENTION_LABEL} ({len(attention_rows)} task{'s' if len(attention_rows) != 1 else ''}):")
        for task, action in attention_rows:
            print(f"  {_watch_needs_attention_message(task, action)}")
    print()
    skipped_summary = skipped if show_skipped else hidden_skipped
    if show_skipped:
        print(
            f"Summary: {actionable} actionable ({resume} resume, {retry} retry), "
            f"{len(attention_rows)} needs attention, {skipped_summary} skipped"
        )
    else:
        print(
            f"Summary: {actionable} actionable ({resume} resume, {retry} retry), "
            f"{len(attention_rows)} needs attention, {skipped_summary} skipped hidden"
        )
    return _RecoveryReport(actionable_count=actionable, resume_count=resume, retry_count=retry)


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
    any_tag: bool = False,
    quiet: bool = False,
    restart_failed: bool = False,
    restart_failed_batch: int = 1,
    max_recovery_attempts: int = 1,
    show_skipped: bool = False,
) -> _CycleResult:
    from ._common import prune_terminal_dead_workers, reconcile_in_progress_tasks

    tags = normalize_tag_filters(tags)

    log.begin_cycle()
    if not dry_run:
        reconcile_in_progress_tasks(config)
        prune_terminal_dead_workers(config)

    live_pids, running_task_ids, anonymous_worker_count = _collect_live_running_state(config, store)
    running_task_id_set = set(running_task_ids)
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
            anonymous_worker_count=anonymous_worker_count,
        ),
    )
    scope_message = _format_scope_message(tags, any_tag=any_tag)
    if scope_message is not None:
        log.emit("INFO", scope_message)

    # 1) Execute advance actions for completed tasks (includes completed plans
    # with no implement child, aligned with gza advance).
    # Merges run first; worker-spawning actions consume available slots.
    isolation_enabled = bool(getattr(config, "main_checkout_isolate", False))
    git = Git(config.project_dir)
    target_branch = git.default_branch()
    impl_based_on_ids = collect_non_dropped_implement_source_ids(store.get_all())
    owner_rows = _query_owner_rows(
        store=store,
        config=config,
        git=git,
        target_branch=target_branch,
        tags=tags,
        any_tag=any_tag,
        max_recovery_attempts=max_recovery_attempts,
        include_skipped=True,
    )
    lifecycle_rows = [
        row
        for row in owner_rows
        if row.lifecycle_action_task is not None and row.lifecycle_action_task.status != "failed"
    ]
    recovery_rows = [row for row in owner_rows if row.recovery_leaf_task is not None]
    recovery_rows = [
        row
        for row in recovery_rows
        if (
            row.recovery_action_task is not None
            and row.recovery_leaf_task is not None
            and row.recovery_action_task.id == row.recovery_leaf_task.id
        )
    ]
    recovery_rows.sort(
        key=lambda row: (
            row.recovery_leaf_task.created_at if row.recovery_leaf_task and row.recovery_leaf_task.created_at else datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(row.owner_task.id),
        )
    )
    if lifecycle_rows:
        current_branch = git.current_branch()
        merge_git: Git | None = None
        merge_actions_available = True
        merge_skip_reason = "merge-not-default-branch"
        can_merge = True

        def _rebuild_isolated_checkout() -> bool:
            nonlocal merge_git, can_merge, merge_skip_reason, merge_actions_available
            try:
                merge_git = _run_with_optional_stdout_suppressed(
                    quiet,
                    lambda: ensure_watch_main_checkout(config, git, target_branch, rebuild=True),
                )
                merge_actions_available = True
                can_merge = True
                merge_skip_reason = "merge-not-default-branch"
                log.emit("INFO", "isolated merge checkout rebuilt")
                return True
            except GitError as exc:
                merge_git = None
                merge_actions_available = False
                can_merge = False
                merge_skip_reason = "merge-isolated-checkout-unavailable"
                log.emit("ERROR", f"isolated merge checkout rebuild failed: {exc}")
                return False

        if isolation_enabled and not dry_run:
            try:
                merge_git = _run_with_optional_stdout_suppressed(
                    quiet,
                    lambda: ensure_watch_main_checkout(config, git, target_branch),
                )
            except GitError as exc:
                log.emit(
                    "WARN",
                    f"isolated merge checkout refresh failed; rebuilding: {exc}",
                )
                _rebuild_isolated_checkout()

        action_plan: list[tuple[LineageOwnerRow, DbTask, dict]] = []
        for row in lifecycle_rows:
            task = row.lifecycle_action_task or row.owner_task
            action_plan.append(
                (
                    row,
                    task,
                    determine_next_action(
                        config,
                        store,
                        git,
                        task,
                        target_branch,
                        impl_based_on_ids=impl_based_on_ids,
                    ),
                )
        )
        action_plan.sort(
            key=lambda item: (
                _WATCH_ADVANCE_ACTION_ORDER.get(item[2].get("type", ""), 1),
                1 if item[1].task_type in {"plan", "explore"} else 0,
            )
        )
        has_merge_action = any(action.get("type") in {"merge", "merge_with_followups"} for _, _, action in action_plan)
        can_merge = merge_actions_available
        if has_merge_action:
            if isolation_enabled:
                if dry_run:
                    can_merge = True
                else:
                    can_merge = merge_git is not None
            else:
                can_merge = _run_with_optional_stdout_suppressed(
                    quiet,
                    lambda: _require_default_branch(git, current_branch, "merge"),
                )

        worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)

        def _watch_spawn_worker(task_obj: DbTask, task_kind: str) -> int:
            assert task_obj.id is not None
            task_id = str(task_obj.id)
            return _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task_id} {task_kind}: worker spawn failed",
                dedupe_key=f"spawn-worker-failed:{task_id}",
                spawn_fn=lambda: _spawn_background_worker(
                    worker_args,
                    config,
                    task_id=task_id,
                    quiet=quiet,
                    prepared_task=task_obj,
                ),
            )

        def _watch_spawn_resume_worker(task_obj: DbTask, task_kind: str) -> int:
            assert task_obj.id is not None
            task_id = str(task_obj.id)
            return _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task_id} {task_kind}: resume worker spawn failed",
                dedupe_key=f"spawn-resume-failed:{task_id}",
                spawn_fn=lambda: _spawn_background_resume_worker(
                    worker_args,
                    config,
                    new_task_id=task_id,
                    quiet=quiet,
                    prepared_task=task_obj,
                ),
            )

        def _watch_spawn_iterate(task_obj: DbTask, task_kind: str) -> int:
            iterate_args = argparse.Namespace(
                max_iterations=max_iterations,
                no_docker=False,
                resume=False,
                retry=False,
                auto_iterate=True,
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
            max_resume_attempts=max_recovery_attempts,
            use_iterate_for_create_implement=True,
            use_iterate_for_needs_rebase=False,
            prepare_task_for_background_start=lambda task, rollback_on_failure: _prepare_task_for_immediate_execution(
                config,
                task,
                rollback_on_failure=rollback_on_failure,
            ),
            prepare_create_review=lambda t: _prepare_create_review_action(store, t),
            create_resume_task=lambda t: _create_resume_task(store, t),
            create_rebase_task=_create_rebase_from_task,
            create_implement_task=_create_implement_from_task,
            spawn_worker=_watch_spawn_worker,
            spawn_resume_worker=_watch_spawn_resume_worker,
            spawn_iterate_worker=_watch_spawn_iterate,
            spawn_iterate_recovery=lambda task_obj, mode, prepared_task: _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task_obj.id} {mode}: iterate worker spawn failed",
                dedupe_key=f"spawn-iterate-failed:{task_obj.id}:{mode}",
                spawn_fn=lambda: _spawn_background_iterate(
                    argparse.Namespace(
                        max_iterations=max_iterations,
                        no_docker=False,
                        resume=mode == "resume",
                        retry=mode == "retry",
                        auto_iterate=True,
                    ),
                    config,
                    task_obj,
                    prepared_task_id=str(prepared_task.id),
                    prepared_resume=mode == "resume",
                    prepared_phase="preloop",
                ),
            ),
            prefer_iterate_for_action=lambda task, action: _watch_iterate_impl_target(
                store=store,
                git=git,
                task=task,
                action=action,
                running_task_ids=running_task_id_set,
                target_branch=target_branch,
            ),
        )

        for row, task, action in action_plan:
            display_task = row.owner_task
            action_type = action.get("type")
            if classify_advance_action(action) == "needs_attention":
                display_task = _resolve_watch_attention_display_task(store, row)
                # Lineage-progress attention comes from the advance action plan only.
                log.emit_attention(
                    attention_key=f"advance-attention:{display_task.id}:{action_type}",
                    message=_watch_needs_attention_message(display_task, action),
                )
                continue

            if classify_advance_action(action) == "skip":
                log.emit(
                    "SKIP",
                    _watch_skip_message(display_task, action),
                    dedupe_key=f"advance-skip:{action_type}:{display_task.id}",
                )
                continue

            if action_type in {"merge", "merge_with_followups"}:
                if not can_merge:
                    if isolation_enabled and merge_skip_reason == "merge-isolated-checkout-unavailable":
                        log.emit(
                            "SKIP",
                            "merge actions skipped: isolated checkout unavailable",
                            dedupe_key="merge-isolated-checkout-unavailable",
                        )
                        continue
                    log.emit(
                        "SKIP",
                        "merge actions skipped: not on default branch",
                        dedupe_key="merge-not-default-branch",
                    )
                    continue
                if dry_run:
                    log.emit("MERGE", f"{display_task.id} -> {target_branch} [dry-run]")
                    work_done = True
                    continue
                merge_execution_git = merge_git if (isolation_enabled and merge_git is not None) else git
                merge_execution_branch = target_branch if isolation_enabled else current_branch
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
                        merge_git=merge_execution_git,
                        merge_current_branch=merge_execution_branch,
                        already_merged_behavior="mark_merged",
                    ),
                )
                rc = merge_result.rc
                for followup_task in merge_result.created_followups:
                    log.emit("FOLLOW", f"{followup_task.id} created from {display_task.id}")
                for followup_task in merge_result.reused_followups:
                    log.emit("FOLLOW", f"{followup_task.id} reused from {display_task.id}")
                if rc == 0:
                    if getattr(merge_result, "status", "merged") == "already_merged":
                        log.emit("INFO", f"{display_task.id} already merged into {target_branch}; marked merged")
                    else:
                        log.emit("MERGE", f"{display_task.id} -> {target_branch}")
                    work_done = True
                else:
                    conflict_handled = False
                    conflict_assessment: _IsolatedMergeFailureAssessment | None = None
                    if isolation_enabled and task.branch is not None:
                        conflict_assessment = _assess_isolated_merge_failure(
                            merge_execution_git,
                            task.branch,
                            target_branch,
                        )
                        if conflict_assessment.is_conflict:
                            conflict_handled = True
                            try:
                                cleanup_failed_merge_checkout(merge_execution_git)
                            except GitError as cleanup_error:
                                log.emit(
                                    "WARN",
                                    (
                                        f"{display_task.id}: isolated checkout cleanup failed after conflict: "
                                        f"{cleanup_error}"
                                    ),
                                )
                                _rebuild_isolated_checkout()
                            try:
                                rebase_task = _create_rebase_from_task(task)
                            except Exception as rebase_error:
                                log.emit("ERROR", f"{display_task.id}: failed to create rebase task ({rebase_error})")
                                continue
                            assert rebase_task.id is not None
                            prepared_rebase_task = _prepare_task_for_immediate_execution(
                                config,
                                rebase_task,
                                rollback_on_failure=True,
                            )
                            if prepared_rebase_task is None:
                                log.emit(
                                    "ERROR",
                                    f"{display_task.id}: failed to prepare merge-conflict rebase task {rebase_task.id}",
                                )
                                continue
                            step1_handled_child_task_ids.add(str(rebase_task.id))
                            work_done = True
                            if slots > 0:
                                rebase_rc = _watch_spawn_worker(prepared_rebase_task, "rebase")
                                if rebase_rc == 0:
                                    log.emit("START", f"{prepared_rebase_task.id} rebase")
                                    started_task_ids.add(str(prepared_rebase_task.id))
                                    slots -= 1
                                else:
                                    log.emit(
                                        "SKIP",
                                        f"{display_task.id}: merge conflict rebase worker spawn failed",
                                        dedupe_key=f"merge-conflict-rebase-spawn-failed:{display_task.id}",
                                    )
                            else:
                                log.emit(
                                    "SKIP",
                                    f"{display_task.id}: merge conflict queued rebase {rebase_task.id} (no free slots)",
                                    dedupe_key=f"merge-conflict-rebase-queued:{display_task.id}",
                                )
                            log.emit(
                                "SKIP",
                                f"{display_task.id}: merge conflict routed to rebase",
                                dedupe_key=f"merge-conflict:{display_task.id}",
                            )
                    if conflict_handled:
                        continue
                    if (
                        conflict_assessment is not None
                        and conflict_assessment.reason == "branch already merged"
                        and task.id is not None
                    ):
                        repaired = reconcile_task_branch_merge_truth(
                            store,
                            git,
                            str(task.id),
                            target_branch=target_branch,
                            include_diff_stats=True,
                            persist=True,
                        )
                        if repaired.ok and repaired.merge_status == "merged":
                            log.emit(
                                "REPAIR",
                                f"{display_task.id}: marked merged after shared reconciliation against {target_branch}",
                            )
                            work_done = True
                            continue
                    if conflict_assessment is not None and conflict_assessment.reason is not None:
                        log.emit(
                            "SKIP",
                            (
                                f"{display_task.id}: merge failed ({conflict_assessment.reason}); "
                                "not routing to rebase"
                            ),
                            dedupe_key=f"merge-failed-non-conflict:{display_task.id}",
                        )
                        continue
                    log.emit(
                        "SKIP",
                        f"{display_task.id}: merge failed",
                        dedupe_key=f"merge-failed:{display_task.id}",
                    )
                continue

            if not is_worker_consuming_advance_action(str(action_type)) or action_type == "resume":
                continue
            if slots <= 0:
                continue

            exec_result = execute_advance_action(task=task, action=action, context=executor_context)
            child_id = exec_result.handled_task_id
            guarded_pending_task_id = exec_result.guarded_pending_task_id

            if exec_result.status == "skip":
                if guarded_pending_task_id is not None:
                    step1_handled_child_task_ids.add(str(guarded_pending_task_id))
                message = exec_result.message
                if action_type == "improve" and display_task.id is not None:
                    message = f"{display_task.id}: {message}"
                attention = resolve_execution_needs_attention(task, exec_result)
                if attention is not None and display_task.id is not None:
                    display_task = _resolve_watch_attention_display_task(store, row)
                    # Orthogonal to advance-plan classification: the action tried to run
                    # and the execution layer reported a worker/startup attention state.
                    log.emit_attention(
                        attention_key=f"advance-attention:{display_task.id}:{attention.action['type']}",
                        message=_watch_needs_attention_message(display_task, attention.action),
                    )
                    continue
                log.emit(
                    "SKIP",
                    message,
                    dedupe_key=f"advance-worker-skip:{action_type}:{display_task.id}:{message}",
                )
                continue

            if exec_result.status == "error":
                if guarded_pending_task_id is not None:
                    step1_handled_child_task_ids.add(str(guarded_pending_task_id))
                if not exec_result.attempted_spawn and display_task.id is not None:
                    log.emit(
                        "ERROR",
                        f"{display_task.id}: {exec_result.message}",
                        dedupe_key=f"advance-worker-error:{action_type}:{display_task.id}:{exec_result.message}",
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
                if guarded_pending_task_id is not None:
                    step1_handled_child_task_ids.add(str(guarded_pending_task_id))
                if exec_result.worker_label == "iterate" and child_id is not None:
                    log.emit("START", f"{child_id} iterate [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "create_review" and display_task.id is not None:
                    log.emit("START", f"(new) review for {display_task.id} [dry-run]")
                elif action_type == "run_review" and child_id is not None:
                    log.emit("START", f"{child_id} review [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "improve":
                    failed_id = exec_result.failed_improve.id if exec_result.failed_improve is not None else None
                    if exec_result.improve_mode == "resume" and failed_id is not None:
                        log.emit("START", f"(resume) improve for {failed_id} [dry-run]")
                    elif exec_result.improve_mode == "retry" and failed_id is not None:
                        log.emit("START", f"(retry) improve for {failed_id} [dry-run]")
                    elif display_task.id is not None:
                        log.emit("START", f"(new) improve for {display_task.id} [dry-run]")
                elif action_type == "run_improve" and child_id is not None:
                    log.emit("START", f"{child_id} improve [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "create_implement" and display_task.id is not None:
                    log.emit("START", f"(new) implement for {display_task.id} [dry-run]")
                elif action_type == "needs_rebase" and display_task.id is not None:
                    log.emit("START", f"(new) rebase for {display_task.id} [dry-run]")
                slots -= 1
                work_done = True
                continue

            if (
                child_id is not None
                and exec_result.worker_label != "iterate"
                and action_type in {"create_review", "improve", "create_implement", "needs_rebase"}
            ):
                step1_handled_child_task_ids.add(str(child_id))
            if guarded_pending_task_id is not None:
                step1_handled_child_task_ids.add(str(guarded_pending_task_id))

            if exec_result.status == "success" and child_id is not None:
                if exec_result.worker_label == "iterate":
                    log.emit("START", f"{child_id} iterate")
                elif action_type in {"create_review", "run_review"}:
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

    # 2) Recovery queue for failed tasks.
    pending_recovery_task_ids: set[str] = set()
    if restart_failed:
        log.emit("PHASE", "recovery queue enabled (--restart-failed)")
    failed_decisions: list[tuple[DbTask, FailedRecoveryDecision]] = []
    actionable_failed: list[tuple[LineageOwnerRow, DbTask, FailedRecoveryDecision]] = []
    for row in recovery_rows:
        failed = row.recovery_leaf_task
        assert failed is not None
        if failed.id is None:
            continue
        decision = decide_failed_task_recovery(store, failed, max_recovery_attempts=max_recovery_attempts)
        failed_decisions.append((failed, decision))
        if decision.action == "skip":
            if restart_failed and show_skipped:
                log.emit(
                    "SKIP",
                    f"{row.owner_task.id} failed {failed.task_type}: {decision.reason_code}",
                    dedupe_key=f"recovery-skip:{row.owner_task.id}:{decision.reason_code}",
                )
            continue
        if decision.recovery_task_id is not None:
            pending_recovery_task_ids.add(decision.recovery_task_id)
        actionable_failed.append((row, failed, decision))

    pending_tasks = _pending_runnable_tasks(store, tags=tags, any_tag=any_tag)
    pending_candidates = [
        task
        for task in pending_tasks
        if task.id is not None
        and str(task.id) not in started_task_ids
        and str(task.id) not in pending_recovery_task_ids
        and str(task.id) not in step1_handled_child_task_ids
    ]
    if restart_failed:
        recovery_slots = max(0, min(slots, restart_failed_batch))
    else:
        # Plain watch keeps pending pickup ahead of failed recovery while still
        # sharing the same bounded recovery policy with restart-failed mode.
        reserved_for_pending = min(slots, len(pending_candidates))
        recovery_slots = max(0, slots - reserved_for_pending)
    started_recovery_task_ids: set[str] = set()
    launched_recovery_count = 0
    for row, failed, decision in actionable_failed:
        if recovery_slots <= 0:
            break
        if failed.id is None:
            continue
        if decision.action == "resume":
            if dry_run:
                destination = decision.recovery_task_id or "(new task)"
                log.emit(
                    "RECOVR",
                    (
                        f"{failed.id} resume via {decision.launch_mode} -> {destination} "
                        f"[owner={row.owner_task.id}] "
                        f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit}) [dry-run]"
                    ),
                )
                slots -= 1
                recovery_slots -= 1
                work_done = True
                launched_recovery_count += 1
                continue
            if decision.launch_mode == "worker":
                if decision.reuse_existing:
                    assert decision.recovery_task_id is not None
                    recovered_task_id = decision.recovery_task_id
                    recovered_task = store.get(recovered_task_id)
                    assert recovered_task is not None
                else:
                    recovered_task = _create_resume_task(store, failed)
                    assert recovered_task.id is not None
                    recovered_task_id = str(recovered_task.id)
                prepared_recovered_task = _prepare_task_for_immediate_execution(
                    config,
                    recovered_task,
                    rollback_on_failure=not decision.reuse_existing,
                )
                if prepared_recovered_task is None:
                    continue
                pending_recovery_task_ids.add(recovered_task_id)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: resume worker spawn failed",
                    dedupe_key=f"spawn-resume-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_resume_worker(
                        argparse.Namespace(no_docker=False, max_turns=None),
                        config,
                        recovered_task_id,
                        quiet=quiet,
                        prepared_task=prepared_recovered_task,
                    ),
                )
            else:
                if decision.reuse_existing:
                    assert decision.recovery_task_id is not None
                    recovered_task = store.get(decision.recovery_task_id)
                    assert recovered_task is not None
                else:
                    recovered_task = _create_resume_task(store, failed)
                prepared_recovered_task = _prepare_task_for_immediate_execution(
                    config,
                    recovered_task,
                    rollback_on_failure=not decision.reuse_existing,
                )
                if prepared_recovered_task is None:
                    continue
                recovered_task_id = str(prepared_recovered_task.id)
                pending_recovery_task_ids.add(recovered_task_id)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_iterate(
                        argparse.Namespace(
                            max_iterations=max_iterations,
                            no_docker=False,
                            resume=True,
                            retry=False,
                            auto_iterate=True,
                        ),
                        config,
                        failed,
                        prepared_task_id=recovered_task_id,
                        prepared_resume=True,
                        prepared_phase="preloop",
                    ),
                )
        else:
            if dry_run:
                destination = decision.recovery_task_id or "(new task)"
                log.emit(
                    "RECOVR",
                    (
                        f"{failed.id} retry via {decision.launch_mode} -> {destination} "
                        f"[owner={row.owner_task.id}] "
                        f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit}) [dry-run]"
                    ),
                )
                slots -= 1
                recovery_slots -= 1
                work_done = True
                launched_recovery_count += 1
                continue
            if decision.reuse_existing:
                assert decision.recovery_task_id is not None
                recovered_task_id = decision.recovery_task_id
                existing_recovered_task = store.get(recovered_task_id)
                assert existing_recovered_task is not None
                recovered_task = existing_recovered_task
            else:
                recovered_task = _create_retry_task(store, failed, automatic_recovery=True)
                assert recovered_task.id is not None
                recovered_task_id = str(recovered_task.id)
            prepared_recovered_task = _prepare_task_for_immediate_execution(
                config,
                recovered_task,
                rollback_on_failure=not decision.reuse_existing,
            )
            if prepared_recovered_task is None:
                continue
            recovered_task_id = str(prepared_recovered_task.id)
            pending_recovery_task_ids.add(recovered_task_id)
            rc = (
                _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: worker spawn failed",
                    dedupe_key=f"spawn-worker-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_worker(
                        argparse.Namespace(no_docker=False, max_turns=None, resume=False),
                        config,
                        task_id=recovered_task_id,
                        quiet=quiet,
                        prepared_task=prepared_recovered_task,
                    ),
                )
                if decision.launch_mode == "worker"
                else _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_iterate(
                        argparse.Namespace(
                            max_iterations=max_iterations,
                            no_docker=False,
                            resume=False,
                            retry=False,
                            auto_iterate=True,
                        ),
                        config,
                        failed,
                        prepared_task_id=recovered_task_id,
                        prepared_resume=False,
                        prepared_phase="preloop",
                    ),
                )
            )

        if rc != 0:
            continue
        started_task_ids.add(recovered_task_id)
        started_recovery_task_ids.add(recovered_task_id)
        slots -= 1
        recovery_slots -= 1
        work_done = True
        launched_recovery_count += 1
        log.emit(
            "RECOVR",
            (
                f"{failed.id} {decision.action} via {decision.launch_mode} -> {recovered_task_id} "
                f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit})"
            ),
        )

    recovery_phase_active = restart_failed and (
        launched_recovery_count > 0
        or len(actionable_failed) > launched_recovery_count
        or any(
            decision.reason_code == "recovery_already_running"
            for _failed, decision in failed_decisions
        )
    )
    if restart_failed and not recovery_phase_active:
        log.emit("PHASE", "recovery queue exhausted; switching to pending queue")

    # 3) Start new queued tasks (consumes slots)
    if not recovery_phase_active:
        log.emit("PHASE", "pending queue active")
    if slots > 0 and not recovery_phase_active:
        for task in pending_tasks:
            if slots <= 0:
                break
            assert task.id is not None
            if str(task.id) in started_task_ids:
                continue
            if restart_failed and str(task.id) in pending_recovery_task_ids:
                continue
            if str(task.id) in step1_handled_child_task_ids:
                continue
            task_type = task.task_type or "implement"
            if task_type == "implement":
                if dry_run:
                    dry_run_prompt = _format_prompt_for_width(
                        task.prompt,
                        prefix=16 + len(f"{task.id} {task_type} \""),
                        suffix=len('" [dry-run]'),
                    )
                    log.emit("START", f"{task.id} {task_type} \"{dry_run_prompt}\" [dry-run]")
                    started_task_ids.add(str(task.id))
                    slots -= 1
                    work_done = True
                    continue
                iterate_args = argparse.Namespace(
                    max_iterations=max_iterations,
                    no_docker=False,
                    resume=False,
                    retry=False,
                    auto_iterate=True,
                )
                prepared_pending_task = _prepare_task_for_immediate_execution(
                    config,
                    task,
                    rollback_on_failure=False,
                )
                if prepared_pending_task is None:
                    log.emit(
                        "START_FAILED",
                        f"{task.id} {task_type}: iterate startup preparation failed",
                        dedupe_key=f"prepare-iterate-failed:{task.id}",
                    )
                    continue
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{task.id} {task_type}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{task.id}",
                    spawn_fn=lambda: _spawn_background_iterate(
                        iterate_args,
                        config,
                        task,
                        prepared_task_id=str(prepared_pending_task.id),
                        prepared_phase="preloop",
                    ),
                )
                if rc != 0:
                    continue
                slots -= 1
                work_done = True
                started_task_ids.add(str(task.id))
                started_prompt = _format_prompt_for_width(
                    task.prompt,
                    prefix=16 + len(f"{task.id} {task_type} \""),
                    suffix=len('"'),
                )
                log.emit("START", f"{task.id} {task_type} \"{started_prompt}\"")
                continue

            if dry_run:
                dry_run_prompt = _format_prompt_for_width(
                    task.prompt,
                    prefix=16 + len(f"{task.id} {task_type} \""),
                    suffix=len('" [dry-run]'),
                )
                log.emit("START", f"{task.id} {task_type} \"{dry_run_prompt}\" [dry-run]")
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
            started_prompt = _format_prompt_for_width(
                task.prompt,
                prefix=16 + len(f"{task.id} {task_type} \""),
                suffix=len('"'),
            )
            log.emit("START", f"{task.id} {task_type} \"{started_prompt}\"")

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
    restart_failed = bool(getattr(args, "restart_failed", False))
    restart_failed_batch = (
        args.restart_failed_batch
        if getattr(args, "restart_failed_batch", None) is not None
        else config.watch.restart_failed_batch
    )
    max_recovery_attempts = (
        args.max_resume_attempts
        if getattr(args, "max_resume_attempts", None) is not None
        else config.max_resume_attempts
    )
    dry_run = bool(getattr(args, "dry_run", False))
    show_skipped = bool(getattr(args, "show_skipped", False))
    quiet = bool(getattr(args, "quiet", False))
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

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
    if restart_failed_batch < 1:
        print("Error: --restart-failed-batch must be a positive integer")
        return 1
    if max_recovery_attempts < 0:
        print("Error: --max-resume-attempts must be non-negative")
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

    try:
        idle_seconds = 0
        failure_streak = 0
        previous_snapshot = _task_snapshot(store)

        # Preview the first watch pass and ask for confirmation before executing
        if restart_failed and dry_run:
            _emit_recovery_dry_run_report(
                store=store,
                tags=tag_filters,
                any_tag=any_tag,
                max_recovery_attempts=max_recovery_attempts,
                show_skipped=show_skipped,
            )
            return 0

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
                restart_failed=restart_failed,
                restart_failed_batch=restart_failed_batch,
                max_recovery_attempts=max_recovery_attempts,
                show_skipped=show_skipped,
            )
            if preview_result.work_done:
                try:
                    answer = input("\nProceed? [y/N] ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    answer = ""
                if answer not in ("y", "yes"):
                    print("Aborted.")
                    return 0

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
                restart_failed_mode=restart_failed,
                max_recovery_attempts=max_recovery_attempts,
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
                restart_failed=restart_failed,
                restart_failed_batch=restart_failed_batch,
                max_recovery_attempts=max_recovery_attempts,
                show_skipped=show_skipped,
            )

            current_snapshot = _task_snapshot(store)
            _emit_transition_events(
                previous_snapshot,
                current_snapshot,
                store=store,
                config=config,
                log=log,
                restart_failed_mode=restart_failed,
                max_recovery_attempts=max_recovery_attempts,
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
                max_recovery_attempts=max_recovery_attempts,
                restart_failed_mode=restart_failed,
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
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    normalized_tag_filters = normalize_tag_filters(tag_filters)

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
            for row in service.run(
                TaskQueryPresets.queue(limit=None, tags=normalized_tag_filters, any_tag=any_tag)
            ).rows
            if isinstance(row, TaskRow) and row.task.id is not None
        }
        is_currently_runnable = str(task_id) in runnable_pending_ids

        if action in {"move", "next", "clear"} and normalized_tag_filters is not None:
            if not task_matches_tag_filters(
                task_tags=task.tags or (),
                tag_filters=normalized_tag_filters,
                any_tag=any_tag,
            ):
                print(_format_queue_scope_error(task_id, normalized_tag_filters, any_tag=any_tag))
                return 1

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
            clear_task_queue_position_scoped(
                store,
                task_id,
                tags=normalized_tag_filters,
                any_tag=any_tag,
            )
            if is_currently_runnable:
                print(f"✓ Cleared explicit queue order for task {task_id}")
            else:
                print(f"✓ Cleared explicit queue order for task {task_id} (task is not currently runnable)")
            return 0

        position = 1 if action == "next" else int(args.position)
        if position < 1:
            print("Error: queue position must be >= 1")
            return 1
        set_task_queue_position_scoped(
            store,
            task_id,
            position=position,
            tags=normalized_tag_filters,
            any_tag=any_tag,
        )
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
        for row in service.run(
            TaskQueryPresets.queue(limit=None, tags=normalized_tag_filters, any_tag=any_tag)
        ).rows
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
        prompt_prefix = len(f"{index:>3}  {task.id}  {position_label}[{lane}] [{task.task_type}] ")
        queue_prompt = _format_prompt_for_width(task.prompt, prefix=prompt_prefix)
        print(f"{index:>3}  {task.id}  {position_label}[{lane}] [{task.task_type}] {queue_prompt}")

    if display_limit is not None and len(pending) > display_limit:
        remaining = len(pending) - display_limit
        plural = "tasks" if remaining != 1 else "task"
        print(f"({remaining} more runnable {plural}; use -n 0, -n -1, or --all to show everything)")

    return 0
