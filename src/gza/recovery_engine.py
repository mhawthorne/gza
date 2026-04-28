"""Shared failed-task recovery decision engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .db import SqliteTaskStore, Task as DbTask, task_id_numeric_key
from .failure_policy import is_resumable_failure_reason

_ACTIONABLE_TYPES = {"implement", "plan", "explore", "fix", "internal"}
_MANUAL_ONLY_REASONS = {
    "TEST_FAILURE",
    "GIT_ERROR",
    "PREREQUISITE_UNMERGED",
    "PR_REQUIRED",
    "MISSING_REPORT_ARTIFACT",
    "KILLED",
    "INTERRUPTED",
    "UNKNOWN",
}
_RETRY_REASONS = {
    "INFRASTRUCTURE_ERROR",
    "PROVIDER_UNAVAILABLE",
    "WORKER_DIED",
    "NO_ACTIVITY",
}


@dataclass(frozen=True)
class FailedRecoveryDecision:
    task_id: str
    action: Literal["resume", "retry", "skip"]
    reason_code: str
    reason_text: str
    launch_mode: Literal["iterate", "worker", "none"]
    attempt_index: int
    attempt_limit: int
    recovery_task_id: str | None = None
    reuse_existing: bool = False


def _parse_completed_at(value: datetime | None) -> datetime:
    return value if isinstance(value, datetime) else datetime.max


def list_failed_tasks_for_recovery(
    store: SqliteTaskStore,
    *,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> list[DbTask]:
    failed = [task for task in store.get_all() if task.status == "failed"]
    if tags:
        from .task_query import normalize_tag_filters, task_matches_tag_filters

        normalized = normalize_tag_filters(tags)
        failed = [
            task
            for task in failed
            if task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized, any_tag=any_tag)
        ]
    failed.sort(key=lambda task: (_parse_completed_at(task.completed_at), task_id_numeric_key(task.id)))
    return failed


def _count_recovery_attempt_depth(store: SqliteTaskStore, task_id: str) -> int:
    depth = 0
    seen: set[str] = set()
    current_id = task_id
    while current_id and current_id not in seen:
        seen.add(current_id)
        task = store.get(current_id)
        if task is None or not task.based_on:
            break
        parent = store.get(task.based_on)
        if parent is None or parent.status != "failed":
            break
        depth += 1
        current_id = str(parent.id)
    return depth


def decide_failed_task_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    max_recovery_attempts: int,
) -> FailedRecoveryDecision:
    assert task.id is not None
    task_id = str(task.id)
    launch_mode: Literal["iterate", "worker", "none"] = "iterate" if task.task_type == "implement" else "worker"

    attempt_depth = _count_recovery_attempt_depth(store, task_id)
    attempt_index = attempt_depth + 1

    def _skip(code: str, text: str) -> FailedRecoveryDecision:
        return FailedRecoveryDecision(
            task_id=task_id,
            action="skip",
            reason_code=code,
            reason_text=text,
            launch_mode="none",
            attempt_index=attempt_index,
            attempt_limit=max_recovery_attempts,
        )

    if task.status != "failed":
        return _skip("not_failed", "task is not failed")

    if task.task_type not in _ACTIONABLE_TYPES:
        return _skip("task_type_out_of_scope", f"task type {task.task_type} is out of scope")

    if max_recovery_attempts >= 0 and attempt_depth >= max_recovery_attempts:
        return _skip("attempt_cap_reached", "automatic recovery attempt limit reached")

    reason = task.failure_reason or "UNKNOWN"
    if reason in _MANUAL_ONLY_REASONS:
        return _skip("manual_failure_reason", f"{reason} requires manual intervention")

    blocked, _blocking_id, _blocking_status = store.is_task_blocked(task)
    if blocked:
        return _skip("dependency_not_ready", "dependency precondition not satisfied")

    children = store.get_based_on_children(task_id)
    if any(child.status == "in_progress" for child in children):
        return _skip("recovery_already_running", "recovery child already in progress")
    pending_children = [child for child in children if child.status == "pending" and child.id is not None]
    if pending_children:
        resume_child = next((child for child in pending_children if child.session_id), None)
        reuse_child = resume_child or pending_children[0]
        reuse_action: Literal["resume", "retry"] = "resume" if resume_child is not None else "retry"
        reason = task.failure_reason or "UNKNOWN"
        return FailedRecoveryDecision(
            task_id=task_id,
            action=reuse_action,
            reason_code=reason,
            reason_text=f"reusing pending {reuse_action} child {reuse_child.id}",
            launch_mode=launch_mode,
            attempt_index=attempt_index,
            attempt_limit=max_recovery_attempts,
            recovery_task_id=str(reuse_child.id),
            reuse_existing=True,
        )
    if any(child.status == "completed" for child in children):
        return _skip("recovery_already_completed", "recovery child already completed")

    if is_resumable_failure_reason(reason) and task.session_id:
        return FailedRecoveryDecision(
            task_id=task_id,
            action="resume",
            reason_code=reason,
            reason_text=f"{reason} with preserved session",
            launch_mode=launch_mode,
            attempt_index=attempt_index,
            attempt_limit=max_recovery_attempts,
        )

    if reason in _RETRY_REASONS or is_resumable_failure_reason(reason):
        reason_text = f"{reason} restart with fresh attempt"
        if is_resumable_failure_reason(reason) and not task.session_id:
            reason_text = f"{reason} without session_id; retry required"
        return FailedRecoveryDecision(
            task_id=task_id,
            action="retry",
            reason_code=reason,
            reason_text=reason_text,
            launch_mode=launch_mode,
            attempt_index=attempt_index,
            attempt_limit=max_recovery_attempts,
        )

    return _skip("no_recovery_path", f"no unattended recovery path for {reason}")
