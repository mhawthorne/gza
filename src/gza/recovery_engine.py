"""Shared automatic recovery policy for failed tasks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from .db import SqliteTaskStore, Task as DbTask, task_id_numeric_key
from .dependency_preconditions import get_unmerged_dependency_precondition
from .failed_task_ordering import sort_failed_tasks
from .failure_policy import is_resumable_failure_reason

_ACTIONABLE_TYPES = {"implement", "plan", "explore", "fix", "internal", "review", "improve", "rebase"}
_MANUAL_ONLY_REASONS = {
    "TEST_FAILURE",
    "GIT_ERROR",
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
_TIMEOUT_STYLE_REASONS = frozenset({"MAX_STEPS", "MAX_TURNS", "TIMEOUT", "TERMINATED"})
_DESCENDANT_SUPERSEDED_REASONS: tuple[tuple[str, str, str], ...] = (
    ("completed", "recovery_already_completed", "recovery descendant already completed"),
    ("in_progress", "recovery_already_running", "recovery descendant already in progress"),
    ("pending", "recovery_already_pending", "recovery descendant already pending"),
)
_DIRECT_CHILD_SUPERSEDED_REASONS: tuple[tuple[str, str, str], ...] = (
    ("completed", "recovery_already_completed", "recovery child already completed"),
    ("in_progress", "recovery_already_running", "recovery child already in progress"),
)

RecoveryAction = Literal["resume", "retry", "skip"]
RecoveryRole = Literal["original", "resume", "retry"]
FailureCategory = Literal["timeout", "retryable", "manual"]


@dataclass(frozen=True)
class FailedRecoveryDecision:
    task_id: str
    action: RecoveryAction
    reason_code: str
    reason_text: str
    launch_mode: Literal["iterate", "worker", "none"]
    attempt_index: int
    attempt_limit: int
    recovery_task_id: str | None = None
    reuse_existing: bool = False


@dataclass(frozen=True)
class RecoveryChainState:
    role: RecoveryRole
    steps: tuple[RecoveryRole, ...]

    @property
    def has_retry(self) -> bool:
        return "retry" in self.steps

    @property
    def has_resume(self) -> bool:
        return "resume" in self.steps


def _is_manual_non_recovery_follow_up_edge(parent: DbTask, child: DbTask) -> bool:
    """Return whether a same-type based_on edge looks like an explicit follow-up, not recovery."""
    return (
        child.status == "completed"
        and parent.session_id is not None
        and child.session_id is not None
        and parent.session_id != child.session_id
        and parent.branch is not None
        and child.branch is not None
        and parent.branch != child.branch
        and child.prompt != parent.prompt
    )


def _is_retry_recovery_edge(parent: DbTask, child: DbTask) -> bool:
    """Return whether a based_on edge should count as a fresh retry attempt."""
    return not _is_manual_non_recovery_follow_up_edge(parent, child)


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
    failed = [task for task in failed if not is_chain_resolved_by_recovery(store, task)]
    return sort_failed_tasks(failed)


def _same_type_recovery_descendants(store: SqliteTaskStore, task: DbTask) -> list[DbTask]:
    if task.id is None:
        return []

    descendants: list[DbTask] = []
    queue: list[DbTask] = [task]
    seen: set[str] = set()

    while queue:
        parent = queue.pop(0)
        if parent.id is None:
            continue
        for child in store.get_based_on_children_by_type(parent.id, task.task_type):
            child_id = child.id
            if child_id is None or child_id in seen:
                continue
            if _classify_recovery_edge(parent, child) is None:
                continue
            seen.add(child_id)
            descendants.append(child)
            queue.append(child)

    return descendants


def classify_failure_reason(reason: str | None) -> FailureCategory:
    if reason in _MANUAL_ONLY_REASONS or reason == "UNKNOWN" or reason is None:
        return "manual"
    if reason == "PREREQUISITE_UNMERGED":
        return "retryable"
    if reason in _RETRY_REASONS:
        return "retryable"
    if reason in _TIMEOUT_STYLE_REASONS or is_resumable_failure_reason(reason):
        return "timeout"
    return "manual"


def _is_resume_recovery_edge(parent: DbTask, child: DbTask) -> bool:
    """Return whether a based_on edge preserves the original execution attempt.

    Recovery role must be inferred from the based_on chain, not from session
    presence on an individual task. A resume edge keeps the same task payload
    and reuses the same execution session/branch; a retry edge starts a fresh
    attempt even though it points to the same failed parent.
    """
    if parent.session_id is None or child.session_id is None:
        return False
    if parent.session_id != child.session_id:
        return False
    if child.depends_on != parent.depends_on:
        return False
    if parent.branch is not None or child.branch is not None:
        return parent.branch == child.branch
    return True


def _classify_recovery_edge(parent: DbTask, child: DbTask) -> RecoveryRole | None:
    if _is_resume_recovery_edge(parent, child):
        return "resume"
    if _is_retry_recovery_edge(parent, child):
        return "retry"
    return None


def get_recovery_chain_state(store: SqliteTaskStore, task: DbTask) -> RecoveryChainState:
    steps_reversed: list[RecoveryRole] = []
    current = task
    seen: set[str] = set()

    while current.id is not None and current.id not in seen:
        seen.add(current.id)
        if not current.based_on:
            break
        parent = store.get(current.based_on)
        if parent is None or parent.task_type != current.task_type or parent.id is None:
            break
        edge = _classify_recovery_edge(parent, current)
        if edge is None:
            break
        steps_reversed.append(edge)
        current = parent

    steps = tuple(reversed(steps_reversed))
    if not steps:
        return RecoveryChainState(role="original", steps=())
    return RecoveryChainState(role=steps[-1], steps=steps)


def get_recovery_chain_root_task_id(store: SqliteTaskStore, task: DbTask) -> str | None:
    """Return the recovery-only lineage root for a task."""
    if task.id is None:
        return None

    current = task
    seen: set[str] = set()
    while current.id is not None and current.id not in seen:
        seen.add(current.id)
        if current.based_on is None:
            break
        parent = store.get(current.based_on)
        if parent is None or parent.task_type != current.task_type or parent.id is None:
            break
        if _classify_recovery_edge(parent, current) is None:
            break
        current = parent
    return current.id


def get_completed_recovery_descendant(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    """Return the terminal completed recovery descendant when a failed chain is fully resolved."""
    if task.id is None or task.status != "failed":
        return None

    descendants = _same_type_recovery_descendants(store, task)
    if not descendants:
        return None

    descendant_ids = {descendant.id for descendant in descendants if descendant.id is not None}
    parent_ids_with_recovery_children = {
        descendant.based_on
        for descendant in descendants
        if descendant.based_on is not None and descendant.based_on != task.id and descendant.based_on in descendant_ids
    }
    terminal_descendants = [
        descendant
        for descendant in descendants
        if descendant.id is not None and descendant.id not in parent_ids_with_recovery_children
    ]
    if not terminal_descendants:
        return None
    if any(descendant.status != "completed" for descendant in terminal_descendants):
        return None

    def _descendant_sort_key(descendant: DbTask) -> tuple[datetime, int]:
        when = descendant.completed_at or descendant.created_at or datetime.min
        if when.tzinfo is not None:
            when = when.astimezone(UTC).replace(tzinfo=None)
        return (when, task_id_numeric_key(descendant.id))

    return max(
        terminal_descendants,
        key=_descendant_sort_key,
    )


def is_chain_resolved_by_recovery(store: SqliteTaskStore, task: DbTask) -> bool:
    """Return whether a failed task's recovery-only chain ends in a completed task."""
    return get_completed_recovery_descendant(store, task) is not None


def _policy_attempt_counters(
    chain: RecoveryChainState,
    *,
    max_recovery_attempts: int,
) -> tuple[int, int]:
    if max_recovery_attempts <= 0:
        return (0, 0)
    attempt_limit = 2
    # Display counters should reflect the bounded shared policy budget, not raw
    # based_on depth, so exhausted chains saturate at N/N instead of N+1/N.
    attempt_index = min(len(chain.steps) + 1, attempt_limit)
    return (attempt_index, attempt_limit)


def _expected_recovery_action(
    task: DbTask,
    *,
    chain: RecoveryChainState,
) -> RecoveryAction | None:
    reason = task.failure_reason or "UNKNOWN"
    category = classify_failure_reason(reason)

    if reason == "PREREQUISITE_UNMERGED":
        return "retry" if chain.role == "original" else None

    if category == "manual":
        return None

    if category == "timeout":
        if task.session_id is None:
            return None
        if chain.role == "original":
            return "resume"
        if chain.role == "retry":
            return "resume"
        return None

    if category == "retryable":
        if chain.role == "original":
            return "retry"
        return None

    return None


def _skip_decision(
    *,
    task_id: str,
    reason_code: str,
    reason_text: str,
    attempt_index: int,
    attempt_limit: int,
) -> FailedRecoveryDecision:
    return FailedRecoveryDecision(
        task_id=task_id,
        action="skip",
        reason_code=reason_code,
        reason_text=reason_text,
        launch_mode="none",
        attempt_index=attempt_index,
        attempt_limit=attempt_limit,
    )


def decide_failed_task_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    max_recovery_attempts: int,
) -> FailedRecoveryDecision:
    assert task.id is not None
    task_id = str(task.id)
    launch_mode: Literal["iterate", "worker", "none"] = "iterate" if task.task_type == "implement" else "worker"
    chain = get_recovery_chain_state(store, task)
    attempt_index, attempt_limit = _policy_attempt_counters(
        chain,
        max_recovery_attempts=max_recovery_attempts,
    )

    if task.status != "failed":
        return _skip_decision(
            task_id=task_id,
            reason_code="not_failed",
            reason_text="task is not failed",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    if task.task_type not in _ACTIONABLE_TYPES:
        return _skip_decision(
            task_id=task_id,
            reason_code="task_type_out_of_scope",
            reason_text=f"task type {task.task_type} is out of scope",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    if max_recovery_attempts <= 0:
        return _skip_decision(
            task_id=task_id,
            reason_code="automatic_recovery_disabled",
            reason_text="automatic recovery is disabled",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    reason = task.failure_reason or "UNKNOWN"
    if reason == "PREREQUISITE_UNMERGED":
        if task.depends_on and store.resolve_dependency_completion(task) is None:
            return _skip_decision(
                task_id=task_id,
                reason_code="dependency_not_ready",
                reason_text="dependency precondition not satisfied",
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
        if get_unmerged_dependency_precondition(store, task) is not None:
            return _skip_decision(
                task_id=task_id,
                reason_code="dependency_not_ready",
                reason_text="dependency precondition not satisfied",
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
    elif classify_failure_reason(reason) == "manual":
        return _skip_decision(
            task_id=task_id,
            reason_code="manual_failure_reason",
            reason_text=f"{reason} requires manual intervention",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    blocked, _blocking_id, _blocking_status = store.is_task_blocked(task)
    if blocked:
        return _skip_decision(
            task_id=task_id,
            reason_code="dependency_not_ready",
            reason_text="dependency precondition not satisfied",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    expected_action = _expected_recovery_action(task, chain=chain)
    if expected_action is None:
        return _skip_decision(
            task_id=task_id,
            reason_code="manual_review_required",
            reason_text="automatic recovery stops here; manual review required",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    children = store.get_based_on_children_by_type(task_id, task.task_type)
    recovery_children = [
        child for child in children
        if _classify_recovery_edge(task, child) is not None
    ]
    matching_children = [
        child for child in recovery_children
        if _classify_recovery_edge(task, child) == expected_action
    ]
    descendants = _same_type_recovery_descendants(store, task)
    direct_child_ids = {child.id for child in recovery_children if child.id is not None}
    deeper_descendants = [child for child in descendants if child.id not in direct_child_ids]
    pending_children = [child for child in matching_children if child.status == "pending" and child.id is not None]
    all_pending_children = [child for child in recovery_children if child.status == "pending" and child.id is not None]

    for status, reason_code, reason_text in _DIRECT_CHILD_SUPERSEDED_REASONS:
        if any(child.status == status for child in recovery_children):
            return _skip_decision(
                task_id=task_id,
                reason_code=reason_code,
                reason_text=reason_text,
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
    for status, reason_code, reason_text in _DESCENDANT_SUPERSEDED_REASONS:
        if any(child.status == status for child in deeper_descendants):
            return _skip_decision(
                task_id=task_id,
                reason_code=reason_code,
                reason_text=reason_text,
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
    if any(child.status == "failed" for child in descendants):
        return _skip_decision(
            task_id=task_id,
            reason_code="recovery_has_newer_failed_descendant",
            reason_text="a newer failed recovery descendant must be recovered first",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if len(all_pending_children) > 1:
        return _skip_decision(
            task_id=task_id,
            reason_code="manual_review_required",
            reason_text="multiple pending recovery children require manual review",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if pending_children:
        reuse_child = pending_children[0]
        return FailedRecoveryDecision(
            task_id=task_id,
            action=expected_action,
            reason_code=reason,
            reason_text=f"reusing pending {expected_action} child {reuse_child.id}",
            launch_mode=launch_mode,
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
            recovery_task_id=str(reuse_child.id),
            reuse_existing=True,
        )
    if all_pending_children:
        return _skip_decision(
            task_id=task_id,
            reason_code="recovery_already_pending",
            reason_text="recovery child already pending",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    reason_text = "dependency merge prerequisite now satisfied"
    if reason != "PREREQUISITE_UNMERGED":
        reason_text = (
            f"{reason} with preserved session"
            if expected_action == "resume"
            else f"{reason} restart with fresh attempt"
        )
    return FailedRecoveryDecision(
        task_id=task_id,
        action=expected_action,
        reason_code=reason,
        reason_text=reason_text,
        launch_mode=launch_mode,
        attempt_index=attempt_index,
        attempt_limit=attempt_limit,
    )


def get_failed_recovery_needs_attention_reason(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    decision: FailedRecoveryDecision | None = None,
    max_recovery_attempts: int,
) -> str | None:
    """Return a shared needs-attention reason slug for failed-task skip decisions."""
    if task.id is None:
        return None
    resolved_decision = decision or decide_failed_task_recovery(
        store,
        task,
        max_recovery_attempts=max_recovery_attempts,
    )
    return _get_failed_recovery_needs_attention_reason(
        store,
        task,
        decision=resolved_decision,
        max_recovery_attempts=max_recovery_attempts,
        seen_task_ids=set(),
    )


def _get_failed_recovery_needs_attention_reason(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    decision: FailedRecoveryDecision,
    max_recovery_attempts: int,
    seen_task_ids: set[str],
) -> str | None:
    if task.id is None or decision.action != "skip":
        return None

    task_id = str(task.id)
    if task_id in seen_task_ids:
        return None
    seen_task_ids.add(task_id)

    if decision.reason_code == "automatic_recovery_disabled":
        return "automatic-recovery-disabled"
    if decision.reason_code == "manual_failure_reason":
        return "manual-failure-reason"
    if decision.reason_code == "manual_review_required":
        if decision.attempt_limit > 0 and decision.attempt_index >= decision.attempt_limit:
            return "max-resume-attempts-reached"
        return "manual-review-required"
    if decision.reason_code != "recovery_has_newer_failed_descendant":
        return None

    failed_descendants = [
        descendant
        for descendant in _same_type_recovery_descendants(store, task)
        if descendant.status == "failed" and descendant.id is not None
    ]
    for descendant in sort_failed_tasks(failed_descendants):
        descendant_decision = decide_failed_task_recovery(
            store,
            descendant,
            max_recovery_attempts=max_recovery_attempts,
        )
        descendant_reason = _get_failed_recovery_needs_attention_reason(
            store,
            descendant,
            decision=descendant_decision,
            max_recovery_attempts=max_recovery_attempts,
            seen_task_ids=seen_task_ids,
        )
        if descendant_reason is not None:
            return "newer-failed-recovery-descendant-needs-attention"
    return None
