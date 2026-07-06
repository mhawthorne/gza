"""Shared automatic recovery policy for failed tasks."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from .branch_publication import load_branch_publication_state
from .config import Config, ConfigError
from .db import (
    MergeTargetResolutionError,
    SqliteTaskStore,
    Task as DbTask,
    TaskStats,
    merge_unit_is_active,
    merge_unit_state_is_inactive_tombstone,
    task_id_numeric_key,
)
from .dependency_preconditions import dependency_readiness, get_unmerged_dependency_precondition, task_is_merged
from .failed_task_ordering import sort_failed_tasks
from .failure_policy import is_resumable_failure_reason
from .failure_reasons import is_readonly_db_failure
from .git import Git, GitError
from .lifecycle_completion import merge_state_is_terminal_for_lifecycle, task_is_complete_for_lifecycle
from .log_paths import ops_log_path_for
from .merge_state import classify_branch_merge_state_for_target, resolve_task_merge_state_for_target
from .operator_state import (
    MOOT_EMPTY_LIFECYCLE_DETAIL,
    MOOT_REDUNDANT_LIFECYCLE_DETAIL,
    effective_no_work_merge_state,
)
from .recovery_read_context import RecoveryReadContext
from .review_scope import resolve_implement_slice_identity

logger = logging.getLogger(__name__)

_REBASE_COMPLETED_LOG_MARKERS = (
    "rebase completed successfully",
    "successfully rebased",
)
_REBASE_CHECKS_PASSED_LOG_MARKERS = (
    "all checks passed",
)
_REBASE_CHECKS_FAILED_LOG_MARKERS = (
    "check failed",
    "checks failed",
    "verification failed",
    "verify failed",
    "test failed",
    "tests failed",
    "error while running checks",
    "error during checks",
)
_REBASE_INFRA_LOG_MARKERS = (
    "git worktree list --porcelain failed",
    "invalid path '/gza-git'",
    "/gza-git/",
    "worktree metadata became unavailable",
)
RETRY_LIMIT_REACHED_ATTENTION_REASON = "retry-limit-reached"

_ACTIONABLE_TYPES = {"implement", "plan", "explore", "fix", "internal", "review", "improve", "rebase"}
_MANUAL_ONLY_REASONS = {
    "CONFIG_ERROR",
    "REBASE_CONFLICT",
    "TEST_FAILURE",
    "GIT_ERROR",
    "MISSING_REPORT_ARTIFACT",
    "KILLED",
    "INTERRUPTED",
    "UNKNOWN",
}
_RETRY_REASONS = {
    "INFRASTRUCTURE_ERROR",
    "PROVIDER_UNAVAILABLE",
    "PROVIDER_EMPTY_TURN",
    "RETRYABLE_PROVIDER_ERROR",
    "WORKER_DIED",
    "WORKSPACE_NOT_POPULATED",
    "NO_ACTIVITY",
}
_RECONCILE_REASONS = frozenset({"BRANCH_UNPUSHABLE"})
_LEGACY_BRANCH_PUBLICATION_REASON = "PR_REQUIRED"
_TIMEOUT_STYLE_REASONS = frozenset({"MAX_STEPS", "MAX_TURNS", "TIMEOUT", "TERMINATED", "TERMINAL_NO_WORK"})
_UNRESOLVED_RECOVERY_TERMINAL_STATUSES = frozenset({"failed", "dropped"})
_UNRESOLVED_RECOVERY_ATTENTION_REASON = "newer-recovery-descendant-needs-attention"
_MERGED_TARGET_RESOLUTION_TYPES = frozenset({"review", "improve", "rebase"})
_MERGEABLE_EXECUTION_STATUSES = frozenset({"completed", "unmerged"})
_DESCENDANT_SUPERSEDED_REASONS: tuple[tuple[str, str, str], ...] = (
    ("completed", "recovery_already_completed", "recovery descendant already completed"),
    ("in_progress", "recovery_already_running", "recovery descendant already in progress"),
    ("pending", "recovery_already_pending", "recovery descendant already pending"),
)
_DIRECT_CHILD_SUPERSEDED_REASONS: tuple[tuple[str, str, str], ...] = (
    ("completed", "recovery_already_completed", "recovery child already completed"),
    ("in_progress", "recovery_already_running", "recovery child already in progress"),
)

RecoveryAction = Literal["resume", "retry", "reconcile", "skip"]
PendingRecoveryExecutionMode = Literal["resume", "retry"]
RecoveryRole = Literal["original", "resume", "retry"]
FailureCategory = Literal["timeout", "retryable", "reconcile", "manual"]
TerminalNoWorkMergeState = Literal["empty", "redundant"]
PrerequisiteUnmergedReconciliation = Literal[
    "dependency_not_ready",
    "moot_empty",
    "moot_redundant",
    "recoverable_real_work",
    "parked_unknown",
]
EmptyTaskRecoveryState = Literal["requires_recovery", "moot", "resolved"]


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


def should_hide_failed_recovery_decision(decision: FailedRecoveryDecision) -> bool:
    """Return whether the decision should stay off operator recovery surfaces."""
    return decision.action == "skip" and decision.reason_code in {
        "merge_unit_superseded",
        "resolved_by_merged_target",
        "same_slice_sibling_landed",
        "merge_unit_empty",
        "merge_unit_redundant",
        "terminal_no_work_recovery_already_resolved",
    }


@dataclass(frozen=True)
class RecoveryChainState:
    role: RecoveryRole
    steps: tuple[RecoveryRole, ...]
    root_task_id: str | None = None
    resolved_task_id: str | None = None

    @property
    def has_retry(self) -> bool:
        return "retry" in self.steps

    @property
    def has_resume(self) -> bool:
        return "resume" in self.steps


@dataclass(frozen=True)
class _RecoveryPolicyEpoch:
    """Effective retry-budget epoch for one recovery-chain evaluation."""

    effective_rearmed_at: datetime | None
    effective_steps: tuple[RecoveryRole, ...]

    @property
    def role(self) -> RecoveryRole:
        if not self.effective_steps:
            return "original"
        return self.effective_steps[-1]


@dataclass(frozen=True)
class _RecoveryChainSnapshot:
    root_task: DbTask
    ancestor_ids: tuple[str, ...]
    steps: tuple[RecoveryRole, ...]
    descendants: tuple[DbTask, ...]
    direct_children: tuple[DbTask, ...]
    deeper_descendants: tuple[DbTask, ...]
    terminal_descendants: tuple[DbTask, ...]
    latest_completed_terminal_descendant: DbTask | None
    completed_terminal_descendant: DbTask | None


@dataclass
class _MergeContext:
    git: Git | None
    default_branch: str | None
    existing_branches: frozenset[str] | None = None
    resolution_error: str | None = None
    branch_resolution: dict[tuple[str, bool | None], str] = field(default_factory=dict)
    repository_inspection_warnings: list[str] = field(default_factory=list)
    _warning_keys: set[str] = field(default_factory=set)


def _record_repository_inspection_warning(
    merge_context: _MergeContext,
    *,
    key: str,
    message: str,
) -> None:
    if key in merge_context._warning_keys:
        return
    merge_context._warning_keys.add(key)
    merge_context.repository_inspection_warnings.append(message)
    logger.debug(message)


def _branch_reachability_warning(detail: str) -> str:
    return (
        "Failed-task recovery could not inspect repository branch reachability; "
        "git branch reachability suppression is unavailable for this run, but "
        "metadata-based same-lineage merged-task suppression may still apply: "
        f"{detail}"
    )


def _matches_shared_recovery_payload(parent: DbTask, child: DbTask) -> bool:
    """Return whether the child still matches the payload copied by recovery helpers."""
    return (
        child.prompt == parent.prompt
        and child.depends_on == parent.depends_on
        and child.tags == parent.tags
        and child.spec == parent.spec
        and child.create_review == parent.create_review
        and child.create_pr == parent.create_pr
        and child.task_type_hint == parent.task_type_hint
    )


def _matches_retry_recovery_invariants(parent: DbTask, child: DbTask) -> bool:
    """Return whether the child still looks like a retry created by shared helpers."""
    if not _matches_shared_recovery_payload(parent, child):
        return False
    if child.same_branch == parent.same_branch:
        return True
    return bool(parent.same_branch and parent.branch and not child.same_branch and child.base_branch == parent.branch)


def _is_manual_non_recovery_follow_up_edge(parent: DbTask, child: DbTask) -> bool:
    """Return whether a same-type based_on edge looks like an explicit follow-up, not recovery."""
    return not _matches_retry_recovery_invariants(parent, child)


def _is_retry_recovery_edge(parent: DbTask, child: DbTask) -> bool:
    """Return whether a based_on edge should count as a fresh retry attempt."""
    return not _is_manual_non_recovery_follow_up_edge(parent, child)


def classify_failure_reason(reason: str | None) -> FailureCategory:
    if reason in _MANUAL_ONLY_REASONS or reason == "UNKNOWN" or reason is None:
        return "manual"
    if reason in _RETRY_REASONS:
        return "retryable"
    if reason == _LEGACY_BRANCH_PUBLICATION_REASON:
        return "reconcile"
    if reason in _RECONCILE_REASONS:
        return "reconcile"
    if reason in _TIMEOUT_STYLE_REASONS or is_resumable_failure_reason(reason):
        return "timeout"
    return "manual"


def _normalize_legacy_branch_publication_failure_reason(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> str:
    """Reclassify legacy ``PR_REQUIRED`` failures to ``BRANCH_UNPUSHABLE`` when possible.

    Historical rows predate the publication split between "push rejected" and
    "PR creation unavailable". Slice 2 chooses the compatibility path of lazily
    rewriting failed ``PR_REQUIRED`` rows to ``BRANCH_UNPUSHABLE`` the first
    time recovery planning evaluates them, so the row takes the countable
    reconcile path without requiring a schema migration.
    """
    reason = task.failure_reason or "UNKNOWN"
    if task.status != "failed" or reason != _LEGACY_BRANCH_PUBLICATION_REASON:
        return reason

    setattr(task, "failure_reason", "BRANCH_UNPUSHABLE")
    if task.id is not None and read_context is not None:
        indexed_task = read_context.get_task(task.id)
        if indexed_task is not None:
            setattr(indexed_task, "failure_reason", "BRANCH_UNPUSHABLE")
        if not read_context.allow_reconcile_mutation:
            read_context.record_legacy_branch_publication_reconciliation(task)
            return "BRANCH_UNPUSHABLE"

    store.update(task)
    return "BRANCH_UNPUSHABLE"


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


def _is_legacy_ambiguous_manual_follow_up(parent: DbTask, child: DbTask) -> bool:
    """Return whether a legacy same-payload edge lacks enough evidence to count as recovery.

    Pre-v41 rows do not persist recovery provenance. When both the session and
    branch changed across a same-payload based_on edge, and the child lacks the
    retry helper's same-branch fork signature, treat the edge as a manual
    follow-up instead of silently suppressing the failed parent.
    """
    if not _matches_shared_recovery_payload(parent, child):
        return False
    if parent.session_id is None or child.session_id is None:
        return False
    if parent.branch is None or child.branch is None:
        return False
    if parent.session_id == child.session_id or parent.branch == child.branch:
        return False
    if parent.same_branch and child.base_branch == parent.branch and not child.same_branch:
        return False
    return True


def _classify_legacy_recovery_edge(parent: DbTask, child: DbTask) -> RecoveryRole | None:
    if _is_resume_recovery_edge(parent, child):
        return "resume"
    if _is_legacy_ambiguous_manual_follow_up(parent, child):
        return None
    if _is_retry_recovery_edge(parent, child):
        return "retry"
    return None


def _classify_recovery_edge(parent: DbTask, child: DbTask) -> RecoveryRole | None:
    if parent.task_type != child.task_type:
        return None
    if child.recovery_origin == "manual":
        return None
    if child.recovery_origin == "resume":
        return "resume"
    if child.recovery_origin == "retry":
        return "retry"
    return _classify_legacy_recovery_edge(parent, child)


def _descendant_sort_key(descendant: DbTask) -> tuple[datetime, int]:
    when = descendant.completed_at or descendant.created_at or datetime.min
    if when.tzinfo is not None:
        when = when.astimezone(UTC).replace(tzinfo=None)
    return (when, task_id_numeric_key(descendant.id))


def _task_merge_state_for_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> str | None:
    if task.id is None:
        return task.merge_status
    unit = (
        read_context.resolve_merge_unit_for_task(task.id)
        if read_context is not None
        else store.resolve_merge_unit_for_task(task.id)
    )
    raw_state = unit.state if unit is not None else task.merge_status
    return effective_no_work_merge_state(task, raw_state)


def is_recovery_suppressed_by_inactive_merge_unit(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether recovery should hide a task attached only to inactive historical units."""
    if task.id is None:
        return False
    units = (
        read_context.list_merge_units_for_task(task.id)
        if read_context is not None
        else tuple(store.list_merge_units_for_task(task.id))
    )
    has_inactive_historical_attachment = False
    for unit in units:
        if merge_unit_is_active(unit):
            return False
        if unit.superseded_by_unit_id is not None or merge_unit_state_is_inactive_tombstone(unit.state):
            has_inactive_historical_attachment = True
    return has_inactive_historical_attachment


def _task_has_executed_resumable_session(task: DbTask) -> bool:
    """Return whether a failed task recorded provider execution for empty-branch recovery.

    This check is intentionally fail-closed: once a resumable ``session_id`` exists,
    missing step/token evidence keeps the task recoverable rather than silently moot.
    Only an explicit all-zero record proves "never actually ran".
    """
    if task.session_id is None:
        return False

    evidence = (task.num_steps_computed, task.num_steps_reported, task.output_tokens)
    if any(value is not None and value > 0 for value in evidence):
        return True
    return not all(value is not None for value in evidence)


def _classify_empty_task_recovery_state(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    merge_state: str | None = None,
    merge_context: _MergeContext | None = None,
    read_context: RecoveryReadContext | None = None,
) -> EmptyTaskRecoveryState:
    """Classify whether a terminal no-work failed task is moot, resolved, or recoverable."""
    if task.status != "failed":
        return "moot"
    resolved_merge_state = (
        merge_state if merge_state is not None else _task_merge_state_for_recovery(store, task, read_context=read_context)
    )
    if resolved_merge_state not in {"empty", "redundant"}:
        return "moot"
    if not _task_has_executed_resumable_session(task):
        return "moot"
    if get_completed_recovery_descendant(store, task, read_context=read_context) is not None:
        return "resolved"
    if get_completed_sibling_recovery(store, task, read_context=read_context) is not None:
        return "resolved"
    if get_completed_same_slice_sibling_attempt(store, task, read_context=read_context) is not None:
        return "resolved"
    resolved_merge_context = merge_context or _load_merge_context(_project_dir_for_store(store))
    if _is_resolved_by_landed_lineage(store, task, merge_context=resolved_merge_context, read_context=read_context):
        return "resolved"
    return "requires_recovery"


def empty_task_requires_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    merge_state: str | None = None,
    merge_context: _MergeContext | None = None,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether a terminal no-work merge unit still represents recoverable failed work."""
    return (
        _classify_empty_task_recovery_state(
            store,
            task,
            merge_state=merge_state,
            merge_context=merge_context,
            read_context=read_context,
        )
        == "requires_recovery"
    )


def resolve_pending_recovery_execution_mode(task: DbTask) -> PendingRecoveryExecutionMode | None:
    """Return how an explicit pending recovery row must execute."""
    if task.status != "pending":
        return None
    if task.recovery_origin == "resume":
        return "resume" if task.session_id else "retry"
    if task.recovery_origin == "retry":
        return "retry"
    if task.session_id and task.based_on:
        return "resume"
    return None


def classify_recovery_row(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> RecoveryRole | None:
    """Return the recovery role carried by this task's based_on edge, if any."""
    if task.id is None or task.based_on is None:
        return None
    parent = read_context.get_task(task.based_on) if read_context is not None else store.get(task.based_on)
    if parent is None:
        return None
    return _classify_recovery_edge(parent, task)


def _task_is_complete_recovery_outcome(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    return task_is_complete_for_lifecycle(
        task,
        merge_state=_task_merge_state_for_recovery(store, task, read_context=read_context),
    )


def _task_has_explicit_terminal_merge_proof_for_same_slice_helper(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    if task.status not in {"completed", "unmerged"}:
        return False
    if task.id is not None:
        unit = (
            read_context.resolve_merge_unit_for_task(task.id)
            if read_context is not None
            else store.resolve_merge_unit_for_task(task.id)
        )
        if unit is not None and merge_unit_is_active(unit):
            return effective_no_work_merge_state(task, unit.state) in {"merged", "empty", "redundant"}
    return merge_state_is_terminal_for_lifecycle(task.merge_status)


def _failed_task_has_active_nonterminal_merge_unit(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    if task.id is None:
        return False
    unit = (
        read_context.resolve_merge_unit_for_task(task.id)
        if read_context is not None
        else store.resolve_merge_unit_for_task(task.id)
    )
    if unit is None or not merge_unit_is_active(unit):
        return False
    return effective_no_work_merge_state(task, unit.state) in {"unmerged", "blocked", "stale"}


def _is_resumable_timeout_implementation(task: DbTask) -> bool:
    return (
        task.task_type == "implement"
        and task.status == "failed"
        and task.session_id is not None
        and classify_failure_reason(task.failure_reason or "UNKNOWN") == "timeout"
    )


def _build_recovery_chain_snapshot(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> _RecoveryChainSnapshot:
    if read_context is not None and task.id is not None:
        cached = read_context.recovery_snapshots.get(task.id)
        if isinstance(cached, _RecoveryChainSnapshot):
            return cached
    steps_reversed: list[RecoveryRole] = []
    ancestor_ids_reversed: list[str] = []
    current = task
    seen_ancestors: set[str] = set()

    while current.id is not None and current.id not in seen_ancestors:
        seen_ancestors.add(current.id)
        ancestor_ids_reversed.append(current.id)
        if not current.based_on:
            break
        parent = read_context.get_task(current.based_on) if read_context is not None else store.get(current.based_on)
        if parent is None or parent.id is None:
            break
        edge = _classify_recovery_edge(parent, current)
        if edge is None:
            break
        steps_reversed.append(edge)
        current = parent

    descendants: list[DbTask] = []
    direct_children: list[DbTask] = []
    queue: list[DbTask] = [task]
    seen_descendants: set[str] = set()

    while queue:
        parent = queue.pop(0)
        if parent.id is None:
            continue
        children = (
            read_context.get_based_on_children_by_type(parent.id, task.task_type)
            if read_context is not None
            else store.get_based_on_children_by_type(parent.id, task.task_type)
        )
        for child in children:
            child_id = child.id
            if child_id is None or child_id in seen_descendants:
                continue
            if _classify_recovery_edge(parent, child) is None:
                continue
            seen_descendants.add(child_id)
            if parent.id == task.id:
                direct_children.append(child)
            descendants.append(child)
            queue.append(child)

    descendant_ids = {descendant.id for descendant in descendants if descendant.id is not None}
    direct_child_ids = {child.id for child in direct_children if child.id is not None}
    parent_ids_with_recovery_children = {
        descendant.based_on
        for descendant in descendants
        if descendant.based_on is not None and descendant.based_on in descendant_ids
    }
    terminal_descendants = [
        descendant
        for descendant in descendants
        if descendant.id is not None and descendant.id not in parent_ids_with_recovery_children
    ]
    latest_completed_terminal_descendant: DbTask | None = None
    if terminal_descendants and all(descendant.status == "completed" for descendant in terminal_descendants):
        latest_completed_terminal_descendant = max(terminal_descendants, key=_descendant_sort_key)
    completed_terminal_descendant: DbTask | None = None
    if terminal_descendants and all(
        _task_is_complete_recovery_outcome(store, descendant, read_context=read_context)
        for descendant in terminal_descendants
    ):
        completed_terminal_descendant = max(terminal_descendants, key=_descendant_sort_key)

    snapshot = _RecoveryChainSnapshot(
        root_task=current,
        ancestor_ids=tuple(reversed(ancestor_ids_reversed)),
        steps=tuple(reversed(steps_reversed)),
        descendants=tuple(descendants),
        direct_children=tuple(direct_children),
        deeper_descendants=tuple(
            descendant for descendant in descendants if descendant.id is not None and descendant.id not in direct_child_ids
        ),
        terminal_descendants=tuple(terminal_descendants),
        latest_completed_terminal_descendant=latest_completed_terminal_descendant,
        completed_terminal_descendant=completed_terminal_descendant,
    )
    if read_context is not None and task.id is not None:
        read_context.recovery_snapshots[task.id] = snapshot
    return snapshot


def get_recovery_chain_state(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> RecoveryChainState:
    snapshot = _build_recovery_chain_snapshot(store, task, read_context=read_context)
    steps = snapshot.steps
    if not steps:
        return RecoveryChainState(
            role="original",
            steps=(),
            root_task_id=snapshot.root_task.id,
            resolved_task_id=snapshot.completed_terminal_descendant.id if snapshot.completed_terminal_descendant else None,
        )
    return RecoveryChainState(
        role=steps[-1],
        steps=steps,
        root_task_id=snapshot.root_task.id,
        resolved_task_id=snapshot.completed_terminal_descendant.id if snapshot.completed_terminal_descendant else None,
    )


def get_recovery_chain_root_task_id(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> str | None:
    """Return the recovery-only lineage root for a task."""
    return _build_recovery_chain_snapshot(store, task, read_context=read_context).root_task.id


def _task_for_recovery_snapshot_id(
    store: SqliteTaskStore,
    *,
    task_id: str,
    read_context: RecoveryReadContext | None,
) -> DbTask | None:
    if read_context is not None:
        return read_context.get_task(task_id)
    return store.get(task_id)


def _recovery_policy_epoch(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    snapshot: _RecoveryChainSnapshot,
    read_context: RecoveryReadContext | None = None,
) -> _RecoveryPolicyEpoch:
    if not store.supports_parked_task_rearms():
        return _RecoveryPolicyEpoch(effective_rearmed_at=None, effective_steps=snapshot.steps)

    latest_rearmed_at: datetime | None = None
    for ancestor_id in snapshot.ancestor_ids:
        rearm_state = store.get_parked_task_rearm(
            subject_kind="task",
            subject_id=ancestor_id,
            attention_reason=RETRY_LIMIT_REACHED_ATTENTION_REASON,
        )
        if rearm_state is None:
            continue
        for candidate_time in (
            rearm_state.manual_rearmed_at if rearm_state.manual_rearm_epoch > 0 else None,
            rearm_state.last_auto_attempt_at,
        ):
            if candidate_time is None:
                continue
            if latest_rearmed_at is None or candidate_time > latest_rearmed_at:
                latest_rearmed_at = candidate_time

    if latest_rearmed_at is None:
        return _RecoveryPolicyEpoch(effective_rearmed_at=None, effective_steps=snapshot.steps)

    ancestry_tasks: list[DbTask] = []
    for ancestor_id in snapshot.ancestor_ids:
        ancestor = _task_for_recovery_snapshot_id(store, task_id=ancestor_id, read_context=read_context)
        if ancestor is None:
            return _RecoveryPolicyEpoch(effective_rearmed_at=latest_rearmed_at, effective_steps=())
        ancestry_tasks.append(ancestor)

    effective_steps: list[RecoveryRole] = []
    for child, step in zip(ancestry_tasks[1:], snapshot.steps, strict=False):
        if child.created_at is not None and child.created_at > latest_rearmed_at:
            effective_steps.append(step)

    return _RecoveryPolicyEpoch(
        effective_rearmed_at=latest_rearmed_at,
        effective_steps=tuple(effective_steps),
    )


def has_recovery_chain_ancestor_in_ids(
    store: SqliteTaskStore,
    task: DbTask,
    ancestor_ids: set[str],
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether this failed task is owned by a completed task already in the plan."""
    snapshot = _build_recovery_chain_snapshot(store, task, read_context=read_context)
    if any(task_id in ancestor_ids for task_id in snapshot.ancestor_ids[:-1]):
        return True
    parent = (
        read_context.get_task(snapshot.root_task.based_on)
        if read_context is not None and snapshot.root_task.based_on
        else store.get(snapshot.root_task.based_on) if snapshot.root_task.based_on else None
    )
    if parent and parent.id and snapshot.root_task.task_type in {"improve", "rebase"} and parent.task_type == "implement":
        return parent.id in ancestor_ids
    return False


def get_completed_recovery_descendant(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Return the terminal completed recovery descendant when a failed chain is fully resolved."""
    if task.id is None or task.status != "failed":
        return None
    return _build_recovery_chain_snapshot(store, task, read_context=read_context).completed_terminal_descendant


def get_completed_sibling_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Return the newest completed automatic sibling recovery that resolves this failed task's parent."""
    if (
        task.id is None
        or task.status != "failed"
        or task.based_on is None
        or task.recovery_origin not in {"resume", "retry"}
    ):
        return None

    parent = read_context.get_task(task.based_on) if read_context is not None else store.get(task.based_on)
    if parent is None or parent.id is None:
        return None

    candidates: list[DbTask] = []
    siblings = (
        read_context.get_based_on_children_by_type(parent.id, task.task_type)
        if read_context is not None
        else store.get_based_on_children_by_type(parent.id, task.task_type)
    )
    for sibling in siblings:
        if sibling.id is None or sibling.id == task.id:
            continue
        if _classify_recovery_edge(parent, sibling) is None:
            continue
        if _task_is_complete_recovery_outcome(store, sibling, read_context=read_context):
            candidates.append(sibling)
            continue
        completed_descendant = _build_recovery_chain_snapshot(
            store,
            sibling,
            read_context=read_context,
        ).completed_terminal_descendant
        if completed_descendant is not None:
            candidates.append(completed_descendant)

    if not candidates:
        return None
    return max(candidates, key=_descendant_sort_key)


def get_completed_same_slice_sibling_attempt(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Return the newest lifecycle-complete same-slice implement sibling, if provable."""
    if (
        task.id is None
        or task.status != "failed"
        or task.task_type != "implement"
        or task.based_on is None
        or _failed_task_has_active_nonterminal_merge_unit(store, task, read_context=read_context)
    ):
        return None

    task_identity = resolve_implement_slice_identity(
        prompt=task.prompt,
        review_scope=task.review_scope,
    )
    if task_identity is None:
        return None

    siblings = (
        read_context.get_based_on_children_by_type(task.based_on, task.task_type)
        if read_context is not None
        else store.get_based_on_children_by_type(task.based_on, task.task_type)
    )
    candidates: list[DbTask] = []
    for sibling in siblings:
        if sibling.id is None or sibling.id == task.id or sibling.status == "dropped":
            continue
        sibling_identity = resolve_implement_slice_identity(
            prompt=sibling.prompt,
            review_scope=sibling.review_scope,
        )
        if sibling_identity != task_identity:
            continue
        if not _task_has_explicit_terminal_merge_proof_for_same_slice_helper(
            store,
            sibling,
            read_context=read_context,
        ):
            continue
        candidates.append(sibling)

    if not candidates:
        return None
    return max(candidates, key=_descendant_sort_key)


def _resolve_impl_ancestor_by_based_on(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Resolve the implementation ancestor by walking structured based_on edges."""
    visited: set[str] = set()
    current: DbTask | None = task
    while current is not None:
        if current.id is not None:
            if current.id in visited:
                return None
            visited.add(current.id)
        if current.task_type == "implement":
            return current
        if current.based_on is None:
            return None
        current = read_context.get_task(current.based_on) if read_context is not None else store.get(current.based_on)
    return None


def _resolve_review_target_implement(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Resolve the implementation task a review was created to evaluate."""
    candidate_ids = tuple(target_id for target_id in (task.depends_on, task.based_on) if target_id is not None)
    if not candidate_ids:
        return None

    candidates: list[DbTask] = []
    seen_ids: set[str] = set()
    for candidate_id in candidate_ids:
        if candidate_id in seen_ids:
            continue
        seen_ids.add(candidate_id)
        candidate = read_context.get_task(candidate_id) if read_context is not None else store.get(candidate_id)
        if candidate is None or candidate.task_type != "implement":
            continue
        candidates.append(candidate)

    if not candidates:
        return None
    unique_ids = {candidate.id for candidate in candidates}
    if len(unique_ids) != 1:
        return None
    return candidates[0]


def _resolve_merged_target_task(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Return the structured implementation target for review/improve/rebase tasks."""
    if task.task_type == "review":
        return _resolve_review_target_implement(store, task, read_context=read_context)
    if task.task_type in {"improve", "rebase"}:
        return _resolve_impl_ancestor_by_based_on(store, task, read_context=read_context)
    return None


def _effective_merge_target_branch(
    store: SqliteTaskStore,
    *,
    merge_context: _MergeContext | None = None,
) -> str:
    merge_context = merge_context or _load_merge_context(_project_dir_for_store(store))
    return _resolve_merge_context_target_branch(store, merge_context)


def is_resolved_by_merged_target(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether a failed side-quest task is obsolete because its target impl merged."""
    if task.id is None or task.status != "failed" or task.task_type not in _MERGED_TARGET_RESOLUTION_TYPES:
        return False
    if task.task_type == "improve" and task.same_branch:
        # Same-branch improve tasks can represent real post-merge follow-up work.
        return False
    target_task = _resolve_merged_target_task(store, task, read_context=read_context)
    if target_task is None:
        return False
    return task_is_merged(store, target_task, read_context=read_context)


def build_merge_context_from_git(git: Git, target_branch: str | None) -> _MergeContext:
    """Construct a _MergeContext from an already-live Git instance.

    Callers that already hold a constructed Git object and know the target branch
    should use this instead of _load_merge_context so no ambient Config.load(discover=True)
    or Git() construction occurs.
    """
    merge_context = _MergeContext(git=git, default_branch=target_branch)
    try:
        merge_context.existing_branches = frozenset(git.local_branch_names())
    except (GitError, OSError, ValueError) as exc:
        _record_repository_inspection_warning(
            merge_context,
            key="local-branch-list",
            message=_branch_reachability_warning(
                "failed to list local branches for recovery-lane batch inspection: "
                f"{exc}"
            ),
        )
        merge_context.existing_branches = None
    return merge_context


def _load_merge_context(project_dir: Path | None = None) -> _MergeContext:
    try:
        config = Config.load(project_dir or Path.cwd(), discover=True)
        git = Git(config.project_dir)
        merge_context = _MergeContext(
            git=git,
            default_branch=git.default_branch(),
        )
        try:
            merge_context.existing_branches = frozenset(git.local_branch_names())
        except (GitError, OSError, ValueError) as exc:
            _record_repository_inspection_warning(
                merge_context,
                key="local-branch-list",
                message=_branch_reachability_warning(
                    "failed to list local branches for recovery-lane batch inspection: "
                    f"{exc}"
                ),
            )
            merge_context.existing_branches = None
        return merge_context
    except (ConfigError, GitError, OSError, ValueError) as exc:
        merge_context = _MergeContext(git=None, default_branch=None, resolution_error=str(exc))
        _record_repository_inspection_warning(
            merge_context,
            key="merge-context-load",
            message=_branch_reachability_warning(f"failed to load repository default-branch context: {exc}"),
        )
        return merge_context


def _resolve_merge_context_target_branch(
    store: SqliteTaskStore,
    merge_context: _MergeContext,
) -> str:
    if merge_context.default_branch:
        return merge_context.default_branch
    project_dir = _project_dir_for_store(store)
    if project_dir is None:
        return store.default_merge_target(strict=False)
    detail = merge_context.resolution_error or "Git returned no default branch"
    raise MergeTargetResolutionError(
        f"Could not determine default merge target for recovery decisions in {project_dir}: {detail}"
    )


def _project_dir_for_store(store: SqliteTaskStore) -> Path | None:
    project_root = getattr(store, "_project_root", None)
    if isinstance(project_root, Path):
        return project_root
    db_parent = store.db_path.parent
    if db_parent.name == ".gza":
        candidate = db_parent.parent
        if (candidate / "gza.yaml").exists():
            return candidate
    return None


def _read_failed_task_log_text(store: SqliteTaskStore, task: DbTask) -> str:
    if not task.log_file:
        return ""
    project_dir = _project_dir_for_store(store)
    stored_path = Path(task.log_file)
    if stored_path.is_absolute():
        conversation_path = stored_path
    elif project_dir is None:
        return ""
    else:
        conversation_path = project_dir / stored_path
    ops_path = ops_log_path_for(conversation_path)
    chunks: list[str] = []
    for path in (conversation_path, ops_path):
        if not path.exists():
            continue
        try:
            chunks.append(path.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            continue
    return "\n".join(chunks).casefold()


def _persist_historical_rebase_success_reconciliation(
    store: SqliteTaskStore,
    task: DbTask,
) -> None:
    original_completed_at = task.completed_at
    head_sha = getattr(task, "head_sha", None)
    base_sha = getattr(task, "base_sha", None)
    store.mark_completed(
        task,
        branch=task.branch,
        log_file=task.log_file,
        report_file=task.report_file,
        output_content=task.output_content,
        has_commits=bool(task.has_commits),
        stats=TaskStats(
            duration_seconds=task.duration_seconds,
            num_steps_reported=task.num_steps_reported,
            num_steps_computed=task.num_steps_computed,
            num_turns_reported=task.num_turns_reported,
            num_turns_computed=task.num_turns_computed,
            cost_usd=task.cost_usd,
            input_tokens=task.input_tokens,
            output_tokens=task.output_tokens,
        ),
        diff_files_changed=task.diff_files_changed,
        diff_lines_added=task.diff_lines_added,
        diff_lines_removed=task.diff_lines_removed,
        changed_diff=task.changed_diff,
        head_sha=head_sha,
        base_sha=base_sha,
        completion_reason=task.completion_reason,
    )
    if original_completed_at is not None:
        task.completed_at = original_completed_at
        store.update(task)


def _reconcile_historical_rebase_success(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    if task.status != "failed" or task.task_type != "rebase" or (task.failure_reason or "UNKNOWN") != "GIT_ERROR":
        return False
    log_text = _read_failed_task_log_text(store, task)
    if not log_text:
        return False
    if is_readonly_db_failure(log_text):
        return False
    if any(marker in log_text for marker in _REBASE_INFRA_LOG_MARKERS):
        return False
    if not any(marker in log_text for marker in _REBASE_COMPLETED_LOG_MARKERS):
        return False
    if not any(marker in log_text for marker in _REBASE_CHECKS_PASSED_LOG_MARKERS):
        return False
    if any(marker in log_text for marker in _REBASE_CHECKS_FAILED_LOG_MARKERS):
        return False
    if read_context is not None and not read_context.allow_reconcile_mutation:
        read_context.record_rebase_success_reconciliation(task)
        return True
    _persist_historical_rebase_success_reconciliation(store, task)
    return True


def _normalize_failed_rebase_reason(store: SqliteTaskStore, task: DbTask, reason: str) -> str:
    if task.task_type != "rebase" or reason != "GIT_ERROR":
        return reason
    log_text = _read_failed_task_log_text(store, task)
    if not log_text:
        return reason
    if is_readonly_db_failure(log_text):
        return "INFRASTRUCTURE_ERROR"
    if any(marker in log_text for marker in _REBASE_INFRA_LOG_MARKERS):
        return "INFRASTRUCTURE_ERROR"
    return reason


def rebase_failure_requires_manual_resolution(store: SqliteTaskStore, task: DbTask) -> bool:
    """Return whether a failed rebase must stay parked for manual resolution."""
    if task.task_type != "rebase" or task.status != "failed":
        return False
    normalized_reason = _normalize_failed_rebase_reason(store, task, task.failure_reason or "UNKNOWN")
    if normalized_reason == "REBASE_CONFLICT":
        return True
    return classify_failure_reason(normalized_reason) == "manual"


def _task_lineage_branch_keys(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> set[str]:
    keys: set[str] = set()
    if task.branch:
        keys.add(task.branch)
    target_task = _resolve_merged_target_task(store, task, read_context=read_context)
    if target_task is not None and target_task.branch:
        keys.add(target_task.branch)
    return keys


def _resolve_task_merge_unit(
    store: SqliteTaskStore,
    task_id: str,
    *,
    read_context: RecoveryReadContext | None = None,
):
    if read_context is not None:
        return read_context.resolve_merge_unit_for_task(task_id)
    return store.resolve_merge_unit_for_task(task_id)


def _is_resolved_by_landed_merge_unit_or_owner(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether DB lifecycle state already proves this failed work landed.

    Owner-row and watch recovery scans call this before any lineage traversal so
    settled merged units contribute near-zero work regardless of age.
    """
    if task.id is None:
        return False
    unit = _resolve_task_merge_unit(store, task.id, read_context=read_context)
    if unit is not None:
        if unit.state == "merged":
            return True
        owner_task_id = unit.owner_task_id
        if owner_task_id and owner_task_id != task.id:
            owner_unit = _resolve_task_merge_unit(store, owner_task_id, read_context=read_context)
            if owner_unit is not None and owner_unit.state == "merged":
                return True
    return False


def _is_independent_follow_up_root(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether the task roots a recovery chain under a non-recovery follow-up."""
    if not task.based_on or task.id is None:
        return False
    parent = read_context.get_task(task.based_on) if read_context is not None else store.get(task.based_on)
    if parent is None:
        return False
    if parent.task_type == task.task_type:
        return _classify_recovery_edge(parent, task) is None
    return True


def _is_resolved_by_landed_lineage(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    merge_context: _MergeContext,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    # This helper only suppresses failed rows during failed-task recovery.
    if task.id is None or task.status != "failed":
        return False
    if _is_resolved_by_landed_merge_unit_or_owner(store, task, read_context=read_context):
        return True

    prefer_explicit_recovery = _is_resumable_timeout_implementation(task)
    target_branch: str | None = None
    if task.branch and not prefer_explicit_recovery:
        target_branch = _effective_merge_target_branch(store, merge_context=merge_context)

    if merge_context.git is not None and target_branch is not None and task.branch:
        try:
            branch_resolution_key = (task.branch, task.has_commits)
            branch_merge_state = merge_context.branch_resolution.get(branch_resolution_key)
            if branch_merge_state is None:
                if merge_context.existing_branches is not None:
                    branch_exists = task.branch in merge_context.existing_branches
                else:
                    branch_exists = merge_context.git.branch_exists(task.branch)
                branch_merge_state = "unmerged"
                if branch_exists:
                    merged_proof = merge_context.git.is_merged(task.branch, target_branch)
                    if merged_proof:
                        branch_merge_state = classify_branch_merge_state_for_target(
                            git=merge_context.git,
                            source_branch=task.branch,
                            target_branch=target_branch,
                            merged_proof=merged_proof,
                            source_has_commits=task.has_commits,
                        ).state
                merge_context.branch_resolution[branch_resolution_key] = branch_merge_state
            branch_merged = branch_merge_state == "merged"
            if branch_merged:
                return True
        except (GitError, AttributeError) as exc:
            _record_repository_inspection_warning(
                merge_context,
                key="branch-reachability-check",
                message=_branch_reachability_warning(
                    f"failed to check whether branch '{task.branch}' reached "
                    f"default branch '{target_branch}': {exc}"
                ),
            )
            pass

    branch_keys = _task_lineage_branch_keys(store, task, read_context=read_context)
    if not branch_keys:
        return False

    recovery_snapshot = _build_recovery_chain_snapshot(store, task, read_context=read_context)
    independent_follow_up_root_id = (
        recovery_snapshot.root_task.id
        if _is_independent_follow_up_root(store, recovery_snapshot.root_task, read_context=read_context)
        else None
    )

    if read_context is not None:
        lineage = read_context.build_lineage(read_context.resolve_lineage_root(task))
    else:
        from .query import build_lineage, resolve_lineage_root

        lineage = tuple(build_lineage(store, resolve_lineage_root(store, task)))
    for lineage_task in lineage:
        if lineage_task.id == task.id:
            continue
        merge_state = lineage_task.merge_status
        if lineage_task.id is not None:
            unit = (
                read_context.resolve_merge_unit_for_task(lineage_task.id)
                if read_context is not None
                else store.resolve_merge_unit_for_task(lineage_task.id)
            )
            if unit is not None:
                merge_state = unit.state
        if merge_state != "merged":
            continue
        if lineage_task.id == independent_follow_up_root_id:
            continue
        if lineage_task.status not in _MERGEABLE_EXECUTION_STATUSES:
            continue
        if branch_keys & _task_lineage_branch_keys(store, lineage_task, read_context=read_context):
            return True
    return False


def resolve_recovery_planning_task(store: SqliteTaskStore, task: DbTask) -> DbTask:
    """Return the task that should own normal lifecycle planning for this lineage."""
    if task.status != "failed":
        return task
    _reconcile_historical_prerequisite_unmerged_failure(store, task)
    snapshot = _build_recovery_chain_snapshot(store, task)
    return snapshot.latest_completed_terminal_descendant or task


def is_chain_resolved_by_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether a failed task's recovery-only chain ends in a completed task."""
    if task.id is None or task.status != "failed":
        return False
    return _build_recovery_chain_snapshot(store, task, read_context=read_context).completed_terminal_descendant is not None


def list_failed_tasks_for_recovery(
    store: SqliteTaskStore,
    *,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    warnings: list[str] | None = None,
    read_context: RecoveryReadContext | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
) -> list[DbTask]:
    if read_context is not None and isinstance(read_context.merge_context, _MergeContext):
        merge_context = read_context.merge_context
    elif isinstance(git, Git) and target_branch is not None:
        merge_context = build_merge_context_from_git(git, target_branch)
    else:
        merge_context = _load_merge_context(_project_dir_for_store(store))
    if read_context is not None and read_context.merge_context is None:
        read_context.merge_context = merge_context
    failed = list(read_context.failed_tasks()) if read_context is not None else [task for task in store.get_all() if task.status == "failed"]
    if read_context is not None and read_context.recovery_scope_task_ids is not None:
        failed = [
            task
            for task in failed
            if task.id is not None and task.id in read_context.recovery_scope_task_ids
        ]
    if tags:
        from .task_query import normalize_tag_filters, task_matches_tag_filters

        normalized = normalize_tag_filters(tags)
        failed = [
            task
            for task in failed
            if task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized, any_tag=any_tag)
        ]
    failed = [task for task in failed if not is_chain_resolved_by_recovery(store, task, read_context=read_context)]
    failed = [
        task
        for task in failed
        if not is_recovery_suppressed_by_inactive_merge_unit(store, task, read_context=read_context)
    ]
    failed = [task for task in failed if not is_resolved_by_merged_target(store, task, read_context=read_context)]
    failed = [
        task
        for task in failed
        if not _is_resolved_by_landed_lineage(
            store,
            task,
            merge_context=merge_context,
            read_context=read_context,
        )
    ]
    failed = [
        task
        for task in failed
        if get_completed_same_slice_sibling_attempt(store, task, read_context=read_context) is None
    ]
    failed = [
        task
        for task in failed
        if _failed_task_requires_operator_recovery(
            store,
            task,
            merge_context=merge_context,
            read_context=read_context,
        )
    ]
    if warnings is not None:
        warnings.extend(merge_context.repository_inspection_warnings)
    return sort_failed_tasks(failed)


def _policy_attempt_counters(
    chain: RecoveryChainState | _RecoveryPolicyEpoch,
    *,
    max_recovery_attempts: int,
    consumed_attempts: int = 0,
) -> tuple[int, int]:
    if max_recovery_attempts <= 0:
        return (0, 0)
    attempt_limit = 2
    # Display counters should reflect the bounded shared policy budget, not raw
    # based_on depth, so exhausted chains saturate at N/N instead of N+1/N.
    attempt_index = min(max(len(chain.effective_steps if isinstance(chain, _RecoveryPolicyEpoch) else chain.steps) + 1, consumed_attempts + 1), attempt_limit)
    return (attempt_index, attempt_limit)


def _list_unresolved_recovery_terminal_descendants(snapshot: _RecoveryChainSnapshot) -> list[DbTask]:
    """Return terminal recovery descendants that leave the chain unresolved."""
    return [
        descendant
        for descendant in snapshot.terminal_descendants
        if descendant.status in _UNRESOLVED_RECOVERY_TERMINAL_STATUSES and descendant.id is not None
    ]


def _matching_failed_terminal_descendants(
    store: SqliteTaskStore,
    snapshot: _RecoveryChainSnapshot,
    *,
    expected_action: RecoveryAction | None,
    effective_rearmed_at: datetime | None = None,
    read_context: RecoveryReadContext | None = None,
) -> list[DbTask]:
    if expected_action is None:
        return []
    return [
        descendant
        for descendant in snapshot.terminal_descendants
        if descendant.status == "failed"
        and (
            effective_rearmed_at is None
            or descendant.created_at is None
            or descendant.created_at > effective_rearmed_at
        )
        and classify_recovery_row(store, descendant, read_context=read_context) == expected_action
    ]


def _expected_recovery_action(
    task: DbTask,
    *,
    reason: str | None = None,
    chain: RecoveryChainState,
    prerequisite_reconciliation: PrerequisiteUnmergedReconciliation | None = None,
) -> RecoveryAction | None:
    reason = reason or task.failure_reason or "UNKNOWN"
    category = classify_failure_reason(reason)

    if reason == "PREREQUISITE_UNMERGED":
        if prerequisite_reconciliation != "recoverable_real_work":
            return None
        if task.session_id is not None:
            if chain.role in {"original", "retry"}:
                return "resume"
            return None
        if chain.role == "original":
            return "retry"
        return None

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
    if category == "reconcile":
        if chain.role == "original":
            return "reconcile"
        return None

    return None


def _task_has_provider_output(task: DbTask) -> bool:
    return bool(task.output_content or task.report_file)


def _task_has_recoverable_real_work(task: DbTask) -> bool:
    return bool(task.has_commits or _task_has_provider_output(task))


def _prerequisite_unmerged_has_recoverable_real_work(task: DbTask) -> bool:
    return _task_has_recoverable_real_work(task)


def _reconcile_recovery_has_branch(task: DbTask) -> bool:
    """Return whether a reconcile recovery has a concrete branch to operate on."""
    return isinstance(task.branch, str) and bool(task.branch.strip())


def _persist_prerequisite_unmerged_no_work_reconciliation(
    store: SqliteTaskStore,
    task: DbTask,
    merge_state: TerminalNoWorkMergeState,
) -> None:
    if task.id is None:
        return
    unit = store.resolve_merge_unit_for_task(task.id)
    if unit is None:
        unit = store.get_or_create_merge_unit_for_task(task)
    if unit is not None and unit.state != merge_state:
        store.set_merge_unit_state(unit.id, merge_state)


def apply_pending_recovery_reconciliations(
    store: SqliteTaskStore,
    *,
    read_context: RecoveryReadContext | None,
) -> None:
    if read_context is None:
        return
    pending_legacy_branch_publication = tuple(read_context.pending_legacy_branch_publication_reconciliations.values())
    read_context.pending_legacy_branch_publication_reconciliations.clear()
    for task in pending_legacy_branch_publication:
        store.update(task)
    pending = tuple(read_context.pending_prerequisite_no_work_reconciliations.values())
    read_context.pending_prerequisite_no_work_reconciliations.clear()
    for task, merge_state in pending:
        _persist_prerequisite_unmerged_no_work_reconciliation(store, task, merge_state)
    pending_rebase_success = tuple(read_context.pending_rebase_success_reconciliations.values())
    read_context.pending_rebase_success_reconciliations.clear()
    for task in pending_rebase_success:
        _persist_historical_rebase_success_reconciliation(store, task)


def _reconcile_historical_prerequisite_unmerged_failure(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    merge_context: _MergeContext | None = None,
    read_context: RecoveryReadContext | None = None,
) -> PrerequisiteUnmergedReconciliation:
    """Reconcile historical pre-provider dependency failures into moot empty work when proven.

    This is the one explicit mutation point for legacy ``PREREQUISITE_UNMERGED``
    rows created before the runner started parking never-ran dependency-blocked
    tasks back in ``pending``.
    """
    if task.status != "failed" or (task.failure_reason or "UNKNOWN") != "PREREQUISITE_UNMERGED":
        return "parked_unknown"
    if task.depends_on is None:
        return "parked_unknown"

    resolved_dependency = (
        read_context.resolve_dependency_completion(task)
        if read_context is not None
        else store.resolve_dependency_completion(task)
    )
    if task.depends_on and resolved_dependency is None:
        return "dependency_not_ready"
    if get_unmerged_dependency_precondition(store, task, read_context=read_context) is not None:
        return "dependency_not_ready"

    if task.id is not None:
        unit = (
            read_context.resolve_merge_unit_for_task(task.id)
            if read_context is not None
            else store.resolve_merge_unit_for_task(task.id)
        )
        if unit is not None:
            stored_state = effective_no_work_merge_state(task, unit.state)
            if stored_state in {"empty", "redundant"}:
                return "moot_redundant" if stored_state == "redundant" else "moot_empty"

    if task.branch:
        try:
            resolved_merge_context = merge_context or _load_merge_context(_project_dir_for_store(store))
            target_branch = _resolve_merge_context_target_branch(store, resolved_merge_context)
            if resolved_merge_context.git is None:
                return "parked_unknown"
            resolved_state = resolve_task_merge_state_for_target(
                store=store,
                task=task,
                git=resolved_merge_context.git,
                target_branch=target_branch,
            )
            if resolved_state in {"merged", "unmerged"}:
                return "recoverable_real_work"
            if resolved_state not in {"empty", "redundant"}:
                return (
                    "recoverable_real_work"
                    if _prerequisite_unmerged_has_recoverable_real_work(task)
                    else "parked_unknown"
                )
        except MergeTargetResolutionError:
            return "parked_unknown"

        no_work_reconciliation: PrerequisiteUnmergedReconciliation = (
            "moot_redundant" if resolved_state == "redundant" else "moot_empty"
        )
        resolved_no_work_state: TerminalNoWorkMergeState = "redundant" if resolved_state == "redundant" else "empty"
        if read_context is not None and not read_context.allow_reconcile_mutation:
            read_context.record_prerequisite_no_work_reconciliation(task, resolved_no_work_state)
            return no_work_reconciliation
        _persist_prerequisite_unmerged_no_work_reconciliation(store, task, resolved_no_work_state)
        return no_work_reconciliation

    if _prerequisite_unmerged_has_recoverable_real_work(task):
        return "recoverable_real_work"

    return "moot_empty"


def _prerequisite_reconciliation_merge_state(
    reconciliation: PrerequisiteUnmergedReconciliation,
) -> TerminalNoWorkMergeState | None:
    if reconciliation == "moot_empty":
        return "empty"
    if reconciliation == "moot_redundant":
        return "redundant"
    return None


def _failed_task_requires_operator_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    merge_context: _MergeContext,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    if _reconcile_historical_rebase_success(store, task, read_context=read_context):
        return False

    def _resolved_recovery_merge_state() -> str | None:
        merge_state = _task_merge_state_for_recovery(store, task, read_context=read_context)
        if (
            merge_state is None
            and merge_context.git is not None
            and task.branch
        ):
            try:
                target_branch = _resolve_merge_context_target_branch(store, merge_context)
            except MergeTargetResolutionError:
                return None
            return resolve_task_merge_state_for_target(
                store=store,
                task=task,
                git=merge_context.git,
                target_branch=target_branch,
            )
        return merge_state

    reason = _normalize_failed_rebase_reason(store, task, task.failure_reason or "UNKNOWN")
    if reason == "PREREQUISITE_UNMERGED":
        reconciliation = _reconcile_historical_prerequisite_unmerged_failure(
            store,
            task,
            merge_context=merge_context,
            read_context=read_context,
        )
        if reconciliation == "dependency_not_ready":
            return False
        reconciliation_merge_state = _prerequisite_reconciliation_merge_state(reconciliation)
        if reconciliation_merge_state is not None:
            return empty_task_requires_recovery(
                store,
                task,
                merge_state=reconciliation_merge_state,
                merge_context=merge_context,
                read_context=read_context,
            )
        if reconciliation == "recoverable_real_work":
            return True
        merge_state = _resolved_recovery_merge_state()
        return merge_state not in {"empty", "redundant"} or empty_task_requires_recovery(
            store,
            task,
            merge_state=merge_state,
            merge_context=merge_context,
            read_context=read_context,
        )
    merge_state = _resolved_recovery_merge_state()
    return merge_state not in {"empty", "redundant"} or empty_task_requires_recovery(
        store,
        task,
        merge_state=merge_state,
        merge_context=merge_context,
        read_context=read_context,
    )


def _skip_decision(
    *,
    task_id: str,
    reason_code: str,
    reason_text: str,
    attempt_index: int,
    attempt_limit: int,
    recovery_task_id: str | None = None,
) -> FailedRecoveryDecision:
    return FailedRecoveryDecision(
        task_id=task_id,
        action="skip",
        reason_code=reason_code,
        reason_text=reason_text,
        launch_mode="none",
        attempt_index=attempt_index,
        attempt_limit=attempt_limit,
        recovery_task_id=recovery_task_id,
    )


def _single_descendant_id_with_status(tasks: list[DbTask], *, status: str) -> str | None:
    matching_ids = sorted(
        {task.id for task in tasks if task.status == status and task.id is not None},
        key=task_id_numeric_key,
    )
    if len(matching_ids) != 1:
        return None
    return matching_ids[0]


def decide_failed_task_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    max_recovery_attempts: int,
    merge_context: _MergeContext | None = None,
    read_context: RecoveryReadContext | None = None,
) -> FailedRecoveryDecision:
    assert task.id is not None
    task_id = str(task.id)
    launch_mode: Literal["iterate", "worker", "none"] = "iterate" if task.task_type == "implement" else "worker"
    snapshot = _build_recovery_chain_snapshot(store, task, read_context=read_context)
    chain = get_recovery_chain_state(store, task, read_context=read_context)
    policy_epoch = _recovery_policy_epoch(store, task, snapshot=snapshot, read_context=read_context)
    attempt_index, attempt_limit = _policy_attempt_counters(chain, max_recovery_attempts=max_recovery_attempts)

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

    if is_recovery_suppressed_by_inactive_merge_unit(store, task, read_context=read_context):
        return _skip_decision(
            task_id=task_id,
            reason_code="merge_unit_superseded",
            reason_text="failed task belongs to a dropped or superseded merge unit",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    if _reconcile_historical_rebase_success(store, task, read_context=read_context):
        return _skip_decision(
            task_id=task_id,
            reason_code="historical_rebase_success_reconciled",
            reason_text="historical rebase log proves task already completed successfully",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    normalized_reason = _normalize_legacy_branch_publication_failure_reason(
        store,
        task,
        read_context=read_context,
    )
    reason = _normalize_failed_rebase_reason(store, task, normalized_reason)
    prerequisite_reconciliation: PrerequisiteUnmergedReconciliation | None = None
    expected_action: RecoveryAction | None
    if reason == "PREREQUISITE_UNMERGED":
        prerequisite_reconciliation = _reconcile_historical_prerequisite_unmerged_failure(
            store,
            task,
            merge_context=merge_context,
            read_context=read_context,
        )
        expected_action = _expected_recovery_action(
            task,
            reason=reason,
            chain=RecoveryChainState(role=policy_epoch.role, steps=policy_epoch.effective_steps),
            prerequisite_reconciliation=prerequisite_reconciliation,
        )
    else:
        expected_action = _expected_recovery_action(
            task,
            reason=reason,
            chain=RecoveryChainState(role=policy_epoch.role, steps=policy_epoch.effective_steps),
        )
    direct_reconcile_attempts = (
        load_branch_publication_state(store, task.id).reconcile_attempts_consumed
        if expected_action == "reconcile"
        else 0
    )
    consumed_attempts = len(
        _matching_failed_terminal_descendants(
            store,
            snapshot,
            expected_action=expected_action,
            effective_rearmed_at=policy_epoch.effective_rearmed_at,
            read_context=read_context,
        )
    ) + direct_reconcile_attempts
    attempt_index, attempt_limit = _policy_attempt_counters(
        policy_epoch,
        max_recovery_attempts=max_recovery_attempts,
        consumed_attempts=consumed_attempts,
    )

    if not _is_resumable_timeout_implementation(task) and is_resolved_by_merged_target(
        store,
        task,
        read_context=read_context,
    ):
        return _skip_decision(
            task_id=task_id,
            reason_code="resolved_by_merged_target",
            reason_text="target implementation already merged",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    same_slice_sibling = get_completed_same_slice_sibling_attempt(
        store,
        task,
        read_context=read_context,
    )
    if same_slice_sibling is not None and same_slice_sibling.id is not None:
        return _skip_decision(
            task_id=task_id,
            reason_code="same_slice_sibling_landed",
            reason_text=f"same-slice sibling attempt {same_slice_sibling.id} already landed",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
            recovery_task_id=same_slice_sibling.id,
        )

    if reason == "PREREQUISITE_UNMERGED":
        reconciliation = prerequisite_reconciliation or "parked_unknown"
        if reconciliation == "dependency_not_ready":
            return _skip_decision(
                task_id=task_id,
                reason_code="dependency_not_ready",
                reason_text="dependency precondition not satisfied",
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
        reconciliation_merge_state = _prerequisite_reconciliation_merge_state(reconciliation)
        if reconciliation_merge_state is not None:
            if empty_task_requires_recovery(
                store,
                task,
                merge_state=reconciliation_merge_state,
                read_context=read_context,
            ):
                return _skip_decision(
                    task_id=task_id,
                    reason_code="legacy_prerequisite_unmerged_parked",
                    reason_text=(
                        f"{reconciliation_merge_state} merge unit is recoverable because provider execution was recorded; "
                        "legacy dependency-merge failure is parked for manual review"
                    ),
                    attempt_index=attempt_index,
                    attempt_limit=attempt_limit,
                )
            return _skip_decision(
                task_id=task_id,
                reason_code="merge_unit_redundant" if reconciliation_merge_state == "redundant" else "merge_unit_empty",
                reason_text=(
                    MOOT_REDUNDANT_LIFECYCLE_DETAIL
                    if reconciliation_merge_state == "redundant"
                    else MOOT_EMPTY_LIFECYCLE_DETAIL
                ),
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
        if reconciliation != "recoverable_real_work":
            return _skip_decision(
                task_id=task_id,
                reason_code="legacy_prerequisite_unmerged_parked",
                reason_text="legacy dependency-merge failure is parked; wait for dependency merge state to reconcile",
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
    merge_state = _task_merge_state_for_recovery(store, task, read_context=read_context)
    if merge_state is None and task.branch:
        _mc = (
            merge_context
            if merge_context is not None
            else (
                read_context.merge_context
                if read_context is not None and isinstance(read_context.merge_context, _MergeContext)
                else _load_merge_context(_project_dir_for_store(store))
            )
        )
        if read_context is not None and read_context.merge_context is None:
            read_context.merge_context = _mc
        if _mc.git is not None:
            try:
                target_branch: str | None = _resolve_merge_context_target_branch(store, _mc)
            except MergeTargetResolutionError as exc:
                logger.warning(
                    "recovery: could not determine merge target for live branch probe of %s: %s",
                    task.branch,
                    exc,
                )
                target_branch = None
            if target_branch:
                try:
                    live_state = resolve_task_merge_state_for_target(
                        store=store,
                        task=task,
                        git=_mc.git,
                        target_branch=target_branch,
                    )
                    if live_state is not None:
                        merge_state = live_state
                except (GitError, MergeTargetResolutionError) as exc:
                    logger.warning(
                        "recovery: live merge-state probe failed for branch %s: %s",
                        task.branch,
                        exc,
                    )
    if merge_state in {"empty", "redundant"}:
        empty_recovery_state = _classify_empty_task_recovery_state(
            store,
            task,
            merge_state=merge_state,
            read_context=read_context,
        )
        if empty_recovery_state == "resolved":
            return _skip_decision(
                task_id=task_id,
                reason_code="terminal_no_work_recovery_already_resolved",
                reason_text="terminal no-work failed task already resolved by landed lineage or completed recovery work",
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
        if empty_recovery_state == "moot":
            return _skip_decision(
                task_id=task_id,
                reason_code="merge_unit_redundant" if merge_state == "redundant" else "merge_unit_empty",
                reason_text=(
                    MOOT_REDUNDANT_LIFECYCLE_DETAIL
                    if merge_state == "redundant"
                    else MOOT_EMPTY_LIFECYCLE_DETAIL
                ),
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
    if reason == "REBASE_CONFLICT":
        return _skip_decision(
            task_id=task_id,
            reason_code="rebase_conflict_requires_manual_resolution",
            reason_text="rebase conflict requires manual resolution",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if reason != "PREREQUISITE_UNMERGED" and classify_failure_reason(reason) == "manual":
        return _skip_decision(
            task_id=task_id,
            reason_code="manual_failure_reason",
            reason_text=f"{reason} requires manual intervention",
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

    if not dependency_readiness(store, task, read_context=read_context).ready:
        return _skip_decision(
            task_id=task_id,
            reason_code="dependency_not_ready",
            reason_text="dependency precondition not satisfied",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    if expected_action is None:
        if reason == "RETRYABLE_PROVIDER_ERROR":
            return _skip_decision(
                task_id=task_id,
                reason_code="retryable_provider_error",
                reason_text="fresh retry already consumed; retryable provider error now requires manual review",
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
        return _skip_decision(
            task_id=task_id,
            reason_code="retry_limit_reached",
            reason_text="automatic recovery stops here; retry limit reached",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if expected_action == "reconcile" and direct_reconcile_attempts >= max_recovery_attempts:
        return _skip_decision(
            task_id=task_id,
            reason_code="retry_limit_reached",
            reason_text="automatic recovery stops here; retry limit reached",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if expected_action == "reconcile" and not _reconcile_recovery_has_branch(task):
        return _skip_decision(
            task_id=task_id,
            reason_code="reconcile_branch_missing",
            reason_text="branch publication failed but the task has no branch to reconcile; manual repair required",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if expected_action == "reconcile":
        launch_mode = "none"

    children = (
        list(read_context.get_based_on_children_by_type(task_id, task.task_type))
        if read_context is not None
        else store.get_based_on_children_by_type(task_id, task.task_type)
    )
    recovery_children = [
        child for child in children
        if _classify_recovery_edge(task, child) is not None
    ]
    matching_children = [
        child for child in recovery_children
        if _classify_recovery_edge(task, child) == expected_action
    ]
    deeper_descendants = list(snapshot.deeper_descendants)
    pending_children = [child for child in matching_children if child.status == "pending" and child.id is not None]
    all_pending_children = [child for child in recovery_children if child.status == "pending" and child.id is not None]
    if any(not dependency_readiness(store, child, read_context=read_context).ready for child in all_pending_children):
        return _skip_decision(
            task_id=task_id,
            reason_code="dependency_not_ready",
            reason_text="dependency precondition not satisfied",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )

    for status, reason_code, reason_text in _DIRECT_CHILD_SUPERSEDED_REASONS:
        if any(child.status == status for child in recovery_children):
            return _skip_decision(
                task_id=task_id,
                reason_code=reason_code,
                reason_text=reason_text,
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
                recovery_task_id=_single_descendant_id_with_status(recovery_children, status=status),
            )
    for status, reason_code, reason_text in _DESCENDANT_SUPERSEDED_REASONS:
        if any(child.status == status for child in deeper_descendants):
            return _skip_decision(
                task_id=task_id,
                reason_code=reason_code,
                reason_text=reason_text,
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
                recovery_task_id=_single_descendant_id_with_status(deeper_descendants, status=status),
            )
    matching_failed_terminals = _matching_failed_terminal_descendants(
        store,
        snapshot,
        expected_action=expected_action,
        effective_rearmed_at=policy_epoch.effective_rearmed_at,
        read_context=read_context,
    )
    unresolved_terminals = [
        descendant
        for descendant in _list_unresolved_recovery_terminal_descendants(snapshot)
        if policy_epoch.effective_rearmed_at is None
        or descendant.created_at is None
        or descendant.created_at > policy_epoch.effective_rearmed_at
    ]
    if any(descendant.status == "dropped" for descendant in unresolved_terminals):
        return _skip_decision(
            task_id=task_id,
            reason_code="recovery_has_newer_unresolved_descendant",
            reason_text="a newer recovery descendant requires manual attention first",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if unresolved_terminals and len(matching_failed_terminals) != len(unresolved_terminals):
        return _skip_decision(
            task_id=task_id,
            reason_code="recovery_has_newer_unresolved_descendant",
            reason_text="a newer recovery descendant requires manual attention first",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if unresolved_terminals and consumed_attempts >= attempt_limit:
        return _skip_decision(
            task_id=task_id,
            reason_code="retry_limit_reached",
            reason_text="automatic recovery stops here; retry limit reached",
            attempt_index=attempt_index,
            attempt_limit=attempt_limit,
        )
    if len(all_pending_children) > 1:
        return _skip_decision(
            task_id=task_id,
            reason_code="recovery_ambiguous",
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
            recovery_task_id=_single_descendant_id_with_status(all_pending_children, status="pending"),
        )
    reason_text = "dependency merge prerequisite now satisfied"
    if reason != "PREREQUISITE_UNMERGED":
        if expected_action == "resume":
            reason_text = f"{reason} with preserved session"
        elif expected_action == "retry":
            reason_text = f"{reason} restart with fresh attempt"
        elif expected_action == "reconcile":
            reason_text = "branch publication failed; reconcile local/origin refs"
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
    read_context: RecoveryReadContext | None = None,
) -> str | None:
    """Return a shared needs-attention reason slug for failed-task skip decisions."""
    if task.id is None:
        return None
    resolved_decision = decision or decide_failed_task_recovery(
        store,
        task,
        max_recovery_attempts=max_recovery_attempts,
        read_context=read_context,
    )
    return _get_failed_recovery_needs_attention_reason(
        store,
        task,
        decision=resolved_decision,
        max_recovery_attempts=max_recovery_attempts,
        seen_task_ids=set(),
        read_context=read_context,
    )


def get_manual_resume_override_descendant(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    decision: FailedRecoveryDecision | None = None,
    max_recovery_attempts: int,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Return the newest failed recovery descendant eligible for manual override."""
    if task.id is None:
        return None
    resolved_decision = decision or decide_failed_task_recovery(
        store,
        task,
        max_recovery_attempts=max_recovery_attempts,
        read_context=read_context,
    )
    if resolved_decision.reason_code not in {"recovery_has_newer_unresolved_descendant", "retry_limit_reached"}:
        return None

    unresolved_descendants = sort_failed_tasks(
        _list_unresolved_recovery_terminal_descendants(
            _build_recovery_chain_snapshot(store, task, read_context=read_context)
        )
    )
    failed_descendants = [
        descendant
        for descendant in unresolved_descendants
        if descendant.status == "failed" and descendant.id is not None
    ]
    if not failed_descendants:
        return None
    return failed_descendants[-1]


def _get_failed_recovery_needs_attention_reason(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    decision: FailedRecoveryDecision,
    max_recovery_attempts: int,
    seen_task_ids: set[str],
    read_context: RecoveryReadContext | None = None,
) -> str | None:
    if task.id is None or decision.action != "skip":
        return None

    task_id = str(task.id)
    if task_id in seen_task_ids:
        return None
    seen_task_ids.add(task_id)

    if decision.reason_code == "automatic_recovery_disabled":
        return "automatic-recovery-disabled"
    if decision.reason_code == "rebase_conflict_requires_manual_resolution":
        return "rebase-failed-needs-manual-resolution"
    if decision.reason_code == "manual_failure_reason":
        return "manual-failure-reason"
    if decision.reason_code == "reconcile_branch_missing":
        return "branch-publication-needs-manual-repair"
    if decision.reason_code == "retryable_provider_error":
        return "retryable-provider-error"
    if decision.reason_code in {"retry_limit_reached", "manual_review_required"}:
        return RETRY_LIMIT_REACHED_ATTENTION_REASON
    if decision.reason_code == "recovery_ambiguous":
        return "recovery-ambiguous"
    if decision.reason_code != "recovery_has_newer_unresolved_descendant":
        return None

    unresolved_descendants = sort_failed_tasks(
        _list_unresolved_recovery_terminal_descendants(
            _build_recovery_chain_snapshot(store, task, read_context=read_context)
        )
    )
    for descendant in unresolved_descendants:
        if descendant.status == "dropped":
            return _UNRESOLVED_RECOVERY_ATTENTION_REASON
        descendant_decision = decide_failed_task_recovery(
            store,
            descendant,
            max_recovery_attempts=max_recovery_attempts,
            read_context=read_context,
        )
        descendant_reason = _get_failed_recovery_needs_attention_reason(
            store,
            descendant,
            decision=descendant_decision,
            max_recovery_attempts=max_recovery_attempts,
            seen_task_ids=seen_task_ids,
            read_context=read_context,
        )
        if descendant_reason is not None:
            return _UNRESOLVED_RECOVERY_ATTENTION_REASON
    return None
