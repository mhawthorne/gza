"""Shared automatic recovery policy for failed tasks."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from .config import Config, ConfigError
from .db import MergeTargetResolutionError, SqliteTaskStore, Task as DbTask, task_id_numeric_key
from .dependency_preconditions import dependency_readiness, get_unmerged_dependency_precondition, task_is_merged
from .failed_task_ordering import sort_failed_tasks
from .failure_policy import is_resumable_failure_reason
from .git import Git, GitError
from .lifecycle_completion import task_is_complete_for_lifecycle
from .merge_state import classify_branch_merge_state_for_target, resolve_task_merge_state_for_target
from .operator_state import MOOT_EMPTY_LIFECYCLE_DETAIL
from .recovery_read_context import RecoveryReadContext

logger = logging.getLogger(__name__)

_ACTIONABLE_TYPES = {"implement", "plan", "explore", "fix", "internal", "review", "improve", "rebase"}
_MANUAL_ONLY_REASONS = {
    "CONFIG_ERROR",
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
    "PROVIDER_EMPTY_TURN",
    "RETRYABLE_PROVIDER_ERROR",
    "WORKER_DIED",
    "NO_ACTIVITY",
}
_TIMEOUT_STYLE_REASONS = frozenset({"MAX_STEPS", "MAX_TURNS", "TIMEOUT", "TERMINATED"})
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

RecoveryAction = Literal["resume", "retry", "skip"]
PendingRecoveryExecutionMode = Literal["resume", "retry"]
RecoveryRole = Literal["original", "resume", "retry"]
FailureCategory = Literal["timeout", "retryable", "manual"]
PrerequisiteUnmergedReconciliation = Literal[
    "dependency_not_ready",
    "moot_empty",
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
        "resolved_by_merged_target",
        "merge_unit_empty",
        "empty_recovery_already_resolved",
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
    branch_resolution: dict[str, str] = field(default_factory=dict)
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
    return unit.state if unit is not None else task.merge_status


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
    """Classify whether an empty failed task is moot, resolved, or still recoverable."""
    if task.status != "failed":
        return "moot"
    resolved_merge_state = (
        merge_state if merge_state is not None else _task_merge_state_for_recovery(store, task, read_context=read_context)
    )
    if resolved_merge_state != "empty":
        return "moot"
    if not _task_has_executed_resumable_session(task):
        return "moot"
    if get_completed_recovery_descendant(store, task, read_context=read_context) is not None:
        return "resolved"
    if get_completed_sibling_recovery(store, task, read_context=read_context) is not None:
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
    """Return whether an empty merge unit still represents recoverable failed work."""
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
    return None


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

    prefer_explicit_recovery = _is_resumable_timeout_implementation(task)
    target_branch: str | None = None
    if task.branch and not prefer_explicit_recovery:
        target_branch = _effective_merge_target_branch(store, merge_context=merge_context)

    if merge_context.git is not None and target_branch is not None and task.branch:
        try:
            branch_merge_state = merge_context.branch_resolution.get(task.branch)
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
                        ).state
                merge_context.branch_resolution[task.branch] = branch_merge_state
            branch_merged = branch_merge_state == "merged" or (
                branch_merge_state == "empty" and _task_has_recoverable_real_work(task)
            )
            if branch_merged:
                return True
        except GitError as exc:
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
) -> list[DbTask]:
    merge_context = (
        read_context.merge_context
        if read_context is not None and isinstance(read_context.merge_context, _MergeContext)
        else _load_merge_context(_project_dir_for_store(store))
    )
    if read_context is not None and read_context.merge_context is None:
        read_context.merge_context = merge_context
    failed = list(read_context.failed_tasks()) if read_context is not None else [task for task in store.get_all() if task.status == "failed"]
    if tags:
        from .task_query import normalize_tag_filters, task_matches_tag_filters

        normalized = normalize_tag_filters(tags)
        failed = [
            task
            for task in failed
            if task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized, any_tag=any_tag)
        ]
    failed = [task for task in failed if not is_chain_resolved_by_recovery(store, task, read_context=read_context)]
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


def _list_unresolved_recovery_terminal_descendants(snapshot: _RecoveryChainSnapshot) -> list[DbTask]:
    """Return terminal recovery descendants that leave the chain unresolved."""
    return [
        descendant
        for descendant in snapshot.terminal_descendants
        if descendant.status in _UNRESOLVED_RECOVERY_TERMINAL_STATUSES and descendant.id is not None
    ]


def _expected_recovery_action(
    task: DbTask,
    *,
    chain: RecoveryChainState,
    prerequisite_reconciliation: PrerequisiteUnmergedReconciliation | None = None,
) -> RecoveryAction | None:
    reason = task.failure_reason or "UNKNOWN"
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

    return None


def _task_has_provider_output(task: DbTask) -> bool:
    return bool(task.output_content or task.report_file)


def _task_has_recoverable_real_work(task: DbTask) -> bool:
    return bool(task.has_commits or _task_has_provider_output(task))


def _prerequisite_unmerged_has_recoverable_real_work(task: DbTask) -> bool:
    return _task_has_recoverable_real_work(task)


def _persist_prerequisite_unmerged_empty_reconciliation(store: SqliteTaskStore, task: DbTask) -> None:
    if task.id is None:
        return
    unit = store.resolve_merge_unit_for_task(task.id)
    if unit is None:
        unit = store.get_or_create_merge_unit_for_task(task)
    if unit is not None and unit.state != "empty":
        store.set_merge_unit_state(unit.id, "empty")


def apply_pending_recovery_reconciliations(
    store: SqliteTaskStore,
    *,
    read_context: RecoveryReadContext | None,
) -> None:
    if read_context is None:
        return
    pending = tuple(read_context.pending_empty_prerequisite_reconciliations.values())
    read_context.pending_empty_prerequisite_reconciliations.clear()
    for task in pending:
        _persist_prerequisite_unmerged_empty_reconciliation(store, task)


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
    if _prerequisite_unmerged_has_recoverable_real_work(task):
        return "recoverable_real_work"

    if task.id is None:
        return "moot_empty"

    unit = (
        read_context.resolve_merge_unit_for_task(task.id)
        if read_context is not None
        else store.resolve_merge_unit_for_task(task.id)
    )
    if unit is not None and unit.state == "empty":
        return "moot_empty"

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
            if resolved_state != "empty":
                return "parked_unknown"
        except MergeTargetResolutionError:
            return "parked_unknown"

        if read_context is not None and not read_context.allow_reconcile_mutation:
            read_context.record_empty_prerequisite_reconciliation(task)
            return "moot_empty"
        _persist_prerequisite_unmerged_empty_reconciliation(store, task)

    return "moot_empty"


def _failed_task_requires_operator_recovery(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    merge_context: _MergeContext,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    reason = task.failure_reason or "UNKNOWN"
    if reason == "PREREQUISITE_UNMERGED":
        reconciliation = _reconcile_historical_prerequisite_unmerged_failure(
            store,
            task,
            merge_context=merge_context,
            read_context=read_context,
        )
        if reconciliation == "dependency_not_ready":
            return False
        if reconciliation == "moot_empty":
            return empty_task_requires_recovery(
                store,
                task,
                merge_state="empty",
                merge_context=merge_context,
                read_context=read_context,
            )
        if reconciliation == "recoverable_real_work":
            return True
        merge_state = _task_merge_state_for_recovery(store, task, read_context=read_context)
        return merge_state != "empty" or empty_task_requires_recovery(
            store,
            task,
            merge_context=merge_context,
            read_context=read_context,
        )
    merge_state = _task_merge_state_for_recovery(store, task, read_context=read_context)
    return merge_state != "empty" or empty_task_requires_recovery(
        store,
        task,
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
    read_context: RecoveryReadContext | None = None,
) -> FailedRecoveryDecision:
    assert task.id is not None
    task_id = str(task.id)
    launch_mode: Literal["iterate", "worker", "none"] = "iterate" if task.task_type == "implement" else "worker"
    chain = get_recovery_chain_state(store, task, read_context=read_context)
    snapshot = _build_recovery_chain_snapshot(store, task, read_context=read_context)
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

    reason = task.failure_reason or "UNKNOWN"
    prerequisite_reconciliation: PrerequisiteUnmergedReconciliation | None = None
    expected_action: RecoveryAction | None
    if reason == "PREREQUISITE_UNMERGED":
        prerequisite_reconciliation = _reconcile_historical_prerequisite_unmerged_failure(
            store,
            task,
            read_context=read_context,
        )
        expected_action = _expected_recovery_action(
            task,
            chain=chain,
            prerequisite_reconciliation=prerequisite_reconciliation,
        )
    else:
        expected_action = _expected_recovery_action(task, chain=chain)

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
        if reconciliation == "moot_empty":
            if empty_task_requires_recovery(store, task, merge_state="empty", read_context=read_context):
                return _skip_decision(
                    task_id=task_id,
                    reason_code="legacy_prerequisite_unmerged_parked",
                    reason_text=(
                        "empty merge unit is recoverable because provider execution was recorded; "
                        "legacy dependency-merge failure is parked for manual review"
                    ),
                    attempt_index=attempt_index,
                    attempt_limit=attempt_limit,
                )
            return _skip_decision(
                task_id=task_id,
                reason_code="merge_unit_empty",
                reason_text=MOOT_EMPTY_LIFECYCLE_DETAIL,
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
    if merge_state == "empty":
        empty_recovery_state = _classify_empty_task_recovery_state(
            store,
            task,
            merge_state=merge_state,
            read_context=read_context,
        )
        if empty_recovery_state == "resolved":
            return _skip_decision(
                task_id=task_id,
                reason_code="empty_recovery_already_resolved",
                reason_text="empty failed task already resolved by landed lineage or completed recovery work",
                attempt_index=attempt_index,
                attempt_limit=attempt_limit,
            )
        if empty_recovery_state == "moot":
            return _skip_decision(
                task_id=task_id,
                reason_code="merge_unit_empty",
                reason_text="moot (empty branch with no recorded provider execution)",
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
    if _list_unresolved_recovery_terminal_descendants(snapshot):
        return _skip_decision(
            task_id=task_id,
            reason_code="recovery_has_newer_unresolved_descendant",
            reason_text="a newer recovery descendant requires manual attention first",
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
    if resolved_decision.reason_code != "recovery_has_newer_unresolved_descendant":
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
    if decision.reason_code == "manual_failure_reason":
        return "manual-failure-reason"
    if decision.reason_code == "retryable_provider_error":
        return "retryable-provider-error"
    if decision.reason_code in {"retry_limit_reached", "manual_review_required"}:
        return "retry-limit-reached"
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
