"""Shared dependency precondition helpers."""

from __future__ import annotations

from dataclasses import dataclass

from .db import SqliteTaskStore, Task as DbTask
from .lifecycle_completion import merge_state_is_terminal_for_lifecycle
from .recovery_read_context import RecoveryReadContext

MERGE_REQUIRED_DEPENDENCY_TASK_TYPES = frozenset({"task", "implement", "improve", "fix", "rebase"})


@dataclass(frozen=True)
class DependencyReadiness:
    """Structured dependency-readiness result shared across lifecycle callers."""

    ready: bool
    reason: str | None = None
    direct_dependency: DbTask | None = None
    resolved_dependency: DbTask | None = None
    blocking_task_id: str | None = None
    blocking_task_status: str | None = None
    blocking_merge_unit_id: str | None = None
    blocking_merge_state: str | None = None
    blocking_merge_unit_owner_task_id: str | None = None
    blocking_source_branch: str | None = None
    blocking_target_branch: str | None = None


def empty_prereq_satisfies_dependency(
    store: SqliteTaskStore,
    prereq: DbTask,
    dependent: DbTask,
) -> bool:
    """Policy hook for whether an empty prerequisite satisfies a dependency.

    Conservative default: an empty prerequisite is not merged work, so
    downstream merge-required tasks stay blocked unless this one policy point is
    deliberately flipped.
    """
    del store, prereq, dependent
    return False


def plan_dependency_awaits_review(task: DbTask) -> bool:
    """Return whether ``task`` is a held plan awaiting explicit release."""
    return task.task_type == "plan" and task.auto_implement is False


def _held_plan_dependency_blocked_readiness(
    direct_dep: DbTask,
    *,
    resolved_dep: DbTask | None,
) -> DependencyReadiness | None:
    """Return the held-plan blocked readiness result for a direct dependency."""
    if not plan_dependency_awaits_review(direct_dep):
        return None
    if direct_dep.status == "dropped":
        return DependencyReadiness(
            ready=False,
            reason="dropped",
            direct_dependency=direct_dep,
            blocking_task_id=direct_dep.id,
            blocking_task_status=direct_dep.status,
        )
    if direct_dep.status == "completed" or resolved_dep is not None:
        return DependencyReadiness(
            ready=False,
            reason="plan_awaiting_review",
            direct_dependency=direct_dep,
            resolved_dependency=resolved_dep or direct_dep,
            blocking_task_id=direct_dep.id,
            blocking_task_status=direct_dep.status,
        )
    return None


def task_is_merged(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether the task no longer requires merge work.

    Merge units are authoritative when present; legacy task-row merge status is
    only a compatibility fallback while a task has no merge unit.
    """
    if task.id is not None:
        unit = (
            read_context.resolve_merge_unit_for_task(task.id)
            if read_context is not None
            else store.resolve_merge_unit_for_task(task.id)
        )
        if unit is not None:
            return merge_state_is_terminal_for_lifecycle(unit.state)
    return merge_state_is_terminal_for_lifecycle(task.merge_status)


def task_satisfies_merge_dependency(
    store: SqliteTaskStore,
    prereq: DbTask,
    dependent: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether a prerequisite satisfies merge-required dependency gating."""
    merge_state = _resolved_merge_state(store, prereq, read_context=read_context)

    if merge_state == "merged":
        return True
    if merge_state == "empty":
        return empty_prereq_satisfies_dependency(store, prereq, dependent)
    return False


def resolved_dependency_satisfies_task_readiness(
    store: SqliteTaskStore,
    prereq: DbTask,
    dependent: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether a resolved completed dependency makes ``dependent`` runnable."""
    if dependent.task_type not in MERGE_REQUIRED_DEPENDENCY_TASK_TYPES:
        return True
    if prereq.task_type not in MERGE_REQUIRED_DEPENDENCY_TASK_TYPES:
        return True
    return task_satisfies_merge_dependency(store, prereq, dependent, read_context=read_context)


def dependency_is_ready(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return whether ``task`` has a dependency state that allows execution/pickup."""
    return dependency_readiness(store, task, read_context=read_context).ready


def _resolved_merge_state(
    store: SqliteTaskStore,
    prereq: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> str | None:
    merge_state = prereq.merge_status
    if prereq.id is not None:
        unit = (
            read_context.resolve_merge_unit_for_task(prereq.id)
            if read_context is not None
            else store.resolve_merge_unit_for_task(prereq.id)
        )
        if unit is not None:
            merge_state = unit.state
    return merge_state


def get_unmerged_dependency_precondition(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Return the resolved dependency still requiring a merge before task execution."""
    readiness = dependency_readiness(store, task, read_context=read_context)
    if readiness.reason != "unmerged":
        return None
    return readiness.resolved_dependency


def dependency_readiness(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DependencyReadiness:
    """Return structured dependency readiness for ``task``."""
    if task.same_branch or not task.depends_on:
        return DependencyReadiness(ready=True)

    direct_dep = read_context.get_task(task.depends_on) if read_context is not None else store.get(task.depends_on)
    if direct_dep is None:
        return DependencyReadiness(
            ready=False,
            reason="missing",
            blocking_task_id=task.depends_on,
            blocking_task_status="missing",
        )

    held_plan_block = _held_plan_dependency_blocked_readiness(direct_dep, resolved_dep=None)
    if held_plan_block is not None:
        return held_plan_block

    resolved_dep = read_context.resolve_dependency_completion(task) if read_context is not None else store.resolve_dependency_completion(task)
    held_plan_block = _held_plan_dependency_blocked_readiness(direct_dep, resolved_dep=resolved_dep)
    if held_plan_block is not None:
        return held_plan_block

    if resolved_dep is None:
        if _resolved_merge_state(store, direct_dep, read_context=read_context) == "empty" and resolved_dependency_satisfies_task_readiness(
            store,
            direct_dep,
            task,
            read_context=read_context,
        ):
            return DependencyReadiness(
                ready=True,
                reason="ready",
                direct_dependency=direct_dep,
                resolved_dependency=direct_dep,
            )
        return DependencyReadiness(
            ready=False,
            reason=direct_dep.status,
            direct_dependency=direct_dep,
            blocking_task_id=direct_dep.id,
            blocking_task_status=direct_dep.status,
        )

    if _resolved_dependency_lineage_satisfies_task_readiness(
        store,
        direct_dep=direct_dep,
        resolved_dep=resolved_dep,
        dependent=task,
        read_context=read_context,
    ):
        return DependencyReadiness(
            ready=True,
            reason="ready",
            direct_dependency=direct_dep,
            resolved_dependency=resolved_dep,
        )

    merge_resolution = (
        read_context.resolve_dependency_merge_unit(task)
        if read_context is not None
        else store.resolve_dependency_merge_unit(task)
    )
    merge_unit = merge_resolution.merge_unit
    merge_unit_owner = (
        store.resolve_merge_unit_owner_task(merge_unit)
        if merge_unit is not None
        else None
    )
    blocking_merge_state = merge_unit.state if merge_unit is not None else resolved_dep.merge_status
    blocking_target_branch = merge_unit.target_branch if merge_unit is not None else store.default_merge_target()
    return DependencyReadiness(
        ready=False,
        reason="unmerged",
        direct_dependency=direct_dep,
        resolved_dependency=resolved_dep,
        blocking_task_id=resolved_dep.id,
        blocking_task_status=resolved_dep.status,
        blocking_merge_unit_id=merge_unit.id if merge_unit is not None else None,
        blocking_merge_state=blocking_merge_state,
        blocking_merge_unit_owner_task_id=(
            merge_unit_owner.id if merge_unit_owner is not None else merge_unit.owner_task_id if merge_unit is not None else None
        ),
        blocking_source_branch=merge_unit.source_branch if merge_unit is not None else resolved_dep.branch,
        blocking_target_branch=blocking_target_branch,
    )


def _resolved_dependency_lineage_satisfies_task_readiness(
    store: SqliteTaskStore,
    *,
    direct_dep: DbTask,
    resolved_dep: DbTask,
    dependent: DbTask,
    read_context: RecoveryReadContext | None = None,
) -> bool:
    """Return readiness using canonical merge-unit state for the dependency lineage."""
    if dependent.task_type not in MERGE_REQUIRED_DEPENDENCY_TASK_TYPES:
        return True
    if resolved_dep.task_type not in MERGE_REQUIRED_DEPENDENCY_TASK_TYPES:
        return True
    merge_resolution = (
        read_context.resolve_dependency_merge_unit(dependent)
        if read_context is not None
        else store.resolve_dependency_merge_unit(dependent)
    )
    merge_unit = merge_resolution.merge_unit
    if merge_unit is None:
        return task_satisfies_merge_dependency(store, resolved_dep, dependent, read_context=read_context)
    if merge_unit.state == "merged":
        return True
    if merge_unit.state == "empty":
        prereq = merge_resolution.attached_task or direct_dep
        return empty_prereq_satisfies_dependency(store, prereq, dependent)
    return False
