"""Shared operator-facing task state helpers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .db import SqliteTaskStore, Task as DbTask
from .dependency_preconditions import resolved_dependency_satisfies_task_readiness
from .recovery_read_context import RecoveryReadContext

if TYPE_CHECKING:
    from .dependency_preconditions import DependencyReadiness

EMPTY_PREREQ_RELEASE_VALVE_DETAIL = (
    "empty prerequisite; manual release tracked by "
    "gza-4072 / `gza edit --clear-depends-on`"
)
MOOT_EMPTY_LIFECYCLE_DETAIL = "moot (no task commits)"


@dataclass(frozen=True)
class EmptyMergeUnitLookup:
    """Result of checking whether a task resolves to an empty merge unit."""

    is_empty: bool
    warning: str | None = None


def blocked_by_empty_prereq_label(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> str | None:
    """Return specialized blocked wording for dependents held on an empty prerequisite."""
    if task.depends_on is None:
        return None
    dep = _resolve_empty_prereq_candidate(store, task, read_context=read_context)
    if dep is None or dep.id is None:
        return None
    if resolved_dependency_satisfies_task_readiness(store, dep, task, read_context=read_context):
        return None
    return f"blocked by {dep.id} ({EMPTY_PREREQ_RELEASE_VALVE_DETAIL})"


def blocked_by_awaiting_plan_review_label(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
    readiness: DependencyReadiness | None = None,
) -> str | None:
    """Return specialized blocked wording for dependents awaiting held-plan release."""
    if task.depends_on is None:
        return None
    if readiness is None:
        from .dependency_preconditions import dependency_readiness

        readiness = dependency_readiness(store, task, read_context=read_context)
    if readiness.reason != "plan_awaiting_review":
        return None
    plan_id = readiness.blocking_task_id or task.depends_on
    if plan_id is None:
        return "blocked: awaiting plan review"
    return (
        f"blocked: awaiting plan review for {plan_id}; "
        f"release with uv run gza implement {plan_id} "
        f"or uv run gza edit {plan_id} --no-hold-for-review"
    )


def blocked_dependency_label(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
    readiness: DependencyReadiness | None = None,
) -> str | None:
    """Return specialized operator wording for blocked dependencies."""
    return blocked_by_empty_prereq_label(store, task, read_context=read_context) or blocked_by_awaiting_plan_review_label(
        store,
        task,
        read_context=read_context,
        readiness=readiness,
    )


def _resolve_empty_prereq_candidate(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    read_context: RecoveryReadContext | None = None,
) -> DbTask | None:
    """Resolve an empty prerequisite, preferring the direct dependency over retry descendants."""
    if task.depends_on is None:
        return None

    dep = read_context.get_task(task.depends_on) if read_context is not None else store.get(task.depends_on)
    if dep is not None and dep.id is not None:
        unit = (
            read_context.resolve_merge_unit_for_task(dep.id)
            if read_context is not None
            else store.resolve_merge_unit_for_task(dep.id)
        )
        if unit is not None and unit.state == "empty":
            return dep

    dep = read_context.resolve_dependency_completion(task) if read_context is not None else store.resolve_dependency_completion(task)
    if dep is None or dep.id is None:
        return None
    unit = (
        read_context.resolve_merge_unit_for_task(dep.id)
        if read_context is not None
        else store.resolve_merge_unit_for_task(dep.id)
    )
    if unit is None or unit.state != "empty":
        return None
    return dep


def moot_empty_lifecycle_detail(merge_state: str | None) -> str | None:
    """Return the lifecycle label for operator surfaces when a row is empty."""
    if merge_state == "empty":
        return MOOT_EMPTY_LIFECYCLE_DETAIL
    return None


def inspect_empty_merge_unit(
    store: SqliteTaskStore | None,
    task: DbTask,
) -> EmptyMergeUnitLookup:
    """Inspect whether ``task`` resolves to an empty merge unit."""
    if store is None or task.id is None:
        return EmptyMergeUnitLookup(is_empty=False)
    try:
        unit = store.resolve_merge_unit_for_task(task.id)
    except (OSError, sqlite3.Error, ValueError) as exc:
        detail = " ".join(str(exc).split()) or exc.__class__.__name__
        return EmptyMergeUnitLookup(
            is_empty=False,
            warning=f"empty/moot merge-unit lookup unavailable: {detail}",
        )
    return EmptyMergeUnitLookup(is_empty=unit is not None and unit.state == "empty")
