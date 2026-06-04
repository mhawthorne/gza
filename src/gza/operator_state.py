"""Shared operator-facing task state helpers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .db import SqliteTaskStore, Task as DbTask
from .dependency_preconditions import resolved_dependency_satisfies_task_readiness

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


def blocked_by_empty_prereq_label(store: SqliteTaskStore, task: DbTask) -> str | None:
    """Return specialized blocked wording for dependents held on an empty prerequisite."""
    if task.depends_on is None:
        return None
    dep = _resolve_empty_prereq_candidate(store, task)
    if dep is None or dep.id is None:
        return None
    if resolved_dependency_satisfies_task_readiness(store, dep, task):
        return None
    return f"blocked by {dep.id} ({EMPTY_PREREQ_RELEASE_VALVE_DETAIL})"


def _resolve_empty_prereq_candidate(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    """Resolve an empty prerequisite, preferring the direct dependency over retry descendants."""
    if task.depends_on is None:
        return None

    dep = store.get(task.depends_on)
    if dep is not None and dep.id is not None:
        unit = store.resolve_merge_unit_for_task(dep.id)
        if unit is not None and unit.state == "empty":
            return dep

    dep = store.resolve_dependency_completion(task)
    if dep is None or dep.id is None:
        return None
    unit = store.resolve_merge_unit_for_task(dep.id)
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
