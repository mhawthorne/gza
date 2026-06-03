"""Shared dependency precondition helpers."""

from __future__ import annotations

from .db import SqliteTaskStore, Task as DbTask
from .lifecycle_completion import merge_state_is_terminal_for_lifecycle

MERGE_REQUIRED_DEPENDENCY_TASK_TYPES = frozenset({"task", "implement", "improve", "fix", "rebase"})


def empty_prereq_satisfies_dependency(
    store: SqliteTaskStore,
    prereq: DbTask,
    dependent: DbTask,
) -> bool:
    """Policy hook for whether an empty prerequisite satisfies a dependency."""
    del store, prereq, dependent
    return False


def task_is_merged(store: SqliteTaskStore, task: DbTask) -> bool:
    """Return whether the task no longer requires merge work.

    Merge units are authoritative when present; legacy task-row merge status is
    only a compatibility fallback while a task has no merge unit.
    """
    if task.id is not None:
        unit = store.resolve_merge_unit_for_task(task.id)
        if unit is not None:
            return merge_state_is_terminal_for_lifecycle(unit.state)
    return merge_state_is_terminal_for_lifecycle(task.merge_status)


def task_satisfies_merge_dependency(
    store: SqliteTaskStore,
    prereq: DbTask,
    dependent: DbTask,
) -> bool:
    """Return whether a prerequisite satisfies merge-required dependency gating."""
    merge_state = _resolved_merge_state(store, prereq)

    if merge_state == "merged":
        return True
    if merge_state == "empty":
        return empty_prereq_satisfies_dependency(store, prereq, dependent)
    return False


def resolved_dependency_satisfies_task_readiness(
    store: SqliteTaskStore,
    prereq: DbTask,
    dependent: DbTask,
) -> bool:
    """Return whether a resolved completed dependency makes ``dependent`` runnable."""
    if prereq.task_type not in MERGE_REQUIRED_DEPENDENCY_TASK_TYPES:
        return True
    if _resolved_merge_state(store, prereq) != "empty":
        return True
    return empty_prereq_satisfies_dependency(store, prereq, dependent)


def dependency_is_ready(
    store: SqliteTaskStore,
    task: DbTask,
) -> bool:
    """Return whether ``task`` has a dependency state that allows execution/pickup."""
    if task.same_branch or not task.depends_on:
        return True

    dep = store.resolve_dependency_completion(task)
    if dep is None:
        return False
    return resolved_dependency_satisfies_task_readiness(store, dep, task)


def _resolved_merge_state(store: SqliteTaskStore, prereq: DbTask) -> str | None:
    merge_state = prereq.merge_status
    if prereq.id is not None:
        unit = store.resolve_merge_unit_for_task(prereq.id)
        if unit is not None:
            merge_state = unit.state
    return merge_state


def get_unmerged_dependency_precondition(
    store: SqliteTaskStore,
    task: DbTask,
) -> DbTask | None:
    """Return the resolved dependency still requiring a merge before task execution."""
    if task.same_branch or not task.depends_on:
        return None

    dep = store.resolve_dependency_completion(task)
    if dep is None:
        return None
    if dep.task_type not in MERGE_REQUIRED_DEPENDENCY_TASK_TYPES:
        return None
    if task_satisfies_merge_dependency(store, dep, task):
        return None
    return dep
