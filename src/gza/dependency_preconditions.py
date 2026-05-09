"""Shared dependency precondition helpers."""

from __future__ import annotations

from .db import SqliteTaskStore, Task as DbTask

MERGE_REQUIRED_DEPENDENCY_TASK_TYPES = frozenset({"task", "implement", "improve", "fix", "rebase"})


def task_is_merged(store: SqliteTaskStore, task: DbTask) -> bool:
    """Return whether the task is merged.

    Merge units are authoritative when present; legacy task-row merge status is
    only a compatibility fallback while a task has no merge unit.
    """
    if task.id is not None:
        unit = store.resolve_merge_unit_for_task(task.id)
        if unit is not None:
            return unit.state == "merged"
    return task.merge_status == "merged"


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
    if task_is_merged(store, dep):
        return None
    return dep
