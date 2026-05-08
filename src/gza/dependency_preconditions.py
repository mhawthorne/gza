"""Shared dependency precondition helpers."""

from __future__ import annotations

from .db import SqliteTaskStore, Task as DbTask

MERGE_REQUIRED_DEPENDENCY_TASK_TYPES = frozenset({"task", "implement", "improve", "fix", "rebase"})


def get_unmerged_dependency_precondition(
    store: SqliteTaskStore,
    task: DbTask,
    target_branch: str | None = None,
) -> DbTask | None:
    """Return the resolved dependency still requiring a merge before task execution."""
    if task.same_branch or not task.depends_on:
        return None

    dep = store.resolve_dependency_completion(task)
    if dep is None:
        return None
    if dep.task_type not in MERGE_REQUIRED_DEPENDENCY_TASK_TYPES:
        return None
    if dep.id is not None:
        unit = store.resolve_merge_unit_for_task(dep.id, target_branch)
        if unit is not None:
            return None if unit.state == "merged" else dep
    if dep.merge_status == "merged":
        return None
    return dep
