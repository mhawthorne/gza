"""Shared dependency precondition helpers."""

from __future__ import annotations

from .db import SqliteTaskStore, Task as DbTask

MERGE_REQUIRED_DEPENDENCY_TASK_TYPES = frozenset({"task", "implement", "improve", "fix", "rebase"})


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
    if dep.merge_status == "merged":
        return None
    return dep
