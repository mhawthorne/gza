"""Shared helpers for resolving task branch lineage."""

from __future__ import annotations

from .db import SqliteTaskStore, Task as DbTask
from .rebase_identity import rebase_persisted_branch_is_authoritative


def resolve_rebase_target_task(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    """Return the canonical lineage task a rebase chain should operate on.

    Active branchful direct rebases, trigger-sourced direct rebases, and active
    branchful retry/resume recovery rebases use their persisted branch as the
    canonical source branch. Legacy branchless rows and terminal historical
    orphan recovery descendants fall back to the lineage walk.
    """
    if task.task_type == "rebase" and task.branch:
        parent_task_type: str | None = None
        if task.based_on is not None:
            parent = store.get(task.based_on)
            parent_task_type = parent.task_type if parent is not None else None
        if rebase_persisted_branch_is_authoritative(task, parent_task_type=parent_task_type):
            return task

    visited_ids: set[str] = set()
    current: DbTask | None = task
    oldest_rebase_with_branch: DbTask | None = None

    while current is not None:
        if current.id is not None:
            if current.id in visited_ids:
                return None
            visited_ids.add(current.id)

        if current.task_type != "rebase":
            return current if current.branch else oldest_rebase_with_branch

        if current.branch:
            oldest_rebase_with_branch = current

        if current.based_on is None:
            break
        current = store.get(current.based_on)

    return oldest_rebase_with_branch


def resolve_rebase_target_branch(store: SqliteTaskStore, task: DbTask) -> str | None:
    """Return the implementation branch a rebase lineage should operate on.

    Active branchful direct and retry/resume rebase rows execute on the same
    persisted source branch used by the singleton guard. Legacy branchless rows
    and terminal historical orphan recovery descendants use lineage fallback.
    """
    target_task = resolve_rebase_target_task(store, task)
    return target_task.branch if target_task is not None else None


def _persist_rebase_base_branch(store: SqliteTaskStore, task: DbTask, target: str) -> str:
    normalized_target = target.strip()
    if not normalized_target:
        return normalized_target
    if task.base_branch == normalized_target or task.id is None:
        return normalized_target
    task.base_branch = normalized_target
    store.update(task)
    return normalized_target


def _resolve_rebase_merge_target_task(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    current: DbTask | None = task
    visited_ids: set[str] = set()

    while current is not None:
        if current.id is not None:
            if current.id in visited_ids:
                return None
            visited_ids.add(current.id)
        if current.task_type != "rebase":
            return current
        if current.based_on is None:
            return None
        current = store.get(current.based_on)

    return None


def resolve_rebase_base_branch(store: SqliteTaskStore, task: DbTask) -> str | None:
    """Return the local target branch for a rebase task.

    Newer rebase rows persist the chosen local target branch at creation time.
    Legacy rows can lack that value; for those, re-derive the canonical local
    target from durable merge-unit metadata for the owning work unit and persist
    the result so future reads stay on the normal fast path.
    """
    if task.task_type != "rebase":
        return None

    persisted_target = (task.base_branch or "").strip()
    if persisted_target:
        return persisted_target

    merge_target_task = _resolve_rebase_merge_target_task(store, task)
    candidate_tasks: tuple[DbTask, ...] = tuple(
        candidate
        for candidate in (
            merge_target_task,
            resolve_rebase_target_task(store, task),
        )
        if candidate is not None
    )
    for candidate in candidate_tasks:
        if candidate.id is None:
            continue
        merge_unit = store.resolve_merge_unit_for_task(candidate.id)
        if merge_unit is None:
            continue
        target = (merge_unit.target_branch or "").strip()
        if target:
            return _persist_rebase_base_branch(store, task, target)
    return None
