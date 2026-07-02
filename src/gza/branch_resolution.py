"""Shared helpers for resolving task branch lineage."""

from __future__ import annotations

from .db import SqliteTaskStore, Task as DbTask


def resolve_rebase_target_task(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    """Return the canonical lineage task a rebase chain should operate on.

    Prefer the nearest non-rebase ancestor when it has a branch. If that
    ancestor lacks a branch, fall back to the oldest rebase task in the chain
    that still recorded one.
    """
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

    Rebase recovery descendants can inherit a failed rebase task whose stored
    branch is already an orphan retry branch. Walk the full based_on lineage and
    prefer the nearest non-rebase ancestor's branch; if that ancestor branch is
    missing, fall back to the oldest recorded rebase branch in the chain.
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
