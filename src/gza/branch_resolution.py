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
