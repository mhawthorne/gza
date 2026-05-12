"""Shared helpers for resolving task branch lineage."""

from __future__ import annotations

from .db import SqliteTaskStore, Task as DbTask


def resolve_rebase_target_branch(store: SqliteTaskStore, task: DbTask) -> str | None:
    """Return the implementation branch a rebase lineage should operate on.

    Rebase recovery descendants can inherit a failed rebase task whose stored
    branch is already an orphan retry branch. Walk the full based_on lineage and
    prefer the nearest non-rebase ancestor's branch; if that ancestor branch is
    missing, fall back to the oldest recorded rebase branch in the chain.
    """
    visited_ids: set[str] = set()
    current: DbTask | None = task
    oldest_rebase_branch: str | None = None

    while current is not None:
        if current.id is not None:
            if current.id in visited_ids:
                return None
            visited_ids.add(current.id)

        if current.task_type != "rebase":
            return current.branch or oldest_rebase_branch

        if current.branch:
            oldest_rebase_branch = current.branch

        if current.based_on is None:
            break
        current = store.get(current.based_on)

    return oldest_rebase_branch
