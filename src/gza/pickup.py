"""Shared queue pickup and worker-slot semantics."""

from .db import SqliteTaskStore, Task

WORKER_CONSUMING_ADVANCE_ACTION_TYPES: frozenset[str] = frozenset(
    {
        "needs_rebase",
        "run_review",
        "run_improve",
        "create_review",
        "create_implement",
        "improve",
        "resume",
    }
)


def is_worker_consuming_advance_action(action_type: str) -> bool:
    """Return True when an advance action consumes a worker slot."""
    return action_type in WORKER_CONSUMING_ADVANCE_ACTION_TYPES


def count_worker_consuming_actions(actions: list[dict]) -> int:
    """Count how many planned advance actions consume worker slots."""
    return sum(
        1
        for action in actions
        if is_worker_consuming_advance_action(str(action.get("type", "")))
    )


def get_runnable_pending_tasks(store: SqliteTaskStore, limit: int | None = None) -> list[Task]:
    """Return pending tasks that default worker pickup can run, in pickup order."""
    return store.get_pending_pickup(limit=limit)
