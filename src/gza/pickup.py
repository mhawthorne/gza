"""Shared queue pickup and worker-slot semantics."""

from datetime import datetime

from .db import SqliteTaskStore, Task

WORKER_CONSUMING_ADVANCE_ACTION_TYPES: frozenset[str] = frozenset(
    {
        "needs_rebase",
        "run_plan_review",
        "run_plan_improve",
        "run_review",
        "run_improve",
        "create_plan_review",
        "create_plan_improve",
        "create_review",
        "create_implement",
        "improve",
        "resume",
        "retry",
    }
)
QUIET_EXEMPT_RECOVERY_ORIGINS: frozenset[str] = frozenset({"retry", "resume"})


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


def effective_edit_time(task: Task) -> datetime | None:
    """Return the timestamp that anchors quiet-period display semantics."""
    return task.last_edited_at or task.created_at


def is_in_quiet_period(task: Task, *, now: datetime, quiet_seconds: int) -> bool:
    """Return whether the task should stay in the queue/next Quiet lane."""
    if quiet_seconds <= 0:
        return False
    if task.recovery_origin in QUIET_EXEMPT_RECOVERY_ORIGINS:
        return False
    if task.urgent:
        return False
    if task.queue_position is not None:
        return False
    effective_at = effective_edit_time(task)
    if effective_at is None:
        return False
    return (now - effective_at).total_seconds() < quiet_seconds


def get_runnable_pending_tasks(
    store: SqliteTaskStore,
    limit: int | None = None,
    group: str | None = None,
    *,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    quiet_seconds: int = 0,
) -> list[Task]:
    """Return pending tasks that default worker pickup can run, in pickup order."""
    return store.get_pending_pickup(
        limit=limit,
        group=group,
        tags=tags,
        any_tag=any_tag,
        quiet_seconds=quiet_seconds,
    )
