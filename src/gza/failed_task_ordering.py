"""Shared failed-task ordering helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from .db import Task as DbTask, task_id_numeric_key

FailedTaskOrder = Literal["created_at_asc"]
DEFAULT_FAILED_TASK_ORDER: FailedTaskOrder = "created_at_asc"


def _normalize_dt(value: datetime | None) -> datetime:
    if not isinstance(value, datetime):
        return datetime.min
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def failed_task_sort_key(task: DbTask, *, order: FailedTaskOrder = DEFAULT_FAILED_TASK_ORDER) -> tuple[datetime, int]:
    if order == "created_at_asc":
        return (_normalize_dt(task.created_at), task_id_numeric_key(task.id))
    raise ValueError(f"Unsupported failed task order: {order}")


def sort_failed_tasks(
    tasks: list[DbTask],
    *,
    order: FailedTaskOrder = DEFAULT_FAILED_TASK_ORDER,
) -> list[DbTask]:
    return sorted(tasks, key=lambda task: failed_task_sort_key(task, order=order))
