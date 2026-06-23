"""Shared manual status-transition mutations used by operator surfaces."""

from __future__ import annotations

from datetime import UTC, datetime

from .config import Config
from .db import SqliteTaskStore, Task as DbTask
from .failure_reasons import mark_task_failed_from_cause


def apply_manual_task_status(
    *,
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    status: str,
    reason: str | None = None,
) -> None:
    """Apply the DB mutation for one supported manual status transition."""
    if status == "failed":
        mark_task_failed_from_cause(
            task=task,
            config=config,
            store=store,
            log_file=task.log_file,
            branch=task.branch,
            has_commits=bool(task.has_commits),
            explicit_reason=reason,
        )
        return
    if status == "pending":
        task.status = status
        task.completed_at = None
        task.failure_reason = None
        task.completion_reason = None
        store.update(task)
        return
    if status == "dropped":
        task.status = status
        task.completed_at = datetime.now(UTC)
        task.failure_reason = None
        task.completion_reason = None
        store.update(task)
        return
    raise ValueError(f"Unsupported manual task status: {status}")
