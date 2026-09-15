"""Shared manual status-transition mutations used by operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .config import Config
from .db import SqliteTaskStore, Task as DbTask
from .failure_reasons import mark_task_failed_from_cause


@dataclass(frozen=True)
class TaskDropCascadeResult:
    """Rows and merge units affected by a deliberate task drop."""

    dropped_task_ids: tuple[str, ...]
    deferred_task_ids: tuple[str, ...] = ()
    tombstoned_merge_unit_ids: tuple[str, ...] = ()


def drop_task_with_scope_cascade(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    reason: str | None = None,
    mode: Literal["automatic", "operator"] = "automatic",
) -> TaskDropCascadeResult:
    """Drop a task plus descendants proven to complete the same merge outcome."""
    if task.id is None:
        return TaskDropCascadeResult(dropped_task_ids=(), deferred_task_ids=(), tombstoned_merge_unit_ids=())
    if mode not in {"automatic", "operator"}:
        raise ValueError(f"Unsupported drop cascade mode: {mode}")
    result = store.drop_task_with_scope_cascade(
        task.id,
        reason=reason,
        mode=mode,
    )
    return TaskDropCascadeResult(
        dropped_task_ids=result.dropped_task_ids,
        deferred_task_ids=result.deferred_task_ids,
        tombstoned_merge_unit_ids=result.tombstoned_merge_unit_ids,
    )


def apply_manual_task_status(
    *,
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    status: str,
    reason: str | None = None,
) -> TaskDropCascadeResult | None:
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
        return None
    if status == "pending":
        task.status = status
        task.completed_at = None
        task.failure_reason = None
        task.completion_reason = None
        task.drop_reason = None
        store.update(task)
        return None
    if status == "dropped":
        return drop_task_with_scope_cascade(store=store, task=task, reason=reason, mode="operator")
    raise ValueError(f"Unsupported manual task status: {status}")
