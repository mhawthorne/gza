"""CLI-facing wrapper around the shared advance engine."""

from __future__ import annotations

from typing import Any

from gza.advance_engine import (
    ADVANCE_RULES,
    WORKER_CONSUMING_ACTIONS,
    AdvanceContext,
    AdvanceRule,
    evaluate_advance_rules,
    is_resumable_failed_task,
    resolve_advance_context,
)
from gza.db import SqliteTaskStore, Task as DbTask


def determine_next_action(
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    *,
    impl_based_on_ids: set[str] | None = None,
    max_resume_attempts: int | None = None,
) -> dict[str, Any]:
    """Backward-compatible entrypoint for advance action selection."""
    return evaluate_advance_rules(
        config,
        store,
        git,
        task,
        target_branch,
        impl_based_on_ids=impl_based_on_ids,
        max_resume_attempts=max_resume_attempts,
    )


__all__ = [
    "ADVANCE_RULES",
    "WORKER_CONSUMING_ACTIONS",
    "AdvanceContext",
    "AdvanceRule",
    "determine_next_action",
    "is_resumable_failed_task",
    "resolve_advance_context",
]
