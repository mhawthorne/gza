"""CLI-facing wrapper around the shared advance engine."""

from __future__ import annotations

from typing import Any

from gza.advance_engine import (
    ADVANCE_RULES,
    NEEDS_ATTENTION_LABEL,
    WORKER_CONSUMING_ACTIONS,
    AdvanceContext,
    AdvanceRule,
    classify_advance_action,
    evaluate_advance_rules,
    failed_recovery_decision_to_action,
    failed_recovery_decision_to_attention_action,
    format_needs_attention_entry,
    format_needs_attention_entry_for_display,
    format_needs_attention_lifecycle,
    get_action_subject_task_id,
    get_needs_attention_reason,
    is_needs_attention_action,
    is_resumable_failed_task,
    require_needs_attention_subject,
    resolve_advance_context,
    resolve_closing_review_action,
    resolve_subject_task,
    with_needs_attention,
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
    persist_post_merge_rebase_state: bool = True,
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
        persist_post_merge_rebase_state=persist_post_merge_rebase_state,
    )


__all__ = [
    "ADVANCE_RULES",
    "NEEDS_ATTENTION_LABEL",
    "WORKER_CONSUMING_ACTIONS",
    "AdvanceContext",
    "AdvanceRule",
    "classify_advance_action",
    "determine_next_action",
    "is_resumable_failed_task",
    "resolve_closing_review_action",
    "failed_recovery_decision_to_action",
    "failed_recovery_decision_to_attention_action",
    "format_needs_attention_entry",
    "format_needs_attention_entry_for_display",
    "format_needs_attention_lifecycle",
    "get_action_subject_task_id",
    "get_needs_attention_reason",
    "is_needs_attention_action",
    "resolve_advance_context",
    "require_needs_attention_subject",
    "resolve_subject_task",
    "with_needs_attention",
]
