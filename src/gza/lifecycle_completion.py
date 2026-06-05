"""Shared task-completion predicates for lifecycle queries and recovery rules."""

from __future__ import annotations

from .db import Task

TERMINAL_MERGE_STATES = frozenset({"merged", "empty"})


def merge_state_is_terminal_for_lifecycle(merge_state: str | None) -> bool:
    """Return whether merge state means no more merge work remains."""
    return merge_state in TERMINAL_MERGE_STATES


def task_is_complete_for_lifecycle(task: Task, *, merge_state: str | None) -> bool:
    """Return whether a task is fully resolved for lifecycle planning.

    Code-producing completed tasks are only complete once merge truth says they
    landed. Non-code completed tasks can resolve without merge state.
    """
    if task.status in {"failed", "pending", "in_progress", "dropped"}:
        return False
    if task.status == "completed":
        if merge_state_is_terminal_for_lifecycle(merge_state):
            return True
        if not task.has_commits:
            return True
        return False
    if task.status == "unmerged":
        return merge_state_is_terminal_for_lifecycle(merge_state)
    return False


def should_auto_create_review_for_completed_code_task(
    task: Task,
    *,
    merge_state: str | None = None,
) -> bool:
    """Return whether auto-review should run for a completed implementation task.

    Policy: reviews require commits; empty completed code work is moot.
    """
    if task.task_type != "implement" or task.status != "completed":
        return True
    if not task.has_commits:
        return False
    if merge_state == "empty":
        return False
    return True


def auto_review_skip_message_for_completed_code_task(
    task: Task,
    *,
    merge_state: str | None = None,
) -> str | None:
    """Return the operator-facing auto-review suppression message, if any."""
    if task.task_type != "implement" or task.status != "completed":
        return None
    if not task.has_commits:
        return (
            f"Skipping auto-review for {task.id}: "
            "completed with no task commits; nothing to review."
        )
    if merge_state == "empty":
        return (
            f"Skipping auto-review for {task.id}: "
            "no unique commits vs target (nothing to review)."
        )
    return None
