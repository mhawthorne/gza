"""Shared task-type definitions and small classification helpers."""

from __future__ import annotations

from typing import Final

TASK_TYPE_TASK: Final = "task"
TASK_TYPE_EXPLORE: Final = "explore"
TASK_TYPE_PLAN: Final = "plan"
TASK_TYPE_PLAN_REVIEW: Final = "plan_review"
TASK_TYPE_PLAN_IMPROVE: Final = "plan_improve"
TASK_TYPE_IMPLEMENT: Final = "implement"
TASK_TYPE_REVIEW: Final = "review"
TASK_TYPE_IMPROVE: Final = "improve"
TASK_TYPE_VERIFY_FIX: Final = "verify_fix"
TASK_TYPE_FIX: Final = "fix"
TASK_TYPE_REBASE: Final = "rebase"
TASK_TYPE_INTERNAL: Final = "internal"

ALL_TASK_TYPES: Final[tuple[str, ...]] = (
    TASK_TYPE_TASK,
    TASK_TYPE_EXPLORE,
    TASK_TYPE_PLAN,
    TASK_TYPE_PLAN_REVIEW,
    TASK_TYPE_PLAN_IMPROVE,
    TASK_TYPE_IMPLEMENT,
    TASK_TYPE_REVIEW,
    TASK_TYPE_IMPROVE,
    TASK_TYPE_VERIFY_FIX,
    TASK_TYPE_FIX,
    TASK_TYPE_REBASE,
    TASK_TYPE_INTERNAL,
)

CLI_FILTER_TASK_TYPES: Final[tuple[str, ...]] = (
    TASK_TYPE_EXPLORE,
    TASK_TYPE_PLAN,
    TASK_TYPE_PLAN_REVIEW,
    TASK_TYPE_PLAN_IMPROVE,
    TASK_TYPE_IMPLEMENT,
    TASK_TYPE_REVIEW,
    TASK_TYPE_IMPROVE,
    TASK_TYPE_VERIFY_FIX,
    TASK_TYPE_FIX,
    TASK_TYPE_REBASE,
    TASK_TYPE_INTERNAL,
)

CLI_ADD_TASK_TYPES: Final[tuple[str, ...]] = (
    TASK_TYPE_EXPLORE,
    TASK_TYPE_PLAN,
    TASK_TYPE_PLAN_REVIEW,
    TASK_TYPE_PLAN_IMPROVE,
    TASK_TYPE_IMPLEMENT,
    TASK_TYPE_REVIEW,
    TASK_TYPE_VERIFY_FIX,
    TASK_TYPE_IMPROVE,
)


def is_known_task_type(task_type: str) -> bool:
    """Return whether ``task_type`` is one of the persisted task types."""
    return task_type in ALL_TASK_TYPES
