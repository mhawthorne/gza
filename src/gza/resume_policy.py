"""Shared policy for resumable failed tasks."""

from __future__ import annotations

from typing import Any

RESUMABLE_FAILURE_REASONS = frozenset({"MAX_STEPS", "MAX_TURNS", "TIMEOUT", "TERMINATED"})


def is_resumable_failure_reason(failure_reason: str | None) -> bool:
    """Return True when a failure reason is auto-resumable."""
    return failure_reason in RESUMABLE_FAILURE_REASONS


def is_resumable_failure(*, status: str | None, failure_reason: str | None, session_id: str | None) -> bool:
    """Return True when task fields satisfy auto-resume requirements."""
    return status == "failed" and session_id is not None and is_resumable_failure_reason(failure_reason)


def is_resumable_failed_task(task: Any) -> bool:
    """Return True when a task object is failed and eligible for automatic resume."""
    return is_resumable_failure(
        status=getattr(task, "status", None),
        failure_reason=getattr(task, "failure_reason", None),
        session_id=getattr(task, "session_id", None),
    )
