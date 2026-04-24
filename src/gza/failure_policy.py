"""Shared failure-reason policy helpers."""

from collections.abc import Sequence

from .resume_policy import RESUMABLE_FAILURE_REASONS as _RESUMABLE_FAILURE_REASONS

RESUMABLE_FAILURE_REASONS: tuple[str, ...] = tuple(
    reason for reason in ("MAX_STEPS", "MAX_TURNS", "TIMEOUT", "TERMINATED")
    if reason in _RESUMABLE_FAILURE_REASONS
)


def is_resumable_failure_reason(reason: str | None) -> bool:
    """Return True when ``reason`` is eligible for auto-resume workflows."""
    return reason in RESUMABLE_FAILURE_REASONS


def resumable_failure_reasons() -> Sequence[str]:
    """Return resumable failure reasons as an ordered sequence."""
    return RESUMABLE_FAILURE_REASONS
