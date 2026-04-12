"""Shared failure-reason policy helpers."""

from collections.abc import Sequence

RESUMABLE_FAILURE_REASONS: tuple[str, ...] = ("MAX_STEPS", "MAX_TURNS")


def is_resumable_failure_reason(reason: str | None) -> bool:
    """Return True when ``reason`` is eligible for auto-resume workflows."""
    return reason in RESUMABLE_FAILURE_REASONS


def resumable_failure_reasons() -> Sequence[str]:
    """Return resumable failure reasons as an ordered sequence."""
    return RESUMABLE_FAILURE_REASONS
