"""Shared helpers for main-verify remediation phase scope."""

from __future__ import annotations

import re
from collections.abc import Iterable

VERIFY_PHASE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def normalize_verify_phase_names(phases: Iterable[object]) -> tuple[str, ...]:
    """Return valid phase names deduplicated in first-observed order."""
    ordered: list[str] = []
    seen: set[str] = set()
    for phase in phases:
        if not isinstance(phase, str):
            continue
        stripped = phase.strip()
        if not stripped or not VERIFY_PHASE_NAME_RE.match(stripped) or stripped in seen:
            continue
        ordered.append(stripped)
        seen.add(stripped)
    return tuple(ordered)


def canonical_verify_phase_names(phases: Iterable[object]) -> tuple[str, ...]:
    """Return valid phase names as a deterministic identity set."""
    return tuple(sorted(set(normalize_verify_phase_names(phases))))


def verify_failure_signature(
    *,
    failing_phases: Iterable[object],
    verify_status: str | None,
    verify_exit_status: str | None,
) -> str:
    """Build the opaque identity for a main/candidate verify failure."""
    canonical = canonical_verify_phase_names(failing_phases)
    if canonical:
        return f"phases:{','.join(canonical)}"
    status = verify_status or "unknown"
    exit_status = verify_exit_status or "unknown"
    return f"status:{status}:exit:{exit_status}"


def parse_verify_phase_signature(signature: object) -> tuple[str, ...]:
    """Return covered phases for a canonical plural signature, otherwise empty."""
    if not isinstance(signature, str) or not signature.startswith("phases:"):
        return ()
    return canonical_verify_phase_names(signature.removeprefix("phases:").split(","))


def verify_phase_label(phases: Iterable[object], *, fallback: str | None = None) -> str:
    """Human-readable phase-scope label."""
    normalized = normalize_verify_phase_names(phases)
    if len(normalized) == 1:
        return f"phase {normalized[0]}"
    if len(normalized) > 1:
        return f"phases {', '.join(normalized)}"
    return fallback or "unstructured verify failure"


def verify_phase_value(phases: Iterable[object], *, fallback: str | None = None) -> str:
    """Human-readable phase value without the phase/phases prefix."""
    normalized = normalize_verify_phase_names(phases)
    if normalized:
        return ", ".join(normalized)
    return fallback or "unknown"
