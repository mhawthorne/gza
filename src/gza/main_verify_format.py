"""Shared formatting for local-target main integration verify state."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from .main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
)
from .main_verify_target import current_local_target_head_sha

MainVerifyTargetProof = Literal["current", "stale", "unproven"]

_MAIN_VERIFY_REMEDIATION_EXHAUSTED_ATTENTION_RE = re.compile(
    r"automatic remediation exhausted after (?P<attempts>\d+/\d+) attempts"
    r"(?: for (?P<signature>.+?) on [^;]+)?"
)


@dataclass(frozen=True)
class MainVerifyRemediationExhaustion:
    attempts: str
    signature: str | None


def parse_main_verify_remediation_exhaustion_message(message: Any) -> MainVerifyRemediationExhaustion | None:
    """Parse all durable and legacy main-verify remediation exhaustion messages."""
    if not isinstance(message, str):
        return None
    exhausted_match = _MAIN_VERIFY_REMEDIATION_EXHAUSTED_ATTENTION_RE.search(message)
    if exhausted_match is None:
        return None
    signature = exhausted_match.group("signature")
    return MainVerifyRemediationExhaustion(
        attempts=exhausted_match.group("attempts"),
        signature=signature.strip() if isinstance(signature, str) and signature.strip() else None,
    )


def main_verify_state_is_remediation_exhausted(state: Any) -> bool:
    return parse_main_verify_remediation_exhaustion_message(getattr(state, "alert_message", None)) is not None


def format_red_duration(red_since: datetime, now: datetime) -> str:
    elapsed = max(0, int((now - red_since).total_seconds()))
    total_minutes = elapsed // 60
    total_hours = elapsed // 3600
    total_days = elapsed // 86400
    if total_days > 0:
        return f"{total_days}d{(total_hours % 24)}h"
    if total_hours > 0:
        return f"{total_hours}h{(total_minutes % 60)}m"
    return f"{total_minutes}m"


def main_verify_state_failure_signature(state: Any) -> str | None:
    signature = getattr(state, "failure_signature", None)
    if isinstance(signature, str) and signature:
        return signature
    message = getattr(state, "alert_message", None) or ""
    if not isinstance(message, str):
        return None
    exhausted = parse_main_verify_remediation_exhaustion_message(message)
    if exhausted is not None:
        return exhausted.signature
    failing_phase = getattr(state, "failing_phase", None)
    if isinstance(failing_phase, str) and failing_phase:
        return f"phase:{failing_phase}"
    verify_status = getattr(state, "verify_status", None)
    verify_exit_status = getattr(state, "verify_exit_status", None)
    if isinstance(verify_status, str) and verify_status:
        return f"status:{verify_status}:exit:{verify_exit_status or 'unknown'}"
    return None


def main_verify_state_is_freshness_unavailable(state: Any) -> bool:
    return getattr(state, "verify_exit_status", None) == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS


def main_verify_state_is_red_verdict(state: Any) -> bool:
    message = getattr(state, "alert_message", None)
    if main_verify_state_is_remediation_exhausted(state):
        return False
    if isinstance(message, str) and message.startswith("main verify RED"):
        return True
    verify_status = getattr(state, "verify_status", None)
    verify_exit_status = getattr(state, "verify_exit_status", None)
    if verify_exit_status in {
        MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    }:
        return False
    return isinstance(verify_status, str) and verify_status not in {"passed", "unavailable"}


def main_verify_state_needs_non_red_attention(state: Any) -> bool:
    message = getattr(state, "alert_message", None)
    if not isinstance(message, str) or not message:
        return False
    return getattr(state, "verify_exit_status", None) == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS


def resolve_main_verify_target_proof(
    state: Any,
    *,
    git: Any | None,
    target_branch: str | None = None,
) -> MainVerifyTargetProof:
    recorded_head = getattr(state, "head_sha", None)
    if not isinstance(recorded_head, str) or not recorded_head:
        return "unproven"
    current_head = current_local_target_head_sha(git, target_branch=target_branch)
    if current_head is None:
        return "unproven"
    if current_head != recorded_head:
        return "stale"
    return "current"


def _format_current_red_message(state: Any) -> str:
    short_sha = (getattr(state, "head_sha", None) or "unknown")[:12]
    failing_phase = getattr(state, "failing_phase", None)
    if isinstance(failing_phase, str) and failing_phase:
        return f"main verify RED at `{short_sha}` - merges halted; phase `{failing_phase}` failing"
    verify_status = getattr(state, "verify_status", None)
    if isinstance(verify_status, str) and verify_status and verify_status != "failed":
        return f"main verify RED at `{short_sha}` - merges halted; verify status `{verify_status}`"
    return f"main verify RED at `{short_sha}` - merges halted"


def _format_unproven_red_message(state: Any) -> str:
    recorded_head = getattr(state, "head_sha", None)
    if not isinstance(recorded_head, str) or not recorded_head:
        return "main verify red evidence unproven at current HEAD; recorded target SHA unavailable"
    return "main verify red evidence unproven at current HEAD; current HEAD identity unavailable"


def _format_stale_red_message() -> str:
    return "main verify red evidence stale at current HEAD; recorded target SHA no longer current"


def _format_current_freshness_unavailable_message(state: Any) -> str:
    short_sha = (getattr(state, "head_sha", None) or "unknown")[:12]
    return f"main verify freshness unproven at `{short_sha}` - merges halted; exact tree fingerprint unavailable"


def _format_unproven_freshness_unavailable_message(state: Any) -> str:
    recorded_head = getattr(state, "head_sha", None)
    if not isinstance(recorded_head, str) or not recorded_head:
        return "main verify freshness unproven at current HEAD; recorded target SHA unavailable"
    return "main verify freshness unproven at current HEAD; exact tree fingerprint unavailable"


def format_main_verify_attention_message(
    state: Any,
    *,
    target_proof: MainVerifyTargetProof = "current",
    now: datetime | None = None,
) -> str | None:
    """Render current operator-facing main-verify attention without trusting persisted SHA text."""
    message = getattr(state, "alert_message", None) or "main verify is red; merges halted"
    red_since = getattr(state, "red_since", None)
    exhausted = parse_main_verify_remediation_exhaustion_message(message)
    if exhausted is not None:
        signature = main_verify_state_failure_signature(state) or "unknown"
        message = (
            f"main verify remediation exhausted for {signature} after "
            f"{exhausted.attempts} attempts; human intervention required"
        )
    elif main_verify_state_is_red_verdict(state):
        if target_proof == "stale":
            return None
        if target_proof == "unproven":
            message = _format_unproven_red_message(state)
        else:
            message = _format_current_red_message(state)
    elif main_verify_state_is_freshness_unavailable(state):
        if target_proof == "current":
            message = _format_current_freshness_unavailable_message(state)
        else:
            message = _format_unproven_freshness_unavailable_message(state)
    if now is not None and red_since is not None:
        return f"{message} (red for {format_red_duration(red_since, now)})"
    return message


def format_main_verify_status_message(
    state: Any,
    *,
    target_proof: MainVerifyTargetProof,
    fallback: str = "main verify is red; merges halted",
    now: datetime | None = None,
) -> str:
    message = format_main_verify_attention_message(state, target_proof=target_proof, now=now)
    if message is not None:
        return message
    return _format_stale_red_message() if target_proof == "stale" else fallback
