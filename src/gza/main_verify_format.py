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
    r"(?: for (?P<signature>.+?) on (?P<fingerprint>.+?))?(?:;|$)"
)
_MAIN_VERIFY_RECOGNIZED_RED_STATUSES = frozenset({"failed"})
_MAIN_VERIFY_KNOWN_NON_RED_STATUSES = frozenset({"passed", "unavailable"})
_MAIN_VERIFY_STATUS_ABSENT = object()


@dataclass(frozen=True)
class MainVerifyRemediationExhaustion:
    attempts: str
    signature: str | None
    fingerprint: str | None = None


@dataclass(frozen=True)
class MainVerifyStatusClassification:
    """Normalized structured/legacy status evidence for a main-verify state."""

    kind: Literal["valid", "legacy_absent", "absent", "missing", "invalid"]
    status: str | None = None


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
        fingerprint=exhausted_match.group("fingerprint"),
    )


def main_verify_state_is_remediation_exhausted(state: Any) -> bool:
    return main_verify_state_exhausted_remediation_attention(state) is not None


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
    exhausted = main_verify_state_exhausted_remediation_attention(state)
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


def main_verify_state_status_classification(state: Any) -> MainVerifyStatusClassification:
    verify_status = getattr(state, "verify_status", _MAIN_VERIFY_STATUS_ABSENT)
    message = getattr(state, "alert_message", None)
    has_legacy_attention = isinstance(message, str) and (
        message.startswith("main verify RED") or parse_main_verify_remediation_exhaustion_message(message) is not None
    )
    if isinstance(verify_status, str):
        if verify_status:
            return MainVerifyStatusClassification("valid", verify_status)
        return MainVerifyStatusClassification("invalid")
    if verify_status is _MAIN_VERIFY_STATUS_ABSENT:
        if has_legacy_attention:
            return MainVerifyStatusClassification("legacy_absent")
        return MainVerifyStatusClassification("absent")
    if verify_status is None:
        if has_legacy_attention:
            return MainVerifyStatusClassification("legacy_absent")
        return MainVerifyStatusClassification("missing")
    return MainVerifyStatusClassification("invalid")


def main_verify_state_gate_disabled(state: Any) -> bool:
    return getattr(state, "gate_enabled", None) is False


def main_verify_state_exhausted_remediation_attention(state: Any) -> MainVerifyRemediationExhaustion | None:
    if main_verify_state_gate_disabled(state):
        return None
    verify_exit_status = getattr(state, "verify_exit_status", None)
    if verify_exit_status in {
        MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    }:
        return None
    status_classification = main_verify_state_status_classification(state)
    if status_classification.kind == "invalid":
        return None
    if (
        status_classification.kind == "valid"
        and status_classification.status not in _MAIN_VERIFY_RECOGNIZED_RED_STATUSES
    ):
        return None
    return parse_main_verify_remediation_exhaustion_message(getattr(state, "alert_message", None))


def main_verify_state_is_red_verdict(state: Any) -> bool:
    if main_verify_state_gate_disabled(state):
        return False
    if main_verify_state_is_remediation_exhausted(state):
        return False
    verify_status = getattr(state, "verify_status", None)
    verify_exit_status = getattr(state, "verify_exit_status", None)
    if verify_exit_status in {
        MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
    }:
        return False
    status_classification = main_verify_state_status_classification(state)
    verify_status = status_classification.status
    if status_classification.kind == "valid" and verify_status in _MAIN_VERIFY_RECOGNIZED_RED_STATUSES:
        return True
    if (
        status_classification.kind in {"invalid", "missing", "absent"}
        or verify_status in _MAIN_VERIFY_KNOWN_NON_RED_STATUSES
        or (status_classification.kind == "valid" and verify_status is not None)
    ):
        return False
    message = getattr(state, "alert_message", None)
    if isinstance(message, str) and message.startswith("main verify RED"):
        return True
    return False


def main_verify_state_needs_non_red_attention(state: Any) -> bool:
    return (
        not main_verify_state_gate_disabled(state)
        and getattr(state, "verify_exit_status", None) == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS
    )


def main_verify_state_halts_merges(state: Any) -> bool:
    status_classification = main_verify_state_status_classification(state)
    if status_classification.kind == "absent":
        return False
    return _verify_result_halts_merges(
        status=status_classification.status,
        gate_enabled=getattr(state, "gate_enabled", None) is not False,
        exit_status=getattr(state, "verify_exit_status", None),
    )


def _verify_result_halts_merges(*, status: str | None, gate_enabled: bool, exit_status: str | None) -> bool:
    if not gate_enabled or status == "passed":
        return False
    if exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS:
        return False
    return True


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


def _format_generic_launch_failed_message() -> str:
    return "main verify misconfigured - verify command launch failed; fix the environment, not the code"


def _format_launch_failed_message(state: Any) -> str:
    message = getattr(state, "alert_message", None)
    if isinstance(message, str) and message.startswith("main verify misconfigured - ") and " at `" not in message:
        return message

    from .main_integration_verify import _build_launch_issue_alert_message, _detect_verify_launch_issue

    issue = _detect_verify_launch_issue(
        verify_output=None,
        verify_exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
        verify_failure=getattr(state, "failure", None),
        failing_phase=getattr(state, "failing_phase", None),
    )
    if issue is not None and issue.tool_name:
        return _build_launch_issue_alert_message(head_sha=getattr(state, "head_sha", None), issue=issue)
    return _format_generic_launch_failed_message()


def _format_unknown_evidence_message(state: Any, *, target_proof: MainVerifyTargetProof) -> str:
    verify_status = getattr(state, "verify_status", None)
    if isinstance(verify_status, str) and verify_status:
        evidence = f"unrecognized verify status `{verify_status}`"
    elif verify_status is not None:
        evidence = "invalid verify status evidence"
    else:
        evidence = "verify status unavailable"
    if target_proof == "current":
        return f"main verify evidence unknown for current HEAD; {evidence}"
    if target_proof == "stale":
        return f"main verify evidence stale for current HEAD; {evidence}"
    return f"main verify evidence unproven for current HEAD; {evidence}"


def format_main_verify_attention_message(
    state: Any,
    *,
    target_proof: MainVerifyTargetProof = "current",
    now: datetime | None = None,
) -> str | None:
    """Render current operator-facing main-verify attention without trusting persisted SHA text."""
    message = getattr(state, "alert_message", None)
    red_since = getattr(state, "red_since", None)
    include_red_duration = False
    exhausted = main_verify_state_exhausted_remediation_attention(state)
    status_classification = main_verify_state_status_classification(state)
    verify_status = status_classification.status
    if main_verify_state_gate_disabled(state):
        message = "main verify disabled; merges allowed"
    elif verify_status == "passed":
        message = "main verify passed; merges allowed"
    elif main_verify_state_is_freshness_unavailable(state):
        if target_proof == "current":
            message = _format_current_freshness_unavailable_message(state)
        else:
            message = _format_unproven_freshness_unavailable_message(state)
    elif main_verify_state_needs_non_red_attention(state):
        message = _format_launch_failed_message(state)
    elif exhausted is not None:
        signature = main_verify_state_failure_signature(state) or "unknown"
        message = (
            f"main verify remediation exhausted for {signature} after "
            f"{exhausted.attempts} attempts; human intervention required"
        )
        include_red_duration = target_proof == "current" and (
            status_classification.kind == "valid"
            and status_classification.status in _MAIN_VERIFY_RECOGNIZED_RED_STATUSES
        )
    elif main_verify_state_is_red_verdict(state):
        if target_proof == "current":
            message = _format_current_red_message(state)
            include_red_duration = True
        elif target_proof == "stale":
            message = _format_stale_red_message()
        else:
            message = _format_unproven_red_message(state)
    else:
        message = _format_unknown_evidence_message(state, target_proof=target_proof)
    if include_red_duration and now is not None and red_since is not None:
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
    return fallback
