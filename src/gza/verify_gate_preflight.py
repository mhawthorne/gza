"""Durable provenance for verify-gate rebase preflights."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

VERIFY_GATE_PREFLIGHT_HEADER = "Verify gate preflight rebase: yes"


@dataclass(frozen=True)
class VerifyGatePreflightProvenance:
    """Facts that bind a completed rebase to one red verify-gate preflight."""

    owner_task_id: str
    reviewed_branch: str
    reviewed_head_sha: str
    verify_command: str
    verify_timeout_seconds: int | None
    verify_timeout_grace_seconds: float | None
    target_branch: str
    target_tip_sha: str
    failed_captured_at: str | None = None


def append_verify_gate_preflight_provenance(
    existing_scope: str | None,
    provenance: VerifyGatePreflightProvenance,
) -> str:
    """Append verify-gate preflight provenance to task review-scope metadata."""
    block = "\n".join(
        (
            VERIFY_GATE_PREFLIGHT_HEADER,
            f"Implementation owner task: {provenance.owner_task_id}",
            f"Reviewed branch: {provenance.reviewed_branch}",
            f"Reviewed head SHA: {provenance.reviewed_head_sha}",
            f"Verify command: {provenance.verify_command}",
            f"Verify timeout seconds: {_format_optional(provenance.verify_timeout_seconds)}",
            f"Verify timeout grace seconds: {_format_optional(provenance.verify_timeout_grace_seconds)}",
            f"Target branch: {provenance.target_branch}",
            f"Target tip SHA: {provenance.target_tip_sha}",
            f"Failed verify captured at: {provenance.failed_captured_at or ''}",
        )
    )
    prefix = (existing_scope or "").strip()
    if not prefix:
        return block
    return f"{prefix}\n\n{block}"


def parse_verify_gate_preflight_provenance(
    text: str | None,
) -> VerifyGatePreflightProvenance | None:
    """Parse verify-gate preflight provenance from task review-scope metadata."""
    if text is None or VERIFY_GATE_PREFLIGHT_HEADER not in text:
        return None
    fields: dict[str, str] = {}
    in_block = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line == VERIFY_GATE_PREFLIGHT_HEADER:
            in_block = True
            continue
        if not in_block:
            continue
        if not line:
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip().lower()] = value.strip()

    owner_task_id = fields.get("implementation owner task")
    reviewed_branch = fields.get("reviewed branch")
    reviewed_head_sha = fields.get("reviewed head sha")
    verify_command = fields.get("verify command")
    target_branch = fields.get("target branch")
    target_tip_sha = fields.get("target tip sha")
    if (
        owner_task_id is None
        or reviewed_branch is None
        or reviewed_head_sha is None
        or verify_command is None
        or target_branch is None
        or target_tip_sha is None
        or not owner_task_id
        or not reviewed_branch
        or not reviewed_head_sha
        or not verify_command
        or not target_branch
        or not target_tip_sha
    ):
        return None
    return VerifyGatePreflightProvenance(
        owner_task_id=owner_task_id,
        reviewed_branch=reviewed_branch,
        reviewed_head_sha=reviewed_head_sha,
        verify_command=verify_command,
        verify_timeout_seconds=_parse_optional_int(fields.get("verify timeout seconds")),
        verify_timeout_grace_seconds=_parse_optional_float(fields.get("verify timeout grace seconds")),
        target_branch=target_branch,
        target_tip_sha=target_tip_sha,
        failed_captured_at=fields.get("failed verify captured at") or None,
    )


def captured_at_to_preflight_value(value: datetime | None) -> str | None:
    """Serialize a captured-at timestamp without making it matching identity."""
    return value.isoformat() if value is not None else None


def _format_optional(value: int | float | None) -> str:
    return "" if value is None else str(value)


def _parse_optional_int(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_optional_float(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    try:
        return float(value)
    except ValueError:
        return None
