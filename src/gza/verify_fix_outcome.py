"""Durable completion outcome for no-source verify_fix timeout recovery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal, cast

from .db import SqliteTaskStore, Task

VERIFY_FIX_COMPLETION_OUTCOME_KIND = "verify_fix_completion_outcome"
VERIFY_FIX_COMPLETION_OUTCOME_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class VerifyFixCompletionOutcome:
    no_source_changes: bool
    completion_head_sha: str | None
    recovery_rerun_attempted: bool


@dataclass(frozen=True)
class VerifyFixCompletionOutcomeInspection:
    state: Literal["absent", "valid", "invalid"]
    outcome: VerifyFixCompletionOutcome | None = None
    invalid_reason: str | None = None


def _validate_completion_outcome_payload(
    payload: object,
    *,
    source: str,
) -> VerifyFixCompletionOutcomeInspection:
    if not isinstance(payload, dict):
        return VerifyFixCompletionOutcomeInspection("invalid", invalid_reason=f"{source} outcome is not an object")
    payload = cast(dict[str, object], payload)
    if payload.get("kind") != VERIFY_FIX_COMPLETION_OUTCOME_KIND:
        return VerifyFixCompletionOutcomeInspection("invalid", invalid_reason=f"{source} outcome has wrong kind")
    if payload.get("schema_version") != VERIFY_FIX_COMPLETION_OUTCOME_SCHEMA_VERSION:
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome has unsupported schema_version",
        )
    no_source_changes = payload.get("no_source_changes")
    if "no_source_changes" not in payload:
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome is missing no_source_changes",
        )
    if not isinstance(no_source_changes, bool):
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome has non-boolean no_source_changes",
        )
    recovery_rerun_attempted = payload.get("recovery_rerun_attempted")
    if "recovery_rerun_attempted" not in payload:
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome is missing recovery_rerun_attempted",
        )
    if not isinstance(recovery_rerun_attempted, bool):
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome has non-boolean recovery_rerun_attempted",
        )
    if "completion_head_sha" not in payload:
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome is missing completion_head_sha",
        )
    completion_head_sha = payload["completion_head_sha"]
    if completion_head_sha is not None and not isinstance(completion_head_sha, str):
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome has non-string completion_head_sha",
        )
    if no_source_changes and not completion_head_sha:
        return VerifyFixCompletionOutcomeInspection(
            "invalid",
            invalid_reason=f"{source} outcome has no-source proof without completion_head_sha",
        )
    return VerifyFixCompletionOutcomeInspection(
        "valid",
        outcome=VerifyFixCompletionOutcome(
            no_source_changes=no_source_changes,
            completion_head_sha=completion_head_sha,
            recovery_rerun_attempted=recovery_rerun_attempted,
        ),
    )


def inspect_verify_fix_completion_outcome(task: Task) -> VerifyFixCompletionOutcomeInspection:
    """Validate the canonical verify_fix completion outcome field."""
    if task.task_type != "verify_fix":
        return VerifyFixCompletionOutcomeInspection("absent")
    raw_outcome = (task.verify_fix_completion_outcome_json or "").strip()
    if not raw_outcome:
        return VerifyFixCompletionOutcomeInspection("absent")
    try:
        payload = json.loads(raw_outcome)
    except json.JSONDecodeError as exc:
        return VerifyFixCompletionOutcomeInspection("invalid", invalid_reason=f"malformed JSON: {exc}")
    return _validate_completion_outcome_payload(payload, source="canonical")


def parse_verify_fix_completion_outcome(task: Task) -> VerifyFixCompletionOutcome | None:
    """Return the structured verify_fix completion outcome, if the task carries one."""
    return inspect_verify_fix_completion_outcome(task).outcome


def effective_verify_fix_completion_outcome(task: Task) -> VerifyFixCompletionOutcome | None:
    """Return structured outcome, including compatible rows with equivalent legacy columns."""
    inspection = inspect_verify_fix_completion_outcome(task)
    if inspection.state == "valid":
        return inspection.outcome
    if inspection.state == "invalid":
        return None
    if task.task_type != "verify_fix":
        return None
    legacy_scope_inspection = inspect_legacy_review_scope_completion_outcome(task.review_scope)
    if legacy_scope_inspection.state == "valid":
        return legacy_scope_inspection.outcome
    if legacy_scope_inspection.state == "invalid":
        return None
    if task.changed_diff is False and task.review_verify_head_sha:
        return VerifyFixCompletionOutcome(
            no_source_changes=True,
            completion_head_sha=task.review_verify_head_sha,
            recovery_rerun_attempted=False,
        )
    return None


def apply_verify_fix_completion_outcome(
    task: Task,
    *,
    no_source_changes: bool,
    completion_head_sha: str | None,
    recovery_rerun_attempted: bool,
) -> None:
    """Write the structured outcome onto a verify_fix task object."""
    if task.task_type != "verify_fix":
        return
    task.changed_diff = not no_source_changes
    task.review_verify_head_sha = completion_head_sha
    task.verify_fix_completion_outcome_json = json.dumps(
        {
            "kind": VERIFY_FIX_COMPLETION_OUTCOME_KIND,
            "schema_version": VERIFY_FIX_COMPLETION_OUTCOME_SCHEMA_VERSION,
            "no_source_changes": no_source_changes,
            "completion_head_sha": completion_head_sha,
            "recovery_rerun_attempted": recovery_rerun_attempted,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def inspect_legacy_review_scope_completion_outcome(scope: str | None) -> VerifyFixCompletionOutcomeInspection:
    """Parse pre-v66 rows that temporarily stored completion outcome in review_scope."""
    raw_scope = (scope or "").strip()
    if not raw_scope:
        return VerifyFixCompletionOutcomeInspection("absent")
    try:
        payload = json.loads(raw_scope)
    except json.JSONDecodeError:
        return VerifyFixCompletionOutcomeInspection("absent")
    if not isinstance(payload, dict):
        return VerifyFixCompletionOutcomeInspection("absent")
    if payload.get("kind") != VERIFY_FIX_COMPLETION_OUTCOME_KIND:
        return VerifyFixCompletionOutcomeInspection("absent")
    if payload.get("schema_version") != VERIFY_FIX_COMPLETION_OUTCOME_SCHEMA_VERSION:
        return VerifyFixCompletionOutcomeInspection("absent")
    return _validate_completion_outcome_payload(payload, source="legacy review_scope")


def persist_verify_fix_completion_outcome(
    store: SqliteTaskStore,
    task: Task,
    *,
    no_source_changes: bool,
    completion_head_sha: str | None,
    recovery_rerun_attempted: bool | None = None,
) -> None:
    """Persist the structured outcome without losing an already-consumed rerun flag."""
    existing = parse_verify_fix_completion_outcome(task)
    apply_verify_fix_completion_outcome(
        task,
        no_source_changes=no_source_changes,
        completion_head_sha=completion_head_sha,
        recovery_rerun_attempted=(
            recovery_rerun_attempted
            if recovery_rerun_attempted is not None
            else (existing.recovery_rerun_attempted if existing is not None else False)
        ),
    )
    store.update(task)
