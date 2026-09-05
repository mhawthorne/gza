"""Shared helpers for persisted and rendered verify-gate provenance."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from gza.artifact_paths import InvalidArtifactPathError, resolve_artifact_path
from gza.artifacts import prepare_command_output_artifact, store_command_output_artifact
from gza.db import SqliteTaskStore, Task
from gza.git import GitError

if TYPE_CHECKING:
    from gza.config import Config


VERIFY_GATE_ARTIFACT_KIND = "verify_gate_result"
VERIFY_GATE_ARTIFACT_LABEL = "verify_gate_result"
VERIFY_GATE_ARTIFACT_SCHEMA_VERSION = 1
INVALID_STRUCTURED_FAILURE_ORIGIN = "__invalid_structured_failure_origin__"
KNOWN_FULL_VERIFY_PHASES = ("ruff", "ty", "mypy", "checks", "unit", "functional")


@dataclass(frozen=True)
class VerifyEpoch:
    """Provenance for one verify gate evaluation at a specific source epoch."""

    reviewed_branch: str | None
    reviewed_head_sha: str | None
    verify_command: str | None
    verify_timeout_seconds: int | None
    verify_timeout_grace_seconds: float | None
    reviewed_tree_sha: str | None = None


@dataclass(frozen=True)
class VerifyGateResult:
    """Neutral persisted verify result for merge-gating evidence."""

    command: str
    status: str
    exit_status: str
    captured_at: datetime
    reviewed_branch: str | None = None
    reviewed_head_sha: str | None = None
    reviewed_tree_sha: str | None = None
    reviewed_base_sha: str | None = None
    working_directory: str | None = None
    failure: str | None = None
    source_task_id: str | None = None
    source_task_type: str | None = None
    output_artifact_id: int | None = None
    output_artifact_task_id: str | None = None
    output_artifact_path: str | None = None
    failure_origin: str | None = None
    phase_summary: dict[str, Any] | None = None
    phase_summary_invalid_reason: str | None = None
    duration_seconds: float | None = None


@dataclass(frozen=True)
class VerifyGateLookup:
    """Resolved latest verify evidence for an epoch."""

    result: VerifyGateResult | None
    source: Literal["owner_artifact", "legacy_review"] | None
    is_current: bool
    has_owner_artifact: bool
    artifact_id: int | None = None
    artifact_metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class LatestVerifyEvidence:
    """Latest persisted verify evidence paired with its original epoch identity."""

    result: VerifyGateResult
    epoch: VerifyEpoch
    source: Literal["owner_artifact", "legacy_review"]
    has_owner_artifact: bool


@dataclass(frozen=True)
class VerifyReadModel:
    """Operator-facing verify read model shared across query and inspect paths."""

    result: VerifyGateResult
    source: Literal["owner_artifact", "legacy_review"]
    is_current: bool
    has_owner_artifact: bool
    owner_task_id: str | None
    source_task_id: str | None
    source_task_type: str | None
    legacy_markdown: str | None = None


@dataclass(frozen=True)
class VerifyGateDecision:
    """Canonical lifecycle verify readiness for one implementation owner."""

    owner_task_id: str | None
    current_epoch: VerifyEpoch | None
    lookup: VerifyGateLookup
    state: Literal["passed", "missing", "stale", "failed", "unavailable"]


@dataclass(frozen=True)
class MergeUnitVerifyEvidenceSelection:
    """Newest current verify evidence selected across one merge unit."""

    owner_task: Task
    source_task: Task
    lookup: VerifyGateLookup


@dataclass(frozen=True)
class FullVerifyRuntimeObservation:
    """Recent successful full-suite runtime evidence from a verify artifact."""

    duration_seconds: float
    captured_at: datetime
    artifact_id: int | None
    task_id: str


def verify_result_is_timeout_origin(result: VerifyGateResult | None) -> bool:
    """Return whether a red verify result is structured timeout evidence."""
    if result is None or result.status != "failed":
        return False
    failure_origin = getattr(result, "failure_origin", None)
    if failure_origin is not None:
        return failure_origin == "timeout"
    exit_status = result.exit_status.strip().lower()
    failure = (result.failure or "").strip().lower()
    if exit_status in {"timed out", "timeout"}:
        return True
    return "verify_command timed out" in failure


def verify_result_is_budget_exceeded(result: VerifyGateResult | None) -> bool:
    """Return whether a timeout result has no executed red phase to fix in code."""
    if not verify_result_is_timeout_origin(result):
        return False
    summary = result.phase_summary if result is not None else None
    if not isinstance(summary, dict):
        return False
    failed = summary.get("failed")
    return isinstance(failed, list) and len(failed) == 0


def verify_result_has_invalid_phase_evidence(result: VerifyGateResult | None) -> bool:
    """Return whether structured timeout routing saw malformed phase evidence."""
    if not verify_result_is_timeout_origin(result):
        return False
    return bool(getattr(result, "phase_summary_invalid_reason", None))


def latest_successful_full_verify_runtime_observation(
    store: SqliteTaskStore,
    owner_task: Task,
    *,
    now: datetime,
    max_age_hours: int,
) -> FullVerifyRuntimeObservation | None:
    """Return the newest recent full ``./bin/tests`` runtime observation for the project."""
    if owner_task.id is None or max_age_hours < 1:
        return None
    cutoff = now - timedelta(hours=max_age_hours)
    for artifact in store.list_project_artifacts(kind=VERIFY_GATE_ARTIFACT_KIND):
        metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
        if metadata is None or metadata.get("schema_version") != VERIFY_GATE_ARTIFACT_SCHEMA_VERSION:
            continue
        result = _artifact_verify_result(metadata)
        if result is None:
            continue
        if result.captured_at < cutoff:
            continue
        if normalized_verify_command(result.command) != "./bin/tests":
            continue
        if result.status != "passed" or result.exit_status != "0":
            continue
        summary = result.phase_summary
        if summary is None:
            continue
        passed = summary.get("passed")
        never_started = summary.get("never_started")
        failed = summary.get("failed")
        running = summary.get("running")
        if (
            set(passed if isinstance(passed, list) else ()) != set(KNOWN_FULL_VERIFY_PHASES)
            or failed != []
            or running != []
            or never_started != []
        ):
            continue
        duration = _coerce_optional_float(result.duration_seconds)
        if isinstance(duration, bool) or not isinstance(duration, int | float) or duration < 0:
            continue
        return FullVerifyRuntimeObservation(
            duration_seconds=float(duration),
            captured_at=result.captured_at,
            artifact_id=artifact.id,
            task_id=artifact.task_id,
        )
    return None


def _extract_verify_phase_events(output: str | None) -> list[dict[str, Any]]:
    if not output:
        return []
    matches = re.finditer(
        r"^gza-verify phase=(?P<status>start|passed|failed) name=(?P<name>[A-Za-z0-9_.-]+)"
        r"(?: duration_seconds=(?P<duration>[0-9.]+))?"
        r"(?: tree_fingerprint=(?P<tree_fingerprint>[0-9a-f]{64}))?$",
        output,
        re.MULTILINE,
    )
    events: list[dict[str, Any]] = []
    for match in matches:
        event: dict[str, Any] = {
            "name": match.group("name"),
            "status": match.group("status"),
        }
        duration = match.group("duration")
        if duration is not None:
            event["duration_seconds"] = float(duration)
        tree_fingerprint = match.group("tree_fingerprint")
        if tree_fingerprint:
            event["tree_fingerprint"] = tree_fingerprint
        events.append(event)
    return events


def _summarize_verify_phase_events(
    events: list[dict[str, Any]],
    *,
    command: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    command_name = normalized_verify_command(command)
    if not events and command_name != "./bin/tests":
        return None, None

    completed: list[dict[str, Any]] = []
    started: list[str] = []
    started_names: set[str] = set()
    terminal_names: set[str] = set()
    active_phase: str | None = None
    for event in events:
        name = event["name"]
        status = event["status"]
        if status == "start":
            if active_phase is not None:
                return None, f"phase {active_phase} lacks terminal before phase {name} starts"
            if name in started_names:
                return None, f"phase {name} has duplicate start"
            if name in terminal_names:
                return None, f"phase {name} start appears after terminal"
            started.append(name)
            started_names.add(name)
            active_phase = name
            continue
        if name not in started_names:
            return None, f"terminal phase lacks start: {name}"
        if name in terminal_names:
            return None, f"phase {name} has duplicate terminal"
        if active_phase != name:
            return None, f"terminal phase out of order: {name}"
        completed.append(event)
        terminal_names.add(name)
        active_phase = None

    running = [name for name in started if name not in terminal_names]
    completed_names = [str(phase["name"]) for phase in completed]
    observed_names = [str(event["name"]) for event in events]
    never_started: list[str] = []
    if command_name == "./bin/tests":
        observed_set = set(observed_names)
        never_started = [phase for phase in KNOWN_FULL_VERIFY_PHASES if phase not in observed_set]
    total_duration_seconds = 0.0
    duration_seen = False
    for phase in completed:
        duration = phase.get("duration_seconds")
        if isinstance(duration, int | float):
            total_duration_seconds += float(duration)
            duration_seen = True

    return {
        "completed": completed,
        "passed": [str(phase["name"]) for phase in completed if phase.get("status") == "passed"],
        "failed": [str(phase["name"]) for phase in completed if phase.get("status") == "failed"],
        "running": running,
        "never_started": never_started,
        "last_observed": observed_names[-1] if observed_names else None,
        "observed_count": len(observed_names),
        "completed_count": len(completed_names),
        "total_duration_seconds": total_duration_seconds if duration_seen else None,
    }, None


def summarize_verify_phases(*, command: str | None, output: str | None) -> dict[str, Any] | None:
    """Summarize structured verify-phase output for operator-facing timeout routing."""
    summary, _invalid_reason = summarize_verify_phases_with_validation(command=command, output=output)
    return summary


def summarize_verify_phases_with_validation(
    *,
    command: str | None,
    output: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Summarize structured verify output while preserving lifecycle-order validation."""
    events = _extract_verify_phase_events(output)
    return _summarize_verify_phase_events(events, command=command)


def validate_verify_phase_summary(
    value: object,
    *,
    require_known_full_verify_partition: bool = False,
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate the complete persisted phase summary schema."""
    if value is None:
        return None, None
    if not isinstance(value, dict):
        return None, "phase_summary must be an object"
    value_dict = cast("dict[str, Any]", value)
    required_list_fields = ("completed", "passed", "failed", "running", "never_started")
    for field in required_list_fields:
        if not isinstance(value_dict.get(field), list):
            return None, f"phase_summary.{field} must be a list"
    completed_list = value_dict["completed"]
    passed_list = value_dict["passed"]
    failed_list = value_dict["failed"]
    running_list = value_dict["running"]
    never_started_list = value_dict["never_started"]
    for field in ("observed_count", "completed_count"):
        raw = value_dict.get(field)
        if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
            return None, f"phase_summary.{field} must be a non-negative integer"
    observed_count = value_dict["observed_count"]
    completed_count = value_dict["completed_count"]
    last_observed = value_dict.get("last_observed")
    if last_observed is not None and not isinstance(last_observed, str):
        return None, "phase_summary.last_observed must be a string or null"
    total_duration = value_dict.get("total_duration_seconds")
    if total_duration is not None:
        if isinstance(total_duration, bool) or not isinstance(total_duration, int | float) or total_duration < 0:
            return None, "phase_summary.total_duration_seconds must be a non-negative number or null"
    completed_names: list[str] = []
    for phase in completed_list:
        if not isinstance(phase, dict):
            return None, "phase_summary.completed entries must be objects"
        name = phase.get("name")
        status = phase.get("status")
        if not isinstance(name, str) or not name:
            return None, "phase_summary.completed entries require string name"
        if status not in {"passed", "failed"}:
            return None, "phase_summary.completed entries require passed or failed status"
        duration = phase.get("duration_seconds")
        if duration is not None:
            if isinstance(duration, bool) or not isinstance(duration, int | float) or duration < 0:
                return None, "phase_summary.completed duration_seconds must be non-negative"
        completed_names.append(name)
    for field, names in (
        ("passed", passed_list),
        ("failed", failed_list),
        ("running", running_list),
        ("never_started", never_started_list),
    ):
        for name in names:
            if not isinstance(name, str) or not name:
                return None, f"phase_summary.{field} entries must be strings"
    passed_names = [str(name) for name in passed_list]
    failed_names = [str(name) for name in failed_list]
    running_names = [str(name) for name in running_list]
    never_started_names = [str(name) for name in never_started_list]
    if completed_count != len(completed_list):
        return None, "phase_summary.completed_count does not match completed"
    minimum_observed_count = completed_count + len(running_names)
    if observed_count < minimum_observed_count:
        return None, "phase_summary.observed_count must include completed phases and running starts"
    if observed_count == 0 and last_observed is not None:
        return None, "phase_summary.last_observed must be null when observed_count is zero"
    if observed_count > 0 and last_observed is None:
        return None, "phase_summary.last_observed is required when observed_count is non-zero"
    for field, names in (
        ("completed", completed_names),
        ("passed", passed_names),
        ("failed", failed_names),
        ("running", running_names),
        ("never_started", never_started_names),
    ):
        if len(names) != len(set(names)):
            return None, f"phase_summary.{field} contains duplicate phases"
    expected_passed = [str(phase["name"]) for phase in completed_list if phase.get("status") == "passed"]
    expected_failed = [str(phase["name"]) for phase in completed_list if phase.get("status") == "failed"]
    if passed_names != expected_passed:
        return None, "phase_summary.passed does not match completed passed phases"
    if failed_names != expected_failed:
        return None, "phase_summary.failed does not match completed failed phases"
    for first_field, first_names, second_field, second_names in (
        ("passed", passed_names, "failed", failed_names),
        ("completed", completed_names, "running", running_names),
        ("completed", completed_names, "never_started", never_started_names),
        ("running", running_names, "never_started", never_started_names),
    ):
        overlap = set(first_names) & set(second_names)
        if overlap:
            return None, f"phase_summary.{first_field} overlaps phase_summary.{second_field}"
    if last_observed is not None and last_observed not in {*completed_names, *running_names}:
        return None, "phase_summary.last_observed must name a completed or running phase"
    all_state_names = completed_names + running_names + never_started_names
    if require_known_full_verify_partition:
        unknown_known_command_names = sorted(set(all_state_names) - set(KNOWN_FULL_VERIFY_PHASES))
        if unknown_known_command_names:
            return None, "phase_summary contains unknown ./bin/tests phases: " + ", ".join(unknown_known_command_names)
        missing_known = [phase for phase in KNOWN_FULL_VERIFY_PHASES if phase not in all_state_names]
        if missing_known:
            return None, "phase_summary omits known ./bin/tests phases: " + ", ".join(missing_known)
    copied: dict[str, Any] = deepcopy(value_dict)
    copied["completed"] = [deepcopy(phase) for phase in completed_list]
    return copied, None


def normalized_verify_command(command: str | None) -> str | None:
    """Return normalized verify command provenance for persisted evidence."""
    if not isinstance(command, str):
        return None
    normalized = command.strip()
    return normalized or None


def make_verify_epoch(
    *,
    reviewed_branch: str | None,
    reviewed_head_sha: str | None,
    verify_command: str | None,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
    reviewed_tree_sha: str | None = None,
) -> VerifyEpoch:
    """Build canonical verify epoch metadata for one branch source epoch."""
    return VerifyEpoch(
        reviewed_branch=reviewed_branch,
        reviewed_head_sha=reviewed_head_sha,
        verify_command=normalized_verify_command(verify_command),
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
        reviewed_tree_sha=reviewed_tree_sha if isinstance(reviewed_tree_sha, str) and reviewed_tree_sha else None,
    )


def verify_epoch_matches(*, expected: VerifyEpoch, candidate: VerifyEpoch) -> bool:
    """Return whether two verify epochs cover the same reviewed branch source.

    The verify command and timeout settings are persisted as run provenance, but
    they are not freshness identity: changing the command or budget does not
    make same-source evidence stale or current.
    """
    if not (
        isinstance(expected.reviewed_branch, str)
        and expected.reviewed_branch.strip()
        and isinstance(candidate.reviewed_branch, str)
        and candidate.reviewed_branch.strip()
    ):
        return False
    if expected.reviewed_branch != candidate.reviewed_branch:
        return False
    head_matches = bool(
        expected.reviewed_head_sha
        and candidate.reviewed_head_sha
        and expected.reviewed_head_sha == candidate.reviewed_head_sha
    )
    tree_matches = bool(
        expected.reviewed_tree_sha
        and candidate.reviewed_tree_sha
        and expected.reviewed_tree_sha == candidate.reviewed_tree_sha
    )
    return head_matches or tree_matches


def _task_verify_result(task: Task) -> VerifyGateResult | None:
    if (
        not task.review_verify_command
        or not task.review_verify_status
        or not task.review_verify_exit_status
        or task.review_verify_captured_at is None
    ):
        return None
    return VerifyGateResult(
        command=task.review_verify_command,
        status=task.review_verify_status,
        exit_status=task.review_verify_exit_status,
        captured_at=task.review_verify_captured_at,
        reviewed_branch=task.review_verify_branch,
        reviewed_head_sha=task.review_verify_head_sha,
        reviewed_tree_sha=None,
        reviewed_base_sha=task.review_verify_base_sha,
        working_directory=task.review_verify_cwd,
        failure=task.review_verify_failure,
        source_task_id=task.id,
        source_task_type=task.task_type,
        output_artifact_path=task.review_verify_artifact_file,
    )


def _result_epoch(
    result: VerifyGateResult,
    *,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
) -> VerifyEpoch:
    return make_verify_epoch(
        reviewed_branch=result.reviewed_branch,
        reviewed_head_sha=result.reviewed_head_sha,
        reviewed_tree_sha=result.reviewed_tree_sha,
        verify_command=result.command,
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
    )


def _legacy_result_epoch(result: VerifyGateResult) -> VerifyEpoch:
    """Build legacy review freshness identity without projecting current config metadata."""
    return _result_epoch(
        result,
        verify_timeout_seconds=None,
        verify_timeout_grace_seconds=None,
    )


def _coerce_optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _coerce_optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _coerce_optional_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _coerce_phase_name_list(value: object) -> list[str] | None:
    if not isinstance(value, list):
        return None
    names: list[str] = []
    for name in value:
        if not isinstance(name, str) or not name:
            return None
        names.append(name)
    return names


def _coerce_failure_origin(result_payload: dict[str, Any]) -> str | None:
    if "failure_origin" not in result_payload:
        return None
    value = result_payload["failure_origin"]
    if isinstance(value, str) and value:
        return value
    return INVALID_STRUCTURED_FAILURE_ORIGIN


def _first_present_str(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _verify_gate_tree_fingerprint(
    *,
    provenance: dict[str, Any] | None,
    aggregate_details: dict[str, Any] | None,
) -> str | None:
    """Return the exact tree fingerprint carried by canonical verify metadata."""
    if isinstance(aggregate_details, dict):
        value = aggregate_details.get("tree_fingerprint")
        if _aggregate_tree_fingerprint_is_complete(aggregate_details) and isinstance(value, str) and value:
            return value
        if _aggregate_details_is_cross_project(aggregate_details):
            return None
    if isinstance(provenance, dict):
        value = provenance.get("tree_fingerprint")
        if isinstance(value, str) and value:
            return value
    return None


def _aggregate_details_is_cross_project(aggregate_details: dict[str, Any]) -> bool:
    return (
        isinstance(aggregate_details.get("scopes"), list)
        or "runnable_count" in aggregate_details
        or "tree_fingerprint_complete" in aggregate_details
        or "tree_fingerprint_contradictory" in aggregate_details
    )


def _aggregate_tree_fingerprint_is_complete(aggregate_details: dict[str, Any]) -> bool:
    if aggregate_details.get("tree_fingerprint_complete") is True:
        return True
    phase_results = aggregate_details.get("phase_results")
    runnable_count = aggregate_details.get("runnable_count")
    if not isinstance(phase_results, list) or not isinstance(runnable_count, int) or isinstance(runnable_count, bool):
        return False
    if runnable_count <= 0 or len(phase_results) != runnable_count:
        return False
    fingerprints: list[str] = []
    for phase in phase_results:
        if not isinstance(phase, dict):
            return False
        value = phase.get("tree_fingerprint")
        if not isinstance(value, str) or not value:
            return False
        fingerprints.append(value)
    return bool(fingerprints) and all(fingerprint == fingerprints[-1] for fingerprint in fingerprints)


def _read_persisted_verify_output(project_dir: Path | None, stored_path: str | None) -> str | None:
    if project_dir is None or stored_path is None:
        return None
    try:
        resolved = resolve_artifact_path(project_dir, stored_path)
    except (InvalidArtifactPathError, OSError, RuntimeError, ValueError):
        return None
    try:
        content = resolved.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    return content if content.strip() else None


def _phase_summary_from_diagnostics_details(value: object) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(value, dict):
        return None, None
    value_dict = cast(dict[str, Any], value)
    phase_results = value_dict.get("phase_results")
    if not isinstance(phase_results, list):
        return None, None
    completed_names_raw = value_dict.get("completed_phase_names")
    if not isinstance(completed_names_raw, list):
        return None, "phase diagnostics completed_phase_names must be a list"
    completed_names: list[str] = []
    for name in completed_names_raw:
        if not isinstance(name, str) or not name:
            return None, "phase diagnostics completed_phase_names entries must be strings"
        completed_names.append(name)
    if len(completed_names) != len(set(completed_names)):
        return None, "phase diagnostics completed_phase_names contains duplicate phases"
    failed_names_raw = value_dict.get("failed_phase_names")
    if not isinstance(failed_names_raw, list):
        return None, "phase diagnostics failed_phase_names must be a list"
    supplied_failed_names: list[str] = []
    for name in failed_names_raw:
        if not isinstance(name, str) or not name:
            return None, "phase diagnostics failed_phase_names entries must be strings"
        supplied_failed_names.append(name)
    if len(supplied_failed_names) != len(set(supplied_failed_names)):
        return None, "phase diagnostics failed_phase_names contains duplicate phases"
    expected_names_raw = value_dict.get("expected_phase_names")
    if not isinstance(expected_names_raw, list):
        return None, "phase diagnostics expected_phase_names must be a list"
    expected_names: list[str] = []
    for name in expected_names_raw:
        if not isinstance(name, str) or not name:
            return None, "phase diagnostics expected_phase_names entries must be strings"
        expected_names.append(name)
    if len(expected_names) != len(set(expected_names)):
        return None, "phase diagnostics expected_phase_names contains duplicate phases"
    expected_phase_partition_raw = value_dict.get("expected_phase_partition")
    if expected_phase_partition_raw in {"known", "unknown"}:
        if expected_phase_partition_raw == "known" and not expected_names:
            return None, "phase diagnostics known expected partition lacks phases"
        if expected_phase_partition_raw == "unknown" and expected_names:
            return None, "phase diagnostics unknown expected partition has phases"
    elif "expected_phase_partition" in value_dict:
        return None, "phase diagnostics expected_phase_partition must be known or unknown"
    not_started_names_raw = value_dict.get("not_started_phase_names")
    never_started_names_raw = value_dict.get("never_started")
    has_not_started = "not_started_phase_names" in value_dict
    has_never_started = "never_started" in value_dict
    not_started_alias = _coerce_phase_name_list(not_started_names_raw) if has_not_started else None
    never_started_alias = _coerce_phase_name_list(never_started_names_raw) if has_never_started else None
    if has_not_started and not_started_alias is None:
        return None, "phase diagnostics not_started_phase_names must be a list"
    if has_never_started and never_started_alias is None:
        return None, "phase diagnostics not_started_phase_names must be a list"
    if has_not_started and has_never_started and not_started_alias != never_started_alias:
        return None, "phase diagnostics not_started_phase_names contradict never_started"
    if has_not_started:
        not_started_names = not_started_alias
    elif has_never_started:
        not_started_names = never_started_alias
    else:
        not_started_names = None
    if not_started_names is None:
        return None, "phase diagnostics not_started_phase_names must be a list"
    if len(not_started_names) != len(set(not_started_names)):
        return None, "phase diagnostics not_started_phase_names contains duplicate phases"
    started_names_raw = value_dict.get("started_phase_names")
    if not isinstance(started_names_raw, list):
        return None, "phase diagnostics started_phase_names must be a list"
    started_names: list[str] = []
    for name in started_names_raw:
        if not isinstance(name, str) or not name:
            return None, "phase diagnostics started_phase_names entries must be strings"
        started_names.append(name)
    if len(started_names) != len(set(started_names)):
        return None, "phase diagnostics started_phase_names contains duplicate phases"
    running_names_raw = value_dict.get("running_phase_names")
    supplied_running_names: list[str] | None = None
    if running_names_raw is not None:
        if not isinstance(running_names_raw, list):
            return None, "phase diagnostics running_phase_names must be a list"
        supplied_running_names = []
        for name in running_names_raw:
            if not isinstance(name, str) or not name:
                return None, "phase diagnostics running_phase_names entries must be strings"
            supplied_running_names.append(name)
        if len(supplied_running_names) != len(set(supplied_running_names)):
            return None, "phase diagnostics running_phase_names contains duplicate phases"
    started_name_set = set(started_names)
    terminal_names: set[str] = set()
    completed: list[dict[str, Any]] = []
    passed: list[str] = []
    failed: list[str] = []
    total_duration = 0.0
    duration_seen = False
    for phase in phase_results:
        if not isinstance(phase, dict):
            return None, "phase diagnostics phase_results entries must be objects"
        name = phase.get("name")
        status = phase.get("status")
        if not isinstance(name, str) or not name:
            return None, "phase diagnostics phase_results entries require string name"
        if status not in {"passed", "failed"}:
            return None, "phase diagnostics phase_results entries require passed or failed status"
        if name not in started_name_set:
            return None, f"terminal phase lacks start: {name}"
        if name in terminal_names:
            return None, f"phase {name} has duplicate terminal"
        terminal_names.add(name)
        completed_phase = cast(dict[str, Any], deepcopy(phase))
        completed.append(completed_phase)
        if status == "passed":
            passed.append(name)
        else:
            failed.append(name)
        duration = phase.get("duration_seconds")
        if isinstance(duration, int | float) and not isinstance(duration, bool):
            total_duration += float(duration)
            duration_seen = True
    if completed_names != [str(phase["name"]) for phase in completed]:
        return None, "phase diagnostics completed_phase_names contradict phase_results"
    if supplied_failed_names != failed:
        return None, "phase diagnostics failed_phase_names contradict phase_results"
    completed_count = value_dict.get("completed_count")
    if completed_count is not None and (
        not isinstance(completed_count, int) or isinstance(completed_count, bool) or completed_count != len(completed)
    ):
        return None, "phase diagnostics completed_count contradict phase_results"
    failed_count = value_dict.get("failed_count")
    if failed_count is not None and (
        not isinstance(failed_count, int) or isinstance(failed_count, bool) or failed_count != len(failed)
    ):
        return None, "phase diagnostics failed_count contradict phase_results"
    running = [name for name in started_names if name not in set(completed_names)]
    if supplied_running_names is not None and supplied_running_names != running:
        return None, "phase diagnostics running_phase_names contradict phase_results"
    if expected_names:
        expected_set = set(expected_names)
        if any(name not in expected_set for name in (*started_names, *completed_names, *not_started_names)):
            return None, "phase diagnostics names outside expected_phase_names"
        expected_not_started = [
            name for name in expected_names if name not in set(started_names) and name not in set(completed_names)
        ]
        if expected_not_started != not_started_names:
            return None, "phase diagnostics not_started_phase_names contradict phase_results"
    elif not_started_names:
        return None, "phase diagnostics not_started_phase_names without expected_phase_names"
    observed_count = len(completed) + len(running)
    last_observed: str | None = None
    if running:
        last_observed = running[-1]
    elif completed:
        last_name = completed[-1].get("name")
        last_observed = last_name if isinstance(last_name, str) else None
    summary = {
        "completed": completed,
        "passed": passed,
        "failed": failed,
        "running": running,
        "never_started": not_started_names,
        "last_observed": last_observed,
        "observed_count": observed_count,
        "completed_count": len(completed),
        "failed_count": len(failed),
        "total_duration_seconds": total_duration if duration_seen else None,
    }
    for key in (
        "passed",
        "failed",
        "running",
        "never_started",
        "last_observed",
        "observed_count",
        "completed_count",
        "failed_count",
        "total_duration_seconds",
    ):
        if key in value_dict:
            summary[key] = deepcopy(value_dict[key])
    return summary, None


def _aggregate_phase_summary_from_details(value: object) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(value, dict):
        return None, None
    lifecycle_invalid_reason = value.get("phase_lifecycle_invalid_reason")
    if isinstance(lifecycle_invalid_reason, str) and lifecycle_invalid_reason:
        return None, lifecycle_invalid_reason
    scopes = value.get("scopes")
    phase_results = value.get("phase_results")
    if not isinstance(scopes, list) and isinstance(phase_results, list):
        if not phase_results:
            return None, "missing phase results"
        aggregate_completed: list[dict[str, Any]] = []
        aggregate_passed: list[str] = []
        aggregate_failed: list[str] = []
        aggregate_total_duration = 0.0
        aggregate_duration_seen = False
        aggregate_completed_names: set[str] = set()
        for phase in phase_results:
            if not isinstance(phase, dict):
                return None, "malformed phase result"
            name = phase.get("name")
            status = phase.get("status")
            if not isinstance(name, str) or not name:
                return None, "malformed phase name"
            if status not in {"passed", "failed"}:
                return None, "malformed phase status"
            aggregate_completed.append(cast(dict[str, Any], deepcopy(phase)))
            aggregate_completed_names.add(name)
            if status == "passed":
                aggregate_passed.append(name)
            else:
                aggregate_failed.append(name)
            duration = phase.get("duration_seconds")
            if isinstance(duration, int | float) and not isinstance(duration, bool):
                aggregate_total_duration += float(duration)
                aggregate_duration_seen = True
        started_names_raw = value.get("started_phase_names")
        if not isinstance(started_names_raw, list):
            return None, "malformed phase name summary"
        started_names: list[str] = []
        for name in started_names_raw:
            if not isinstance(name, str) or not name:
                return None, "malformed phase name summary"
            started_names.append(name)
        has_not_started = "not_started_phase_names" in value
        has_never_started = "never_started" in value
        not_started_alias = _coerce_phase_name_list(value.get("not_started_phase_names")) if has_not_started else None
        never_started_alias = _coerce_phase_name_list(value.get("never_started")) if has_never_started else None
        if has_not_started and not_started_alias is None:
            return None, "malformed phase name summary"
        if has_never_started and never_started_alias is None:
            return None, "malformed phase name summary"
        if has_not_started and has_never_started and not_started_alias != never_started_alias:
            return None, "not-started phase aliases contradict"
        if has_not_started:
            aggregate_never_started = not_started_alias
        elif has_never_started:
            aggregate_never_started = never_started_alias
        else:
            aggregate_never_started = None
        if aggregate_never_started is None:
            return None, "malformed phase name summary"
        aggregate_running = [name for name in started_names if name not in aggregate_completed_names]
        completed_count = value.get("completed_count")
        if completed_count is not None and (
            not isinstance(completed_count, int)
            or isinstance(completed_count, bool)
            or completed_count != len(aggregate_completed)
        ):
            return None, "completed_count contradicts results"
        failed_count = value.get("failed_count")
        if failed_count is not None and (
            not isinstance(failed_count, int)
            or isinstance(failed_count, bool)
            or failed_count != len(aggregate_failed)
        ):
            return None, "failed_count contradicts results"
        aggregate_summary: dict[str, Any] = {
            "completed": aggregate_completed,
            "passed": aggregate_passed,
            "failed": aggregate_failed,
            "running": aggregate_running,
            "never_started": aggregate_never_started,
            "last_observed": aggregate_running[-1] if aggregate_running else aggregate_completed[-1]["name"],
            "observed_count": len(aggregate_completed) + len(aggregate_running),
            "completed_count": len(aggregate_completed),
            "total_duration_seconds": aggregate_total_duration if aggregate_duration_seen else None,
        }
        return validate_verify_phase_summary(aggregate_summary)
    if not isinstance(scopes, list):
        return None, None
    completed: list[dict[str, Any]] = []
    passed: list[str] = []
    failed: list[str] = []
    running: list[str] = []
    never_started: list[str] = []
    observed_count = 0
    total_duration = 0.0
    duration_seen = False
    last_observed: str | None = None
    runnable_seen = False
    seen_scope_identities: set[str] = set()
    expected_names: list[str] = []
    for index, scope in enumerate(scopes):
        if not isinstance(scope, dict):
            return None, f"aggregate scope entry {index}: must be an object"
        scope_identity = scope.get("scope")
        if not isinstance(scope_identity, str) or not scope_identity:
            return None, f"aggregate scope entry {index}: scope identity is required"
        if scope_identity in seen_scope_identities:
            return None, "duplicate scope identity"
        seen_scope_identities.add(scope_identity)
        scope_invalid_reason = scope.get("phase_summary_invalid_reason")
        if isinstance(scope_invalid_reason, str) and scope_invalid_reason:
            return None, scope_invalid_reason
        if scope.get("status") == "skipped":
            continue
        runnable_seen = True
        command = _coerce_optional_str(scope.get("command_identity"))
        scope_summary = scope.get("phase_summary")
        scope_valid_summary, invalid_reason = validate_verify_phase_summary(
            scope_summary,
            require_known_full_verify_partition=normalized_verify_command(command) == "./bin/tests",
        )
        if invalid_reason is not None:
            return None, f"aggregate scope {scope_identity}: {invalid_reason}"
        diagnostics_summary, diagnostics_invalid_reason = _phase_summary_from_diagnostics_details(
            scope.get("phase_diagnostics")
        )
        if diagnostics_invalid_reason is not None:
            return None, f"aggregate scope {scope_identity}: {diagnostics_invalid_reason}"
        diagnostics_valid_summary: dict[str, Any] | None = None
        if diagnostics_summary is not None:
            diagnostics_valid_summary, diagnostics_summary_invalid_reason = validate_verify_phase_summary(
                diagnostics_summary,
                require_known_full_verify_partition=normalized_verify_command(command) == "./bin/tests",
            )
            if diagnostics_summary_invalid_reason is not None:
                return None, f"aggregate scope {scope_identity}: {diagnostics_summary_invalid_reason}"
        if scope_valid_summary is not None and diagnostics_valid_summary is not None:
            if scope_valid_summary.get("completed") != diagnostics_valid_summary.get("completed"):
                return None, f"aggregate scope {scope_identity}: phase_summary contradicts phase_diagnostics"
            diagnostics = scope.get("phase_diagnostics")
            for key in ("passed", "failed", "running", "never_started"):
                if scope_valid_summary.get(key) != diagnostics_valid_summary.get(key):
                    return None, f"aggregate scope {scope_identity}: phase_summary contradicts phase_diagnostics"
            for key in (
                "last_observed",
                "observed_count",
                "completed_count",
                "failed_count",
                "total_duration_seconds",
            ):
                if isinstance(diagnostics, dict) and key in diagnostics and key in scope_valid_summary:
                    if scope_valid_summary.get(key) != diagnostics_valid_summary.get(key):
                        return None, f"aggregate scope {scope_identity}: phase_summary contradicts phase_diagnostics"
        if scope_valid_summary is None:
            scope_valid_summary = diagnostics_valid_summary
        if scope_valid_summary is None:
            return None, f"aggregate scope {scope_identity}: phase_summary is missing"
        prefix = scope_identity

        def _scoped(name: str) -> str:
            return f"{prefix}:{name}"
        for phase in scope_valid_summary["completed"]:
            scoped_phase = deepcopy(phase)
            scoped_phase["name"] = _scoped(str(phase["name"]))
            completed.append(scoped_phase)
            duration = phase.get("duration_seconds")
            if isinstance(duration, int | float) and not isinstance(duration, bool):
                total_duration += float(duration)
                duration_seen = True
        passed.extend(_scoped(str(name)) for name in scope_valid_summary["passed"])
        failed.extend(_scoped(str(name)) for name in scope_valid_summary["failed"])
        running.extend(_scoped(str(name)) for name in scope_valid_summary["running"])
        never_started.extend(_scoped(str(name)) for name in scope_valid_summary["never_started"])
        scope_expected_partition = "unknown"
        scope_expected_names: list[str] = []
        diagnostics = scope.get("phase_diagnostics")
        if isinstance(diagnostics, dict):
            raw_partition = diagnostics.get("expected_phase_partition")
            raw_expected_names = diagnostics.get("expected_phase_names")
            coerced_expected_names = _coerce_phase_name_list(raw_expected_names)
            if raw_partition == "known" and coerced_expected_names:
                scope_expected_partition = "known"
                scope_expected_names = coerced_expected_names
            elif raw_partition not in {"known", "unknown"} and coerced_expected_names:
                scope_expected_partition = "known"
                scope_expected_names = coerced_expected_names
        command_identity = _coerce_optional_str(scope.get("command_identity"))
        if normalized_verify_command(command_identity) == "./bin/tests":
            scope_expected_partition = "known"
            scope_expected_names = list(KNOWN_FULL_VERIFY_PHASES)
        if scope_expected_partition == "known":
            expected_names.extend(_scoped(str(name)) for name in scope_expected_names)
        observed_count += int(scope_valid_summary["observed_count"])
        if isinstance(scope_valid_summary["last_observed"], str):
            last_observed = _scoped(scope_valid_summary["last_observed"])
    if not runnable_seen:
        return None, None
    supplied_expected_names = value.get("expected_phase_names")
    if supplied_expected_names is not None:
        coerced_expected = _coerce_phase_name_list(supplied_expected_names)
        if coerced_expected is None:
            return None, "malformed phase name summary"
        if coerced_expected != expected_names:
            return None, "scoped expected phases contradict aggregate"
    summary: dict[str, Any] = {
        "completed": completed,
        "passed": passed,
        "failed": failed,
        "running": running,
        "never_started": never_started,
        "last_observed": last_observed,
        "observed_count": observed_count,
        "completed_count": len(completed),
        "failed_count": len(failed),
        "total_duration_seconds": total_duration if duration_seen else None,
    }
    return validate_verify_phase_summary(summary)


def _phase_summary_from_canonical_validation(validation: object) -> dict[str, Any]:
    phase_results = tuple(getattr(validation, "phase_results"))
    completed_phase_names = tuple(getattr(validation, "completed_phase_names"))
    started_phase_names = tuple(getattr(validation, "started_phase_names"))
    not_started_phase_names = tuple(getattr(validation, "not_started_phase_names"))
    completed: list[dict[str, Any]] = []
    passed: list[str] = []
    failed: list[str] = []
    total_duration = 0.0
    duration_seen = False
    for phase, completed_name in zip(phase_results, completed_phase_names, strict=True):
        copied = deepcopy(phase)
        copied.pop("scope", None)
        copied["name"] = completed_name
        completed.append(copied)
        if copied.get("status") == "passed":
            passed.append(completed_name)
        elif copied.get("status") == "failed":
            failed.append(completed_name)
        duration = copied.get("duration_seconds")
        if isinstance(duration, int | float) and not isinstance(duration, bool):
            duration_value = float(duration)
            if math.isfinite(duration_value) and duration_value >= 0:
                total_duration += duration_value
                duration_seen = True
    completed_name_set = set(completed_phase_names)
    running = [name for name in started_phase_names if name not in completed_name_set]
    last_observed = running[-1] if running else (completed[-1]["name"] if completed else None)
    return {
        "completed": completed,
        "passed": passed,
        "failed": failed,
        "running": running,
        "never_started": list(not_started_phase_names),
        "last_observed": last_observed,
        "observed_count": len(completed) + len(running),
        "completed_count": len(completed),
        "failed_count": len(failed),
        "total_duration_seconds": total_duration if duration_seen else None,
    }


def _resolve_artifact_phase_summary(
    metadata: dict[str, Any],
    *,
    command: str,
    exit_status: str,
    failure: str | None,
    failure_origin: str | None,
    output_artifact_path: str | None,
    project_dir: Path | None,
) -> tuple[dict[str, Any] | None, str | None]:
    metadata_invalid_reason = metadata.get("phase_summary_invalid_reason")
    if isinstance(metadata_invalid_reason, str) and metadata_invalid_reason:
        return None, metadata_invalid_reason
    if "aggregate_details" in metadata:
        aggregate_details = metadata.get("aggregate_details")
        if not isinstance(aggregate_details, dict):
            return None, "malformed aggregate_details: must be an object"
        from gza.runner import PHASE_EVIDENCE_INDETERMINATE, validate_verify_phase_evidence_from_metadata

        validation = validate_verify_phase_evidence_from_metadata(metadata)
        if validation.state == PHASE_EVIDENCE_INDETERMINATE:
            return None, validation.reason or "indeterminate phase evidence"
        return validate_verify_phase_summary(_phase_summary_from_canonical_validation(validation))

    summary, invalid_reason = validate_verify_phase_summary(
        metadata.get("phase_summary"),
        require_known_full_verify_partition=normalized_verify_command(command) == "./bin/tests",
    )
    if invalid_reason is not None or summary is not None:
        return summary, invalid_reason

    if failure_origin != "timeout":
        if failure_origin is not None:
            return None, None
        if exit_status.strip().lower() not in {"timed out", "timeout"} and "verify_command timed out" not in (
            failure or ""
        ).strip().lower():
            return None, None

    output = _read_persisted_verify_output(project_dir, output_artifact_path)
    if output is None:
        return None, "phase_summary missing and persisted verify output is unavailable"
    parsed, parse_invalid_reason = summarize_verify_phases_with_validation(command=command, output=output)
    if parse_invalid_reason is not None:
        return None, parse_invalid_reason
    if parsed is None:
        return None, "phase_summary missing and persisted verify output has no structured phase evidence"
    return validate_verify_phase_summary(parsed)


def _artifact_verify_result(metadata: dict[str, Any], *, project_dir: Path | None = None) -> VerifyGateResult | None:
    result_payload = metadata.get("result")
    if not isinstance(result_payload, dict):
        return None
    captured_at_raw = result_payload.get("captured_at")
    if not isinstance(captured_at_raw, str):
        return None
    try:
        captured_at = datetime.fromisoformat(captured_at_raw)
    except ValueError:
        return None
    command = _coerce_optional_str(result_payload.get("command"))
    status = _coerce_optional_str(result_payload.get("status"))
    exit_status = _coerce_optional_str(result_payload.get("exit_status"))
    if command is None or status is None or exit_status is None:
        return None
    failure_origin = _coerce_failure_origin(result_payload)
    output_artifact_path = _coerce_optional_str(metadata.get("output_artifact_path"))
    phase_summary, phase_summary_invalid_reason = _resolve_artifact_phase_summary(
        metadata,
        command=command,
        exit_status=exit_status,
        failure=_coerce_optional_str(result_payload.get("failure")),
        failure_origin=failure_origin,
        output_artifact_path=output_artifact_path,
        project_dir=project_dir,
    )
    return VerifyGateResult(
        command=command,
        status=status,
        exit_status=exit_status,
        captured_at=captured_at,
        reviewed_branch=_coerce_optional_str(result_payload.get("reviewed_branch")),
        reviewed_head_sha=_coerce_optional_str(result_payload.get("reviewed_head_sha")),
        reviewed_tree_sha=_coerce_optional_str(result_payload.get("reviewed_tree_sha")),
        reviewed_base_sha=_coerce_optional_str(result_payload.get("reviewed_base_sha")),
        working_directory=_coerce_optional_str(result_payload.get("working_directory")),
        failure=_coerce_optional_str(result_payload.get("failure")),
        source_task_id=_coerce_optional_str(metadata.get("source_task_id")),
        source_task_type=_coerce_optional_str(metadata.get("source_task_type")),
        output_artifact_id=_coerce_optional_int(metadata.get("output_artifact_id")),
        output_artifact_task_id=_coerce_optional_str(metadata.get("output_artifact_task_id")),
        output_artifact_path=output_artifact_path,
        failure_origin=failure_origin,
        phase_summary=phase_summary,
        phase_summary_invalid_reason=phase_summary_invalid_reason,
        duration_seconds=_coerce_optional_float(result_payload.get("duration_seconds")),
    )


def _artifact_verify_epoch(metadata: dict[str, Any]) -> VerifyEpoch | None:
    epoch_payload = metadata.get("verify_epoch")
    if not isinstance(epoch_payload, dict):
        return None
    return make_verify_epoch(
        reviewed_branch=_coerce_optional_str(epoch_payload.get("reviewed_branch")),
        reviewed_head_sha=_coerce_optional_str(epoch_payload.get("reviewed_head_sha")),
        reviewed_tree_sha=_coerce_optional_str(epoch_payload.get("reviewed_tree_sha")),
        verify_command=_coerce_optional_str(epoch_payload.get("verify_command")),
        verify_timeout_seconds=_coerce_optional_int(epoch_payload.get("verify_timeout_seconds")),
        verify_timeout_grace_seconds=_coerce_optional_float(epoch_payload.get("verify_timeout_grace_seconds")),
    )


def latest_verify_result_for_epoch(
    store: SqliteTaskStore,
    owner_task: Task,
    *,
    current_epoch: VerifyEpoch | None,
    project_dir: Path | None = None,
) -> VerifyGateLookup:
    """Return the latest verify result for ``current_epoch`` with legacy fallback."""
    if owner_task.id is None:
        return VerifyGateLookup(result=None, source=None, is_current=False, has_owner_artifact=False)

    owner_artifacts = store.list_artifacts(owner_task.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    if owner_artifacts:
        latest_result: VerifyGateResult | None = None
        for artifact in owner_artifacts:
            metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
            if metadata is None or metadata.get("schema_version") != VERIFY_GATE_ARTIFACT_SCHEMA_VERSION:
                continue
            result = _artifact_verify_result(metadata, project_dir=project_dir)
            epoch = _artifact_verify_epoch(metadata)
            if result is None or epoch is None:
                continue
            if latest_result is None:
                latest_result = result
            if current_epoch is not None and verify_epoch_matches(expected=current_epoch, candidate=epoch):
                return VerifyGateLookup(
                    result=result,
                    source="owner_artifact",
                    is_current=True,
                    has_owner_artifact=True,
                    artifact_id=artifact.id,
                    artifact_metadata=metadata,
                )
        return VerifyGateLookup(
            result=latest_result,
            source="owner_artifact" if latest_result is not None else None,
            is_current=False,
            has_owner_artifact=True,
        )

    latest_legacy: VerifyGateResult | None = None
    for review in store.get_reviews_for_task(owner_task.id):
        legacy = _task_verify_result(review)
        if legacy is None:
            continue
        if latest_legacy is None:
            latest_legacy = legacy
        legacy_epoch = _legacy_result_epoch(legacy)
        if current_epoch is not None and verify_epoch_matches(expected=current_epoch, candidate=legacy_epoch):
            return VerifyGateLookup(
                result=legacy,
                source="legacy_review",
                is_current=True,
                has_owner_artifact=False,
            )
    return VerifyGateLookup(
        result=latest_legacy,
        source="legacy_review" if latest_legacy is not None else None,
        is_current=False,
        has_owner_artifact=False,
    )


def latest_verify_evidence_for_owner(
    store: SqliteTaskStore,
    owner_task: Task,
    *,
    project_dir: Path | None = None,
) -> LatestVerifyEvidence | None:
    """Return the latest persisted verify evidence and its original epoch identity."""
    if owner_task.id is None:
        return None

    owner_artifacts = store.list_artifacts(owner_task.id, kind=VERIFY_GATE_ARTIFACT_KIND)
    if owner_artifacts:
        for artifact in owner_artifacts:
            metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
            if metadata is None or metadata.get("schema_version") != VERIFY_GATE_ARTIFACT_SCHEMA_VERSION:
                continue
            result = _artifact_verify_result(metadata, project_dir=project_dir)
            epoch = _artifact_verify_epoch(metadata)
            if result is None or epoch is None:
                continue
            return LatestVerifyEvidence(
                result=result,
                epoch=epoch,
                source="owner_artifact",
                has_owner_artifact=True,
            )
        return None

    for review in store.get_reviews_for_task(owner_task.id):
        legacy = _task_verify_result(review)
        if legacy is None:
            continue
        return LatestVerifyEvidence(
            result=legacy,
            epoch=_legacy_result_epoch(legacy),
            source="legacy_review",
            has_owner_artifact=False,
        )
    return None


def review_task_verify_epoch(task: Task, config: object | None) -> VerifyEpoch | None:
    """Build a canonical verify epoch from persisted review-era verify fields."""
    if task.task_type != "review":
        return None
    command = normalized_verify_command(task.review_verify_command)
    if command is None:
        return None
    return make_verify_epoch(
        reviewed_branch=task.review_verify_branch,
        reviewed_head_sha=task.review_verify_head_sha,
        reviewed_tree_sha=None,
        verify_command=command,
        verify_timeout_seconds=None,
        verify_timeout_grace_seconds=None,
    )


def owner_task_verify_epoch(task: Task, config: object | None, git: object | None) -> VerifyEpoch | None:
    """Build the current canonical verify epoch for an implementation owner."""
    branch = task.branch
    command = normalized_verify_command(getattr(config, "verify_command", None))
    if not branch or command is None or git is None:
        return None
    rev_parse = getattr(git, "rev_parse_if_exists", None)
    if not callable(rev_parse):
        return None
    try:
        head_sha = rev_parse(branch)
    except (AssertionError, GitError, OSError, RuntimeError, ValueError):
        return None
    if not isinstance(head_sha, str) or not head_sha:
        return None
    tree_sha: str | None = None
    resolve_refs = getattr(git, "resolve_refs", None)
    if callable(resolve_refs):
        try:
            resolved = resolve_refs([branch], peel="tree")
        except (AssertionError, GitError, OSError, RuntimeError, TypeError, ValueError):
            resolved = {}
        resolved_tree = resolved.get(branch) if isinstance(resolved, dict) else None
        if isinstance(resolved_tree, str) and resolved_tree:
            tree_sha = resolved_tree
    timeout_seconds = getattr(config, "autonomous_verify_timeout_seconds", None)
    timeout_grace_seconds = getattr(config, "review_verify_timeout_grace_seconds", None)
    return make_verify_epoch(
        reviewed_branch=branch,
        reviewed_head_sha=head_sha,
        reviewed_tree_sha=tree_sha,
        verify_command=command,
        verify_timeout_seconds=timeout_seconds if isinstance(timeout_seconds, int) else None,
        verify_timeout_grace_seconds=(
            float(timeout_grace_seconds)
            if isinstance(timeout_grace_seconds, (int, float)) and not isinstance(timeout_grace_seconds, bool)
            else None
        ),
    )


def resolve_verify_gate_decision(
    store: SqliteTaskStore,
    owner_task: Task,
    *,
    config: object | None,
    git: object | None,
) -> VerifyGateDecision:
    """Return the canonical lifecycle verify readiness for one implementation owner."""
    current_epoch = owner_task_verify_epoch(owner_task, config, git)
    project_dir = getattr(config, "project_dir", None)
    lookup = latest_verify_result_for_epoch(
        store,
        owner_task,
        current_epoch=current_epoch,
        project_dir=project_dir if isinstance(project_dir, Path) else None,
    )

    if lookup.result is None:
        state: Literal["passed", "missing", "stale", "failed", "unavailable"] = "missing"
    elif not lookup.is_current:
        state = "stale"
    elif lookup.result.status == "passed":
        state = "passed"
    elif lookup.result.status == "unavailable":
        state = "unavailable"
    else:
        state = "failed"

    return VerifyGateDecision(
        owner_task_id=owner_task.id,
        current_epoch=current_epoch,
        lookup=lookup,
        state=state,
    )


def select_current_merge_unit_verify_evidence(
    store: SqliteTaskStore,
    owner_task: Task,
    *,
    current_epoch: VerifyEpoch | None,
    member_tasks: Iterable[Task] | None = None,
) -> MergeUnitVerifyEvidenceSelection | None:
    """Return the newest current same-epoch verify evidence across a merge unit."""
    if owner_task.id is None or current_epoch is None:
        return None
    members: Iterable[Task]
    if member_tasks is None:
        unit = store.resolve_merge_unit_for_task(owner_task.id)
        if unit is None:
            return None
        members = store.list_tasks_for_merge_unit(unit.id)
    else:
        members = member_tasks
    if members is None:
        return None

    candidates: list[tuple[datetime, Task, VerifyGateLookup]] = []
    owner_lookup = latest_verify_result_for_epoch(store, owner_task, current_epoch=current_epoch)
    if owner_lookup.is_current and owner_lookup.result is not None:
        candidates.append((owner_lookup.result.captured_at, owner_task, owner_lookup))
    for member in members:
        if member.id is None or member.id == owner_task.id:
            continue
        lookup = latest_verify_result_for_epoch(store, member, current_epoch=current_epoch)
        if lookup.is_current and lookup.result is not None:
            candidates.append((lookup.result.captured_at, member, lookup))
    if not candidates:
        return None

    _captured_at, source_task, lookup = max(candidates, key=lambda candidate: candidate[0])
    return MergeUnitVerifyEvidenceSelection(
        owner_task=owner_task,
        source_task=source_task,
        lookup=lookup,
    )


def task_has_current_passing_verify_evidence(
    store: SqliteTaskStore,
    owner_task: Task,
    *,
    config: object | None,
    git: object | None,
) -> bool:
    """Return whether canonical verify evidence is current and passing."""
    return (
        resolve_verify_gate_decision(
            store,
            owner_task,
            config=config,
            git=git,
        ).state
        == "passed"
    )


def resolve_verify_owner_task(store: SqliteTaskStore, task: Task) -> Task:
    """Resolve the canonical owner row for verify evidence attached to ``task``."""
    if task.task_type == "review":
        for related_id in (task.depends_on, task.based_on):
            if isinstance(related_id, str):
                owner = store.get(related_id)
                if owner is not None:
                    return owner
    return task


def resolve_verify_read_model(
    store: SqliteTaskStore,
    task: Task,
    *,
    owner_task: Task | None = None,
    current_epoch: VerifyEpoch | None,
) -> VerifyReadModel | None:
    """Resolve the shared operator-facing verify read model for one task surface."""
    owner = owner_task or resolve_verify_owner_task(store, task)

    lookup = latest_verify_result_for_epoch(store, owner, current_epoch=current_epoch)
    if lookup.result is None or lookup.source is None:
        return None

    legacy_markdown: str | None = None
    if lookup.source == "legacy_review" and lookup.result.source_task_id:
        source_task = store.get(lookup.result.source_task_id)
        if source_task is not None:
            legacy_markdown = source_task.review_verify_markdown

    return VerifyReadModel(
        result=lookup.result,
        source=lookup.source,
        is_current=lookup.is_current,
        has_owner_artifact=lookup.has_owner_artifact,
        owner_task_id=owner.id,
        source_task_id=lookup.result.source_task_id,
        source_task_type=lookup.result.source_task_type,
        legacy_markdown=legacy_markdown,
    )


def verify_output_artifact_path(read_model: VerifyReadModel) -> str | None:
    """Return the canonical captured output artifact path for one verify read model."""
    return read_model.result.output_artifact_path


def read_verify_output_excerpt(
    project_dir: Path,
    read_model: VerifyReadModel,
    *,
    max_lines: int = 20,
    max_chars: int = 4000,
) -> str | None:
    """Read a bounded text excerpt from the canonical verify output artifact, if present."""
    from gza.artifact_paths import InvalidArtifactPathError, resolve_artifact_path

    artifact_path = verify_output_artifact_path(read_model)
    if not artifact_path:
        return None
    try:
        resolved_path = resolve_artifact_path(project_dir, artifact_path)
    except InvalidArtifactPathError:
        return None
    if not resolved_path.exists():
        return None
    content = resolved_path.read_text(encoding="utf-8", errors="replace").strip()
    if not content:
        return None
    lines = content.splitlines()
    excerpt = "\n".join(lines[:max_lines])
    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars].rstrip()
    return excerpt


def persist_verify_gate_artifact(
    store: SqliteTaskStore,
    config: Config,
    *,
    owner_task: Task,
    source_task: Task,
    result: Any,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
    output_artifact_id: int | None = None,
    output_artifact_task_id: str | None = None,
    output_artifact_path: str | None = None,
    producer: str,
    provenance: dict[str, Any] | None = None,
    aggregate_details: dict[str, Any] | None = None,
) -> None:
    """Persist canonical owner-attached verify-gate evidence."""
    if owner_task.id is None:
        return
    payload = build_verify_gate_artifact_payload(
        result=result,
        source_task=source_task,
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
        output_artifact_id=output_artifact_id,
        output_artifact_task_id=output_artifact_task_id,
        output_artifact_path=output_artifact_path,
        provenance=provenance,
        aggregate_details=aggregate_details,
    )
    store_command_output_artifact(
        store,
        owner_task,
        config,
        kind=VERIFY_GATE_ARTIFACT_KIND,
        producer=producer,
        label=VERIFY_GATE_ARTIFACT_LABEL,
        output=json.dumps(payload, sort_keys=True, indent=2) + "\n",
        command=getattr(result, "command", None),
        status=getattr(result, "status", None),
        exit_status=getattr(result, "exit_status", None),
        head_sha=getattr(result, "reviewed_head_sha", None),
        metadata=payload,
        created_at=getattr(result, "captured_at"),
        content_type="application/json",
    )


def persist_recredited_verify_gate_artifact(
    store: SqliteTaskStore,
    config: Config,
    *,
    owner_task: Task,
    evidence_holder_task: Task,
    result: VerifyGateResult,
    source_metadata: dict[str, Any] | None,
    producer: str,
) -> None:
    """Copy selected verify evidence to a new owner without rewriting run provenance."""
    if owner_task.id is None:
        return
    payload = deepcopy(source_metadata) if source_metadata is not None else None
    fallback_reason: str | None = None
    if not isinstance(payload, dict) or payload.get("schema_version") != VERIFY_GATE_ARTIFACT_SCHEMA_VERSION:
        source_task, fallback_reason = _resolve_recredited_source_task(
            store,
            result=result,
            payload_source_task_id=None,
            evidence_holder_task=evidence_holder_task,
        )
        payload = build_verify_gate_artifact_payload(
            result=result,
            source_task=source_task,
            verify_timeout_seconds=None,
            verify_timeout_grace_seconds=None,
            output_artifact_id=result.output_artifact_id,
            output_artifact_task_id=result.output_artifact_task_id,
            output_artifact_path=result.output_artifact_path,
        )
    else:
        source_task, fallback_reason = _resolve_recredited_source_task(
            store,
            result=result,
            payload_source_task_id=_coerce_optional_str(payload.get("source_task_id")),
            evidence_holder_task=evidence_holder_task,
        )
        payload["source_task_id"] = source_task.id
        payload["source_task_type"] = source_task.task_type

    reconciliation = {
        "producer": producer,
        "credited_owner_task_id": owner_task.id,
        "evidence_holder_task_id": evidence_holder_task.id,
        "evidence_holder_task_type": evidence_holder_task.task_type,
    }
    if fallback_reason is not None:
        reconciliation["source_provenance_fallback_reason"] = fallback_reason
    payload["reconciliation"] = reconciliation

    store_command_output_artifact(
        store,
        owner_task,
        config,
        kind=VERIFY_GATE_ARTIFACT_KIND,
        producer=producer,
        label=VERIFY_GATE_ARTIFACT_LABEL,
        output=json.dumps(payload, sort_keys=True, indent=2) + "\n",
        command=result.command,
        status=result.status,
        exit_status=result.exit_status,
        head_sha=result.reviewed_head_sha,
        metadata=payload,
        created_at=result.captured_at,
        content_type="application/json",
    )


def _resolve_recredited_source_task(
    store: SqliteTaskStore,
    *,
    result: VerifyGateResult,
    payload_source_task_id: str | None,
    evidence_holder_task: Task,
) -> tuple[Task, str | None]:
    """Resolve original source provenance for a recredited verify artifact."""
    for source_task_id in (payload_source_task_id, result.source_task_id):
        if source_task_id is None:
            continue
        source_task = store.get(source_task_id)
        if source_task is not None:
            return source_task, None

    if payload_source_task_id is not None or result.source_task_id is not None:
        return evidence_holder_task, "unresolvable_source_provenance"
    return evidence_holder_task, "missing_source_provenance"


def build_verify_gate_artifact_payload(
    *,
    result: Any,
    source_task: Task,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
    output_artifact_id: int | None = None,
    output_artifact_task_id: str | None = None,
    output_artifact_path: str | None = None,
    provenance: dict[str, Any] | None = None,
    aggregate_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build canonical owner-attached verify-gate artifact metadata."""

    epoch = make_verify_epoch(
        reviewed_branch=getattr(result, "reviewed_branch", None),
        reviewed_head_sha=getattr(result, "reviewed_head_sha", None),
        reviewed_tree_sha=getattr(result, "reviewed_tree_sha", None),
        verify_command=getattr(result, "command", None),
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
    )
    result_payload = {
        "command": getattr(result, "command", None),
        "status": getattr(result, "status", None),
        "exit_status": getattr(result, "exit_status", None),
        "captured_at": getattr(result, "captured_at").isoformat(),
        "reviewed_branch": getattr(result, "reviewed_branch", None),
        "reviewed_head_sha": getattr(result, "reviewed_head_sha", None),
        "reviewed_tree_sha": getattr(result, "reviewed_tree_sha", None),
        "reviewed_base_sha": getattr(result, "reviewed_base_sha", None),
        "working_directory": getattr(result, "working_directory", None),
        "failure": getattr(result, "failure", None),
    }
    failure_origin = getattr(result, "failure_origin", None)
    if failure_origin is not None:
        result_payload["failure_origin"] = failure_origin
    duration_seconds = _coerce_optional_float(getattr(result, "duration_seconds", None))
    if duration_seconds is not None:
        result_payload["duration_seconds"] = duration_seconds
    result_output = getattr(result, "output", None)
    phase_summary = None
    phase_summary_invalid_reason = None
    if result_output is not None:
        phase_summary, phase_summary_invalid_reason = summarize_verify_phases_with_validation(
            command=getattr(result, "command", None),
            output=result_output,
        )
        if phase_summary_invalid_reason is None:
            phase_summary, phase_summary_invalid_reason = validate_verify_phase_summary(
                phase_summary,
                require_known_full_verify_partition=normalized_verify_command(getattr(result, "command", None))
                == "./bin/tests",
            )
    payload = {
        "schema_version": VERIFY_GATE_ARTIFACT_SCHEMA_VERSION,
        "verify_epoch": {
            "reviewed_branch": epoch.reviewed_branch,
            "reviewed_head_sha": epoch.reviewed_head_sha,
            "reviewed_tree_sha": epoch.reviewed_tree_sha,
            "verify_command": epoch.verify_command,
            "verify_timeout_seconds": epoch.verify_timeout_seconds,
            "verify_timeout_grace_seconds": epoch.verify_timeout_grace_seconds,
        },
        "result": result_payload,
        "source_task_id": source_task.id,
        "source_task_type": source_task.task_type,
        "output_artifact_id": output_artifact_id,
        "output_artifact_task_id": output_artifact_task_id,
        "output_artifact_path": output_artifact_path,
    }
    if phase_summary is not None:
        payload["phase_summary"] = phase_summary
    if phase_summary_invalid_reason is not None:
        payload["phase_summary_invalid_reason"] = phase_summary_invalid_reason
    tree_fingerprint = _verify_gate_tree_fingerprint(
        provenance=provenance,
        aggregate_details=aggregate_details,
    )
    if tree_fingerprint is not None:
        payload["tree_fingerprint"] = tree_fingerprint
    if provenance is not None:
        payload["provenance"] = provenance
    if aggregate_details is not None:
        payload["aggregate_details"] = aggregate_details
    return payload


def persist_verify_gate_artifact_with_verify_fix_outcome(
    store: SqliteTaskStore,
    config: Config,
    *,
    owner_task: Task,
    source_task: Task,
    result: Any,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
    verify_fix_task: Task,
    verify_fix_outcome_json: str,
    no_source_changes: bool,
    completion_head_sha: str | None,
    output_artifact_id: int | None = None,
    output_artifact_task_id: str | None = None,
    output_artifact_path: str | None = None,
    producer: str,
    provenance: dict[str, Any] | None = None,
    aggregate_details: dict[str, Any] | None = None,
) -> None:
    """Persist authoritative rerun evidence and consumed outcome in one DB transaction."""
    if owner_task.id is None or verify_fix_task.id is None:
        return
    payload = build_verify_gate_artifact_payload(
        result=result,
        source_task=source_task,
        verify_timeout_seconds=verify_timeout_seconds,
        verify_timeout_grace_seconds=verify_timeout_grace_seconds,
        output_artifact_id=output_artifact_id,
        output_artifact_task_id=output_artifact_task_id,
        output_artifact_path=output_artifact_path,
        provenance=provenance,
        aggregate_details=aggregate_details,
    )
    output = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    prepared = prepare_command_output_artifact(
        Path(config.project_dir).resolve(),
        owner_task.id,
        label=VERIFY_GATE_ARTIFACT_LABEL,
        output=output,
        created_at=getattr(result, "captured_at"),
    )
    with store._connect() as conn:  # noqa: SLF001 - shared persistence invariant needs one transaction.
        conn.execute("BEGIN")
        try:
            store._add_artifact_conn(  # noqa: SLF001
                conn,
                owner_task.id,
                kind=VERIFY_GATE_ARTIFACT_KIND,
                label=VERIFY_GATE_ARTIFACT_LABEL,
                path=prepared.path,
                content_type="application/json",
                byte_size=prepared.bytes,
                sha256=prepared.digest,
                created_at=getattr(result, "captured_at"),
                producer=producer,
                command=getattr(result, "command", None),
                status=getattr(result, "status", None),
                exit_status=getattr(result, "exit_status", None),
                head_sha=getattr(result, "reviewed_head_sha", None),
                metadata=payload,
            )
            conn.execute(
                """
                UPDATE tasks
                SET changed_diff = ?,
                    review_verify_head_sha = ?,
                    verify_fix_completion_outcome_json = ?
                WHERE project_id = ? AND id = ?
                """,
                (
                    0 if no_source_changes else 1,
                    completion_head_sha,
                    verify_fix_outcome_json,
                    store._project_id,  # noqa: SLF001
                    verify_fix_task.id,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    verify_fix_task.changed_diff = not no_source_changes
    verify_fix_task.review_verify_head_sha = completion_head_sha
    verify_fix_task.verify_fix_completion_outcome_json = verify_fix_outcome_json


def refresh_preserved_rebase_review_verify_heads(
    store: SqliteTaskStore,
    impl_task: Task | None,
    *,
    branch: str | None,
    old_head_sha: str | None,
    new_head_sha: str | None,
) -> int:
    """Retarget preserved verify evidence to a rewritten same-diff branch head.

    When a completed same-branch rebase proves the tracked diff is unchanged, the
    latest completed review remains valid. For verify-only review blockers, the
    latest review's runner-owned verify failure provenance and any no-op improve
    verify provenance for that review must follow the rewritten branch head so the
    persisted mergeable recognition continues to match the current tip.
    """
    if impl_task is None or impl_task.id is None or impl_task.task_type != "implement":
        return 0
    if not branch or not old_head_sha or not new_head_sha or old_head_sha == new_head_sha:
        return 0

    reviews = [review for review in store.get_reviews_for_task(impl_task.id) if review.status == "completed"]
    if not reviews:
        return 0
    latest_review = max(
        reviews,
        key=lambda review: (review.completed_at or review.created_at, review.created_at),
    )
    if latest_review.id is None:
        return 0

    refreshed = 0
    if latest_review.review_verify_branch == branch and latest_review.review_verify_head_sha == old_head_sha:
        latest_review.review_verify_head_sha = new_head_sha
        store.update(latest_review)
        refreshed += 1

    for improve in store.get_improve_tasks_for(impl_task.id, latest_review.id):
        if improve.status != "completed" or improve.changed_diff is not False:
            continue
        if improve.review_verify_branch != branch or improve.review_verify_head_sha != old_head_sha:
            continue
        improve.review_verify_head_sha = new_head_sha
        store.update(improve)
        refreshed += 1

    return refreshed
