"""Shared local-main integration verify helpers for advance/watch."""

from __future__ import annotations

import json
import platform
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePath
from typing import Any, Literal, cast

from .artifact_paths import InvalidArtifactPathError, resolve_artifact_path
from .config import Config
from .db import SqliteTaskStore, Task
from .git import Git
from .off_topic_verify import extract_pytest_failing_nodeids
from .runner import (
    _capture_review_verify_result,
    _compute_tree_fingerprint,
    _extract_review_verify_phase_results,
    _format_review_verify_result,
    _make_review_verify_result,
    _resolve_review_verify_timeout_settings,
    _run_review_verify_command,
)

MAIN_INTEGRATION_VERIFY_PROMPT = "System alert: local main integration verify"
MAIN_INTEGRATION_VERIFY_REASON = "main-integration-verify-red"
MAIN_INTEGRATION_VERIFY_TAG = "system-main-verify"
MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS = "tree fingerprint unavailable"
MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS = "launch failed"
MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE = "watch-main-integration-verify-remediation"
VERIFY_COMMAND_OUTPUT_ARTIFACT_KIND = "verify_command_output"
MAIN_VERIFY_REMEDIATION_ARTIFACT_MAX_BYTES = 32 * 1024
MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_LINES = 24
MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_CHARS = 2000
_VERIFY_PHASE_LAUNCH_FAILURE_RE = re.compile(
    r"verify_phase: failed to launch command (?P<command>\[.+?\]): (?P<detail>.+)"
)
_COMMAND_NOT_FOUND_RE = re.compile(
    r"(?m)^(?:(?:ba|z|da|k)?sh:\s*(?:(?:line )?\d+:\s*)?)?(?P<tool>[^:\s`]+): "
    r"(?P<detail>command not found|No such file or directory|not found)$"
)
_COMMAND_NOT_EXECUTABLE_RE = re.compile(
    r"(?m)^(?:(?:ba|z|da|k)?sh:\s*(?:(?:line )?\d+:\s*)?)?(?P<tool>[^:\s`]+): "
    r"(?P<detail>Permission denied|cannot execute|Exec format error|not executable)$"
)
_FAILED_TO_SPAWN_RE = re.compile(r"Failed to spawn:\s*`(?P<tool>[^`]+)`(?::\s*(?P<detail>.+))?")
_OSERROR_TOOL_RE = re.compile(
    r"(?P<detail>No such file or directory|Permission denied|Exec format error):\s*['\"](?P<tool>[^'\"]+)['\"]"
)


@dataclass(frozen=True)
class MainIntegrationVerifyState:
    """Persisted verification state for the canonical local target branch."""

    task: Task
    gate_enabled: bool
    verify_command: str | None
    verify_timeout_seconds: int | None
    verify_timeout_grace_seconds: float | None
    environment_identity: MainIntegrationVerifyEnvironmentIdentity | None
    tree_fingerprint: str | None
    head_sha: str | None
    verify_status: str | None
    verify_exit_status: str | None
    failure_signature: str | None
    failure: str | None
    failing_phase: str | None
    alert_message: str | None
    pending_retirement_signatures: tuple[str, ...]
    red_since: datetime | None
    captured_at: datetime | None


@dataclass(frozen=True)
class MainIntegrationVerifyCheck:
    """Outcome of checking whether local-main integration verify must run."""

    state: MainIntegrationVerifyState
    performed_verify: bool
    current_tree_fingerprint: str | None
    is_current: bool
    merges_halted: bool
    needs_attention: bool = False
    remediation: MainIntegrationVerifyRemediation | None = None
    resolved_red_signature: str | None = None
    verify_runs: int = 0


@dataclass(frozen=True)
class MainIntegrationVerifyRemediation:
    """Caller-facing classification for verify failures that need follow-up work."""

    kind: Literal["deflake", "fix"]
    signature: str
    tree_fingerprint: str | None
    failing_phase: str | None
    failure: str | None
    artifact_path: str | None
    failing_test_ids: tuple[str, ...]
    verify_excerpt: str | None


@dataclass(frozen=True)
class MainIntegrationVerifyGateIdentity:
    """Configured verify-gate identity that invalidates stale checkpoints."""

    gate_enabled: bool
    verify_command: str | None
    verify_timeout_seconds: int | None
    verify_timeout_grace_seconds: float | None
    environment_identity: MainIntegrationVerifyEnvironmentIdentity | None


@dataclass(frozen=True)
class MainIntegrationVerifyEnvironmentIdentity:
    """Compact runtime identity for a persisted main integration verify checkpoint."""

    runner_class: Literal["host", "container"]
    platform_system: str
    platform_machine: str
    python_implementation: str | None
    python_version: str
    python_executable_family: str | None = None

    def to_payload(self) -> dict[str, str]:
        payload = {
            "runner_class": self.runner_class,
            "platform_system": self.platform_system,
            "platform_machine": self.platform_machine,
            "python_version": self.python_version,
        }
        if self.python_implementation is not None:
            payload["python_implementation"] = self.python_implementation
        if self.python_executable_family is not None:
            payload["python_executable_family"] = self.python_executable_family
        return payload

    @classmethod
    def from_payload(cls, payload: object) -> MainIntegrationVerifyEnvironmentIdentity | None:
        if not isinstance(payload, dict):
            return None
        runner_class = payload.get("runner_class")
        platform_system = payload.get("platform_system")
        platform_machine = payload.get("platform_machine")
        python_implementation = payload.get("python_implementation")
        python_executable_family = payload.get("python_executable_family")
        python_version = payload.get("python_version")
        if runner_class not in {"host", "container"}:
            return None
        if not all(isinstance(value, str) and value for value in (
            platform_system,
            platform_machine,
            python_version,
        )):
            return None
        if python_implementation is not None and not isinstance(python_implementation, str):
            return None
        if python_executable_family is not None and not isinstance(python_executable_family, str):
            return None
        if python_implementation == "":
            python_implementation = None
        if python_executable_family == "":
            python_executable_family = None
        if python_executable_family is None:
            legacy_python_executable = payload.get("python_executable")
            if isinstance(legacy_python_executable, str) and legacy_python_executable:
                python_executable_family = _normalize_python_executable_family(
                    legacy_python_executable
                )
        typed_runner_class = cast(Literal["host", "container"], runner_class)
        typed_platform_system = cast(str, platform_system)
        typed_platform_machine = cast(str, platform_machine)
        typed_python_implementation = cast(str | None, python_implementation)
        typed_python_version = cast(str, python_version)
        return cls(
            runner_class=typed_runner_class,
            platform_system=typed_platform_system,
            platform_machine=typed_platform_machine,
            python_implementation=typed_python_implementation,
            python_version=typed_python_version,
            python_executable_family=python_executable_family,
        )


@dataclass(frozen=True)
class CandidateIntegrationVerifyEvidence:
    """Structured verification evidence for one exact candidate checkout."""

    gate_enabled: bool
    verify_command: str | None
    verify_timeout_seconds: int | None
    verify_timeout_grace_seconds: float | None
    environment_identity: MainIntegrationVerifyEnvironmentIdentity | None
    tree_fingerprint: str | None
    head_sha: str | None
    verify_status: str | None
    verify_exit_status: str | None
    failure: str | None
    failing_phase: str | None
    reviewed_branch: str | None
    working_directory: str | None
    captured_at: datetime | None


@dataclass(frozen=True)
class CandidateIntegrationVerifyCheck:
    """Outcome of verifying one exact candidate checkout without mutating main state."""

    evidence: CandidateIntegrationVerifyEvidence
    classification: Literal["pass", "red", "deterministic_red", "flake", "unavailable"]
    merges_halted: bool
    remediation: MainIntegrationVerifyRemediation | None = None
    verify_runs: int = 0


@dataclass(frozen=True)
class VerifyLaunchIssue:
    """Environment/config issue that blocked the verify tool from launching."""

    phase_name: str | None
    tool_name: str | None
    detail: str


IntegrationVerifyEvidence = MainIntegrationVerifyState | CandidateIntegrationVerifyEvidence


def _find_main_integration_verify_task(store: SqliteTaskStore) -> Task | None:
    for task in store.get_all():
        if task.task_type != "internal":
            continue
        if task.prompt != MAIN_INTEGRATION_VERIFY_PROMPT:
            continue
        return task
    return None


def ensure_main_integration_verify_task(store: SqliteTaskStore) -> Task:
    """Return the durable internal row that stores main integration verify state."""
    existing = _find_main_integration_verify_task(store)
    if existing is not None:
        return existing

    task = store.add(
        MAIN_INTEGRATION_VERIFY_PROMPT,
        task_type="internal",
        tags=(MAIN_INTEGRATION_VERIFY_TAG,),
        skip_learnings=True,
    )
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = False
    store.update(task)
    return task


def _parse_main_integration_verify_payload(task: Task) -> dict[str, Any]:
    raw = task.output_content
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def load_main_integration_verify_state(store: SqliteTaskStore) -> MainIntegrationVerifyState | None:
    """Return the persisted local-main verify state when it exists."""
    task = _find_main_integration_verify_task(store)
    if task is None:
        return None
    payload = _parse_main_integration_verify_payload(task)
    captured_at_raw = payload.get("captured_at")
    captured_at = None
    if isinstance(captured_at_raw, str):
        try:
            captured_at = datetime.fromisoformat(captured_at_raw)
        except ValueError:
            captured_at = None
    red_since_raw = payload.get("red_since")
    red_since = None
    if isinstance(red_since_raw, str):
        try:
            red_since = datetime.fromisoformat(red_since_raw)
        except ValueError:
            red_since = None
    failing_phase = payload.get("failing_phase") if isinstance(payload.get("failing_phase"), str) else None
    failure_signature = payload.get("failure_signature") if isinstance(payload.get("failure_signature"), str) else None
    if (
        failure_signature is None
        and _verify_result_halts_merges(
            status=task.review_verify_status,
            gate_enabled=_payload_gate_enabled(task, payload),
            exit_status=task.review_verify_exit_status,
        )
    ):
        failure_signature = _verify_failure_signature(
            failing_phase=failing_phase,
            verify_status=task.review_verify_status,
            verify_exit_status=task.review_verify_exit_status,
        )
    pending_retirement_signatures_raw = payload.get("pending_retirement_signatures")
    pending_retirement_signatures = (
        tuple(
            signature
            for signature in pending_retirement_signatures_raw
            if isinstance(signature, str) and signature
        )
        if isinstance(pending_retirement_signatures_raw, list)
        else ()
    )
    return MainIntegrationVerifyState(
        task=task,
        gate_enabled=_payload_gate_enabled(task, payload),
        verify_command=_payload_verify_command(task, payload),
        verify_timeout_seconds=_coerce_optional_int(payload.get("verify_timeout_seconds")),
        verify_timeout_grace_seconds=_coerce_optional_float(payload.get("verify_timeout_grace_seconds")),
        environment_identity=MainIntegrationVerifyEnvironmentIdentity.from_payload(
            payload.get("environment_identity")
        ),
        tree_fingerprint=payload.get("tree_fingerprint") if isinstance(payload.get("tree_fingerprint"), str) else None,
        head_sha=payload.get("head_sha") if isinstance(payload.get("head_sha"), str) else task.review_verify_head_sha,
        verify_status=task.review_verify_status,
        verify_exit_status=task.review_verify_exit_status,
        failure_signature=failure_signature,
        failure=task.review_verify_failure,
        failing_phase=failing_phase,
        alert_message=payload.get("alert_message") if isinstance(payload.get("alert_message"), str) else None,
        pending_retirement_signatures=pending_retirement_signatures,
        red_since=red_since,
        captured_at=captured_at or task.review_verify_captured_at,
    )


def _verify_failure_phase_name(output: str | None) -> str | None:
    for phase in _extract_review_verify_phase_results(output):
        if phase.get("status") == "failed":
            name = phase.get("name")
            if isinstance(name, str) and name:
                return name
    return None


def _verify_tree_fingerprint(output: str | None) -> str | None:
    fingerprint: str | None = None
    for phase in _extract_review_verify_phase_results(output):
        candidate = phase.get("tree_fingerprint")
        if isinstance(candidate, str) and candidate:
            fingerprint = candidate
    return fingerprint


def _verify_failure_signature(
    *,
    failing_phase: str | None,
    verify_status: str | None,
    verify_exit_status: str | None,
) -> str:
    if failing_phase:
        return f"phase:{failing_phase}"
    status = verify_status or "unknown"
    exit_status = verify_exit_status or "unknown"
    return f"status:{status}:exit:{exit_status}"


def _build_integration_verify_remediation(
    *,
    kind: Literal["deflake", "fix"],
    state: IntegrationVerifyEvidence,
) -> MainIntegrationVerifyRemediation:
    artifact_path: str | None = None
    artifact_output: str | None = None
    return MainIntegrationVerifyRemediation(
        kind=kind,
        signature=_verify_failure_signature(
            failing_phase=state.failing_phase,
            verify_status=state.verify_status,
            verify_exit_status=state.verify_exit_status,
        ),
        tree_fingerprint=state.tree_fingerprint,
        failing_phase=state.failing_phase,
        failure=state.failure,
        artifact_path=artifact_path,
        failing_test_ids=extract_pytest_failing_nodeids(artifact_output) if artifact_output else (),
        verify_excerpt=_build_main_verify_excerpt(artifact_output),
    )


def _build_main_integration_verify_remediation(
    *,
    kind: Literal["deflake", "fix"],
    config: Config,
    store: SqliteTaskStore,
    state: MainIntegrationVerifyState,
) -> MainIntegrationVerifyRemediation:
    remediation = _build_integration_verify_remediation(kind=kind, state=state)
    artifact_path, artifact_output = _load_main_verify_artifact_evidence(
        config=config,
        store=store,
        task=state.task,
    )
    return replace(
        remediation,
        artifact_path=artifact_path,
        failing_test_ids=extract_pytest_failing_nodeids(artifact_output) if artifact_output else (),
        verify_excerpt=_build_main_verify_excerpt(artifact_output),
    )


def _candidate_review_verify_artifact_paths(
    store: SqliteTaskStore,
    task: Task,
) -> tuple[str, ...]:
    candidates: list[str] = []
    preferred = task.review_verify_artifact_file
    if preferred:
        candidates.append(preferred)
    if task.id is None:
        return tuple(candidates)
    artifacts = store.list_artifacts(task.id, kind=VERIFY_COMMAND_OUTPUT_ARTIFACT_KIND)
    candidates.extend(
        artifact.path
        for artifact in artifacts
        if artifact.path and artifact.path != preferred
    )
    return tuple(candidates)


def _read_main_verify_artifact_tail(
    *,
    config: Config,
    stored_path: str,
) -> str | None:
    try:
        artifact_path = resolve_artifact_path(Path(config.project_dir), stored_path)
    except InvalidArtifactPathError:
        return None
    try:
        with artifact_path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            start = max(0, size - MAIN_VERIFY_REMEDIATION_ARTIFACT_MAX_BYTES)
            handle.seek(start)
            return handle.read(MAIN_VERIFY_REMEDIATION_ARTIFACT_MAX_BYTES).decode("utf-8", errors="replace")
    except OSError:
        return None


def _load_main_verify_artifact_evidence(
    *,
    config: Config,
    store: SqliteTaskStore,
    task: Task,
) -> tuple[str | None, str | None]:
    for stored_path in _candidate_review_verify_artifact_paths(store, task):
        try:
            resolve_artifact_path(Path(config.project_dir), stored_path)
        except InvalidArtifactPathError:
            continue
        artifact_output = _read_main_verify_artifact_tail(config=config, stored_path=stored_path)
        if artifact_output is None:
            continue
        if not artifact_output.strip():
            continue
        return stored_path, artifact_output
    return None, None


def _build_main_verify_excerpt(output: str | None) -> str | None:
    if not output:
        return None
    lines = output.splitlines()
    if not lines:
        return None
    summary_index = next(
        (index for index, line in enumerate(lines) if "short test summary info" in line.lower()),
        None,
    )
    if summary_index is None:
        excerpt_lines = lines[-MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_LINES :]
    else:
        start = max(0, summary_index - 8)
        excerpt_lines = lines[start : start + MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_LINES]
    excerpt = "\n".join(excerpt_lines).strip()
    if not excerpt:
        return None
    if len(excerpt) <= MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_CHARS:
        return excerpt
    trimmed = excerpt[-MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_CHARS :].lstrip()
    return f"[...]\n{trimmed}"


def _build_red_alert_message(
    *,
    head_sha: str | None,
    verify_status: str | None,
    failing_phase: str | None,
) -> str:
    short_sha = (head_sha or "unknown")[:12]
    if failing_phase:
        return f"main verify RED at `{short_sha}` - merges halted; phase `{failing_phase}` failing"
    if verify_status and verify_status != "failed":
        return f"main verify RED at `{short_sha}` - merges halted; verify status `{verify_status}`"
    return f"main verify RED at `{short_sha}` - merges halted"


def _normalize_verify_tool_name(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip().strip("'").strip('"')
    if not stripped:
        return None
    return PurePath(stripped).name or stripped


def _summarize_launch_issue_detail(detail: str) -> str:
    lowered = detail.lower()
    if (
        "command not found" in lowered
        or "no such file or directory" in lowered
        or lowered.endswith("not found")
    ):
        return "not on PATH"
    if (
        "permission denied" in lowered
        or "cannot execute" in lowered
        or "exec format error" in lowered
        or "not executable" in lowered
    ):
        return "not executable"
    return detail.strip()


def _extract_launch_issue_tool_from_command_repr(command_repr: str) -> str | None:
    match = re.search(r"'([^']+)'", command_repr)
    if match is None:
        return None
    return _normalize_verify_tool_name(match.group(1))


def _extract_launch_issue_from_text(text: str) -> VerifyLaunchIssue | None:
    for pattern in (_COMMAND_NOT_FOUND_RE, _COMMAND_NOT_EXECUTABLE_RE):
        match = pattern.search(text)
        if match is not None:
            return VerifyLaunchIssue(
                phase_name=None,
                tool_name=_normalize_verify_tool_name(match.group("tool")),
                detail=_summarize_launch_issue_detail(match.group("detail")),
            )

    spawn_match = _FAILED_TO_SPAWN_RE.search(text)
    if spawn_match is not None:
        detail = spawn_match.group("detail") or spawn_match.group(0)
        return VerifyLaunchIssue(
            phase_name=None,
            tool_name=_normalize_verify_tool_name(spawn_match.group("tool")),
            detail=_summarize_launch_issue_detail(detail),
        )

    oserror_match = _OSERROR_TOOL_RE.search(text)
    if oserror_match is not None:
        return VerifyLaunchIssue(
            phase_name=None,
            tool_name=_normalize_verify_tool_name(oserror_match.group("tool")),
            detail=_summarize_launch_issue_detail(oserror_match.group("detail")),
        )

    return None


def _detect_verify_launch_issue(
    *,
    verify_output: str | None,
    verify_exit_status: str | None,
    verify_failure: str | None,
    failing_phase: str | None,
) -> VerifyLaunchIssue | None:
    output = verify_output or ""
    for line in output.splitlines():
        match = _VERIFY_PHASE_LAUNCH_FAILURE_RE.search(line)
        if match is None:
            continue
        return VerifyLaunchIssue(
            phase_name=failing_phase,
            tool_name=_extract_launch_issue_tool_from_command_repr(match.group("command")),
            detail=_summarize_launch_issue_detail(match.group("detail")),
        )

    if verify_exit_status in {"126", "127"}:
        issue = _extract_launch_issue_from_text(output)
        if issue is not None:
            return replace(issue, phase_name=failing_phase)

    if verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS and verify_failure:
        failure_detail = verify_failure.removeprefix("failed to launch verify_command: ").strip()
        issue = _extract_launch_issue_from_text(failure_detail)
        if issue is not None:
            return replace(issue, phase_name=failing_phase)
        return VerifyLaunchIssue(
            phase_name=failing_phase,
            tool_name=None,
            detail=_summarize_launch_issue_detail(failure_detail),
        )
    return None


def _build_launch_issue_failure(issue: VerifyLaunchIssue) -> str:
    if issue.tool_name and issue.phase_name:
        return (
            f"verify_command environment error: could not launch `{issue.tool_name}` "
            f"for phase `{issue.phase_name}` ({issue.detail})"
        )
    if issue.tool_name:
        return f"verify_command environment error: could not launch `{issue.tool_name}` ({issue.detail})"
    return f"verify_command environment error: could not launch verify tooling ({issue.detail})"


def _build_launch_issue_alert_message(*, head_sha: str | None, issue: VerifyLaunchIssue) -> str:
    short_sha = (head_sha or "unknown")[:12]
    if issue.tool_name and issue.phase_name:
        return (
            f"main verify misconfigured at `{short_sha}` - could not launch `{issue.tool_name}` "
            f"for phase `{issue.phase_name}` ({issue.detail}); fix the environment, not the code"
        )
    if issue.tool_name:
        return (
            f"main verify misconfigured at `{short_sha}` - could not launch `{issue.tool_name}` "
            f"({issue.detail}); fix the environment, not the code"
        )
    return (
        f"main verify misconfigured at `{short_sha}` - could not launch verify tooling "
        f"({issue.detail}); fix the environment, not the code"
    )


def _build_freshness_unavailable_failure() -> str:
    return "could not prove exact local target tree freshness because the tree fingerprint is unavailable"


def _build_freshness_unavailable_alert_message(*, head_sha: str | None) -> str:
    short_sha = (head_sha or "unknown")[:12]
    return f"main verify freshness unproven at `{short_sha}` - merges halted; exact tree fingerprint unavailable"


def _verify_result_halts_merges(
    *,
    status: str | None,
    gate_enabled: bool,
    exit_status: str | None,
) -> bool:
    if not gate_enabled or status == "passed":
        return False
    if exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS:
        return False
    return True


def _verify_result_needs_attention(
    *,
    status: str | None,
    gate_enabled: bool,
    exit_status: str | None,
    alert_message: str | None,
) -> bool:
    if not gate_enabled or not alert_message:
        return False
    return _verify_result_halts_merges(
        status=status,
        gate_enabled=gate_enabled,
        exit_status=exit_status,
    ) or exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS


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


def _normalized_verify_command(config: Config) -> str:
    return config.verify_command.strip() if isinstance(config.verify_command, str) else ""


def _normalize_python_executable_family(executable: str) -> str | None:
    normalized = PurePath(executable.strip()).name.lower()
    return normalized or None


def _current_verify_environment_identity(
    *,
    runner_class: Literal["host", "container"],
) -> MainIntegrationVerifyEnvironmentIdentity:
    return MainIntegrationVerifyEnvironmentIdentity(
        runner_class=runner_class,
        platform_system=platform.system(),
        platform_machine=platform.machine(),
        python_implementation=platform.python_implementation(),
        python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
    )


def _current_gate_identity(
    config: Config,
    *,
    runner_class: Literal["host", "container"],
) -> MainIntegrationVerifyGateIdentity:
    verify_command = _normalized_verify_command(config)
    gate_enabled = bool(verify_command)
    if not gate_enabled:
        return MainIntegrationVerifyGateIdentity(
            gate_enabled=False,
            verify_command=None,
            verify_timeout_seconds=None,
            verify_timeout_grace_seconds=None,
            environment_identity=None,
        )

    timeout_seconds, timeout_grace_seconds = _resolve_review_verify_timeout_settings(config)
    return MainIntegrationVerifyGateIdentity(
        gate_enabled=True,
        verify_command=verify_command,
        verify_timeout_seconds=timeout_seconds,
        verify_timeout_grace_seconds=timeout_grace_seconds,
        environment_identity=_current_verify_environment_identity(runner_class=runner_class),
    )


def _payload_gate_enabled(task: Task, payload: dict[str, Any]) -> bool:
    persisted = payload.get("gate_enabled")
    if isinstance(persisted, bool):
        return persisted
    if task.review_verify_exit_status == "not configured":
        return False
    if task.review_verify_failure == "verify_command is not configured":
        return False
    return task.review_verify_status is not None


def _payload_verify_command(task: Task, payload: dict[str, Any]) -> str | None:
    persisted = _coerce_optional_str(payload.get("verify_command"))
    if persisted is not None:
        return persisted
    if task.review_verify_command == "(verify_command unavailable)":
        return None
    return _coerce_optional_str(task.review_verify_command)


def _gate_identity_matches(
    state: MainIntegrationVerifyState,
    current_gate: MainIntegrationVerifyGateIdentity,
) -> bool:
    return (
        state.gate_enabled == current_gate.gate_enabled
        and state.verify_command == current_gate.verify_command
        and state.verify_timeout_seconds == current_gate.verify_timeout_seconds
        and state.verify_timeout_grace_seconds == current_gate.verify_timeout_grace_seconds
        and (
            current_gate.environment_identity is None
            or _environment_identity_matches(
                state.environment_identity,
                current_gate.environment_identity,
            )
        )
    )


def _environment_identity_matches(
    persisted: MainIntegrationVerifyEnvironmentIdentity | None,
    current: MainIntegrationVerifyEnvironmentIdentity,
) -> bool:
    if persisted is None:
        return False
    if (
        persisted.runner_class != current.runner_class
        or persisted.platform_system != current.platform_system
        or persisted.platform_machine != current.platform_machine
        or persisted.python_version != current.python_version
    ):
        return False
    if (
        persisted.python_implementation is not None
        and current.python_implementation is not None
        and persisted.python_implementation != current.python_implementation
    ):
        return False
    if (
        persisted.python_executable_family is not None
        and current.python_executable_family is not None
        and persisted.python_executable_family != current.python_executable_family
    ):
        return False
    return True


def _persist_main_integration_verify_payload(
    store: SqliteTaskStore,
    task: Task,
    *,
    gate_enabled: bool,
    verify_command: str | None,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
    environment_identity: MainIntegrationVerifyEnvironmentIdentity | None,
    tree_fingerprint: str | None,
    head_sha: str | None,
    failure_signature: str | None,
    failing_phase: str | None,
    alert_message: str | None,
    pending_retirement_signatures: tuple[str, ...],
    red_since: datetime | None,
    captured_at: datetime,
) -> None:
    task.output_content = json.dumps(
        {
            "gate_enabled": gate_enabled,
            "verify_command": verify_command,
            "verify_timeout_seconds": verify_timeout_seconds,
            "verify_timeout_grace_seconds": verify_timeout_grace_seconds,
            "environment_identity": environment_identity.to_payload() if environment_identity is not None else None,
            "tree_fingerprint": tree_fingerprint,
            "head_sha": head_sha,
            "failure_signature": failure_signature,
            "failing_phase": failing_phase,
            "alert_message": alert_message,
            "pending_retirement_signatures": list(pending_retirement_signatures),
            "red_since": red_since.isoformat() if red_since is not None else None,
            "captured_at": captured_at.isoformat(),
        },
        sort_keys=True,
    )
    task.status = "completed"
    task.completed_at = captured_at
    task.has_commits = False
    store.update(task)


def _pending_retirement_signatures_from_state(state: Any) -> tuple[str, ...]:
    raw = getattr(state, "pending_retirement_signatures", ())
    if not isinstance(raw, tuple):
        return ()
    return tuple(signature for signature in raw if isinstance(signature, str) and signature)


def persist_main_integration_verify_alert_message(
    store: SqliteTaskStore,
    *,
    state: MainIntegrationVerifyState,
    alert_message: str,
) -> MainIntegrationVerifyState:
    """Persist a replacement durable alert message while preserving verify identity."""
    captured_at = state.captured_at or state.task.completed_at or datetime.now(UTC)
    failure_signature = getattr(state, "failure_signature", None)
    if not isinstance(failure_signature, str) or not failure_signature:
        failing_phase = getattr(state, "failing_phase", None)
        verify_status = getattr(state, "verify_status", None)
        verify_exit_status = getattr(state, "verify_exit_status", None)
        if _verify_result_halts_merges(
            status=verify_status if isinstance(verify_status, str) else None,
            gate_enabled=state.gate_enabled,
            exit_status=verify_exit_status if isinstance(verify_exit_status, str) else None,
        ) and isinstance(failing_phase, str) and failing_phase:
            failure_signature = _verify_failure_signature(
                failing_phase=failing_phase,
                verify_status=verify_status if isinstance(verify_status, str) else None,
                verify_exit_status=verify_exit_status if isinstance(verify_exit_status, str) else None,
            )
        elif _verify_result_halts_merges(
            status=verify_status if isinstance(verify_status, str) else None,
            gate_enabled=state.gate_enabled,
            exit_status=verify_exit_status if isinstance(verify_exit_status, str) else None,
        ) and isinstance(verify_status, str) and verify_status:
            failure_signature = _verify_failure_signature(
                failing_phase=None,
                verify_status=verify_status,
                verify_exit_status=verify_exit_status if isinstance(verify_exit_status, str) else None,
            )
        else:
            failure_signature = None
    _persist_main_integration_verify_payload(
        store,
        state.task,
        gate_enabled=state.gate_enabled,
        verify_command=state.verify_command,
        verify_timeout_seconds=state.verify_timeout_seconds,
        verify_timeout_grace_seconds=state.verify_timeout_grace_seconds,
        environment_identity=getattr(state, "environment_identity", None),
        tree_fingerprint=state.tree_fingerprint,
        head_sha=state.head_sha,
        failure_signature=failure_signature,
        failing_phase=state.failing_phase,
        alert_message=alert_message,
        pending_retirement_signatures=_pending_retirement_signatures_from_state(state),
        red_since=getattr(state, "red_since", None),
        captured_at=captured_at,
    )
    refreshed = load_main_integration_verify_state(store)
    assert refreshed is not None
    return refreshed


def persist_main_integration_verify_pending_retire_signatures(
    store: SqliteTaskStore,
    *,
    state: MainIntegrationVerifyState,
    pending_retirement_signatures: tuple[str, ...],
) -> MainIntegrationVerifyState:
    """Persist the set of green-but-live remediation signatures awaiting safe retirement."""
    captured_at = state.captured_at or state.task.completed_at or datetime.now(UTC)
    _persist_main_integration_verify_payload(
        store,
        state.task,
        gate_enabled=state.gate_enabled,
        verify_command=state.verify_command,
        verify_timeout_seconds=state.verify_timeout_seconds,
        verify_timeout_grace_seconds=state.verify_timeout_grace_seconds,
        environment_identity=getattr(state, "environment_identity", None),
        tree_fingerprint=state.tree_fingerprint,
        head_sha=state.head_sha,
        failure_signature=getattr(state, "failure_signature", None),
        failing_phase=state.failing_phase,
        alert_message=state.alert_message,
        pending_retirement_signatures=pending_retirement_signatures,
        red_since=getattr(state, "red_since", None),
        captured_at=captured_at,
    )
    refreshed = load_main_integration_verify_state(store)
    assert refreshed is not None
    return refreshed


def _coerce_result_to_freshness_unavailable(
    result,
) -> Any:
    return _make_review_verify_result(
        result.command,
        status="unavailable",
        exit_status=MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
        captured_at=result.captured_at,
        reviewed_branch=result.reviewed_branch,
        reviewed_head_sha=result.reviewed_head_sha,
        reviewed_base_sha=result.reviewed_base_sha,
        working_directory=result.working_directory,
        failure=_build_freshness_unavailable_failure(),
        output=result.output,
    )


def _checkpoint_is_current(
    state: MainIntegrationVerifyState,
    *,
    config: Config,
    current_gate: MainIntegrationVerifyGateIdentity,
    current_tree_fingerprint: str | None,
    current_head_sha: str | None,
) -> bool:
    if not _gate_identity_matches(state, current_gate):
        return False
    if state.verify_exit_status == MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS:
        return False
    if state.gate_enabled:
        if _verify_result_halts_merges(
            status=state.verify_status,
            gate_enabled=state.gate_enabled,
            exit_status=state.verify_exit_status,
        ):
            captured_at = state.captured_at
            if captured_at is None:
                return False
            if datetime.now(UTC) - captured_at >= timedelta(minutes=config.main_integration_verify_red_ttl_minutes):
                return False
        return bool(
            current_tree_fingerprint
            and state.tree_fingerprint
            and current_tree_fingerprint == state.tree_fingerprint
        )
    return bool(current_head_sha and state.head_sha and current_head_sha == state.head_sha)


def run_main_integration_verify(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    reason: str,
    runner_class: Literal["host", "container"] = "host",
) -> MainIntegrationVerifyState:
    """Run the configured verify gate against the current local target checkout."""
    task = ensure_main_integration_verify_task(store)
    prior_state = load_main_integration_verify_state(store)
    captured_at = datetime.now(UTC)
    head_sha = _coerce_optional_str(git.rev_parse_if_exists("HEAD"))
    gate = _current_gate_identity(config, runner_class=runner_class)
    verify_command = gate.verify_command or ""
    gate_enabled = gate.gate_enabled

    if not gate_enabled:
        result = _make_review_verify_result(
            "(verify_command unavailable)",
            status="unavailable",
            exit_status="not configured",
            captured_at=captured_at,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            working_directory=str(git.repo_dir),
            failure="verify_command is not configured",
        )
    else:
        assert gate.verify_timeout_seconds is not None
        assert gate.verify_timeout_grace_seconds is not None
        result = _run_review_verify_command(
            verify_command,
            cwd=git.repo_dir,
            reviewed_branch=git.current_branch(),
            reviewed_head_sha=head_sha,
            timeout_seconds=gate.verify_timeout_seconds,
            timeout_grace_seconds=gate.verify_timeout_grace_seconds,
        )

    failing_phase = _verify_failure_phase_name(result.output)
    launch_issue = _detect_verify_launch_issue(
        verify_output=result.output,
        verify_exit_status=result.exit_status,
        verify_failure=result.failure,
        failing_phase=failing_phase,
    )
    if launch_issue is not None:
        result = _make_review_verify_result(
            result.command,
            status="unavailable",
            exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
            captured_at=result.captured_at,
            reviewed_branch=result.reviewed_branch,
            reviewed_head_sha=result.reviewed_head_sha,
            reviewed_base_sha=result.reviewed_base_sha,
            working_directory=result.working_directory,
            failure=_build_launch_issue_failure(launch_issue),
            output=result.output,
        )
    tree_fingerprint = _verify_tree_fingerprint(result.output) or _compute_tree_fingerprint(git)
    capture_metadata: dict[str, str] = {"reason": reason}
    if gate_enabled and result.status == "passed" and tree_fingerprint is None:
        result = _coerce_result_to_freshness_unavailable(result)
        capture_metadata["freshness_proof"] = "unavailable"
    markdown = _format_review_verify_result(result)
    _capture_review_verify_result(
        config,
        store,
        task,
        result,
        markdown=markdown,
        producer="main_integration_verify",
        metadata=capture_metadata,
    )
    alert_message = (
        _build_launch_issue_alert_message(head_sha=head_sha, issue=launch_issue)
        if gate_enabled and launch_issue is not None
        else
        _build_freshness_unavailable_alert_message(head_sha=head_sha)
        if gate_enabled
        and result.status == "unavailable"
        and result.exit_status == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS
        else _build_red_alert_message(
            head_sha=head_sha,
            verify_status=result.status,
            failing_phase=failing_phase,
        )
        if _verify_result_halts_merges(
            status=result.status,
            gate_enabled=gate_enabled,
            exit_status=result.exit_status,
        )
        else None
    )
    if _verify_result_halts_merges(
        status=result.status,
        gate_enabled=gate_enabled,
        exit_status=result.exit_status,
    ):
        if prior_state is not None and _verify_result_halts_merges(
            status=prior_state.verify_status,
            gate_enabled=prior_state.gate_enabled,
            exit_status=prior_state.verify_exit_status,
        ):
            red_since = prior_state.red_since or result.captured_at
        else:
            red_since = result.captured_at
    else:
        red_since = None
    pending_retirement_signatures: tuple[str, ...] = ()
    if gate_enabled and result.status == "passed":
        pending_retirement_signatures = _pending_retirement_signatures_from_state(prior_state)
    _persist_main_integration_verify_payload(
        store,
        task,
        gate_enabled=gate_enabled,
        verify_command=gate.verify_command,
        verify_timeout_seconds=gate.verify_timeout_seconds,
        verify_timeout_grace_seconds=gate.verify_timeout_grace_seconds,
        environment_identity=gate.environment_identity,
        tree_fingerprint=tree_fingerprint,
        head_sha=head_sha,
        failure_signature=(
            _verify_failure_signature(
                failing_phase=failing_phase,
                verify_status=result.status,
                verify_exit_status=result.exit_status,
            )
            if _verify_result_halts_merges(
                status=result.status,
                gate_enabled=gate_enabled,
                exit_status=result.exit_status,
            )
            else None
        ),
        failing_phase=failing_phase,
        alert_message=alert_message,
        pending_retirement_signatures=pending_retirement_signatures,
        red_since=red_since,
        captured_at=result.captured_at,
    )
    state = load_main_integration_verify_state(store)
    assert state is not None
    return state


def _run_integration_verify_with_red_reruns(
    run_once: Callable[[str], IntegrationVerifyEvidence],
    *,
    reason: str,
    red_reruns: int,
    prior_red_state: IntegrationVerifyEvidence | None = None,
) -> tuple[
    IntegrationVerifyEvidence,
    MainIntegrationVerifyRemediation | None,
    IntegrationVerifyEvidence | None,
    int,
]:
    """Run verify, optionally rerunning red verdicts to classify flakes vs deterministic reds."""
    state = run_once(reason)
    verify_runs = 1
    if red_reruns <= 0:
        return state, None, None, verify_runs

    reference_state = (
        prior_red_state
        if prior_red_state is not None
        and _verify_result_halts_merges(
            status=prior_red_state.verify_status,
            gate_enabled=prior_red_state.gate_enabled,
            exit_status=prior_red_state.verify_exit_status,
        )
        else state
    )
    if not _verify_result_halts_merges(
        status=reference_state.verify_status,
        gate_enabled=reference_state.gate_enabled,
        exit_status=reference_state.verify_exit_status,
    ):
        return state, None, None, verify_runs

    if not _verify_result_halts_merges(
        status=state.verify_status,
        gate_enabled=state.gate_enabled,
        exit_status=state.verify_exit_status,
    ):
        return (
            state,
            _build_integration_verify_remediation(kind="deflake", state=reference_state),
            reference_state,
            verify_runs,
        )

    confirmed_red_state = state
    for attempt in range(1, red_reruns + 1):
        rerun_state = run_once(f"{reason}-rerun-{attempt}")
        verify_runs += 1
        if not _verify_result_halts_merges(
            status=rerun_state.verify_status,
            gate_enabled=rerun_state.gate_enabled,
            exit_status=rerun_state.verify_exit_status,
        ):
            return (
                rerun_state,
                _build_integration_verify_remediation(kind="deflake", state=confirmed_red_state),
                confirmed_red_state,
                verify_runs,
            )
        confirmed_red_state = rerun_state
        state = rerun_state

    return (
        state,
        _build_integration_verify_remediation(kind="fix", state=confirmed_red_state),
        confirmed_red_state,
        verify_runs,
    )


def _run_main_integration_verify_with_red_reruns(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    reason: str,
    red_reruns: int,
    runner_class: Literal["host", "container"] = "host",
    prior_red_state: MainIntegrationVerifyState | None = None,
) -> tuple[MainIntegrationVerifyState, MainIntegrationVerifyRemediation | None, int]:
    state, remediation, remediation_source_state, verify_runs = _run_integration_verify_with_red_reruns(
        lambda run_reason: run_main_integration_verify(
            config,
            store,
            git,
            reason=run_reason,
            runner_class=runner_class,
        ),
        reason=reason,
        red_reruns=red_reruns,
        prior_red_state=prior_red_state,
    )
    main_state = cast(MainIntegrationVerifyState, state)
    if remediation is not None:
        remediation_source = cast(
            MainIntegrationVerifyState,
            remediation_source_state if remediation_source_state is not None else main_state,
        )
        artifact_path, artifact_output = _load_main_verify_artifact_evidence(
            config=config,
            store=store,
            task=remediation_source.task,
        )
        remediation = replace(
            remediation,
            artifact_path=artifact_path,
            failing_test_ids=extract_pytest_failing_nodeids(artifact_output) if artifact_output else (),
            verify_excerpt=_build_main_verify_excerpt(artifact_output),
        )
    return main_state, remediation, verify_runs


def check_main_integration_verify(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    reason: str,
    force: bool = False,
    red_reruns: int = 0,
    runner_class: Literal["host", "container"] = "host",
) -> MainIntegrationVerifyCheck:
    """Reuse or refresh local-main verify state for the current tree and gate identity."""
    current_tree_fingerprint = _compute_tree_fingerprint(git)
    current_head_sha = _coerce_optional_str(git.rev_parse_if_exists("HEAD"))
    current_gate = _current_gate_identity(config, runner_class=runner_class)
    state = load_main_integration_verify_state(store)
    checkpoint_is_current = state is not None and _checkpoint_is_current(
        state,
        config=config,
        current_gate=current_gate,
        current_tree_fingerprint=current_tree_fingerprint,
        current_head_sha=current_head_sha,
    )
    if checkpoint_is_current and not force and not (
        state is not None
        and red_reruns > 0
        and _verify_result_halts_merges(
            status=state.verify_status,
            gate_enabled=state.gate_enabled,
            exit_status=state.verify_exit_status,
        )
    ):
        assert state is not None
        return MainIntegrationVerifyCheck(
            state=state,
            performed_verify=False,
            current_tree_fingerprint=current_tree_fingerprint,
            is_current=True,
            merges_halted=_verify_result_halts_merges(
                status=state.verify_status,
                gate_enabled=state.gate_enabled,
                exit_status=state.verify_exit_status,
            ),
            needs_attention=_verify_result_needs_attention(
                status=state.verify_status,
                gate_enabled=state.gate_enabled,
                exit_status=state.verify_exit_status,
                alert_message=state.alert_message,
            ),
        )

    prior_red_signature = None
    if state is not None and _verify_result_halts_merges(
        status=state.verify_status,
        gate_enabled=state.gate_enabled,
        exit_status=state.verify_exit_status,
    ):
        prior_red_signature = _verify_failure_signature(
            failing_phase=state.failing_phase,
            verify_status=state.verify_status,
            verify_exit_status=state.verify_exit_status,
        )

    refreshed, remediation, verify_runs = _run_main_integration_verify_with_red_reruns(
        config,
        store,
        git,
        reason=reason,
        red_reruns=red_reruns,
        runner_class=runner_class,
        prior_red_state=state if checkpoint_is_current else None,
    )
    resolved_red_signature = None
    if not _verify_result_halts_merges(
        status=refreshed.verify_status,
        gate_enabled=refreshed.gate_enabled,
        exit_status=refreshed.verify_exit_status,
    ):
        resolved_red_signature = prior_red_signature
        if resolved_red_signature is None and remediation is not None and remediation.kind == "deflake":
            resolved_red_signature = remediation.signature
    return MainIntegrationVerifyCheck(
        state=refreshed,
        performed_verify=True,
        current_tree_fingerprint=current_tree_fingerprint,
        is_current=True,
        merges_halted=_verify_result_halts_merges(
            status=refreshed.verify_status,
            gate_enabled=refreshed.gate_enabled,
            exit_status=refreshed.verify_exit_status,
        ),
        needs_attention=_verify_result_needs_attention(
            status=refreshed.verify_status,
            gate_enabled=refreshed.gate_enabled,
            exit_status=refreshed.verify_exit_status,
            alert_message=refreshed.alert_message,
        ),
        remediation=remediation,
        resolved_red_signature=resolved_red_signature,
        verify_runs=verify_runs,
    )


def run_candidate_integration_verify(
    config: Config,
    git: Git,
    *,
    reason: str,
    runner_class: Literal["host", "container"] = "host",
) -> CandidateIntegrationVerifyEvidence:
    """Run the configured verify gate against an exact candidate checkout."""
    del reason
    captured_at = datetime.now(UTC)
    head_sha = _coerce_optional_str(git.rev_parse_if_exists("HEAD"))
    gate = _current_gate_identity(config, runner_class=runner_class)
    verify_command = gate.verify_command or ""
    gate_enabled = gate.gate_enabled
    current_branch = git.current_branch()
    working_directory = str(git.repo_dir)

    if not gate_enabled:
        result = _make_review_verify_result(
            "(verify_command unavailable)",
            status="unavailable",
            exit_status="not configured",
            captured_at=captured_at,
            reviewed_branch=current_branch,
            reviewed_head_sha=head_sha,
            working_directory=working_directory,
            failure="verify_command is not configured",
        )
    else:
        assert gate.verify_timeout_seconds is not None
        assert gate.verify_timeout_grace_seconds is not None
        result = _run_review_verify_command(
            verify_command,
            cwd=git.repo_dir,
            reviewed_branch=current_branch,
            reviewed_head_sha=head_sha,
            timeout_seconds=gate.verify_timeout_seconds,
            timeout_grace_seconds=gate.verify_timeout_grace_seconds,
        )

    failing_phase = _verify_failure_phase_name(result.output)
    launch_issue = _detect_verify_launch_issue(
        verify_output=result.output,
        verify_exit_status=result.exit_status,
        verify_failure=result.failure,
        failing_phase=failing_phase,
    )
    if launch_issue is not None:
        result = _make_review_verify_result(
            result.command,
            status="unavailable",
            exit_status=MAIN_INTEGRATION_VERIFY_LAUNCH_FAILED_EXIT_STATUS,
            captured_at=result.captured_at,
            reviewed_branch=result.reviewed_branch,
            reviewed_head_sha=result.reviewed_head_sha,
            reviewed_base_sha=result.reviewed_base_sha,
            working_directory=result.working_directory,
            failure=_build_launch_issue_failure(launch_issue),
            output=result.output,
        )
    tree_fingerprint = _verify_tree_fingerprint(result.output) or _compute_tree_fingerprint(git)
    if gate_enabled and result.status == "passed" and tree_fingerprint is None:
        result = _coerce_result_to_freshness_unavailable(result)

    return CandidateIntegrationVerifyEvidence(
        gate_enabled=gate_enabled,
        verify_command=gate.verify_command,
        verify_timeout_seconds=gate.verify_timeout_seconds,
        verify_timeout_grace_seconds=gate.verify_timeout_grace_seconds,
        environment_identity=gate.environment_identity,
        tree_fingerprint=tree_fingerprint,
        head_sha=head_sha,
        verify_status=result.status,
        verify_exit_status=result.exit_status,
        failure=result.failure,
        failing_phase=failing_phase,
        reviewed_branch=result.reviewed_branch,
        working_directory=result.working_directory,
        captured_at=result.captured_at,
    )


def _classify_candidate_integration_verify(
    evidence: CandidateIntegrationVerifyEvidence,
    remediation: MainIntegrationVerifyRemediation | None,
) -> Literal["pass", "red", "deterministic_red", "flake", "unavailable"]:
    if evidence.verify_status == "unavailable":
        return "unavailable"
    if remediation is not None and remediation.kind == "deflake":
        return "flake"
    if _verify_result_halts_merges(
        status=evidence.verify_status,
        gate_enabled=evidence.gate_enabled,
        exit_status=evidence.verify_exit_status,
    ):
        if remediation is not None and remediation.kind == "fix":
            return "deterministic_red"
        return "red"
    return "pass"


def check_candidate_integration_verify(
    config: Config,
    git: Git,
    *,
    reason: str,
    red_reruns: int = 0,
    runner_class: Literal["host", "container"] = "host",
) -> CandidateIntegrationVerifyCheck:
    """Run candidate integration verify for an exact checkout without touching main state."""
    evidence, remediation, _remediation_source_state, verify_runs = _run_integration_verify_with_red_reruns(
        lambda run_reason: run_candidate_integration_verify(
            config,
            git,
            reason=run_reason,
            runner_class=runner_class,
        ),
        reason=reason,
        red_reruns=red_reruns,
    )
    evidence = cast(CandidateIntegrationVerifyEvidence, evidence)
    classification = _classify_candidate_integration_verify(evidence, remediation)
    return CandidateIntegrationVerifyCheck(
        evidence=evidence,
        classification=classification,
        merges_halted=_verify_result_halts_merges(
            status=evidence.verify_status,
            gate_enabled=evidence.gate_enabled,
            exit_status=evidence.verify_exit_status,
        ),
        remediation=remediation,
        verify_runs=verify_runs,
    )


def current_main_integration_verify_alert(
    store: SqliteTaskStore,
    git: Git,
    config: Config,
    *,
    runner_class: Literal["host", "container"] = "host",
) -> MainIntegrationVerifyState | None:
    """Return the current red-main alert when it still matches the live local tree."""
    state = load_main_integration_verify_state(store)
    if state is None or not _verify_result_halts_merges(
        status=state.verify_status,
        gate_enabled=state.gate_enabled,
        exit_status=state.verify_exit_status,
    ):
        return None
    if not _gate_identity_matches(state, _current_gate_identity(config, runner_class=runner_class)):
        return None
    default_branch = git.default_branch()
    current_head_sha = _coerce_optional_str(git.rev_parse_if_exists(default_branch))
    if git.current_branch() == default_branch:
        current_tree_fingerprint = _compute_tree_fingerprint(git)
        if current_tree_fingerprint and state.tree_fingerprint:
            return state if current_tree_fingerprint == state.tree_fingerprint else None
        return replace(
            state,
            verify_status="unavailable",
            verify_exit_status=MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS,
            failure=_build_freshness_unavailable_failure(),
            alert_message=_build_freshness_unavailable_alert_message(head_sha=current_head_sha or state.head_sha),
        )
    if current_head_sha and state.head_sha:
        return state if current_head_sha == state.head_sha else None
    return None
