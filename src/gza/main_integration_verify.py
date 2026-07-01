"""Shared local-main integration verify helpers for advance/watch."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

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
MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE = "watch-main-integration-verify-remediation"
VERIFY_COMMAND_OUTPUT_ARTIFACT_KIND = "verify_command_output"
MAIN_VERIFY_REMEDIATION_ARTIFACT_MAX_BYTES = 32 * 1024
MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_LINES = 24
MAIN_VERIFY_REMEDIATION_EXCERPT_MAX_CHARS = 2000


@dataclass(frozen=True)
class MainIntegrationVerifyState:
    """Persisted verification state for the canonical local target branch."""

    task: Task
    gate_enabled: bool
    verify_command: str | None
    verify_timeout_seconds: int | None
    verify_timeout_grace_seconds: float | None
    tree_fingerprint: str | None
    head_sha: str | None
    verify_status: str | None
    verify_exit_status: str | None
    failure: str | None
    failing_phase: str | None
    alert_message: str | None
    captured_at: datetime | None


@dataclass(frozen=True)
class MainIntegrationVerifyCheck:
    """Outcome of checking whether local-main integration verify must run."""

    state: MainIntegrationVerifyState
    performed_verify: bool
    current_tree_fingerprint: str | None
    is_current: bool
    merges_halted: bool
    remediation: MainIntegrationVerifyRemediation | None = None
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
    return MainIntegrationVerifyState(
        task=task,
        gate_enabled=_payload_gate_enabled(task, payload),
        verify_command=_payload_verify_command(task, payload),
        verify_timeout_seconds=_coerce_optional_int(payload.get("verify_timeout_seconds")),
        verify_timeout_grace_seconds=_coerce_optional_float(payload.get("verify_timeout_grace_seconds")),
        tree_fingerprint=payload.get("tree_fingerprint") if isinstance(payload.get("tree_fingerprint"), str) else None,
        head_sha=payload.get("head_sha") if isinstance(payload.get("head_sha"), str) else task.review_verify_head_sha,
        verify_status=task.review_verify_status,
        verify_exit_status=task.review_verify_exit_status,
        failure=task.review_verify_failure,
        failing_phase=payload.get("failing_phase") if isinstance(payload.get("failing_phase"), str) else None,
        alert_message=payload.get("alert_message") if isinstance(payload.get("alert_message"), str) else None,
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


def _build_main_integration_verify_remediation(
    *,
    kind: Literal["deflake", "fix"],
    config: Config,
    store: SqliteTaskStore,
    state: MainIntegrationVerifyState,
) -> MainIntegrationVerifyRemediation:
    artifact_path, artifact_output = _load_main_verify_artifact_evidence(
        config=config,
        store=store,
        task=state.task,
    )
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


def _build_freshness_unavailable_failure() -> str:
    return "could not prove exact local target tree freshness because the tree fingerprint is unavailable"


def _build_freshness_unavailable_alert_message(*, head_sha: str | None) -> str:
    short_sha = (head_sha or "unknown")[:12]
    return f"main verify freshness unproven at `{short_sha}` - merges halted; exact tree fingerprint unavailable"


def _verify_result_is_red(*, status: str | None, gate_enabled: bool) -> bool:
    return gate_enabled and status != "passed"


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


def _current_gate_identity(config: Config) -> MainIntegrationVerifyGateIdentity:
    verify_command = _normalized_verify_command(config)
    gate_enabled = bool(verify_command)
    if not gate_enabled:
        return MainIntegrationVerifyGateIdentity(
            gate_enabled=False,
            verify_command=None,
            verify_timeout_seconds=None,
            verify_timeout_grace_seconds=None,
        )

    timeout_seconds, timeout_grace_seconds = _resolve_review_verify_timeout_settings(config)
    return MainIntegrationVerifyGateIdentity(
        gate_enabled=True,
        verify_command=verify_command,
        verify_timeout_seconds=timeout_seconds,
        verify_timeout_grace_seconds=timeout_grace_seconds,
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
    )


def _persist_main_integration_verify_payload(
    store: SqliteTaskStore,
    task: Task,
    *,
    gate_enabled: bool,
    verify_command: str | None,
    verify_timeout_seconds: int | None,
    verify_timeout_grace_seconds: float | None,
    tree_fingerprint: str | None,
    head_sha: str | None,
    failing_phase: str | None,
    alert_message: str | None,
    captured_at: datetime,
) -> None:
    task.output_content = json.dumps(
        {
            "gate_enabled": gate_enabled,
            "verify_command": verify_command,
            "verify_timeout_seconds": verify_timeout_seconds,
            "verify_timeout_grace_seconds": verify_timeout_grace_seconds,
            "tree_fingerprint": tree_fingerprint,
            "head_sha": head_sha,
            "failing_phase": failing_phase,
            "alert_message": alert_message,
            "captured_at": captured_at.isoformat(),
        },
        sort_keys=True,
    )
    task.status = "completed"
    task.completed_at = captured_at
    task.has_commits = False
    store.update(task)


def persist_main_integration_verify_alert_message(
    store: SqliteTaskStore,
    *,
    state: MainIntegrationVerifyState,
    alert_message: str,
) -> MainIntegrationVerifyState:
    """Persist a replacement durable alert message while preserving verify identity."""
    captured_at = state.captured_at or state.task.completed_at or datetime.now(UTC)
    _persist_main_integration_verify_payload(
        store,
        state.task,
        gate_enabled=state.gate_enabled,
        verify_command=state.verify_command,
        verify_timeout_seconds=state.verify_timeout_seconds,
        verify_timeout_grace_seconds=state.verify_timeout_grace_seconds,
        tree_fingerprint=state.tree_fingerprint,
        head_sha=state.head_sha,
        failing_phase=state.failing_phase,
        alert_message=alert_message,
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
    if state.gate_enabled:
        if _verify_result_is_red(status=state.verify_status, gate_enabled=state.gate_enabled):
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
) -> MainIntegrationVerifyState:
    """Run the configured verify gate against the current local target checkout."""
    task = ensure_main_integration_verify_task(store)
    captured_at = datetime.now(UTC)
    head_sha = _coerce_optional_str(git.rev_parse_if_exists("HEAD"))
    gate = _current_gate_identity(config)
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
        _build_freshness_unavailable_alert_message(head_sha=head_sha)
        if gate_enabled
        and result.status == "unavailable"
        and result.exit_status == MAIN_INTEGRATION_VERIFY_FRESHNESS_UNAVAILABLE_EXIT_STATUS
        else _build_red_alert_message(
            head_sha=head_sha,
            verify_status=result.status,
            failing_phase=failing_phase,
        )
        if _verify_result_is_red(status=result.status, gate_enabled=gate_enabled)
        else None
    )
    _persist_main_integration_verify_payload(
        store,
        task,
        gate_enabled=gate_enabled,
        verify_command=gate.verify_command,
        verify_timeout_seconds=gate.verify_timeout_seconds,
        verify_timeout_grace_seconds=gate.verify_timeout_grace_seconds,
        tree_fingerprint=tree_fingerprint,
        head_sha=head_sha,
        failing_phase=failing_phase,
        alert_message=alert_message,
        captured_at=result.captured_at,
    )
    state = load_main_integration_verify_state(store)
    assert state is not None
    return state


def _run_main_integration_verify_with_red_reruns(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    reason: str,
    red_reruns: int,
    prior_red_state: MainIntegrationVerifyState | None = None,
) -> tuple[MainIntegrationVerifyState, MainIntegrationVerifyRemediation | None, int]:
    """Run verify, optionally rerunning red verdicts to classify flakes vs deterministic reds."""
    state = run_main_integration_verify(config, store, git, reason=reason)
    verify_runs = 1
    if red_reruns <= 0:
        return state, None, verify_runs

    reference_state = (
        prior_red_state
        if prior_red_state is not None
        and _verify_result_is_red(status=prior_red_state.verify_status, gate_enabled=prior_red_state.gate_enabled)
        else state
    )
    if not _verify_result_is_red(status=reference_state.verify_status, gate_enabled=reference_state.gate_enabled):
        return state, None, verify_runs

    if not _verify_result_is_red(status=state.verify_status, gate_enabled=state.gate_enabled):
        return (
            state,
            _build_main_integration_verify_remediation(
                kind="deflake",
                config=config,
                store=store,
                state=reference_state,
            ),
            verify_runs,
        )

    confirmed_red_state = state
    for attempt in range(1, red_reruns + 1):
        rerun_state = run_main_integration_verify(
            config,
            store,
            git,
            reason=f"{reason}-rerun-{attempt}",
        )
        verify_runs += 1
        if not _verify_result_is_red(status=rerun_state.verify_status, gate_enabled=rerun_state.gate_enabled):
            return (
                rerun_state,
                _build_main_integration_verify_remediation(
                    kind="deflake",
                    config=config,
                    store=store,
                    state=confirmed_red_state,
                ),
                verify_runs,
            )
        confirmed_red_state = rerun_state
        state = rerun_state

    return (
        state,
        _build_main_integration_verify_remediation(
            kind="fix",
            config=config,
            store=store,
            state=confirmed_red_state,
        ),
        verify_runs,
    )


def check_main_integration_verify(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    reason: str,
    force: bool = False,
    red_reruns: int = 0,
) -> MainIntegrationVerifyCheck:
    """Reuse or refresh local-main verify state for the current tree and gate identity."""
    current_tree_fingerprint = _compute_tree_fingerprint(git)
    current_head_sha = _coerce_optional_str(git.rev_parse_if_exists("HEAD"))
    current_gate = _current_gate_identity(config)
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
        and _verify_result_is_red(status=state.verify_status, gate_enabled=state.gate_enabled)
    ):
        assert state is not None
        return MainIntegrationVerifyCheck(
            state=state,
            performed_verify=False,
            current_tree_fingerprint=current_tree_fingerprint,
            is_current=True,
            merges_halted=_verify_result_is_red(
                status=state.verify_status,
                gate_enabled=state.gate_enabled,
            ),
        )

    refreshed, remediation, verify_runs = _run_main_integration_verify_with_red_reruns(
        config,
        store,
        git,
        reason=reason,
        red_reruns=red_reruns,
        prior_red_state=state if checkpoint_is_current else None,
    )
    return MainIntegrationVerifyCheck(
        state=refreshed,
        performed_verify=True,
        current_tree_fingerprint=current_tree_fingerprint,
        is_current=True,
        merges_halted=_verify_result_is_red(
            status=refreshed.verify_status,
            gate_enabled=refreshed.gate_enabled,
        ),
        remediation=remediation,
        verify_runs=verify_runs,
    )


def current_main_integration_verify_alert(
    store: SqliteTaskStore,
    git: Git,
    config: Config,
) -> MainIntegrationVerifyState | None:
    """Return the current red-main alert when it still matches the live local tree."""
    state = load_main_integration_verify_state(store)
    if state is None or not _verify_result_is_red(
        status=state.verify_status,
        gate_enabled=state.gate_enabled,
    ):
        return None
    if not _gate_identity_matches(state, _current_gate_identity(config)):
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
