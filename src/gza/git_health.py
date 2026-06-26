"""Durable host-side git worktree health state."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Config
from .db import SqliteTaskStore, Task
from .git import (
    Git,
    GitError,
    GitWorktreeHealthProbe,
    WorktreeAdminMetadataIssue,
    WorktreeAdminMetadataValidation,
    validate_host_worktree_admin_metadata,
)

GIT_WORKTREE_HEALTH_PROMPT = "System alert: git worktree health"
GIT_WORKTREE_HEALTH_REASON = "git-worktree-health-red"
GIT_WORKTREE_HEALTH_TAG = "system-git-health"

# Backward-compatible aliases for existing watch wiring/tests.
GIT_HEALTH_PROMPT = GIT_WORKTREE_HEALTH_PROMPT
GIT_HEALTH_REASON = GIT_WORKTREE_HEALTH_REASON
GIT_HEALTH_TAG = GIT_WORKTREE_HEALTH_TAG


@dataclass(frozen=True)
class GitWorktreeHealthFinding:
    """Persisted scanner finding for host-side worktree admin metadata."""

    registration_name: str
    admin_file: str
    admin_path: str
    value: str
    problem: str
    details: str
    expected_value: str | None = None
    suspected_container_path_marker: str | None = None


@dataclass(frozen=True)
class GitWorktreeHealthState:
    """Persisted git-health state for the shared checkout."""

    task: Task | None
    reason: str
    dispatch_halted: bool
    probe_command: str
    probe_returncode: int | None
    probe_stdout: str | None
    probe_stderr: str | None
    raw_failure_text: str | None
    compact_failure: str | None
    suspected_container_path_marker: str | None
    metadata_findings: tuple[GitWorktreeHealthFinding, ...]
    metadata_scan_error: str | None
    remediation_message: str | None
    alert_message: str | None
    captured_at: datetime | None


@dataclass(frozen=True)
class GitWorktreeHealthCheck:
    """Outcome of checking global shared-worktree health."""

    state: GitWorktreeHealthState
    probe: GitWorktreeHealthProbe
    dispatch_halted: bool


# Backward-compatible aliases for existing watch wiring/tests.
GitHealthState = GitWorktreeHealthState
GitHealthCheck = GitWorktreeHealthCheck


def _find_git_worktree_health_task(store: SqliteTaskStore) -> Task | None:
    for task in store.get_all():
        if task.task_type != "internal":
            continue
        if task.prompt != GIT_WORKTREE_HEALTH_PROMPT:
            continue
        return task
    return None


def ensure_git_worktree_health_task(store: SqliteTaskStore) -> Task:
    """Return the durable internal row that stores git-health state."""
    existing = _find_git_worktree_health_task(store)
    if existing is not None:
        return existing

    task = store.add(
        GIT_WORKTREE_HEALTH_PROMPT,
        task_type="internal",
        tags=(GIT_WORKTREE_HEALTH_TAG,),
        skip_learnings=True,
    )
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = False
    store.update(task)
    return task


def ensure_git_health_task(store: SqliteTaskStore) -> Task:
    """Backward-compatible wrapper for the renamed helper."""
    return ensure_git_worktree_health_task(store)


def _parse_git_worktree_health_payload(task: Task) -> dict[str, Any]:
    raw = task.output_content
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _coerce_optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _coerce_optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _compact_failure_text(raw_failure_text: str) -> str:
    pieces = [line.strip() for line in raw_failure_text.splitlines() if line.strip()]
    compacted = " ".join(pieces) if pieces else raw_failure_text.strip()
    return re.sub(r"\s+", " ", compacted).strip()


def _tail_text(text: str, *, max_chars: int = 400) -> str:
    normalized = text.strip()
    if len(normalized) <= max_chars:
        return normalized
    return normalized[-max_chars:]


def _issue_to_finding(issue: WorktreeAdminMetadataIssue) -> GitWorktreeHealthFinding:
    return GitWorktreeHealthFinding(
        registration_name=issue.registration_name,
        admin_file=issue.admin_file,
        admin_path=str(issue.admin_path),
        value=issue.value,
        problem=issue.problem,
        details=issue.details,
        expected_value=issue.expected_value,
        suspected_container_path_marker=issue.suspected_container_path_marker,
    )


def _parse_findings(payload: dict[str, Any]) -> tuple[GitWorktreeHealthFinding, ...]:
    raw_findings = payload.get("metadata_findings")
    if not isinstance(raw_findings, list):
        return ()
    findings: list[GitWorktreeHealthFinding] = []
    for raw in raw_findings:
        if not isinstance(raw, dict):
            continue
        registration_name = _coerce_optional_str(raw.get("registration_name"))
        admin_file = _coerce_optional_str(raw.get("admin_file"))
        admin_path = _coerce_optional_str(raw.get("admin_path"))
        value = _coerce_optional_str(raw.get("value"))
        problem = _coerce_optional_str(raw.get("problem"))
        details = _coerce_optional_str(raw.get("details"))
        if not all((registration_name, admin_file, admin_path, value, problem, details)):
            continue
        assert registration_name is not None
        assert admin_file is not None
        assert admin_path is not None
        assert value is not None
        assert problem is not None
        assert details is not None
        findings.append(
            GitWorktreeHealthFinding(
                registration_name=registration_name,
                admin_file=admin_file,
                admin_path=admin_path,
                value=value,
                problem=problem,
                details=details,
                expected_value=_coerce_optional_str(raw.get("expected_value")),
                suspected_container_path_marker=_coerce_optional_str(raw.get("suspected_container_path_marker")),
            )
        )
    return tuple(findings)


def load_git_worktree_health_state(store: SqliteTaskStore) -> GitWorktreeHealthState | None:
    """Return the persisted git-health state when it exists."""
    task = _find_git_worktree_health_task(store)
    if task is None:
        return None
    payload = _parse_git_worktree_health_payload(task)
    captured_at_raw = payload.get("captured_at")
    captured_at = None
    if isinstance(captured_at_raw, str):
        try:
            captured_at = datetime.fromisoformat(captured_at_raw)
        except ValueError:
            captured_at = None
    dispatch_halted = payload.get("dispatch_halted")
    return GitWorktreeHealthState(
        task=task,
        reason=_coerce_optional_str(payload.get("reason")) or GIT_WORKTREE_HEALTH_REASON,
        dispatch_halted=bool(dispatch_halted) if isinstance(dispatch_halted, bool) else False,
        probe_command=_coerce_optional_str(payload.get("probe_command")) or "git worktree list --porcelain",
        probe_returncode=_coerce_optional_int(payload.get("probe_returncode")),
        probe_stdout=_coerce_optional_str(payload.get("probe_stdout")),
        probe_stderr=_coerce_optional_str(payload.get("probe_stderr")),
        raw_failure_text=_coerce_optional_str(payload.get("raw_failure_text")),
        compact_failure=_coerce_optional_str(payload.get("compact_failure")),
        suspected_container_path_marker=_coerce_optional_str(payload.get("suspected_container_path_marker")),
        metadata_findings=_parse_findings(payload),
        metadata_scan_error=_coerce_optional_str(payload.get("metadata_scan_error")),
        remediation_message=_coerce_optional_str(payload.get("remediation_message")),
        alert_message=_coerce_optional_str(payload.get("alert_message")),
        captured_at=captured_at or task.completed_at,
    )


def load_git_health_state(store: SqliteTaskStore) -> GitWorktreeHealthState | None:
    """Backward-compatible wrapper for the renamed helper."""
    return load_git_worktree_health_state(store)


def _probe_git_worktree_health(git: Git) -> GitWorktreeHealthProbe:
    worktree_list_attr = getattr(git, "__dict__", {}).get("worktree_list")
    if callable(worktree_list_attr):
        command = "git worktree list --porcelain"
        try:
            worktree_list_attr()
        except GitError as error:
            return GitWorktreeHealthProbe(
                command=command,
                returncode=1,
                stdout="",
                stderr=str(error).strip() or error.__class__.__name__,
            )
        return GitWorktreeHealthProbe(command=command, returncode=0, stdout="", stderr="")

    probe_method = getattr(git, "worktree_health_probe", None)
    if callable(probe_method):
        probe = probe_method()
        if isinstance(probe, GitWorktreeHealthProbe):
            return probe

    command = "git worktree list --porcelain"
    worktree_list = getattr(git, "worktree_list", None)
    if callable(worktree_list):
        try:
            worktree_list()
        except GitError as error:
            return GitWorktreeHealthProbe(
                command=command,
                returncode=1,
                stdout="",
                stderr=str(error).strip() or error.__class__.__name__,
            )
        return GitWorktreeHealthProbe(command=command, returncode=0, stdout="", stderr="")

    return GitWorktreeHealthProbe(
        command=command,
        returncode=1,
        stdout="",
        stderr=f"{git.__class__.__name__} does not expose a worktree health probe",
    )


def _validate_admin_metadata(git: Git) -> WorktreeAdminMetadataValidation:
    return validate_host_worktree_admin_metadata(git)


def _format_metadata_scan_error(error: BaseException) -> str:
    message = str(error).strip() or error.__class__.__name__
    return f"{error.__class__.__name__}: {message}"


def _combine_probe_failure_text(probe: GitWorktreeHealthProbe) -> str:
    stderr = probe.stderr.strip()
    stdout = probe.stdout.strip()
    if stderr and stdout:
        return f"{stderr}\n{stdout}"
    return stderr or stdout or f"{probe.command} exited with status {probe.returncode}"


def _display_path(path_text: str, repo_dir: Path | None) -> str:
    path = Path(path_text)
    if repo_dir is not None:
        try:
            return str(path.relative_to(repo_dir))
        except ValueError:
            pass
    return path_text


def _build_remediation_message(
    *,
    repo_dir: Path | None,
    findings: tuple[GitWorktreeHealthFinding, ...],
    metadata_scan_error: str | None,
) -> str:
    parts = [
        "Inspect `.git/worktrees/*/commondir` for container-only paths such as `/gza-git/common`."
        " Inspect `.git/worktrees/*/gitdir` for container-only paths such as `/gza-git`."
    ]
    if metadata_scan_error is not None:
        parts.append(
            "The host admin metadata scanner failed before it could complete, so suspect-file evidence may be incomplete: "
            f"{metadata_scan_error}."
        )
    if findings:
        suspect_parts = []
        for finding in findings:
            admin_path = _display_path(finding.admin_path, repo_dir)
            suspect_parts.append(f"`{admin_path}` (`{finding.value}`)")
        parts.append(f"Suspect admin files: {', '.join(suspect_parts)}.")
    if any(finding.admin_file == "commondir" for finding in findings):
        parts.append(
            "For a corrupt `commondir`, restore `../..` only after confirming the worktree registration belongs to this repo."
        )
    parts.append("Re-run `git worktree list --porcelain`; watch will resume once the probe is green.")
    return " ".join(parts)


def _build_alert_message(
    *,
    probe: GitWorktreeHealthProbe,
    compact_failure: str,
    remediation_message: str,
    metadata_scan_error: str | None,
) -> str:
    if metadata_scan_error is not None and not probe.failed:
        return (
            "git worktree health RED - dispatch halted; "
            f"host worktree metadata scan failed: {metadata_scan_error}. "
            f"{remediation_message} No tasks were started or marked failed by this halt."
        )
    return (
        "git worktree health RED - dispatch halted; "
        f"`git worktree list` failed (exit {probe.returncode}): {compact_failure}. "
        f"{remediation_message} No tasks were started or marked failed by this halt."
    )


def _persist_git_worktree_health_payload(
    store: SqliteTaskStore,
    task: Task,
    *,
    reason: str,
    dispatch_halted: bool,
    probe: GitWorktreeHealthProbe,
    raw_failure_text: str | None,
    compact_failure: str | None,
    suspected_container_path_marker: str | None,
    metadata_findings: tuple[GitWorktreeHealthFinding, ...],
    metadata_scan_error: str | None,
    remediation_message: str | None,
    alert_message: str | None,
    captured_at: datetime,
) -> None:
    task.output_content = json.dumps(
        {
            "alert_message": alert_message,
            "captured_at": captured_at.isoformat(),
            "compact_failure": compact_failure,
            "dispatch_halted": dispatch_halted,
            "metadata_findings": [
                {
                    "admin_file": finding.admin_file,
                    "admin_path": finding.admin_path,
                    "details": finding.details,
                    "expected_value": finding.expected_value,
                    "problem": finding.problem,
                    "registration_name": finding.registration_name,
                    "suspected_container_path_marker": finding.suspected_container_path_marker,
                    "value": finding.value,
                }
                for finding in metadata_findings
            ],
            "metadata_scan_error": metadata_scan_error,
            "probe_command": probe.command,
            "probe_returncode": probe.returncode,
            "probe_stderr": _tail_text(probe.stderr) or None,
            "probe_stdout": _tail_text(probe.stdout) or None,
            "raw_failure_text": raw_failure_text,
            "reason": reason,
            "remediation_message": remediation_message,
            "suspected_container_path_marker": suspected_container_path_marker,
        },
        sort_keys=True,
    )
    task.status = "completed"
    task.completed_at = captured_at
    task.has_commits = False
    store.update(task)


def _build_state(
    *,
    task: Task | None,
    reason: str,
    dispatch_halted: bool,
    probe: GitWorktreeHealthProbe,
    raw_failure_text: str | None,
    compact_failure: str | None,
    suspected_container_path_marker: str | None,
    metadata_findings: tuple[GitWorktreeHealthFinding, ...],
    metadata_scan_error: str | None,
    remediation_message: str | None,
    alert_message: str | None,
    captured_at: datetime,
) -> GitWorktreeHealthState:
    return GitWorktreeHealthState(
        task=task,
        reason=reason,
        dispatch_halted=dispatch_halted,
        probe_command=probe.command,
        probe_returncode=probe.returncode,
        probe_stdout=_tail_text(probe.stdout) or None,
        probe_stderr=_tail_text(probe.stderr) or None,
        raw_failure_text=raw_failure_text,
        compact_failure=compact_failure,
        suspected_container_path_marker=suspected_container_path_marker,
        metadata_findings=metadata_findings,
        metadata_scan_error=metadata_scan_error,
        remediation_message=remediation_message,
        alert_message=alert_message,
        captured_at=captured_at,
    )


def check_git_worktree_health(
    config: Config | None,
    store: SqliteTaskStore,
    git: Git,
    *,
    reason: str = GIT_WORKTREE_HEALTH_REASON,
    persist: bool = True,
) -> GitWorktreeHealthCheck:
    """Probe shared git worktree health and optionally persist the verdict."""
    del config  # reserved for future config-driven container path roots
    captured_at = datetime.now(UTC)
    probe = _probe_git_worktree_health(git)
    metadata_scan_error = None
    validation: WorktreeAdminMetadataValidation | None
    try:
        validation = _validate_admin_metadata(git)
    except (GitError, OSError) as error:
        metadata_scan_error = _format_metadata_scan_error(error)
        validation = None
    repo_dir = getattr(git, "repo_dir", None)
    repo_path = repo_dir if isinstance(repo_dir, Path) else None

    findings = tuple(_issue_to_finding(issue) for issue in validation.issues) if validation is not None else ()
    suspected_container_path_marker = (
        validation.suspected_container_path_marker if validation is not None else None
    )

    if probe.failed or metadata_scan_error is not None:
        raw_failure_text = _combine_probe_failure_text(probe) if probe.failed else metadata_scan_error
        assert raw_failure_text is not None
        compact_failure = _compact_failure_text(raw_failure_text)
        remediation_message = _build_remediation_message(
            repo_dir=repo_path,
            findings=findings,
            metadata_scan_error=metadata_scan_error,
        )
        alert_message = _build_alert_message(
            probe=probe,
            compact_failure=compact_failure,
            remediation_message=remediation_message,
            metadata_scan_error=metadata_scan_error,
        )
        if persist:
            task = ensure_git_worktree_health_task(store)
            _persist_git_worktree_health_payload(
                store,
                task,
                reason=reason,
                dispatch_halted=True,
                probe=probe,
                raw_failure_text=raw_failure_text,
                compact_failure=compact_failure,
                suspected_container_path_marker=suspected_container_path_marker,
                metadata_findings=findings,
                metadata_scan_error=metadata_scan_error,
                remediation_message=remediation_message,
                alert_message=alert_message,
                captured_at=captured_at,
            )
            state = load_git_worktree_health_state(store)
            assert state is not None
            return GitWorktreeHealthCheck(state=state, probe=probe, dispatch_halted=True)
        state = _build_state(
            task=None,
            reason=reason,
            dispatch_halted=True,
            probe=probe,
            raw_failure_text=raw_failure_text,
            compact_failure=compact_failure,
            suspected_container_path_marker=suspected_container_path_marker,
            metadata_findings=findings,
            metadata_scan_error=metadata_scan_error,
            remediation_message=remediation_message,
            alert_message=alert_message,
            captured_at=captured_at,
        )
        return GitWorktreeHealthCheck(state=state, probe=probe, dispatch_halted=True)

    existing_task = _find_git_worktree_health_task(store) if persist else None
    if persist and existing_task is not None:
        _persist_git_worktree_health_payload(
            store,
            existing_task,
            reason=reason,
            dispatch_halted=False,
            probe=probe,
            raw_failure_text=None,
            compact_failure=None,
            suspected_container_path_marker=None,
            metadata_findings=(),
            metadata_scan_error=None,
            remediation_message=None,
            alert_message=None,
            captured_at=captured_at,
        )
        state = load_git_worktree_health_state(store)
        assert state is not None
        return GitWorktreeHealthCheck(state=state, probe=probe, dispatch_halted=False)

    state = _build_state(
        task=None,
        reason=reason,
        dispatch_halted=False,
        probe=probe,
        raw_failure_text=None,
        compact_failure=None,
        suspected_container_path_marker=None,
        metadata_findings=(),
        metadata_scan_error=None,
        remediation_message=None,
        alert_message=None,
        captured_at=captured_at,
    )
    return GitWorktreeHealthCheck(state=state, probe=probe, dispatch_halted=False)


def check_git_health(store: SqliteTaskStore, git: Git, *, persist: bool = True) -> GitWorktreeHealthCheck:
    """Backward-compatible wrapper for existing watch wiring."""
    return check_git_worktree_health(None, store, git, persist=persist)


def current_git_health_alert(store: SqliteTaskStore) -> GitWorktreeHealthState | None:
    """Return the current persisted git-health alert when the last probe is red."""
    state = load_git_worktree_health_state(store)
    if state is None or not state.dispatch_halted:
        return None
    return state


__all__ = [
    "GIT_HEALTH_PROMPT",
    "GIT_HEALTH_REASON",
    "GIT_HEALTH_TAG",
    "GIT_WORKTREE_HEALTH_PROMPT",
    "GIT_WORKTREE_HEALTH_REASON",
    "GIT_WORKTREE_HEALTH_TAG",
    "GitHealthCheck",
    "GitHealthState",
    "GitWorktreeHealthCheck",
    "GitWorktreeHealthFinding",
    "GitWorktreeHealthState",
    "check_git_health",
    "check_git_worktree_health",
    "current_git_health_alert",
    "ensure_git_health_task",
    "ensure_git_worktree_health_task",
    "load_git_health_state",
    "load_git_worktree_health_state",
]
