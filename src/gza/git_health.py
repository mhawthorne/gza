"""Durable host-side git worktree health probe state."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .db import SqliteTaskStore, Task
from .git import Git, GitError, git_error_indicates_containerized_worktree_metadata_failure

GIT_HEALTH_PROMPT = "System alert: git worktree health"
GIT_HEALTH_REASON = "git-worktree-health-red"
GIT_HEALTH_TAG = "system-git-health"

_KNOWN_METADATA_FAILURE_HINTS = (
    "commondir",
    "/gza-git/common",
    "not a git repository",
)


@dataclass(frozen=True)
class GitHealthState:
    """Persisted git-health probe state."""

    task: Task | None
    reason: str
    dispatch_halted: bool
    probe_command: str
    compact_failure: str | None
    raw_failure_text: str | None
    alert_message: str | None
    captured_at: datetime | None


@dataclass(frozen=True)
class GitHealthCheck:
    """Outcome of probing host-side git worktree health."""

    state: GitHealthState
    dispatch_halted: bool


def _find_git_health_task(store: SqliteTaskStore) -> Task | None:
    for task in store.get_all():
        if task.task_type != "internal":
            continue
        if task.prompt != GIT_HEALTH_PROMPT:
            continue
        return task
    return None


def ensure_git_health_task(store: SqliteTaskStore) -> Task:
    """Return the durable internal row that stores git-health probe state."""
    existing = _find_git_health_task(store)
    if existing is not None:
        return existing

    task = store.add(
        GIT_HEALTH_PROMPT,
        task_type="internal",
        tags=(GIT_HEALTH_TAG,),
        skip_learnings=True,
    )
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = False
    store.update(task)
    return task


def _parse_git_health_payload(task: Task) -> dict[str, Any]:
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


def _compact_failure_text(raw_failure_text: str) -> str:
    pieces = [line.strip() for line in raw_failure_text.splitlines() if line.strip()]
    compacted = " ".join(pieces) if pieces else raw_failure_text.strip()
    return re.sub(r"\s+", " ", compacted).strip()


def _build_repair_guidance(raw_failure_text: str) -> str:
    normalized = raw_failure_text.lower()
    if git_error_indicates_containerized_worktree_metadata_failure(raw_failure_text) or any(
        hint in normalized for hint in _KNOWN_METADATA_FAILURE_HINTS
    ):
        return (
            "Inspect `.git/worktrees/*/commondir` for container-only paths such as "
            "`/gza-git/common`, then repair or prune the broken host-side worktree metadata."
        )
    return "Repair the repository worktree metadata from the host checkout."


def _build_alert_message(compact_failure: str, raw_failure_text: str) -> str:
    guidance = _build_repair_guidance(raw_failure_text)
    return (
        "git worktree health RED - dispatch halted; `git worktree list` failed: "
        f"{compact_failure}. {guidance} Then rerun `uv run gza watch`. "
        "No tasks were started or marked failed by this halt."
    )


def _persist_git_health_payload(
    store: SqliteTaskStore,
    task: Task,
    *,
    reason: str,
    dispatch_halted: bool,
    compact_failure: str | None,
    raw_failure_text: str | None,
    alert_message: str | None,
    captured_at: datetime,
) -> None:
    task.output_content = json.dumps(
        {
            "alert_message": alert_message,
            "captured_at": captured_at.isoformat(),
            "compact_failure": compact_failure,
            "dispatch_halted": dispatch_halted,
            "probe_command": "git worktree list --porcelain",
            "raw_failure_text": raw_failure_text,
            "reason": reason,
        },
        sort_keys=True,
    )
    task.status = "completed"
    task.completed_at = captured_at
    task.has_commits = False
    store.update(task)


def load_git_health_state(store: SqliteTaskStore) -> GitHealthState | None:
    """Return the persisted git-health state when it exists."""
    task = _find_git_health_task(store)
    if task is None:
        return None
    payload = _parse_git_health_payload(task)
    captured_at_raw = payload.get("captured_at")
    captured_at = None
    if isinstance(captured_at_raw, str):
        try:
            captured_at = datetime.fromisoformat(captured_at_raw)
        except ValueError:
            captured_at = None
    dispatch_halted = payload.get("dispatch_halted")
    return GitHealthState(
        task=task,
        reason=_coerce_optional_str(payload.get("reason")) or GIT_HEALTH_REASON,
        dispatch_halted=bool(dispatch_halted) if isinstance(dispatch_halted, bool) else False,
        probe_command=_coerce_optional_str(payload.get("probe_command")) or "git worktree list --porcelain",
        compact_failure=_coerce_optional_str(payload.get("compact_failure")),
        raw_failure_text=_coerce_optional_str(payload.get("raw_failure_text")),
        alert_message=_coerce_optional_str(payload.get("alert_message")),
        captured_at=captured_at or task.completed_at,
    )


def check_git_health(store: SqliteTaskStore, git: Git, *, persist: bool = True) -> GitHealthCheck:
    """Probe host-side git worktree health and optionally persist the current verdict."""
    captured_at = datetime.now(UTC)
    existing_task = _find_git_health_task(store) if persist else None
    try:
        git.worktree_list()
    except (GitError, OSError) as error:
        raw_failure_text = str(error).strip() or error.__class__.__name__
        compact_failure = _compact_failure_text(raw_failure_text)
        alert_message = _build_alert_message(compact_failure, raw_failure_text)
        if persist:
            task = existing_task or ensure_git_health_task(store)
            _persist_git_health_payload(
                store,
                task,
                reason=GIT_HEALTH_REASON,
                dispatch_halted=True,
                compact_failure=compact_failure,
                raw_failure_text=raw_failure_text,
                alert_message=alert_message,
                captured_at=captured_at,
            )
            state = load_git_health_state(store)
            assert state is not None
            return GitHealthCheck(state=state, dispatch_halted=state.dispatch_halted)

        state = GitHealthState(
            task=None,
            reason=GIT_HEALTH_REASON,
            dispatch_halted=True,
            probe_command="git worktree list --porcelain",
            compact_failure=compact_failure,
            raw_failure_text=raw_failure_text,
            alert_message=alert_message,
            captured_at=captured_at,
        )
        return GitHealthCheck(state=state, dispatch_halted=True)

    if persist and existing_task is not None:
        _persist_git_health_payload(
            store,
            existing_task,
            reason=GIT_HEALTH_REASON,
            dispatch_halted=False,
            compact_failure=None,
            raw_failure_text=None,
            alert_message=None,
            captured_at=captured_at,
        )
        state = load_git_health_state(store)
        assert state is not None
        return GitHealthCheck(state=state, dispatch_halted=state.dispatch_halted)

    state = GitHealthState(
        task=None,
        reason=GIT_HEALTH_REASON,
        dispatch_halted=False,
        probe_command="git worktree list --porcelain",
        compact_failure=None,
        raw_failure_text=None,
        alert_message=None,
        captured_at=captured_at,
    )
    return GitHealthCheck(state=state, dispatch_halted=state.dispatch_halted)


def current_git_health_alert(store: SqliteTaskStore) -> GitHealthState | None:
    """Return the current persisted git-health alert when the last probe is red."""
    state = load_git_health_state(store)
    if state is None or not state.dispatch_halted:
        return None
    return state


__all__ = [
    "GIT_HEALTH_PROMPT",
    "GIT_HEALTH_REASON",
    "GIT_HEALTH_TAG",
    "GitHealthCheck",
    "GitHealthState",
    "check_git_health",
    "current_git_health_alert",
    "ensure_git_health_task",
    "load_git_health_state",
]
