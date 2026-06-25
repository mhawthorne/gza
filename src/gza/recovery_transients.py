"""Helpers for transient recovery cooldown behavior."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .db import Task as DbTask

_TRANSIENT_RECOVERY_BACKOFF_FACTORS: tuple[int, ...] = (1, 2, 5, 10)
_PROVIDER_CAPACITY_TRANSIENT_FAILURE_REASONS = frozenset(
    {
        "PROVIDER_UNAVAILABLE",
        "RETRYABLE_PROVIDER_ERROR",
    }
)
_EXECUTION_SENSITIVE_TRANSIENT_FAILURE_REASONS = frozenset(
    {
        "INFRASTRUCTURE_ERROR",
        "WORKER_DIED",
        "NO_ACTIVITY",
    }
)
_PROVIDER_CAPACITY_MESSAGE_SNIPPETS = (
    "selected model is at capacity",
    "model is at capacity",
    "at capacity. try again shortly",
)


@dataclass(frozen=True)
class TransientRecoveryTerminal:
    """Semantic classification for one transient failed recovery/improve attempt."""

    code: str
    failure_reason: str
    fingerprint: str


def _has_durable_progress(task: DbTask) -> bool:
    if task.has_commits:
        return True
    if task.changed_diff is True:
        return True
    return any(
        value is not None and value > 0
        for value in (
            task.diff_files_changed,
            task.diff_lines_added,
            task.diff_lines_removed,
        )
    )


def _has_meaningful_execution_evidence(task: DbTask) -> bool:
    return any(
        value is not None and value > 0
        for value in (
            task.num_steps_reported,
            task.num_steps_computed,
            task.num_turns_reported,
            task.num_turns_computed,
            task.output_tokens,
        )
    )


def _task_log_path(task: DbTask, *, project_dir: Path | None) -> Path | None:
    raw_log_file = getattr(task, "log_file", None)
    if not isinstance(raw_log_file, str) or not raw_log_file.strip():
        return None
    log_path = Path(raw_log_file)
    if log_path.is_absolute():
        return log_path
    if project_dir is None:
        return None
    return project_dir / log_path


def _message_contains_capacity_proof(message: str | None) -> bool:
    if not isinstance(message, str):
        return False
    normalized = message.strip().lower()
    return any(snippet in normalized for snippet in _PROVIDER_CAPACITY_MESSAGE_SNIPPETS)


def _log_has_explicit_provider_capacity_evidence(task: DbTask, *, project_dir: Path | None) -> bool:
    log_path = _task_log_path(task, project_dir=project_dir)
    if log_path is None or not log_path.exists():
        return False
    try:
        for raw_line in log_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if _message_contains_capacity_proof(line):
                return True
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            message = payload.get("message")
            if _message_contains_capacity_proof(message if isinstance(message, str) else None):
                return True
            error = payload.get("error")
            if isinstance(error, dict):
                error_message = error.get("message")
                if _message_contains_capacity_proof(error_message if isinstance(error_message, str) else None):
                    return True
    except OSError:
        return False
    return False


def _has_explicit_provider_capacity_evidence(task: DbTask, *, project_dir: Path | None) -> bool:
    output_content = getattr(task, "output_content", None)
    if _message_contains_capacity_proof(output_content if isinstance(output_content, str) else None):
        return True
    return _log_has_explicit_provider_capacity_evidence(task, project_dir=project_dir)


def classify_transient_recovery_terminal(
    task: DbTask | None,
    *,
    project_dir: Path | None = None,
) -> TransientRecoveryTerminal | None:
    """Return a transient classification for one failed recovery/improve attempt.

    This is intentionally narrow: explicit provider-capacity failures remain transient
    when they produced no durable progress even if incidental provider counters were
    recorded, while infrastructure/worker deaths and timeouts still fail closed once
    meaningful execution evidence exists.
    """
    if task is None or task.status != "failed":
        return None
    failure_reason = str(task.failure_reason or "").strip().upper()
    if not failure_reason:
        return None
    if _has_durable_progress(task):
        return None
    if failure_reason in _PROVIDER_CAPACITY_TRANSIENT_FAILURE_REASONS:
        if _has_meaningful_execution_evidence(task) and not _has_explicit_provider_capacity_evidence(
            task,
            project_dir=project_dir,
        ):
            return None
        return TransientRecoveryTerminal(
            code=failure_reason.lower().replace("_", "-"),
            failure_reason=failure_reason,
            fingerprint=f"transient:{task.task_type}:{failure_reason}",
        )
    if _has_meaningful_execution_evidence(task):
        return None
    if failure_reason in _EXECUTION_SENSITIVE_TRANSIENT_FAILURE_REASONS:
        return TransientRecoveryTerminal(
            code=failure_reason.lower().replace("_", "-"),
            failure_reason=failure_reason,
            fingerprint=f"transient:{task.task_type}:{failure_reason}",
        )
    if failure_reason == "TIMEOUT":
        return TransientRecoveryTerminal(
            code="timeout-before-execution",
            failure_reason=failure_reason,
            fingerprint=f"transient:{task.task_type}:TIMEOUT:no-execution",
        )
    return None


def compute_transient_recovery_backoff_seconds(config: Config, streak: int) -> int:
    """Return the bounded cooldown for one transient recovery streak.

    The schedule is anchored to ``watch.failure_backoff_initial`` and follows
    ``1x, 2x, 5x, 10x``, then doubles from there until capped by
    ``watch.transient_recovery_backoff_max``.
    """
    if streak <= 0:
        return 0
    initial = config.watch.failure_backoff_initial
    maximum = config.watch.transient_recovery_backoff_max
    if streak <= len(_TRANSIENT_RECOVERY_BACKOFF_FACTORS):
        return min(initial * _TRANSIENT_RECOVERY_BACKOFF_FACTORS[streak - 1], maximum)
    base = initial * _TRANSIENT_RECOVERY_BACKOFF_FACTORS[-1]
    growth = 2 ** (streak - len(_TRANSIENT_RECOVERY_BACKOFF_FACTORS))
    return min(base * growth, maximum)
