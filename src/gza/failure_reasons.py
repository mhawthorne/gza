"""Shared failure-reason resolution and persistence helpers."""

from pathlib import Path

from .config import Config
from .db import DB_UNSET, SqliteTaskStore, Task, TaskStats, extract_failure_reason
from .resume_policy import is_resumable_failure_reason

TERMINAL_NO_WORK_FAILURE_REASON = "TERMINAL_NO_WORK"
TERMINATED_FAILURE_REASON = "TERMINATED"
_TERMINAL_NO_WORK_OVERRIDEABLE_FAILURE_REASONS = frozenset(
    {
        "INFRASTRUCTURE_ERROR",
        "PROVIDER_UNAVAILABLE",
        "RETRYABLE_PROVIDER_ERROR",
    }
)
_RUNNER_OWNED_LOG_FALLBACK_REASONS = frozenset(
    {
        "BRANCH_UNPUSHABLE",
        "EXTRACTION_PRECHECK_FAILED",
        "GIT_ERROR",
        "INTERRUPTED",
        "KILLED",
        "MAX_STEPS",
        "MAX_TURNS",
        "MISSING_REPORT_ARTIFACT",
        "NO_ACTIVITY",
        "PREREQUISITE_UNMERGED",
        "PR_REQUIRED",
        "PROVIDER_UNAVAILABLE",
        TERMINAL_NO_WORK_FAILURE_REASON,
        "TERMINATED",
        "TIMEOUT",
        "WORKER_DIED",
    }
)

__all__ = [
    "TERMINAL_NO_WORK_FAILURE_REASON",
    "TERMINATED_FAILURE_REASON",
    "mark_task_failed_from_cause",
    "preserves_failure_reason_over_terminal_no_work",
    "resolve_failure_reason",
    "terminal_no_work_failure_reason",
]


def _limit_reached(limit: int | None, *observed_counts: int | None) -> bool:
    """Return whether any observed usage count reached the configured limit."""
    return isinstance(limit, int) and any(
        count is not None and count >= limit
        for count in observed_counts
    )


def _extract_log_fallback_failure_reason(log_file: Path) -> str:
    """Return a log-scraped fallback reason after filtering runner-owned signals."""
    reason = extract_failure_reason(log_file)
    if reason in _RUNNER_OWNED_LOG_FALLBACK_REASONS:
        return "UNKNOWN"
    return reason


def terminal_no_work_failure_reason(merge_state: str | None) -> str | None:
    """Return the canonical failure reason for a proven terminal no-work branch."""
    if merge_state in {"empty", "redundant"}:
        return TERMINAL_NO_WORK_FAILURE_REASON
    return None


def preserves_failure_reason_over_terminal_no_work(failure_reason: str) -> bool:
    """Return whether a concrete retryable failure should outrank no-work classification."""
    return (
        failure_reason in _TERMINAL_NO_WORK_OVERRIDEABLE_FAILURE_REASONS
        or is_resumable_failure_reason(failure_reason)
    )


def resolve_failure_reason(
    *,
    explicit_reason: str | None = None,
    interrupt_signal: str | None = None,
    interrupted: bool = False,
    error_type: str | None,
    exit_code: int | None,
    log_file: Path | None,
    stats: TaskStats | None = None,
    step_limit: int | None = None,
    turn_limit: int | None = None,
    fallback_to_log: bool = False,
) -> str:
    """Resolve the recorded failure reason from the trigger that actually fired."""
    if explicit_reason is not None:
        return explicit_reason
    if interrupted:
        return TERMINATED_FAILURE_REASON if interrupt_signal == "SIGTERM" else "INTERRUPTED"
    if exit_code == 124:
        return "TIMEOUT"
    observed_steps_computed = stats.num_steps_computed if stats is not None else None
    observed_steps_reported = stats.num_steps_reported if stats is not None else None
    observed_turns_computed = stats.num_turns_computed if stats is not None else None
    observed_turns_reported = stats.num_turns_reported if stats is not None else None
    if error_type == "max_turns" and _limit_reached(
        turn_limit,
        observed_turns_computed,
        observed_turns_reported,
    ):
        return "MAX_TURNS"
    if error_type == "max_steps" and _limit_reached(
        step_limit,
        observed_steps_computed,
        observed_steps_reported,
    ):
        return "MAX_STEPS"
    if error_type == "config_error":
        return "CONFIG_ERROR"
    if error_type == "provider_unavailable":
        return "PROVIDER_UNAVAILABLE"
    if error_type == "retryable_provider_error":
        return "RETRYABLE_PROVIDER_ERROR"
    if error_type == "infrastructure_error":
        return "INFRASTRUCTURE_ERROR"
    if fallback_to_log and log_file is not None:
        return _extract_log_fallback_failure_reason(log_file)
    return "UNKNOWN"


def _task_log_storage_path(config: Config, path: Path) -> str:
    """Convert a task log path to the DB storage string."""
    try:
        return str(path.relative_to(config.project_dir))
    except ValueError:
        return str(path)


def mark_task_failed_from_cause(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    log_file: Path | str | None,
    stats: TaskStats | None = None,
    branch: str | None = None,
    has_commits: bool | None = None,
    explicit_reason: str | None = None,
    interrupt_signal: str | None = None,
    interrupted: bool = False,
    error_type: str | None = None,
    exit_code: int | None = None,
    step_limit: int | None = None,
    turn_limit: int | None = None,
    fallback_to_log: bool = False,
    head_sha: str | None | object = DB_UNSET,
    base_sha: str | None | object = DB_UNSET,
) -> str:
    """Persist a failed task using the shared failure-reason owner."""
    resolved_reason = resolve_failure_reason(
        explicit_reason=explicit_reason,
        interrupt_signal=interrupt_signal,
        interrupted=interrupted,
        error_type=error_type,
        exit_code=exit_code,
        log_file=log_file if isinstance(log_file, Path) else None,
        stats=stats,
        step_limit=step_limit,
        turn_limit=turn_limit,
        fallback_to_log=fallback_to_log,
    )
    log_file_storage = _task_log_storage_path(config, log_file) if isinstance(log_file, Path) else log_file
    store.mark_failed(
        task,
        log_file=log_file_storage,
        stats=stats,
        branch=branch,
        has_commits=bool(has_commits),
        failure_reason=resolved_reason,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return resolved_reason
