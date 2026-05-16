"""Main Gza runner orchestration."""

import inspect
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import gza.colors as _colors

from .branch_naming import generate_branch_name
from .branch_resolution import resolve_rebase_target_branch
from .commit_messages import build_task_commit_message
from .config import (
    APP_NAME,
    DEFAULT_REVIEW_CONTEXT_FILE_LIMIT,
    DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD,
    DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD,
    DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS,
    BranchStrategy,
    Config,
)
from .console import (
    console,
    error_message,
    task_footer,
    task_header,
)
from .db import SqliteTaskStore, Task, TaskStats, extract_failure_reason as _extract_failure_reason, task_id_numeric_key
from .dependency_preconditions import get_unmerged_dependency_precondition
from .extractions import (
    MANIFEST_FILENAME,
    PATCH_FILENAME,
    ExtractionError,
    copy_bundle_to_worktree,
    extraction_bundle_path,
    load_manifest,
    load_patch_text,
    parse_patch_touched_paths,
    resolve_manifest_patch_path,
)
from .failure_reasons import (
    mark_task_failed_from_cause as _mark_task_failed,
    resolve_failure_reason as _resolve_failure_reason,
)
from .git import Git, GitApplyResult, GitError, cleanup_worktree_for_branch, parse_diff_numstat
from .github import GitHub, GitHubError
from .learnings import maybe_auto_regenerate_learnings
from .lineage import get_plan_for_task
from .log_paths import TaskLogPaths, ops_log_path_for, resolve_ops_log_path, resolve_task_log_paths
from .pr_ops import build_task_pr_content, ensure_task_pr, sync_task_branch_if_live_pr
from .prompt_sanitization import sanitize_provider_prompt
from .prompts import PromptBuilder
from .providers import Provider, RunResult, get_provider
from .providers.base import PreflightCheckResult
from .rebase_diff import (
    RebaseDiffBaseline,
    capture_rebase_diff_baseline,
    compute_rebase_changed_diff,
)
from .rebase_publish import publish_rebased_branch
from .rebase_validation import (
    RuffDiagnostic,
    capture_rebase_validation_baseline,
    is_rebase_in_progress,
    validate_rebase_resolution_output,
)
from .review_tasks import DuplicateReviewError, create_review_task, extract_followup_prompt_parts
from .review_verdict import (
    compute_review_score,
    parse_review_report,
    parse_review_template,
    parse_review_verdict,
    validate_review_report_contract,
)
from .sync_ops import resolve_branch_pr
from .task_slug import (
    extract_task_id_suffix,
    get_base_task_slug,
    strip_derived_implement_prefixes,
)

logger = logging.getLogger(__name__)

# Keep the legacy patch target available for extraction tests that stub the
# fallback parser on ``gza.runner``.
extract_failure_reason = _extract_failure_reason

EXTRACTION_PRECHECK_FAILURE_REASON = "EXTRACTION_PRECHECK_FAILED"
EXTRACTION_ALREADY_MERGED_COMPLETION_REASON = "EXTRACTION_ALREADY_MERGED"

PR_REQUIRED_FAILURE_REASON = "PR_REQUIRED"
REBASE_VALIDATION_FAILURE_REASON = "REBASE_VALIDATION_FAILED"


@dataclass(frozen=True)
class RunInvocationContext:
    """Execution invocation metadata for runner UX/provenance behavior."""

    command: str
    execution_mode: str
    interaction_mode: str = "observe_only"


@dataclass(frozen=True)
class ExtractionSeedResult:
    """Outcome of extraction bundle preflight/application."""

    seeded_paths: frozenset[str] = frozenset()
    completion_reason: str | None = None


@dataclass(frozen=True)
class ResolvedRunFailure:
    """Resolved provider-run failure with user-facing status text."""

    reason: str
    status: str
    outcome_message: str


def _rebase_validation_failure() -> ResolvedRunFailure:
    return ResolvedRunFailure(
        reason=REBASE_VALIDATION_FAILURE_REASON,
        status="failed",
        outcome_message=f"Outcome: failed ({REBASE_VALIDATION_FAILURE_REASON})",
    )


def _git_error_failure() -> ResolvedRunFailure:
    return ResolvedRunFailure(
        reason="GIT_ERROR",
        status="Failed: git error",
        outcome_message="Outcome: failed (GIT_ERROR)",
    )


def _interrupt_signal_name() -> str | None:
    """Return the current interrupt signal name, if one was recorded."""
    return os.environ.get("GZA_INTERRUPT_SIGNAL")


def _provider_accepts_ops_log_file(provider: Provider) -> bool:
    """Return whether provider.run accepts an explicit ops log path."""
    params = inspect.signature(provider.run).parameters.values()
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == "ops_log_file"
        for parameter in params
    )


def _call_provider_run(
    provider: Provider,
    config: Config,
    prompt: str,
    log_file: Path,
    work_dir: Path,
    *,
    provider_run_kwargs: dict[str, Any],
) -> RunResult:
    """Run a provider while tolerating legacy test doubles without ops_log_file."""
    try:
        return provider.run(
            config,
            prompt,
            log_file,
            work_dir,
            **provider_run_kwargs,
        )
    except TypeError as exc:
        if "unexpected keyword argument 'ops_log_file'" not in str(exc):
            raise
        fallback_kwargs = dict(provider_run_kwargs)
        fallback_kwargs.pop("ops_log_file", None)
        return provider.run(
            config,
            prompt,
            log_file,
            work_dir,
            **fallback_kwargs,
        )


def _interruption_metadata() -> dict[str, str]:
    """Return structured metadata describing the current interrupt context."""
    metadata: dict[str, str] = {}
    signal_name = os.environ.get("GZA_INTERRUPT_SIGNAL")
    if signal_name:
        metadata["signal"] = signal_name
    source = os.environ.get("GZA_INTERRUPT_SOURCE")
    if source:
        metadata["source"] = source
    detail = os.environ.get("GZA_INTERRUPT_DETAIL")
    if detail:
        metadata["detail"] = detail
    return metadata


def _resolve_run_failure(
    *,
    provider_name: str,
    timeout_minutes: int,
    step_limit: int | None,
    turn_limit: int | None,
    error_type: str | None,
    exit_code: int,
    log_file: Path,
    stats: TaskStats,
) -> ResolvedRunFailure | None:
    """Resolve a provider-run failure and the matching operator-facing messages."""
    if exit_code == 0 and error_type is None:
        return None

    reason = _resolve_failure_reason(
        error_type=error_type,
        exit_code=exit_code,
        log_file=log_file,
        stats=stats,
        step_limit=step_limit,
        turn_limit=turn_limit,
        fallback_to_log=True,
    )

    if reason == "TIMEOUT":
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: {provider_name} timed out after {timeout_minutes} minutes",
            outcome_message=f"Outcome: failed (timeout after {timeout_minutes}m)",
        )
    if reason == "MAX_STEPS":
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: max steps of {step_limit} exceeded",
            outcome_message="Outcome: failed (max_steps)",
        )
    if reason == "MAX_TURNS":
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: max turns of {turn_limit} exceeded",
            outcome_message="Outcome: failed (max_turns)",
        )
    if error_type is not None and exit_code == 0:
        return ResolvedRunFailure(
            reason=reason,
            status=f"Failed: {provider_name} reported {error_type}",
            outcome_message=f"Outcome: failed (error_type={error_type})",
        )
    return ResolvedRunFailure(
        reason=reason,
        status=f"Failed: {provider_name} exited with code {exit_code}",
        outcome_message=f"Outcome: failed (exit_code={exit_code})",
    )
def _write_stats_entry(log_file: Path, stats: TaskStats) -> None:
    """Write the standard stats log entry for a completed provider run."""
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "stats",
            "message": (
                f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, "
                f"{stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}"
            ),
            "duration_seconds": stats.duration_seconds,
            "cost_usd": stats.cost_usd,
            "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0,
        },
    )


def _finalize_completed_code_task(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    log_file: Path,
    branch_name: str,
    output_content: str | None,
    stats: TaskStats,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
) -> None:
    """Write terminal success logs and persist completed state for a code task."""
    # Write final log entries before marking completed in DB, so that
    # `gza log -f` (which checks task status) doesn't break out of the
    # follow loop before the log file is fully written.
    write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0})
    _write_stats_entry(log_file, stats)

    # Mark completed — after log entries are flushed so readers see the
    # full log before the status transitions away from in_progress.
    store.mark_completed(
        task,
        branch=branch_name,
        log_file=str(log_file.relative_to(config.project_dir)),
        output_content=output_content,
        has_commits=True,
        stats=stats,
        diff_files_changed=diff_files,
        diff_lines_added=diff_added,
        diff_lines_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )


def _finalize_rebase_completion(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    worktree_git: Git,
    branch_name: str,
    stats: TaskStats,
    log_file: Path,
    output_content: str | None,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
    task_logger: "TaskExecutionLogger",
    target_branch: str,
    create_pr: bool = False,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    fix_was_merged_before_run: bool = False,
    rebase_diff_baseline: RebaseDiffBaseline | None = None,
) -> int:
    """Publish a completed rebase before persisting completed task state."""
    post_complete_rc = _post_complete_code_task(
        task,
        config,
        store,
        worktree_git,
        branch_name,
        stats,
        task_logger=task_logger,
        target_branch=target_branch,
        fix_commits_ahead_before_run=fix_commits_ahead_before_run,
        fix_default_branch=fix_default_branch,
        fix_was_merged_before_run=fix_was_merged_before_run,
        rebase_diff_baseline=rebase_diff_baseline,
    )
    if post_complete_rc != 0:
        return post_complete_rc
    if create_pr:
        pr_ready = _ensure_work_pr_for_completed_code_task(task, config, store, worktree_git)
        if not pr_ready:
            print("Error: Task requested PR creation/reuse, aborting before rebase completion")
            task.output_content = output_content
            task.diff_files_changed = diff_files
            task.diff_lines_added = diff_added
            task.diff_lines_removed = diff_removed
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                has_commits=True,
                stats=stats,
                branch=branch_name,
                explicit_reason=PR_REQUIRED_FAILURE_REASON,
                error_type=None,
                exit_code=1,
                head_sha=head_sha,
                base_sha=base_sha,
            )
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "message": "Outcome: failed (PR_REQUIRED)",
                    "exit_code": 1,
                },
            )
            _write_stats_entry(log_file, stats)
            return 1
    _finalize_completed_code_task(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        branch_name=branch_name,
        output_content=output_content,
        stats=stats,
        diff_files=diff_files,
        diff_added=diff_added,
        diff_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return 0


def _finalize_already_published_rebase_pr_retry(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    branch_name: str,
    stats: TaskStats,
    log_file: Path,
    output_content: str | None,
    diff_files: int,
    diff_added: int,
    diff_removed: int,
    head_sha: str | None,
    base_sha: str | None,
    task_logger: "TaskExecutionLogger",
) -> int:
    """Complete a rebase PR retry after the rebase-side effects already ran.

    A rebase task only reaches ``PR_REQUIRED`` after ``_post_complete_code_task``
    has already published the rebased branch and recorded the rebase-only
    review/merge-state side effects. Retrying PR creation must therefore verify
    the current branch tip is still published without replaying those rebase
    completion effects against a post-rebase baseline.
    """

    publish_rebased_branch(
        git,
        branch=branch_name,
        baseline=None,
        logger=task_logger,
    )
    pr_ready = _ensure_work_pr_for_completed_code_task(task, config, store, git)
    if not pr_ready:
        print("Error: Task requested PR creation/reuse, aborting before rebase completion")
        task.output_content = output_content
        task.diff_files_changed = diff_files
        task.diff_lines_added = diff_added
        task.diff_lines_removed = diff_removed
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            has_commits=True,
            stats=stats,
            branch=branch_name,
            explicit_reason=PR_REQUIRED_FAILURE_REASON,
            error_type=None,
            exit_code=1,
            head_sha=head_sha,
            base_sha=base_sha,
        )
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "outcome",
                "message": "Outcome: failed (PR_REQUIRED)",
                "exit_code": 1,
            },
        )
        _write_stats_entry(log_file, stats)
        return 1
    _finalize_completed_code_task(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        branch_name=branch_name,
        output_content=output_content,
        stats=stats,
        diff_files=diff_files,
        diff_added=diff_added,
        diff_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return 0


def _record_run_failure(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    log_file: Path,
    stats: TaskStats,
    failure: ResolvedRunFailure,
    exit_code: int,
    branch: str | None = None,
    worktree: Path | None = None,
    has_commits: bool | None = None,
) -> None:
    """Emit failure logs/footer and persist the resolved failure reason."""
    task_footer(
        task,
        stats,
        status=failure.status,
        branch=branch,
        worktree=worktree,
        store=store,
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "outcome",
            "message": failure.outcome_message,
            "exit_code": exit_code,
            "failure_reason": failure.reason,
        },
    )
    _write_stats_entry(log_file, stats)
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        stats=stats,
        branch=branch,
        has_commits=has_commits,
        explicit_reason=failure.reason,
        error_type=None,
        exit_code=exit_code,
    )


_TASK_EXECUTION_MODE_BY_INVOCATION_MODE: dict[str, str] = {
    "background_worker": "worker_background",
    "foreground_worker": "worker_foreground",
    "foreground_inline": "foreground_inline",
    "foreground_attach_resume": "foreground_attach_resume",
}

__all__ = [
    "RunInvocationContext",
    "run",
    "build_prompt",
    "write_log_entry",
    "write_ops_entry",
    "TaskExecutionLogger",
    "ensure_task_log_path",
    "ensure_task_log_paths",
    "task_log_storage_path",
    "extract_content_from_log",
    "get_effective_config_for_task",
    "post_review_to_pr",
    "open_task_startup_log",
    "open_task_startup_logs",
    "rename_startup_log_to_slug",
]


def write_log_entry(log_file: "Path", entry: dict) -> None:
    """Append a JSONL entry to the task log file."""
    target = log_file
    payload = dict(entry)
    if payload.get("type") == "gza" and log_file.suffix == ".log":
        target = ops_log_path_for(log_file)
        target.parent.mkdir(parents=True, exist_ok=True)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        if not log_file.exists():
            log_file.touch()
        payload.setdefault("stream", "ops")
        payload.setdefault("source", "gza")
        payload.setdefault("timestamp", _ops_timestamp())
    try:
        with open(target, "a") as f:
            f.write(json.dumps(payload) + "\n")
            f.flush()
    except Exception:
        logger.warning("Failed to write log entry to %s", target, exc_info=True)


def _ops_timestamp() -> str:
    """Return ISO-8601 UTC timestamp for structured ops entries."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def write_ops_entry(ops_log_file: "Path", entry: dict) -> None:
    """Append a structured JSONL entry to the task ops log file."""
    payload = dict(entry)
    payload.setdefault("type", "gza")
    payload.setdefault("stream", "ops")
    payload.setdefault("source", "gza")
    payload.setdefault("timestamp", _ops_timestamp())
    write_log_entry(ops_log_file, payload)


def task_log_storage_path(config: Config, path: Path) -> str:
    """Convert a task log path to the DB storage string (project-relative when possible)."""
    try:
        return str(path.relative_to(config.project_dir))
    except ValueError:
        return str(path)


def ensure_task_log_path(config: Config, store: SqliteTaskStore, task: Task) -> Path:
    """Ensure the task owns a canonical conversation log path and persist it."""
    paths = ensure_task_log_paths(config, store, task)
    return paths.conversation


def ensure_task_log_paths(config: Config, store: SqliteTaskStore, task: Task) -> TaskLogPaths:
    """Ensure the task owns canonical conversation and ops log paths."""
    paths = resolve_task_log_paths(config, task)
    paths.conversation.parent.mkdir(parents=True, exist_ok=True)
    if not paths.conversation.exists():
        paths.conversation.touch()
    storage_path = task_log_storage_path(config, paths.conversation)
    if task.log_file != storage_path:
        task.log_file = storage_path
        store.update(task)
    return paths


def prepare_task_startup_phase(config: Config, store: SqliteTaskStore, task: Task) -> Task:
    """Synchronously materialize task startup metadata before execution detaches."""
    if task.slug is None:
        git = Git(config.project_dir)
        slug_override = _compute_slug_override(task, store)
        task.slug = generate_slug(
            task.prompt,
            existing_id=None,
            log_path=config.log_path,
            git=git,
            store=store,
            exclude_task_id=task.id,
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=task.task_type_hint,
        )

    ensure_task_log_paths(config, store, task)
    # Phase 1 ends here: the task row is durably committed with its slug and log
    # path before provider preflight, worktree setup, or detached execution starts.
    if task.id is None:
        return task
    return store.get(task.id) or task


def remove_task_startup_artifacts(config: Config, task: Task) -> None:
    """Best-effort cleanup for startup artifacts created before execution begins."""
    paths = resolve_task_log_paths(config, task)
    for path in {
        paths.conversation,
        paths.ops,
        paths.startup_conversation,
        paths.startup_ops,
    }:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning("Failed to remove startup artifact %s", path, exc_info=True)


class TaskExecutionLogger:
    """Emit provider-agnostic task execution events to the canonical ops log."""

    def __init__(self, ops_log_file: Path, *, echo: bool = True) -> None:
        self.ops_log_file = ops_log_file
        self.echo = echo

    def _emit(self, subtype: str, message: str, *, stderr: bool = False, extra: dict | None = None) -> None:
        payload: dict[str, object] = {
            "type": "gza",
            "subtype": subtype,
            "message": message,
        }
        if extra:
            payload.update(extra)
        write_ops_entry(self.ops_log_file, payload)
        if self.echo:
            print(message, file=sys.stderr if stderr else sys.stdout)

    def info(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("info", message, extra=extra)

    def warning(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("warning", message, stderr=True, extra=extra)

    def error(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("error", message, stderr=True, extra=extra)

    def phase(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("phase", message, extra=extra)

    def command(self, message: str, *, extra: dict | None = None) -> None:
        self._emit("command", message, extra=extra)


def open_task_startup_log(config: Config, task: Task) -> Path:
    """Return the startup log path for a task, creating parent directories."""
    return open_task_startup_logs(config, task).startup_conversation


def open_task_startup_logs(config: Config, task: Task) -> TaskLogPaths:
    """Return startup log paths for a task, creating parent directories."""
    paths = resolve_task_log_paths(config, task)
    selected_conversation = paths.startup_conversation
    selected_ops = paths.startup_ops
    if task.log_file or task.slug:
        selected_conversation = paths.conversation
        selected_ops = paths.ops
    selected_conversation.parent.mkdir(parents=True, exist_ok=True)
    if not selected_conversation.exists():
        selected_conversation.touch()
    return TaskLogPaths(
        conversation=paths.conversation,
        ops=paths.ops,
        startup_conversation=selected_conversation,
        startup_ops=selected_ops,
        layout=paths.layout,
    )


def rename_startup_log_to_slug(config: Config, startup_log: Path, slug: str) -> Path:
    """Rename startup conversation and ops logs to final slug log paths."""
    final_log = config.log_path / f"{slug}.log"
    final_ops = resolve_ops_log_path(config, final_log)
    startup_ops = resolve_ops_log_path(config, startup_log)
    if startup_log != final_log:
        final_log.parent.mkdir(parents=True, exist_ok=True)
        if startup_log.exists():
            startup_log.replace(final_log)
    if startup_ops != final_ops:
        final_ops.parent.mkdir(parents=True, exist_ok=True)
        if startup_ops.exists():
            startup_ops.replace(final_ops)
    return final_log


def write_worker_start_event(ops_log_file: "Path", *, resumed: bool) -> None:
    """Write a worker start lifecycle event when running under worker mode."""
    if os.environ.get("GZA_WORKER_MODE") != "1":
        return
    worker_id = os.environ.get("GZA_WORKER_ID")
    if not worker_id:
        return
    mode = "pipe mode, resumed" if resumed else "pipe mode"
    write_ops_entry(
        ops_log_file,
        {
            "subtype": "worker_lifecycle",
            "event": "start",
            "worker_id": worker_id,
            "message": f"Worker {worker_id} started ({mode})",
        },
    )


def _resolve_default_invocation_context() -> "RunInvocationContext":
    """Build default invocation context from process mode."""
    if os.environ.get("GZA_WORKER_MODE") == "1":
        return RunInvocationContext(command="work", execution_mode="background_worker")
    return RunInvocationContext(command="work", execution_mode="foreground_worker")


def _task_execution_mode_from_invocation(invocation: "RunInvocationContext") -> str:
    """Map runner invocation mode to persisted task execution mode."""
    return _TASK_EXECUTION_MODE_BY_INVOCATION_MODE.get(invocation.execution_mode, "worker_foreground")


def _resolve_interaction_mode(
    invocation: "RunInvocationContext",
    provider: "Provider",
) -> str:
    """Resolve actual interaction mode using provider capabilities."""
    requested = invocation.interaction_mode
    if requested == "auto":
        resolved = "interactive" if provider.supports_interactive_foreground else "observe_only"
    elif requested == "interactive" and not provider.supports_interactive_foreground:
        resolved = "observe_only"
    else:
        resolved = requested

    return resolved


def write_execution_provenance_event(
    ops_log_file: Path,
    *,
    invocation: "RunInvocationContext",
    provider: "Provider",
    interaction_mode: str,
    resumed: bool,
) -> None:
    """Write structured runner execution provenance before provider launch."""
    provider_name = provider.name.lower()
    canonical_execution_mode = _task_execution_mode_from_invocation(invocation)
    worker_mode = canonical_execution_mode in {"worker_background", "worker_foreground"}
    message = (
        f"Execution: command={invocation.command}, mode={canonical_execution_mode}, "
        f"interaction={interaction_mode}, provider={provider_name}, resumed={resumed}"
    )
    write_ops_entry(
        ops_log_file,
        {
            "subtype": "execution",
            "message": message,
            "command": invocation.command,
            "execution_mode": canonical_execution_mode,
            "interaction_mode": interaction_mode,
            "provider": provider_name,
            "worker_mode": worker_mode,
            "resumed": resumed,
        },
    )


def _mark_preflight_provider_unavailable(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
    invocation: "RunInvocationContext",
    interaction_mode: str,
    resume: bool,
    message: str,
) -> None:
    """Persist preflight credential failures as provider-unavailable task failures."""
    log_file = ensure_task_log_path(config, store, task)

    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(
        log_file,
        {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug or ''}".strip()},
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": f"Provider: {provider.name}, Model: {config.model or 'default'}",
        },
    )
    write_execution_provenance_event(
        log_file,
        invocation=invocation,
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "outcome",
            "message": message,
            "failure_reason": "PROVIDER_UNAVAILABLE",
        },
    )
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        explicit_reason="PROVIDER_UNAVAILABLE",
        error_type=None,
        exit_code=None,
    )


def _normalize_preflight_result(result: bool | PreflightCheckResult) -> PreflightCheckResult:
    """Normalize legacy bool provider preflight results to structured outcomes."""
    if isinstance(result, PreflightCheckResult):
        return result
    if result:
        return PreflightCheckResult.success()
    return PreflightCheckResult.failure(
        failure_reason="PROVIDER_UNAVAILABLE",
        message="Preflight failed: provider credential verification failed",
    )


def _mark_preflight_failure(
    *,
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
    invocation: "RunInvocationContext",
    interaction_mode: str,
    resume: bool,
    message: str,
    failure_reason: str,
) -> None:
    """Persist structured preflight failures as task failures with provenance."""
    log_file = (
        config.project_dir / Path(task.log_file)
        if task.log_file
        else open_task_startup_log(config, task)
    )
    log_file_relative = str(log_file.relative_to(config.project_dir))
    if task.log_file != log_file_relative:
        task.log_file = log_file_relative
        store.update(task)

    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(
        log_file,
        {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug or ''}".strip()},
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": f"Provider: {provider.name}, Model: {config.model or 'default'}",
        },
    )
    write_execution_provenance_event(
        log_file,
        invocation=invocation,
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "outcome",
            "message": message,
            "failure_reason": failure_reason,
        },
    )
    _mark_task_failed(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        explicit_reason=failure_reason,
        error_type=None,
        exit_code=None,
    )


def extract_content_from_log(log_file: "Path") -> str | None:
    """Scan a JSONL log file for a provider 'result' entry and return its text.

    Providers emit a ``{"type": "result", "result": "<text>"}`` line when the
    agent finishes.  If the agent output the review (or plan/explore) as text
    rather than writing the expected file artifact, the content lives here.

    Returns the last non-empty result entry, since a resumed session may emit
    an intermediate result before the final one.
    """
    last_result: str | None = None
    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("type") == "result":
                        result_text = entry.get("result", "")
                        if isinstance(result_text, str) and result_text.strip():
                            last_result = result_text
                except json.JSONDecodeError:
                    continue
    except OSError:
        logger.warning("Failed to read log file %s for content recovery", log_file)
    return last_result


def _persist_run_steps_from_result(
    store: SqliteTaskStore,
    run_id: str,
    provider_name: str,
    result: RunResult,
) -> bool:
    """Persist provider-emitted step/substep events into run_steps tables."""
    accumulated = getattr(result, "_accumulated_data", None)
    if not isinstance(accumulated, dict):
        return False
    events: list[Any] = accumulated.get("run_step_events")  # type: ignore[assignment]
    if not isinstance(events, list):
        return False
    store.set_log_schema_version(run_id, 2)

    has_non_completed = any(
        isinstance(event, dict) and str(event.get("outcome") or "completed") != "completed"
        for event in events
    )
    fallback_outcome: str | None = None
    if not has_non_completed:
        if result.error_type in ("max_steps", "max_turns"):
            fallback_outcome = "interrupted"
        elif result.error_type is not None or result.exit_code != 0:
            fallback_outcome = "failed"
        if fallback_outcome is not None:
            for event in reversed(events):
                if isinstance(event, dict):
                    cast(dict[str, Any], event)["outcome"] = fallback_outcome
                    break

    for event in events:
        if not isinstance(event, dict):
            continue
        step_ref = store.emit_step(
            run_id,
            event.get("message_text"),
            provider=provider_name,
            message_role=str(event.get("message_role") or "assistant"),
            legacy_turn_id=event.get("legacy_turn_id"),
            legacy_event_id=event.get("legacy_event_id"),
        )
        for substep in event.get("substeps", []):
            if not isinstance(substep, dict):
                continue
            store.emit_substep(
                step_ref,
                str(substep.get("type") or "event"),
                substep.get("payload"),
                source=str(substep.get("source") or "provider"),
                call_id=substep.get("call_id"),
                legacy_turn_id=substep.get("legacy_turn_id"),
                legacy_event_id=substep.get("legacy_event_id"),
            )
        store.finalize_step(
            step_ref,
            str(event.get("outcome") or "completed"),
            event.get("summary"),
        )
    return True


def get_effective_config_for_task(task: Task, config: Config) -> tuple[str | None, str, int]:
    """Get the effective model, provider, and max_steps for a task.

    Priority order for provider selection:
    1. Explicit task-specific provider override (task.provider when provider_is_explicit)
    2. Task-type route (config.task_providers.<task_type>)
    3. Config default (config.provider, already env-merged in Config.load)

    Priority order for model selection:
    1. Task-specific model (task.model)
    2. Provider-aware config resolution (Config.get_model_for_task)

    Priority order for max_steps selection:
    1. Provider-aware config resolution (Config.get_max_steps_for_task)

    Args:
        task: The task to get config for
        config: The base configuration

    Returns:
        Tuple of (model, provider, max_steps) where model can be None
    """
    provider_override = task.provider if task.provider_is_explicit and task.provider else None
    provider = provider_override if provider_override else config.get_provider_for_task(task.task_type)
    model = task.model if task.model else config.get_model_for_task(task.task_type, provider)
    max_steps = config.get_max_steps_for_task(task.task_type, provider)
    return model, provider, max_steps


DEFAULT_REPORT_DIR = f".{APP_NAME}/explorations"
PLAN_DIR = f".{APP_NAME}/plans"
REVIEW_DIR = f".{APP_NAME}/reviews"
INTERNAL_DIR = f".{APP_NAME}/internal"
SUMMARY_DIR = f".{APP_NAME}/summaries"
WIP_DIR = f".{APP_NAME}/wip"
BACKUP_DIR = f".{APP_NAME}/backups"


def get_task_output_paths(
    task: Task, project_dir: Path
) -> tuple[Path | None, Path | None]:
    """Determine report_path and summary_path for a task based on its type.

    This is the single source of truth for where task outputs go.
    Used by the runner and by ``gza show --prompt``.

    Returns:
        (report_path, summary_path) — one or both may be None.
    """
    report_path: Path | None = None
    summary_path: Path | None = None

    if not task.slug:
        return None, None

    if task.task_type in ("task", "implement", "improve", "fix", "rebase"):
        summary_path = project_dir / SUMMARY_DIR / f"{task.slug}.md"
    elif task.task_type == "explore":
        report_path = project_dir / DEFAULT_REPORT_DIR / f"{task.slug}.md"
    elif task.task_type == "plan":
        report_path = project_dir / PLAN_DIR / f"{task.slug}.md"
    elif task.task_type == "review":
        report_path = project_dir / REVIEW_DIR / f"{task.slug}.md"
    elif task.task_type in ("internal", "learn"):
        report_path = project_dir / INTERNAL_DIR / f"{task.slug}.md"
    else:
        report_path = project_dir / DEFAULT_REPORT_DIR / f"{task.slug}.md"

    return report_path, summary_path


# Diff size thresholds for tiered diff strategy in review prompts
DIFF_SMALL_THRESHOLD = DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD
DIFF_MEDIUM_THRESHOLD = DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD
REVIEW_CONTEXT_FILE_LIMIT = DEFAULT_REVIEW_CONTEXT_FILE_LIMIT
REVIEW_IMPROVE_LINEAGE_LIMIT = 5
REVIEW_IMPROVE_SUMMARY_MAX_CHARS = 320
REVIEW_VERIFY_OUTPUT_MAX_CHARS = 4000
REVIEW_VERIFY_TIMEOUT_SECONDS = DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS
COMMIT_SUBJECT_MAX_CHARS = 72


def _extract_review_verdict(content: str | None) -> str | None:
    """Backward-compatible wrapper around the shared verdict parser."""
    return parse_review_verdict(content)


def _backup_sqlite_file(source_path: Path, destination_path: Path) -> None:
    """Copy a SQLite database file using SQLite's backup API."""
    source = sqlite3.connect(str(source_path))
    try:
        destination = sqlite3.connect(str(destination_path))
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()


def backup_database(db_path: Path, project_dir: Path) -> None:
    """Create an hourly backup of the SQLite database if one doesn't exist yet.

    Checks if a backup for the current hour already exists. If not, creates
    a timestamped backup using SQLite's backup API (safe for concurrent access).

    Backup filename format: gza-YYYYMMDDHH.db (e.g., gza-2026021414.db)

    Args:
        db_path: Path to the source SQLite database
        project_dir: Project directory (used for project-local DB backup location)
    """
    if not db_path.exists():
        return

    local_db = project_dir / f".{APP_NAME}/{APP_NAME}.db"
    if db_path.resolve() == local_db.resolve():
        backup_dir = project_dir / BACKUP_DIR
    else:
        backup_dir = db_path.parent / "backups"
    hour_stamp = datetime.now().strftime("%Y%m%d%H")
    backup_path = backup_dir / f"gza-{hour_stamp}.db"

    if backup_path.exists():
        return

    backup_dir.mkdir(parents=True, exist_ok=True)

    _backup_sqlite_file(db_path, backup_path)


def load_dotenv(project_dir: Path) -> None:
    """Load .env files from project .gza dir, project root, and home directory.

    Load order (lowest priority first — higher-priority sources are loaded last and
    use override=True to win over shell environment variables and earlier sources):
    1. ~/.{APP_NAME}/.env (home defaults, lowest priority; uses setdefault)
    2. <project_dir>/.env (overrides shell vars and home defaults)
    3. <project_dir>/.gza/.env (highest priority; overrides project .env and shell vars)

    Shell environment variables are preserved unless overridden by sources loaded
    with override=True (i.e., project .env and .gza/.env).
    """
    def _load(path: Path, override: bool) -> None:
        if not path.exists():
            return
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    k = key.strip()
                    v = value.strip()
                    if override:
                        os.environ[k] = v
                    else:
                        os.environ.setdefault(k, v)

    # Lowest priority: home ~/.{APP_NAME}/.env (does not override shell or project values)
    _load(Path.home() / f".{APP_NAME}" / ".env", override=False)

    # Mid priority: project root .env (overrides shell and home; backwards compat)
    _load(project_dir / ".env", override=True)

    # Highest priority: project .gza/.env (shared across worktrees via symlink)
    _load(project_dir / f".{APP_NAME}" / ".env", override=True)


def slugify(text: str, max_length: int = 50) -> str:
    """Convert text to a URL/filename-safe slug."""
    # Lowercase and replace spaces/special chars with hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', text.lower())
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    # Truncate to max length, avoiding cutting mid-word
    if len(slug) > max_length:
        slug = slug[:max_length].rsplit('-', 1)[0]
    return slug


def generate_slug(
    prompt: str,
    existing_id: str | None = None,
    log_path: Path | None = None,
    git: Git | None = None,
    store: SqliteTaskStore | None = None,
    exclude_task_id: str | None = None,
    project_name: str | None = None,
    project_prefix: str | None = None,
    slug_override: str | None = None,
    branch_strategy: "BranchStrategy | None" = None,
    explicit_type: str | None = None,
) -> str:
    """Generate a task slug in YYYYMMDD-{project_prefix}-slug format, with suffix for retries."""
    if existing_id:
        # This is a retry - strip any existing suffix to get base
        base_id = re.sub(r'-\d+$', '', existing_id)
    else:
        # Fresh task - generate base ID
        date_prefix = datetime.now().strftime("%Y%m%d")
        if slug_override is not None:
            # slug_override already encodes full lineage context
            # (e.g. "0000mr-rev-myproj-add-feature")
            # so do not prepend project_prefix — it would double-embed the prefix for chained tasks
            base_id = f"{date_prefix}-{slug_override}"
        else:
            slug = slugify(prompt)
            if project_prefix:
                base_id = f"{date_prefix}-{project_prefix}-{slug}"
            else:
                base_id = f"{date_prefix}-{slug}"

    # Check if base ID is available
    if not _slug_exists(
        base_id,
        log_path=log_path,
        git=git,
        project_name=project_name,
        prompt=prompt,
        branch_strategy=branch_strategy,
        explicit_type=explicit_type,
        project_prefix=project_prefix,
        store=store,
        exclude_task_id=exclude_task_id,
    ):
        return base_id

    # Find next available suffix
    suffix = 2
    new_id = f"{base_id}-{suffix}"
    while _slug_exists(
        new_id,
        log_path=log_path,
        git=git,
        project_name=project_name,
        prompt=prompt,
        branch_strategy=branch_strategy,
        explicit_type=explicit_type,
        project_prefix=project_prefix,
        store=store,
        exclude_task_id=exclude_task_id,
    ):
        suffix += 1
        new_id = f"{base_id}-{suffix}"
    return new_id


def _compute_slug_override(task: "Task", store: "SqliteTaskStore") -> str | None:
    """Compute a semantic slug override for review/implement/improve tasks."""
    if task.task_type not in {"review", "implement", "improve"}:
        return None

    def _known_lineage_suffixes(candidate: Task) -> set[str]:
        suffixes: set[str] = set()
        current: Task | None = candidate
        visited: set[str] = set()
        while current is not None and current.id is not None and current.id not in visited:
            visited.add(current.id)
            suffix = extract_task_id_suffix(current.id)
            if suffix:
                suffixes.add(suffix)
            if current.based_on is None:
                break
            current = store.get(current.based_on)
        return suffixes

    def _slug_from_task(candidate: Task) -> str:
        base_slug = get_base_task_slug(candidate.slug) if candidate.slug else None
        if base_slug:
            normalized = strip_derived_implement_prefixes(
                base_slug,
                known_task_id_suffixes=_known_lineage_suffixes(candidate),
            )
            if normalized:
                return normalized
            return base_slug
        return slugify(candidate.prompt)

    if task.task_type == "review":
        if task.depends_on is None:
            return slugify(task.prompt)
        target = store.get(task.depends_on)
        if target is None:
            logger.warning(
                "Slug override review target missing for task #%s: depends_on=%s; "
                "falling back to review task prompt",
                task.id,
                task.depends_on,
            )
            return slugify(task.prompt)
        return _slug_from_task(target)

    anchor_id = task.based_on or task.depends_on
    if anchor_id is None:
        return slugify(task.prompt)

    root = store.get(anchor_id)
    if root is None:
        logger.warning(
            "Slug override ancestor missing for task #%s while walking based_on chain: "
            "missing_parent=%s; using task prompt",
            task.id,
            anchor_id,
        )
        return slugify(task.prompt)

    seen: set[str] = set()
    last_resolved = root
    while root.based_on:
        next_id = root.based_on
        if root.id is not None:
            seen.add(root.id)
        if next_id in seen:
            logger.warning(
                "Slug override cycle detected for task #%s while walking based_on chain: "
                "ancestor=%s; using last resolved ancestor #%s",
                task.id,
                next_id,
                last_resolved.id,
            )
            break
        parent = store.get(next_id)
        if parent is None:
            logger.warning(
                "Slug override ancestor missing for task #%s while walking based_on chain: "
                "missing_parent=%s; using last resolved ancestor #%s",
                task.id,
                next_id,
                last_resolved.id,
            )
            break
        last_resolved = parent
        root = parent

    return _slug_from_task(last_resolved)


def _slug_exists(
    task_id: str,
    log_path: Path | None,
    git: Git | None,
    project_name: str | None,
    prompt: str = "",
    branch_strategy: "BranchStrategy | None" = None,
    explicit_type: str | None = None,
    project_prefix: str | None = None,
    store: SqliteTaskStore | None = None,
    exclude_task_id: str | None = None,
) -> bool:
    """Check if a slug is already in use (task row, log file, or branch exists)."""
    if store is not None:
        existing = store.get_by_slug(task_id)
        if existing is not None and existing.id != exclude_task_id:
            return True
    # Check log file
    if log_path and (log_path / f"{task_id}.log").exists():
        return True
    # Check branch using the actual branch naming pattern from config
    if git and project_name:
        if branch_strategy is not None:
            branch_name = generate_branch_name(
                pattern=branch_strategy.pattern,
                project_name=project_name,
                task_slug=task_id,
                prompt=prompt,
                default_type=branch_strategy.default_type,
                explicit_type=explicit_type,
                # task.id is not yet assigned at slug-generation time; patterns
                # that depend on {task_id} won't collision-check cleanly.
                task_id="",
                project_prefix=project_prefix or "",
            )
        else:
            # Fallback for callers that don't supply a strategy (e.g., tests or legacy callers).
            branch_name = f"{project_name}/{task_id}"
        if git.branch_exists(branch_name):
            return True
    return False


def build_prompt(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    report_path: Path | None = None,
    summary_path: Path | None = None,
    git: Git | None = None,
    review_verify_result: str | None = None,
) -> str:
    """Build the prompt for Claude."""
    return PromptBuilder().build(
        task,
        config,
        store,
        report_path=report_path,
        summary_path=summary_path,
        git=git,
        review_verify_result=review_verify_result,
    )


def _get_task_output(task: Task, project_dir: Path) -> str | None:
    """Get task output content, preferring DB over filesystem.

    Auto-sync: If report_file exists and is newer than completed_at,
    read from disk instead of DB (allows users to edit plans).
    """
    # Check if file has been modified after task completion
    if task.report_file and task.completed_at:
        path = project_dir / task.report_file
        if path.exists():
            file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            # If file is newer than task completion, read from file
            if file_mtime > task.completed_at:
                return path.read_text()

    # Prefer DB content (works in distributed mode)
    if task.output_content:
        return task.output_content

    # Fall back to file (local mode, backward compat)
    if task.report_file:
        path = project_dir / task.report_file
        if path.exists():
            return path.read_text()

    # Final fallback for code-task summaries when report_file/output_content are absent.
    # This supports older tasks where summary content exists only on disk.
    if task.slug and task.task_type in {"task", "implement", "improve", "fix"}:
        summary_path = project_dir / SUMMARY_DIR / f"{task.slug}.md"
        if summary_path.exists():
            return summary_path.read_text()

    return None


def _compact_output_summary(content: str, max_chars: int = REVIEW_IMPROVE_SUMMARY_MAX_CHARS) -> str:
    """Reduce markdown output content to a compact, single-line summary."""
    lines = []
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line == "```" or line.startswith("```"):
            continue
        if line.startswith("#"):
            line = line.lstrip("#").strip()
            if not line:
                continue
        if line.startswith("- "):
            line = line[2:].strip()
        lines.append(line)
        if len(lines) >= 4:
            break

    compact = " ".join(lines).strip()
    compact = re.sub(r"\s+", " ", compact)
    if len(compact) > max_chars:
        return compact[: max_chars - 3].rstrip() + "..."
    return compact


def _truncate_to_word_boundary(text: str, max_chars: int) -> str:
    """Truncate text on word boundaries, adding ellipsis when shortened."""
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= max_chars:
        return compact

    cutoff = max_chars - 3
    if cutoff <= 0:
        return "." * max_chars

    candidate = compact[:cutoff].rstrip()
    split = candidate.rfind(" ")
    if split > 0:
        candidate = candidate[:split].rstrip()
    if not candidate:
        candidate = compact[:cutoff].rstrip()
    return f"{candidate}..."


def _decode_subprocess_output(output: str | bytes | None) -> str:
    """Normalize subprocess output payloads to text."""
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output


def _combine_review_verify_output(*parts: str | bytes | None) -> str:
    """Combine stdout/stderr fragments for review verify reporting."""
    return "\n".join(
        text.strip()
        for text in (_decode_subprocess_output(part) for part in parts)
        if text.strip()
    ).strip()


def _format_review_verify_failure(
    command: str,
    *,
    exit_status: str,
    failure: str,
    output: str | bytes | None = None,
) -> str:
    """Format timeout/launch verify failures as prompt context."""
    trimmed_output = _truncate_to_word_boundary(
        _combine_review_verify_output(output) or failure,
        REVIEW_VERIFY_OUTPUT_MAX_CHARS,
    )
    return "\n".join(
        [
            "## verify_command result",
            "",
            f"- Command: `{command}`",
            "- Status: failed",
            f"- Exit status: {exit_status}",
            f"- Failure: {failure}",
            "",
            "Failing output (trimmed):",
            "```text",
            trimmed_output,
            "```",
        ]
    )


def _format_review_verify_result(command: str, result: subprocess.CompletedProcess[str]) -> str:
    """Format a review-iteration verify result as prompt context."""
    status = "passed" if result.returncode == 0 else "failed"
    lines = [
        "## verify_command result",
        "",
        f"- Command: `{command}`",
        f"- Status: {status}",
        f"- Exit status: {result.returncode}",
    ]
    if result.returncode != 0:
        trimmed_output = _truncate_to_word_boundary(
            _combine_review_verify_output(result.stdout, result.stderr) or "(no failing output captured)",
            REVIEW_VERIFY_OUTPUT_MAX_CHARS,
        )
        lines.extend(
            [
                "",
                "Failing output (trimmed):",
                "```text",
                trimmed_output,
                "```",
            ]
        )
    return "\n".join(lines)


def _run_review_verify_command(
    verify_command: str,
    *,
    cwd: Path,
    timeout_seconds: int = REVIEW_VERIFY_TIMEOUT_SECONDS,
) -> str:
    """Run the configured verify command for an autonomous review iteration."""
    try:
        result = subprocess.run(
            ["bash", "-lc", verify_command],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return _format_review_verify_failure(
            verify_command,
            exit_status="timed out",
            failure=f"verify_command timed out after {timeout_seconds}s",
            output=_combine_review_verify_output(exc.stdout, exc.stderr),
        )
    except OSError as exc:
        return _format_review_verify_failure(
            verify_command,
            exit_status="launch failed",
            failure=f"failed to launch verify_command: {exc}",
        )
    return _format_review_verify_result(verify_command, result)


def _default_code_task_commit_subject(task_slug: str | None, task_db_id: str | None) -> str:
    """Build deterministic fallback commit subject for code tasks."""
    if task_slug and task_slug.strip():
        return f"gza task {task_slug.strip()}"
    if task_db_id is not None:
        return f"Task {task_db_id}"
    return "gza task"


def _build_code_task_commit_subject(task_prompt: str, worktree_summary_path: Path, fallback_subject: str | None = None) -> str:
    """Build commit subject from worktree summary, with prompt fallback."""
    fallback = (fallback_subject or "").strip() or "gza task"
    if worktree_summary_path.exists():
        try:
            summary_content = worktree_summary_path.read_text().strip()
        except (OSError, UnicodeError):
            logger.warning(
                "Failed to read summary file for commit subject at %s; falling back",
                worktree_summary_path,
                exc_info=True,
            )
        else:
            if summary_content:
                compact_summary = _compact_output_summary(summary_content)
                summary_subject = _truncate_to_word_boundary(compact_summary, max_chars=COMMIT_SUBJECT_MAX_CHARS)
                if summary_subject:
                    return summary_subject

    prompt_subject = _truncate_to_word_boundary(task_prompt, max_chars=COMMIT_SUBJECT_MAX_CHARS)
    if prompt_subject:
        return prompt_subject
    return fallback


def _is_improve_in_impl_chain(improve_task: Task, impl_task: Task, tasks_by_id: dict[str, Task]) -> bool:
    """Return True when an improve task belongs to an implementation's improve chain."""
    if impl_task.id is None or improve_task.based_on is None:
        return False
    current_based_on = improve_task.based_on
    seen: set[str] = set()
    while True:
        if current_based_on == impl_task.id:
            return True
        if current_based_on in seen:
            return False
        seen.add(current_based_on)
        parent = tasks_by_id.get(current_based_on)
        if parent is None or parent.task_type != "improve" or parent.based_on is None:
            return False
        current_based_on = parent.based_on


def _get_completed_improves_for_implementation_chain(store: SqliteTaskStore, impl_task: Task) -> list[Task]:
    """Collect completed improve tasks tied to an implementation, including retry/resume descendants."""
    all_tasks = store.get_all()
    tasks_by_id = {task.id: task for task in all_tasks if task.id is not None}
    return [
        task for task in all_tasks
        if task.task_type == "improve"
        and task.id is not None
        and task.status == "completed"
        and _is_improve_in_impl_chain(task, impl_task, tasks_by_id)
    ]


def _build_review_improve_lineage_context(review_task: Task, impl_task: Task, store: SqliteTaskStore, project_dir: Path) -> str:
    """Build compact improve lineage context for review prompts."""
    improves = _get_completed_improves_for_implementation_chain(store, impl_task)
    if not improves:
        return ""

    review_created_at = review_task.created_at
    prior_improves = []
    for improve in improves:
        if review_created_at is None:
            prior_improves.append(improve)
            continue
        if improve.created_at is None:
            if review_task.id is not None and improve.id is not None and task_id_numeric_key(improve.id) < task_id_numeric_key(review_task.id):
                prior_improves.append(improve)
            continue

        if (improve.created_at, task_id_numeric_key(improve.id)) < (review_created_at, task_id_numeric_key(review_task.id)):
            prior_improves.append(improve)

    if not prior_improves:
        return ""

    # Most recent first by completion/creation, then id.
    prior_improves.sort(
        key=lambda t: (
            t.completed_at or t.created_at or datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(t.id),
        ),
        reverse=True,
    )

    included = prior_improves[:REVIEW_IMPROVE_LINEAGE_LIMIT]
    omitted_count = max(0, len(prior_improves) - len(included))
    n_iterations = len(prior_improves)
    latest_improve = prior_improves[0]
    latest_review_task = store.get(latest_improve.depends_on) if latest_improve.depends_on else None
    latest_review_report = (
        parse_review_report(_get_task_output(latest_review_task, project_dir))
        if latest_review_task is not None
        else None
    )
    state_parts = [
        f"prior iterations: {n_iterations}",
        f"latest review: {latest_improve.depends_on or 'unknown'}",
        f"verdict={latest_review_report.verdict if latest_review_report is not None and latest_review_report.verdict else 'unknown'}",
        f"score={latest_review_task.review_score if latest_review_task is not None and latest_review_task.review_score is not None else 'unknown'}",
        f"latest improve: {latest_improve.id or 'unknown'}",
        f"status={latest_improve.status or 'unknown'}",
    ]
    if omitted_count:
        state_parts.append(f"older iterations omitted: {omitted_count}")

    lines = [
        "## Improve Lineage Context",
        "",
        "Prior iteration history is coordination context only; it is not evidence that any blocker is still open.",
        "Current state: " + ", ".join(state_parts) + ".",
        "",
    ]

    for index, improve in enumerate(included, start=1):
        iteration_number = n_iterations - (index - 1)
        review_task_for_iteration = store.get(improve.depends_on) if improve.depends_on else None
        review_report = (
            parse_review_report(_get_task_output(review_task_for_iteration, project_dir))
            if review_task_for_iteration is not None
            else None
        )
        verdict = review_report.verdict if review_report is not None else None
        score = (
            review_task_for_iteration.review_score
            if review_task_for_iteration is not None and review_task_for_iteration.review_score is not None
            else "unknown"
        )
        completed = improve.completed_at.isoformat() if improve.completed_at is not None else "unknown"
        lines.append(
            f"- iteration {iteration_number}: review {improve.depends_on or '?'} "
            f"verdict={verdict or 'unknown'} score={score} -> "
            f"improve {improve.id or '?'} status={improve.status or 'unknown'} completed={completed}"
        )

    return "\n".join(lines)


def _parse_changed_files_from_numstat(numstat_output: str) -> list[str]:
    """Extract changed file paths from git diff --numstat output."""
    changed_files: list[str] = []
    for line in numstat_output.splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        changed_files.append(parts[2].strip())
    return changed_files


def _build_review_diff_context(
    git: Git,
    revision_range: str,
    branch_name: str,
    *,
    diff_small_threshold: int = DIFF_SMALL_THRESHOLD,
    diff_medium_threshold: int = DIFF_MEDIUM_THRESHOLD,
    review_context_file_limit: int = REVIEW_CONTEXT_FILE_LIMIT,
) -> str:
    """Build self-contained review diff context for prompts."""
    numstat_output = git.get_diff_numstat(revision_range)
    if not isinstance(numstat_output, str):
        numstat_output = ""
    files_changed, lines_added, lines_removed = parse_diff_numstat(numstat_output)
    total_lines = lines_added + lines_removed
    changed_files = _parse_changed_files_from_numstat(numstat_output)

    parts = [
        "## Implementation Diff Context",
        "",
        f"Implementation branch: {branch_name}",
        f"Revision range: {revision_range}",
        f"Files changed: {files_changed}, lines added: {lines_added}, lines removed: {lines_removed}",
    ]

    if changed_files:
        parts.append("")
        parts.append("Changed files:")
        for file_path in changed_files:
            parts.append(f"- {file_path}")

    stat_summary = git.get_diff_stat(revision_range)
    if not isinstance(stat_summary, str):
        stat_summary = ""
    if stat_summary:
        parts.append("")
        parts.append("Diff summary:")
        parts.append(stat_summary)

    if total_lines < diff_small_threshold:
        diff_content = git.get_diff(revision_range)
        if not isinstance(diff_content, str):
            diff_content = ""
        if diff_content:
            parts.append("")
            parts.append("Full diff:")
            parts.append(diff_content)
        return "\n".join(parts)

    if total_lines < diff_medium_threshold:
        diff_content = git.get_diff(revision_range)
        if not isinstance(diff_content, str):
            diff_content = ""
        if diff_content:
            parts.append("")
            parts.append("Full diff:")
            parts.append(diff_content)
        return "\n".join(parts)

    # Large diff: include targeted per-file diff excerpts for the most relevant files.
    selected_files = changed_files[:review_context_file_limit]
    if selected_files:
        excerpt_result = git._run(
            "diff",
            "--unified=8",
            revision_range,
            "--",
            *selected_files,
            check=False,
        )
        excerpt_stdout = excerpt_result.stdout if isinstance(excerpt_result.stdout, str) else ""
        excerpt_content = excerpt_stdout.strip()
        if excerpt_content:
            parts.append("")
            parts.append(
                f"Targeted diff excerpts (first {len(selected_files)} changed files; total changed lines: {total_lines}):"
            )
            parts.append(excerpt_content)
        if len(changed_files) > len(selected_files):
            parts.append("")
            parts.append(
                f"Additional changed files not expanded inline: {len(changed_files) - len(selected_files)}"
            )

    return "\n".join(parts)


def _build_context_from_chain(
    task: Task,
    store: SqliteTaskStore,
    project_dir: Path,
    git: Git | None,
    config: Config | None = None,
    review_verify_result: str | None = None,
) -> str:
    """Build context by walking the depends_on and based_on chain."""
    context_parts = []

    def _int_or_default(value: object, default: int) -> int:
        return value if isinstance(value, int) else default

    # For improve tasks, include review feedback and original plan
    if task.task_type == "improve":
        impl_ancestor = _resolve_impl_ancestor(store, task)
        if impl_ancestor is not None and impl_ancestor.id is not None:
            unresolved_comments = store.get_comments(
                impl_ancestor.id,
                unresolved_only=True,
                created_on_or_before=task.created_at,
            )
            if unresolved_comments:
                context_parts.append("## Comments:\n")
                for comment in unresolved_comments:
                    source_author = f"source={comment.source}"
                    if comment.author:
                        source_author += f", author={comment.author}"
                    context_parts.append(
                        f"- #{comment.id} ({comment.created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC, {source_author})"
                    )
                    context_parts.append(comment.content)

        # Get the review we're addressing
        if task.depends_on:
            review_task = store.get(task.depends_on)
            if review_task and review_task.task_type == "review":
                review_content = _get_task_output(review_task, project_dir)
                if review_content:
                    context_parts.append("## Review feedback to address:\n")
                    context_parts.append(review_content)
                else:
                    context_parts.append(
                        "## Review feedback to address:\n"
                        f"(review task {review_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )

        if impl_ancestor is not None:
            plan_task = get_plan_for_task(store, impl_ancestor)
            if plan_task:
                plan_content = _get_task_output(plan_task, project_dir)
                if plan_content:
                    context_parts.append("\n## Original plan:\n")
                    context_parts.append(plan_content)
                else:
                    context_parts.append(
                        "\n## Original plan:\n"
                        f"(plan task {plan_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )

    if task.task_type == "fix":
        root_impl = _resolve_root_implementation_for_fix(task, store)
        if root_impl is not None and root_impl.id is not None:
            context_parts.append("## Fix Rescue Context\n")
            context_parts.append(f"Root implementation: {root_impl.id}")

            reviews = [
                candidate
                for candidate in store.get_reviews_for_task(root_impl.id)
                if candidate.status == "completed"
            ]
            latest_review = reviews[0] if reviews else None
            if latest_review is not None and latest_review.id is not None:
                context_parts.append(f"Latest completed review: {latest_review.id}")
                latest_review_content = _get_task_output(latest_review, project_dir)
                if latest_review_content:
                    context_parts.append("\n## Review feedback to address:\n")
                    context_parts.append(latest_review_content)

            repeated = _extract_repeated_required_fixes(reviews[:2], project_dir)
            if repeated:
                context_parts.append("\n## Repeated Blockers\n")
                context_parts.extend(f"- {item}" for item in repeated)

            failed_improves = [
                candidate
                for candidate in store.get_improve_tasks_by_root(root_impl.id)
                if candidate.status == "failed"
            ]
            if failed_improves:
                latest_failed_improve = max(
                    failed_improves,
                    key=lambda candidate: candidate.completed_at or candidate.created_at or datetime.min.replace(tzinfo=UTC),
                )
                if latest_failed_improve.id is not None:
                    context_parts.append(f"\nLatest failed improve/resume attempt: {latest_failed_improve.id}")
                    context_parts.append(_extract_failure_context(latest_failed_improve, project_dir))

            failed_impl_retries = [
                candidate
                for candidate in store.get_based_on_children(root_impl.id)
                if candidate.task_type == "implement" and candidate.status == "failed"
            ]
            if failed_impl_retries:
                latest_failed_impl = max(
                    failed_impl_retries,
                    key=lambda candidate: candidate.completed_at or candidate.created_at or datetime.min.replace(tzinfo=UTC),
                )
                if latest_failed_impl.id is not None:
                    context_parts.append(
                        f"Latest failed implementation retry/resume attempt: {latest_failed_impl.id}"
                    )

            plan_task = get_plan_for_task(store, root_impl)

            if plan_task:
                plan_content = _get_task_output(plan_task, project_dir)
                if plan_content:
                    context_parts.append("\n## Original plan:\n")
                    context_parts.append(plan_content)
                else:
                    context_parts.append(
                        "\n## Original plan:\n"
                        f"(plan task {plan_task.id} exists but content unavailable on this machine - flag as blocker)"
                    )
            elif root_impl.prompt:
                context_parts.append("\n## Original request:\n")
                context_parts.append(root_impl.prompt)

    # For implement tasks, include plan from lineage chain.
    if task.task_type == "implement":
        followup_parts = extract_followup_prompt_parts(task.prompt)
        if followup_parts is not None:
            marker = "## Follow-up finding to implement:"
            if marker in task.prompt:
                context_parts.append(task.prompt[task.prompt.index(marker):].strip())
        plan_task = get_plan_for_task(store, task)
        if plan_task:
            plan_content = _get_task_output(plan_task, project_dir)
            if plan_content:
                context_parts.append("## Plan to implement:\n")
                context_parts.append(plan_content)

    # For review tasks, include both plan and diff
    if task.task_type == "review":
        # Find the implement task via depends_on
        if task.depends_on:
            impl_task = store.get(task.depends_on)
            if impl_task:
                # Include spec file content if the implementation task has a spec field
                if impl_task.spec:
                    spec_path = project_dir / impl_task.spec
                    if spec_path.exists():
                        spec_content = spec_path.read_text()
                        context_parts.append(f"## Specification\n\nThe following specification file ({impl_task.spec}) provides context for this implementation:\n\n{spec_content}")

                # Inject ask context: plan output for plan-driven work, else full original request.
                plan_task = get_plan_for_task(store, impl_task)

                if plan_task:
                    plan_content = _get_task_output(plan_task, project_dir)
                    if plan_content:
                        context_parts.append("\n## Original plan:\n")
                        context_parts.append(plan_content)
                    else:
                        context_parts.append(
                            "\n## Original plan:\n"
                            f"(plan task {plan_task.id} exists but content unavailable on this machine - flag as blocker)"
                        )
                elif impl_task.prompt:
                    context_parts.append("\n## Original request:\n")
                    context_parts.append(impl_task.prompt)

                if review_verify_result:
                    context_parts.append("\n")
                    context_parts.append(review_verify_result)

                # Get diff if we have a branch (tiered strategy based on diff size)
                if impl_task.branch and git:
                    try:
                        default_branch = git.default_branch()
                        revision_range = f"{default_branch}...{impl_task.branch}"
                        context_parts.append(
                            _build_review_diff_context(
                                git,
                                revision_range,
                                impl_task.branch,
                                diff_small_threshold=_int_or_default(
                                    getattr(config, "review_diff_small_threshold", None),
                                    DIFF_SMALL_THRESHOLD,
                                ),
                                diff_medium_threshold=_int_or_default(
                                    getattr(config, "review_diff_medium_threshold", None),
                                    DIFF_MEDIUM_THRESHOLD,
                                ),
                                review_context_file_limit=_int_or_default(
                                    getattr(config, "review_context_file_limit", None),
                                    REVIEW_CONTEXT_FILE_LIMIT,
                                ),
                            )
                        )
                    except GitError:
                        pass  # Ignore git errors

                improve_lineage_context = _build_review_improve_lineage_context(task, impl_task, store, project_dir)
                if improve_lineage_context:
                    context_parts.append(improve_lineage_context)

    # Fallback for generic based_on references
    if task.based_on and not context_parts:
        parent_task = store.get(task.based_on)
        if parent_task and parent_task.report_file:
            context_parts.append(f"This task is based on the findings in: {parent_task.report_file}")
            context_parts.append("Read and review that report for context before implementing.")
        elif parent_task:
            context_parts.append(f"This task is a follow-up to task {parent_task.id}: {parent_task.prompt[:100]}")

    return "\n".join(context_parts) if context_parts else ""


def _resolve_root_implementation_for_fix(task: Task, store: SqliteTaskStore) -> Task | None:
    """Resolve the implementation root for fix and resumed/retried fix chains."""
    visited: set[str] = set()
    current: Task | None = task
    while current is not None:
        if current.id is not None:
            if current.id in visited:
                return None
            visited.add(current.id)
        if current.task_type == "implement":
            return current
        if current.based_on is None:
            return None
        current = store.get(current.based_on)
    return None


def _resolve_impl_ancestor(store: SqliteTaskStore, task: Task) -> Task | None:
    """Resolve the implementation ancestor by walking based_on lineage."""
    if task.task_type == "implement":
        return task
    visited: set[str] = set()
    current: Task | None = task
    while current is not None:
        if current.id is not None:
            if current.id in visited:
                return None
            visited.add(current.id)
        if current.task_type == "implement":
            return current
        if current.based_on is None:
            return None
        current = store.get(current.based_on)
    return None


def _is_recovered_rebase_lineage(task: Task, *, resume: bool) -> bool:
    """Return whether rebase diff classification must fail closed for this run."""
    if task.task_type != "rebase":
        return False
    if resume:
        return True
    return task.recovery_origin in {"resume", "retry"}


def _normalize_repeated_blocker_text(text: str) -> str:
    """Normalize blocker text for repeated-fix matching."""
    return " ".join(text.split()).strip().lower()


def _extract_blocker_signal_lines(blocker_body: str) -> list[str]:
    """Extract potential blocker-fix signal lines from canonical blocker body text."""
    signals: list[str] = []
    for raw_line in blocker_body.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered.startswith("required tests:") or lowered.startswith("recommended tests:"):
            continue
        if lowered.startswith("evidence:") or lowered.startswith("impact:"):
            continue
        if ":" in stripped:
            _, value = stripped.split(":", 1)
            if value.strip():
                signals.append(value.strip())
                continue
        signals.append(stripped)
    return signals


def _extract_required_fix_candidates(content: str) -> dict[str, str]:
    """Extract blocker/fix candidates from parsed review markdown plus legacy fallbacks."""
    candidates: dict[str, str] = {}

    parsed = parse_review_report(content)
    for finding in parsed.findings:
        if finding.severity != "BLOCKER":
            continue

        signals: list[str] = []
        if finding.fix_or_followup:
            signals.append(finding.fix_or_followup)
        signals.extend(_extract_blocker_signal_lines(finding.body))
        signals.append(finding.title)

        for signal in signals:
            normalized = _normalize_repeated_blocker_text(signal)
            if normalized:
                candidates.setdefault(normalized, signal.strip())

    for match in re.finditer(r"(?im)^Required fix:\s*(.+)$", content):
        signal = match.group(1).strip()
        normalized = _normalize_repeated_blocker_text(signal)
        if normalized:
            candidates.setdefault(normalized, signal)

    return candidates


def _extract_repeated_required_fixes(reviews: list[Task], project_dir: Path) -> list[str]:
    """Extract repeated blockers from the most recent completed reviews."""
    if len(reviews) < 2:
        return []

    required_by_review: list[dict[str, str]] = []
    for review in reviews:
        content = _get_task_output(review, project_dir) or ""
        required_by_review.append(_extract_required_fix_candidates(content))

    repeated_keys = set(required_by_review[0]).intersection(required_by_review[1])
    repeated = [required_by_review[0][key] for key in repeated_keys]
    return sorted(repeated, key=str.lower)


def _extract_failure_context(task: Task, project_dir: Path) -> str:
    """Return a compact failed-attempt context block for fix rescue prompts."""
    lines: list[str] = []
    if task.failure_reason:
        lines.append(f"failure_reason={task.failure_reason}")
    if task.log_file:
        log_path = project_dir / Path(task.log_file)
        if log_path.exists():
            try:
                tail = log_path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
            except OSError:
                tail = []
            if tail:
                lines.extend(tail[-20:])
    if not lines:
        return "(no failed-attempt context available)"
    return "\n".join(lines)


def _run_result_to_stats(result: RunResult) -> TaskStats:
    """Convert a provider RunResult to TaskStats for storage."""
    return TaskStats(
        duration_seconds=result.duration_seconds,
        num_steps_reported=result.num_steps_reported,
        num_steps_computed=result.num_steps_computed,
        num_turns_reported=result.num_turns_reported,
        num_turns_computed=result.num_turns_computed,
        cost_usd=result.cost_usd,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        tokens_estimated=result.tokens_estimated,
        cost_estimated=result.cost_estimated,
    )


def _save_wip_changes(
    task: Task,
    worktree_git: Git,
    config: Config,
    branch_name: str,
) -> None:
    """Save WIP changes when task fails or is interrupted.

    This does two things:
    1. Commits any uncommitted changes with --no-verify
    2. Backs up the diff to .gza/wip/<task-id>.diff

    Args:
        task: The task that failed/was interrupted
        worktree_git: Git instance for the worktree
        config: Configuration object
        branch_name: Name of the branch with the WIP changes
    """
    # Check if there are any changes to save
    if not worktree_git.has_changes("."):
        return

    # Create WIP directory
    wip_dir = config.project_dir / WIP_DIR
    wip_dir.mkdir(parents=True, exist_ok=True)

    # Stage tracked modifications/deletions only (avoid staging unrelated files)
    worktree_git._run("add", "--update", ".", check=False)
    # Also stage any new untracked files (agent-created files)
    untracked = worktree_git._run("ls-files", "--others", "--exclude-standard", check=False).stdout
    for f in untracked.splitlines():
        if f.strip():
            worktree_git.add(f.strip())
    diff = worktree_git._run("diff", "--cached", check=False).stdout

    # Save diff to backup file
    if task.slug and diff:
        wip_file = wip_dir / f"{task.slug}.diff"
        wip_file.write_text(diff)
        console.print(f"[yellow]Saved WIP diff to: {wip_file.relative_to(config.project_dir)}[/yellow]")

    # Commit changes with --no-verify
    try:
        worktree_git._run("commit", "--no-verify", "-m", f"WIP: gza task interrupted\n\nTask ID: {task.slug}")
        console.print(f"[yellow]Saved WIP commit on branch: {branch_name}[/yellow]")
    except GitError as e:
        # If commit fails, that's okay - we have the diff backup
        console.print(f"[yellow]Warning: Could not create WIP commit: {e}[/yellow]")


def _restore_wip_changes(
    task: Task,
    worktree_git: Git,
    config: Config,
    branch_name: str,
    original_task_id: str | None = None,
) -> None:
    """Restore WIP changes when resuming a task.

    Checks if the branch has a WIP commit. If not, tries to apply the
    stored diff from .gza/wip/<task-id>.diff.

    Args:
        task: The task being resumed
        worktree_git: Git instance for the worktree
        config: Configuration object
        branch_name: Name of the branch to restore WIP changes to
        original_task_id: Optional task_id of the original failed task (for
            finding the WIP diff file when resuming via a new task).
    """
    if not task.slug and not original_task_id:
        return

    # Check if the last commit is a WIP commit
    try:
        last_commit_msg = worktree_git._run("log", "-1", "--pretty=%B", check=False).stdout.strip()
        if last_commit_msg.startswith("WIP: gza task interrupted"):
            console.print("[green]Found WIP commit on branch - resuming from there[/green]")
            return
    except GitError:
        pass

    # No WIP commit found - try to apply stored diff.
    # When resuming via a new task, the WIP diff was saved with the original
    # task's id, so check that first, then fall back to the new task's id.
    wip_dir = config.project_dir / WIP_DIR
    wip_file = None
    for candidate_id in filter(None, [original_task_id, task.slug]):
        candidate = wip_dir / f"{candidate_id}.diff"
        if candidate.exists():
            wip_file = candidate
            break

    if wip_file and wip_file.exists():
        diff_content = wip_file.read_text()
        if diff_content.strip():
            console.print(f"[yellow]WIP commit not found - applying stored diff from {wip_file.relative_to(config.project_dir)}[/yellow]")
            try:
                # Apply the diff
                result = worktree_git._run("apply", "--cached", stdin=diff_content.encode(), check=False)
                if result.returncode == 0:
                    # Commit the restored changes
                    worktree_git._run("commit", "--no-verify", "-m", f"WIP: restored from diff\n\nTask ID: {task.slug}")
                    console.print("[green]Successfully restored WIP changes from diff[/green]")
                else:
                    console.print(f"[yellow]Warning: Could not apply WIP diff: {result.stderr}[/yellow]")
            except GitError as e:
                console.print(f"[yellow]Warning: Could not apply WIP diff: {e}[/yellow]")


def _squash_wip_commits(
    worktree_git: Git,
    task: Task,
) -> None:
    """Squash WIP commits into the final commit.

    If there are WIP commits on the branch, this will squash them
    into the final task commit before marking the task complete.

    Args:
        worktree_git: Git instance for the worktree
        task: The task being completed
    """
    # Check if there are any WIP commits to squash
    try:
        # Look for WIP commits in the recent history
        log_output = worktree_git._run("log", "-10", "--pretty=%s", check=False).stdout.strip()
        if not log_output:
            return

        commit_messages = log_output.split("\n")
        wip_count = sum(1 for msg in commit_messages if msg.startswith("WIP:"))

        if wip_count == 0:
            return

        console.print(f"[yellow]Found {wip_count} WIP commit(s) - squashing into final commit[/yellow]")

        # Use git reset --soft to squash commits
        # Reset back to before the WIP commits, keeping all changes staged
        worktree_git._run("reset", "--soft", f"HEAD~{wip_count}")

        console.print("[green]WIP commits squashed successfully[/green]")

    except GitError as e:
        # If squashing fails, log but continue - the WIP commits will remain
        console.print(f"[yellow]Warning: Could not squash WIP commits: {e}[/yellow]")


def post_review_to_pr(
    review_task: Task,
    impl_task: Task,
    store: SqliteTaskStore,
    project_dir: Path,
    required: bool = False,
) -> None:
    """Post a review task's output to its associated PR.

    Args:
        review_task: The review task
        impl_task: The implementation task being reviewed
        store: Task store
        project_dir: Project directory
        required: If True, error if PR not found; if False, skip silently
    """
    gh = GitHub()

    # Check gh is available
    if not gh.is_available():
        if required:
            print("Error: GitHub CLI not available, cannot post review")
            return
        else:
            print("Info: GitHub CLI not available, skipping PR comment")
            return

    # Find an open PR, preferring cached metadata but falling back to branch lookup.
    pr_number = None
    if impl_task.branch:
        try:
            resolved_pr = resolve_branch_pr(
                gh,
                impl_task.branch,
                cached_pr_numbers=((impl_task.pr_number,) if impl_task.pr_number is not None else ()),
                allow_discovery=True,
            )
        except GitHubError as exc:
            if required:
                print(f"Error: Failed to look up PR for task {impl_task.id}: {exc}")
            else:
                print(f"Info: Failed to look up PR for task {impl_task.id}, skipping PR comment: {exc}")
            return
        if resolved_pr.details is not None and resolved_pr.details.state == "open":
            pr_number = resolved_pr.details.number
            impl_task.pr_number = resolved_pr.details.number
            impl_task.pr_state = resolved_pr.details.state
            impl_task.pr_last_synced_at = datetime.now(UTC)
            store.update(impl_task)
            if resolved_pr.source == "cached":
                print(f"Found PR #{pr_number} (cached)")
            else:
                print(f"Found PR #{pr_number} for branch {impl_task.branch}")

    if not pr_number:
        if required:
            print(f"Error: No PR found for task {impl_task.id}")
            if impl_task.branch:
                print(f"Branch '{impl_task.branch}' has no associated PR")
            else:
                print("Task has no branch")
            return
        else:
            print(f"Info: No PR found for task {impl_task.id}, skipping PR comment")
            return

    # Get review content
    review_content = _get_task_output(review_task, project_dir)
    if not review_content:
        print(f"Warning: Review task {review_task.id} has no output content")
        return

    # Format as PR comment
    comment_body = f"""## 🤖 Automated Code Review

**Review Task**: {review_task.id}
**Implementation Task**: {impl_task.id}

---

{review_content}

---

*Generated by `gza review` task*
"""

    # Post to PR
    try:
        gh.add_pr_comment(pr_number, comment_body)
        print(f"✓ Posted review to PR #{pr_number}")
    except GitHubError as e:
        print(f"Warning: Failed to post review to PR: {e}")


def _create_and_run_review_task(
    completed_task: Task,
    config: Config,
    store: SqliteTaskStore,
) -> int:
    """Create and immediately execute a review task for a completed implementation.

    Returns:
        Exit code from running the review task.
    """
    review_target = completed_task
    if completed_task.task_type == "improve":
        resolved_impl = _resolve_impl_ancestor(store, completed_task)
        if resolved_impl is None:
            console.print(
                f"\n[yellow]Could not resolve the implementation ancestor for improve task {completed_task.id}; "
                "skipping auto-review.[/yellow]"
            )
            return 0
        review_target = resolved_impl
    elif completed_task.task_type == "fix":
        resolved_impl = _resolve_root_implementation_for_fix(completed_task, store)
        if resolved_impl is None:
            console.print(
                f"\n[yellow]Could not resolve the implementation ancestor for fix task {completed_task.id}; "
                "skipping auto-review.[/yellow]"
            )
            return 0
        review_target = resolved_impl

    try:
        review_task = create_review_task(
            store, review_target, prompt_mode="auto",
            project_prefix=config.project_prefix or None,
        )
    except DuplicateReviewError as e:
        review_task = e.active_review
        if review_task.status == "in_progress":
            console.print(
                f"\n[yellow]Review task {review_task.id} is already in progress; skipping.[/yellow]"
            )
            return 0
        console.print(
            f"\n[yellow]Review task {review_task.id} is already {review_task.status}; running it.[/yellow]"
        )

    console.print(f"\n[bold cyan]=== Auto-created review task {review_task.id} ===[/bold cyan]")
    console.print("Running review task...")

    # Run the review task immediately
    # Note: PR posting happens in _run_non_code_task, no need to do it here
    return run(config, task_id=review_task.id)


def _sync_completed_code_task_branch_for_live_pr(
    task: Task,
    store: SqliteTaskStore,
    git: Git,
) -> bool:
    """Best-effort sync for same-branch code tasks that already have an open PR.

    Returns False only when follow-up review should be held until the branch is
    published. Lookup or `gh` availability gaps preserve the historical
    auto-review flow because no PR-facing action can be taken anyway.
    """
    task_label = f"{task.task_type.capitalize()} task {task.id}"
    result = sync_task_branch_if_live_pr(task, store, git)
    if result.ok or result.status == "gh_unavailable":
        return True

    if result.status == "lookup_failed":
        print(
            f"Warning: {task_label} completed, but gza could not look up a live PR for "
            f"branch '{task.branch}': {result.error}. Continuing with auto-review without PR sync."
        )
        return True
    if result.status == "push_failed":
        pr_ref = f"PR #{result.pr_number}" if result.pr_number is not None else "the live PR"
        print(
            f"Warning: {task_label} completed, but branch '{task.branch}' could not be "
            f"pushed to {pr_ref}: {result.error}"
        )
    else:
        print(
            f"Warning: {task_label} completed, but branch '{task.branch}' could not be "
            "synchronized for follow-up PR actions."
        )
    return False


def _ensure_work_pr_for_completed_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
) -> bool:
    """Ensure a PR exists for a completed code task branch when `gza work --pr` is set.

    Returns:
        True when PR requirements are satisfied or PR creation is not applicable.
        False when explicit PR creation was requested but could not be fulfilled.
    """
    if not task.branch:
        return True

    default_branch = git.default_branch()
    if git.count_commits_ahead(task.branch, default_branch) <= 0:
        print(f"Info: Task {task.id} has no commits on branch '{task.branch}', skipping PR creation")
        return True

    result = ensure_task_pr(
        task,
        store,
        git,
        content_builder=lambda: build_task_pr_content(task, git, config, store),
        draft=False,
        merged_behavior="skip",
    )
    if result.ok and result.status == "cached" and result.pr_number:
        print(f"Info: Reusing cached PR #{result.pr_number} for task {task.id}: {result.pr_url}")
        return True
    if result.ok and result.status == "existing":
        print(f"Info: Reusing existing PR for branch {task.branch}: {result.pr_url}")
        return True
    if result.ok and result.status == "merged":
        print(f"Info: Branch '{task.branch}' is already merged into {default_branch}, skipping PR creation")
        return True
    if result.ok and result.status == "created":
        print(f"✓ Created PR: {result.pr_url}")
        return True
    if result.status == "gh_unavailable":
        print("Error: GitHub CLI (gh) not available, cannot create PR")
    elif result.status == "push_failed":
        print(f"Error: Failed to push branch '{task.branch}' before PR creation: {result.error}")
    elif result.status == "lookup_failed":
        print(f"Error: Failed to look up PR for task {task.id}: {result.error}")
    elif result.status == "create_failed":
        print(f"Error: Failed to create PR for task {task.id}: {result.error}")
    else:
        print(f"Error: Failed to ensure PR for task {task.id}")
    return False


def _copy_learnings_to_worktree(config: Config, worktree_path: Path) -> None:
    """Copy .gza/learnings.md into the worktree so the agent can read it.

    The learnings file lives in config.project_dir/.gza/ which is gitignored
    and not present in worktrees. The agent prompt references it as a relative
    path, so it must exist in the worktree for the agent to find it.
    """
    import shutil

    src = config.project_dir / ".gza" / "learnings.md"
    if not src.exists():
        return
    dst_dir = worktree_path / ".gza"
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / "learnings.md")


def _count_patch_hunks(patch_text: str) -> int:
    """Count unified-diff hunks in patch text."""
    return sum(1 for line in patch_text.splitlines() if line.startswith("@@"))


def _write_runtime_patch_file(bundle_dir: Path, filename: str, patch_text: str) -> Path:
    """Persist a runtime-generated patch alongside the copied extraction bundle."""
    patch_path = bundle_dir / filename
    patch_path.write_text(patch_text)
    return patch_path


_UNMERGED_PORCELAIN_STATUSES = frozenset({"DD", "AU", "UD", "UA", "DU", "AA", "UU"})


def _git_apply_failure_message(patch_path: Path, result: GitApplyResult) -> str:
    """Format a consistent error message for failed patch applications."""
    error_output = result.error_output
    return f"git apply --3way {patch_path} failed:\n{error_output}"


def _apply_left_relevant_conflicts(
    worktree_git: Git,
    touched_paths: set[str],
) -> bool:
    """Return True when `git apply --3way` left unmerged entries on seeded paths."""
    for status, path in worktree_git.status_porcelain():
        if path not in touched_paths:
            continue
        if status in _UNMERGED_PORCELAIN_STATUSES:
            return True
    return False


def _already_merged_extraction_seed_result(
    task: Task,
    log_file: Path,
    *,
    message: str,
) -> ExtractionSeedResult:
    """Log and return the canonical extraction already-merged completion outcome."""
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": message,
            "completion_reason": EXTRACTION_ALREADY_MERGED_COMPLETION_REASON,
        },
    )
    return ExtractionSeedResult(
        completion_reason=EXTRACTION_ALREADY_MERGED_COMPLETION_REASON,
    )


def _seed_extraction_bundle_if_present(
    task: Task,
    config: Config,
    worktree_path: Path,
    worktree_git: Git,
    log_file: Path,
    *,
    resume: bool,
) -> ExtractionSeedResult:
    """Copy/apply extraction bundle before provider execution when configured for the task."""
    if resume or not task.slug:
        return ExtractionSeedResult()

    project_bundle_dir = extraction_bundle_path(config.project_dir, task.slug)
    if not project_bundle_dir.exists():
        return ExtractionSeedResult()

    worktree_bundle_dir = copy_bundle_to_worktree(project_bundle_dir, worktree_path)
    manifest = load_manifest(worktree_bundle_dir / MANIFEST_FILENAME)
    manifest_target_slug = manifest.get("target_slug")
    manifest_target_task_id = manifest.get("target_task_id")
    if not isinstance(manifest_target_slug, str) or not manifest_target_slug:
        raise ExtractionError("Extraction manifest missing required target identity field: target_slug")
    if not isinstance(manifest_target_task_id, str) or not manifest_target_task_id:
        raise ExtractionError("Extraction manifest missing required target identity field: target_task_id")
    if manifest_target_slug != task.slug or manifest_target_task_id != task.id:
        raise ExtractionError(
            "Extraction bundle target identity mismatch "
            f"(manifest task={manifest_target_task_id} slug={manifest_target_slug}, "
            f"current task={task.id} slug={task.slug})"
        )

    patch_path = resolve_manifest_patch_path(
        worktree_bundle_dir,
        manifest.get("patch_path", PATCH_FILENAME),
    )

    patch_text = load_patch_text(patch_path)
    stored_touched_paths = parse_patch_touched_paths(patch_text)
    if not stored_touched_paths:
        raise ExtractionError("Extraction patch has no touched file paths")

    declared_raw = manifest.get("touched_paths")
    if declared_raw is None and "touched_paths" not in manifest:
        declared_raw = manifest.get("selected_paths", [])

    if not isinstance(declared_raw, (list, tuple)):
        raise ExtractionError("Extraction manifest selected/touched path declarations must be a list")

    declared_paths: set[str] = set()
    for path_value in declared_raw:
        if not isinstance(path_value, str) or not path_value:
            raise ExtractionError("Extraction manifest selected/touched paths must be non-empty strings")
        declared_paths.add(path_value)

    if not declared_paths:
        raise ExtractionError("Extraction manifest is missing selected/touched path declarations")

    unexpected = sorted(set(stored_touched_paths) - declared_paths)
    if unexpected:
        raise ExtractionError(
            "Extraction patch touches undeclared paths: " + ", ".join(unexpected),
        )

    selected_paths_raw = manifest.get("selected_paths")
    if not isinstance(selected_paths_raw, (list, tuple)) or not selected_paths_raw:
        raise ExtractionError("Extraction manifest selected_paths must be a non-empty list")
    if any(not isinstance(path_value, str) or not path_value for path_value in selected_paths_raw):
        raise ExtractionError("Extraction manifest selected_paths must contain non-empty strings")
    selected_paths: tuple[str, ...] = tuple(selected_paths_raw)

    stored_hunk_count = _count_patch_hunks(patch_text)
    source_branch = manifest.get("source_branch")
    source_base_ref = manifest.get("source_base_ref")
    source_commits_raw = manifest.get("source_commits", [])
    if source_commits_raw is None:
        source_commits_raw = []
    if not isinstance(source_commits_raw, (list, tuple)):
        raise ExtractionError("Extraction manifest source_commits must be a list when present")
    if any(not isinstance(value, str) or not value for value in source_commits_raw):
        raise ExtractionError("Extraction manifest source_commits must contain non-empty strings")
    source_commits = tuple(source_commits_raw)

    current_patch_text: str | None = None
    runtime_refresh_available = False
    source_context: dict[str, object]
    refresh_message: str

    if source_commits:
        missing_commits = [commit for commit in source_commits if not worktree_git.ref_exists(commit)]
        if not missing_commits:
            runtime_refresh_available = True
            current_patch_parts = [
                worktree_git.get_commit_patch_for_paths(commit, selected_paths, binary=True).rstrip("\n")
                for commit in source_commits
            ]
            current_patch_text = "\n".join(part for part in current_patch_parts if part).strip("\n")
            if current_patch_text:
                current_patch_text += "\n"
            source_context = {
                "source_commits": list(source_commits),
                "selected_paths": list(selected_paths),
            }
            refresh_message = (
                f"Extraction patch runtime refresh: re-derived hunks={{rederived}}, stored hunks={stored_hunk_count}"
            )
        else:
            source_context = {
                "source_commits": list(source_commits),
                "selected_paths": list(selected_paths),
            }
            refresh_message = (
                "Extraction patch runtime refresh: re-derived hunks=unavailable "
                f"(source commits unreachable: {', '.join(missing_commits)}), stored hunks={stored_hunk_count}"
            )
    else:
        if not isinstance(source_branch, str) or not source_branch:
            raise ExtractionError("Extraction manifest missing required source_branch")
        if not isinstance(source_base_ref, str) or not source_base_ref:
            raise ExtractionError("Extraction manifest missing required source_base_ref")
        revision_range = f"{source_base_ref}...{source_branch}"
        source_context = {
            "source_branch": source_branch,
            "source_base_ref": source_base_ref,
            "selected_paths": list(selected_paths),
        }
        if worktree_git.ref_exists(source_branch):
            if not worktree_git.ref_exists(source_base_ref):
                raise ExtractionError(f"Extraction source base ref not found: {source_base_ref}")
            runtime_refresh_available = True
            current_patch_text = worktree_git.get_diff_patch_for_paths(
                revision_range,
                selected_paths,
                binary=True,
            )
            refresh_message = (
                f"Extraction patch runtime refresh: re-derived hunks={{rederived}}, stored hunks={stored_hunk_count}"
            )
        else:
            refresh_message = (
                f"Extraction patch runtime refresh: re-derived hunks=unavailable "
                f"(source branch '{source_branch}' unreachable), stored hunks={stored_hunk_count}"
            )

    if runtime_refresh_available:
        assert current_patch_text is not None
        current_hunk_count = _count_patch_hunks(current_patch_text)
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "info",
                "message": refresh_message.format(rederived=current_hunk_count),
                **source_context,
                "rederived_hunk_count": current_hunk_count,
                "stored_hunk_count": stored_hunk_count,
            },
        )
        if not current_patch_text.strip():
            empty_message = (
                f"Extraction source diff is empty against current base; marking task {task.id} "
                f"{EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
            )
            if source_commits:
                empty_message = (
                    f"Extraction source commit set is empty for selected paths; marking task {task.id} "
                    f"{EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
                )
            return _already_merged_extraction_seed_result(
                task,
                log_file,
                message=empty_message,
            )

        if not source_commits:
            assert isinstance(source_base_ref, str)
            assert isinstance(source_branch, str)
            current_base_delta = worktree_git.get_diff_patch_for_paths(
                f"{source_base_ref}..{source_branch}",
                selected_paths,
                binary=True,
            )
            if not current_base_delta.strip():
                return _already_merged_extraction_seed_result(
                    task,
                    log_file,
                    message=(
                        "Extraction source branch adds nothing to the current base for selected paths; "
                        f"marking task {task.id} {EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
                    ),
                )
        current_touched_paths = parse_patch_touched_paths(current_patch_text)
        if not current_touched_paths:
            raise ExtractionError("Runtime re-derived extraction patch has no touched file paths")
        unexpected_runtime = sorted(set(current_touched_paths) - declared_paths)
        if unexpected_runtime:
            raise ExtractionError(
                "Runtime extraction patch touches undeclared paths: " + ", ".join(unexpected_runtime),
            )

        runtime_patch_path = _write_runtime_patch_file(
            worktree_bundle_dir,
            "selected.runtime.patch",
            current_patch_text,
        )
        reverse_check_result = worktree_git.reverse_check_patch_file_result(runtime_patch_path)
        if reverse_check_result.returncode == 0:
            return _already_merged_extraction_seed_result(
                task,
                log_file,
                message=(
                    "Extraction source changes are already present on selected paths; "
                    f"marking task {task.id} {EXTRACTION_ALREADY_MERGED_COMPLETION_REASON}"
                ),
            )
        apply_result = worktree_git.apply_patch_file_result(runtime_patch_path)
        if apply_result.returncode != 0:
            if _apply_left_relevant_conflicts(worktree_git, set(current_touched_paths)):
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "warning",
                        "message": (
                            f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                            f"using runtime re-derived patch with conflicts ({len(current_touched_paths)} files); "
                            "provider must resolve conflict markers"
                        ),
                        "seeded_paths": sorted(current_touched_paths),
                        "patch_source": "rederived",
                        "apply_conflicts": True,
                    },
                )
                return ExtractionSeedResult(seeded_paths=frozenset(current_touched_paths))
            raise GitError(_git_apply_failure_message(runtime_patch_path, apply_result))
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "info",
                "message": (
                    f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                    f"using runtime re-derived patch ({len(current_touched_paths)} files)"
                ),
                "seeded_paths": sorted(current_touched_paths),
                "patch_source": "rederived",
            },
        )
        return ExtractionSeedResult(seeded_paths=frozenset(current_touched_paths))

    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": refresh_message,
            **source_context,
            "rederived_hunk_count": None,
            "stored_hunk_count": stored_hunk_count,
        },
    )
    apply_result = worktree_git.apply_patch_file_result(patch_path)
    if apply_result.returncode != 0:
        if _apply_left_relevant_conflicts(worktree_git, set(stored_touched_paths)):
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "warning",
                    "message": (
                        f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                        f"using stored patch fallback with conflicts ({len(stored_touched_paths)} files); "
                        "provider must resolve conflict markers"
                    ),
                    "seeded_paths": sorted(stored_touched_paths),
                    "patch_source": "stored_fallback",
                    "apply_conflicts": True,
                },
            )
            return ExtractionSeedResult(seeded_paths=frozenset(stored_touched_paths))
        raise GitError(_git_apply_failure_message(patch_path, apply_result))
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "info",
            "message": (
                f"Applied extraction seed bundle from {project_bundle_dir.relative_to(config.project_dir)} "
                f"using stored patch fallback ({len(stored_touched_paths)} files)"
            ),
            "seeded_paths": sorted(stored_touched_paths),
            "patch_source": "stored_fallback",
        },
    )
    return ExtractionSeedResult(seeded_paths=frozenset(stored_touched_paths))


def _resolve_task_db_path(config: Config) -> Path:
    """Resolve the live task DB path for worktree snapshotting."""
    db_path = getattr(config, "db_path", None)
    if isinstance(db_path, Path):
        return db_path

    project_dir = getattr(config, "project_dir", None)
    if isinstance(project_dir, Path):
        return project_dir / ".gza" / "gza.db"

    return Path(".gza") / "gza.db"


def _snapshot_task_db_to_worktree(db_path: Path, worktree_path: Path) -> None:
    """Create a consistent read-only DB snapshot in the task worktree.

    Uses SQLite's backup API so the snapshot is transactionally consistent even
    while the live DB is being written by the host runner.
    """
    if not db_path.exists():
        return

    dst_dir = worktree_path / ".gza"
    dst_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = dst_dir / "gza.db"

    if snapshot_path.exists():
        snapshot_path.unlink()

    _backup_sqlite_file(db_path, snapshot_path)

    snapshot_path.chmod(0o444)


def _create_local_dep_symlinks(config: Config, worktree_path: Path) -> None:
    """Create symlinks for local path dependencies so uv can resolve them in worktrees.

    Parses [tool.uv.sources] from the project's pyproject.toml and creates
    symlinks in the worktree's ancestor directories so that relative path
    references resolve to the same real directories as they would from the
    original project root.
    """
    pyproject = config.project_dir / "pyproject.toml"
    if not pyproject.exists():
        return

    try:
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        logger.warning("Failed to read/parse %s; skipping local dep symlinks", pyproject)
        return

    sources = data.get("tool", {}).get("uv", {}).get("sources", {})
    if not sources:
        return

    for _dep_name, entry in sources.items():
        if not isinstance(entry, dict):
            continue
        raw_path = entry.get("path")
        if not raw_path:
            continue
        dep_rel = Path(raw_path)
        # Skip absolute paths — they work everywhere without symlinks
        if dep_rel.is_absolute():
            continue
        dep_real_path = (config.project_dir / dep_rel).resolve()
        if not dep_real_path.exists():
            logger.debug("Local dep %s does not exist on disk; skipping symlink", dep_real_path)
            continue
        # Compute where the symlink should land (resolve relative path from worktree)
        symlink_location = (worktree_path / dep_rel).resolve()
        # Skip paths inside the worktree itself (workspace members)
        try:
            symlink_location.relative_to(worktree_path)
            continue
        except ValueError:
            pass
        symlink_location.parent.mkdir(parents=True, exist_ok=True)
        if symlink_location.exists() or symlink_location.is_symlink():
            if symlink_location.is_symlink() and symlink_location.resolve() == dep_real_path:
                logger.debug("Symlink %s already points to %s; skipping", symlink_location, dep_real_path)
                continue
            logger.warning(
                "Path %s already exists and does not point to %s; skipping symlink creation",
                symlink_location,
                dep_real_path,
            )
            continue
        try:
            symlink_location.symlink_to(dep_real_path)
            logger.info("Created symlink %s -> %s", symlink_location, dep_real_path)
        except FileExistsError:
            # Lost the race with a concurrent task — verify the winner created the right symlink
            if symlink_location.is_symlink() and symlink_location.resolve() == dep_real_path:
                logger.debug("Symlink %s created by concurrent task; skipping", symlink_location)
            else:
                logger.warning(
                    "Path %s appeared during symlink creation and does not point to %s; skipping",
                    symlink_location,
                    dep_real_path,
                )


def run(
    config: Config,
    task_id: str | None = None,
    resume: bool = False,
    open_after: bool = False,
    skip_precondition_check: bool = False,
    on_task_claimed: Callable[[Task], None] | None = None,
    create_pr: bool = False,
    invocation: RunInvocationContext | None = None,
) -> int:
    """Run Gza on the next pending task or a specific task.

    Uses git worktrees to isolate task execution from the main working directory.
    This allows concurrent work in the main checkout while gza runs.

    Args:
        config: Configuration object
        task_id: Optional specific task ID to run. If None, runs next pending task.
        resume: If True, resume from previous session using stored session_id.
        open_after: If True, open the report file in $EDITOR after completion (for review tasks).
        skip_precondition_check: If True, skip dependency merge precondition checks.
        on_task_claimed: Optional callback invoked after task ownership is established.
        create_pr: If True, create/reuse a PR after successful code-task completion.
        invocation: Optional execution invocation context for UX/provenance.
    """
    load_dotenv(config.project_dir)

    # Create hourly backup before running
    backup_database(config.db_path, config.project_dir)

    # Load tasks from SQLite
    store = SqliteTaskStore.from_config(config)
    invocation_context = invocation or _resolve_default_invocation_context()
    task_execution_mode = _task_execution_mode_from_invocation(invocation_context)

    pr_retry_mode = False
    if task_id:
        task = store.get(task_id)
        if not task:
            error_message(f"Error: Task {task_id} not found")
            return 1

        # Resume mode validation
        if resume:
            if task.status not in ("failed", "pending"):
                error_message(f"Error: Can only resume failed tasks (task is {task.status})")
                return 1
            if not task.session_id:
                error_message(f"Error: Task {task_id} has no session ID (cannot resume)")
                console.print(
                    "Use 'gza retry' to create a new retry attempt with a fresh conversation instead"
                    " (implement retries may fork fresh; same-branch follow-ups stay on the shared branch)"
                )
                return 1
            if task.status == "pending":
                assert task.id is not None
                claimed = store.try_mark_in_progress(task.id, os.getpid())
                if claimed is None:
                    refreshed = store.get(task.id)
                    status = refreshed.status if refreshed else "unknown"
                    error_message(f"Error: Task {task_id} is no longer pending (status: {status})")
                    return 1
                task = claimed
                task.execution_mode = task_execution_mode
                assert task.id is not None
                store.set_execution_mode(task.id, task_execution_mode)
            else:
                task.status = "in_progress"
                task.started_at = datetime.now(UTC)
                task.completed_at = None
                task.failure_reason = None
                task.completion_reason = None
                task.running_pid = os.getpid()
                task.execution_mode = task_execution_mode
                store.update(task)
        else:
            # Check if task is blocked by dependencies
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                error_message(f"Error: Task {task_id} is blocked by task {blocking_id} ({blocking_status})")
                return 1
            requested_create_pr = bool(create_pr or task.create_pr)
            allow_pr_retry = (
                requested_create_pr
                and task.status == "failed"
                and task.failure_reason == PR_REQUIRED_FAILURE_REASON
            )
            if task.status == "in_progress":
                task.running_pid = os.getpid()
                task.execution_mode = task_execution_mode
                store.update(task)
            elif allow_pr_retry:
                task.status = "in_progress"
                task.started_at = datetime.now(UTC)
                task.completed_at = None
                task.running_pid = os.getpid()
                task.execution_mode = task_execution_mode
                store.update(task)
                pr_retry_mode = True
            elif task.status != "pending":
                error_message(f"Error: Task {task_id} is no longer pending (status: {task.status})")
                return 1
            else:
                assert task.id is not None
                claimed = store.try_mark_in_progress(task.id, os.getpid())
                if claimed is None:
                    refreshed = store.get(task.id)
                    status = refreshed.status if refreshed else "unknown"
                    error_message(f"Error: Task {task_id} is no longer pending (status: {status})")
                    return 1
                task = claimed
                task.execution_mode = task_execution_mode
                assert task.id is not None
                store.set_execution_mode(task.id, task_execution_mode)
    else:
        if resume:
            error_message("Error: Cannot resume without specifying a task ID")
            return 1
        task = None
        while True:
            candidate = store.get_next_pending()
            if candidate is None:
                break
            assert candidate.id is not None
            claimed = store.try_mark_in_progress(candidate.id, os.getpid())
            if claimed is None:
                continue
            task = claimed
            task.execution_mode = task_execution_mode
            assert task.id is not None
            store.set_execution_mode(task.id, task_execution_mode)
            break

    if not task:
        console.print("No pending tasks found")
        return 0
    requested_create_pr = bool(create_pr or task.create_pr)
    if on_task_claimed is not None:
        on_task_claimed(task)
    if pr_retry_mode:
        return _retry_pr_required_code_task_completion(task, config, store)

    # Get effective model and provider for this task
    effective_model, effective_provider, effective_max_steps = get_effective_config_for_task(task, config)

    # Persist resolved model/provider to the task DB row immediately so analytics
    # can track which configuration actually ran, even if it crashes before completion.
    # provider_is_explicit is intentionally left unchanged so resolved provider
    # state does not become a sticky override for future executions.
    task.model = effective_model
    task.provider = effective_provider
    store.update(task)

    # Create a modified config with task-specific settings
    from copy import copy
    task_config = copy(config)
    task_config.model = effective_model or ""
    task_config.provider = effective_provider
    task_config.reasoning_effort = config.get_reasoning_effort_for_task(task.task_type, effective_provider) or ""
    task_config.max_steps = effective_max_steps
    task_config.max_turns = effective_max_steps

    # Get the provider for this task
    provider = get_provider(task_config)
    resolved_interaction_mode = _resolve_interaction_mode(invocation_context, provider)
    preflight_logs = ensure_task_log_paths(config, store, task)

    if not provider.check_credentials():
        error_message(f"Error: No {provider.name} credentials found")
        console.print(f"  {provider.credential_setup_hint}")
        _mark_preflight_provider_unavailable(
            task=task,
            config=config,
            store=store,
            provider=provider,
            invocation=invocation_context,
            interaction_mode=resolved_interaction_mode,
            resume=resume,
            message=f"Preflight failed: missing {provider.name} credentials",
        )
        return 1

    # Verify credentials work before proceeding
    console.print(f"Verifying {provider.name} credentials...")
    preflight_result = _normalize_preflight_result(
        provider.verify_credentials(task_config, log_file=preflight_logs.ops)
    )
    if not preflight_result.ok:
        _mark_preflight_failure(
            task=task,
            config=config,
            store=store,
            provider=provider,
            invocation=invocation_context,
            interaction_mode=resolved_interaction_mode,
            resume=resume,
            message=preflight_result.message or f"Preflight failed: {provider.name} verification failed",
            failure_reason=preflight_result.failure_reason or "PROVIDER_UNAVAILABLE",
        )
        return 1
    rc = _colors.RUNNER_COLORS
    console.print(f"[{rc.success}]Credentials verified ✓[/{rc.success}]")

    # Setup git on the main repo (for worktree operations)
    git = Git(config.project_dir)
    default_branch = git.default_branch()

    # Pull latest on default branch (without switching away from user's current branch)
    # We do this by fetching and then basing the worktree on origin/default_branch
    try:
        git._run("fetch", "origin", default_branch)
    except GitError:
        pass  # May fail if offline, continue anyway

    # Generate slug — checks for collisions with existing branches/logs.
    # Always generate when slug is not set (new tasks, including new resume tasks).
    # Keep existing slug only when resuming a task that already has one assigned.
    if task.slug is None:
        slug_override = _compute_slug_override(task, store)
        task.slug = generate_slug(
            task.prompt,
            existing_id=None,
            log_path=config.log_path,
            git=git,
            store=store,
            exclude_task_id=task.id,
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=task.task_type_hint,
        )
    if task.slug and task.log_file:
        startup_log = config.project_dir / Path(task.log_file)
        if startup_log.name.endswith(".startup.log"):
            slug_log = rename_startup_log_to_slug(config, startup_log, task.slug)
            slug_log_relative = str(slug_log.relative_to(config.project_dir))
            if task.log_file != slug_log_relative:
                task.log_file = slug_log_relative
                store.update(task)

    task_header(
        task.prompt,
        str(task.id) if task.id is not None else "",
        task.task_type,
        slug=task.slug,
    )
    if invocation_context.execution_mode == "foreground_inline":
        if resolved_interaction_mode == "interactive":
            console.print(
                f"Foreground inline execution: interactive mode for provider '{provider.name.lower()}'. "
                "Press Ctrl-C to interrupt.",
            )
        else:
            console.print(
                f"Foreground inline execution: observe-only for provider '{provider.name.lower()}'. "
                "Interrupt to redirect.",
            )

    return _run_inner(
        task,
        task_config,
        config,
        store,
        provider,
        git,
        resume=resume,
        open_after=open_after,
        skip_precondition_check=skip_precondition_check,
        create_pr=requested_create_pr,
        invocation=invocation_context,
        interaction_mode=resolved_interaction_mode,
    )


def _check_dependency_merge_precondition(
    task: Task,
    store: SqliteTaskStore,
    git: Git,
    *,
    default_branch: str,
) -> tuple[Task | None, str | None, str | None]:
    """Return unmet dependency merge prerequisite or a git operational error."""
    dep = get_unmerged_dependency_precondition(store, task)
    if dep is None:
        return (None, None, None)
    return (dep, default_branch, None)


def _resolve_code_task_branch_name(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    *,
    resume: bool,
) -> str | None:
    """Resolve the branch name for implement/improve task execution."""
    if resume and task.branch:
        # Resume uses the existing branch from the failed task
        branch_name = task.branch
        console.print(f"Resuming on existing branch: [blue]{branch_name}[/blue]")
        return branch_name

    if resume:
        # Resume but branch wasn't saved - derive from task_id using branch naming strategy
        assert config.branch_strategy is not None
        assert task.slug is not None
        branch_name = generate_branch_name(
            pattern=config.branch_strategy.pattern,
            project_name=config.project_name,
            task_slug=task.slug,
            prompt=task.prompt,
            default_type=config.branch_strategy.default_type,
            explicit_type=task.task_type_hint,
            task_id=task.id or "",
            project_prefix=config.project_prefix,
        )
        console.print(f"Resuming on branch: [blue]{branch_name}[/blue]")
        return branch_name

    if task.same_branch:
        if task.task_type == "rebase":
            rebase_branch = resolve_rebase_target_branch(store, task)
            if rebase_branch and git.branch_exists(rebase_branch):
                console.print(f"Using rebase target branch: [blue]{rebase_branch}[/blue]")
                return rebase_branch
            if rebase_branch:
                error_message(
                    f"Error: Rebase task {task.id} resolved target branch {rebase_branch} but it does not exist"
                )
                return None
        merge_unit = store.resolve_merge_unit_for_task(task.id) if task.id is not None else None
        canonical_same_branch = merge_unit.source_branch if merge_unit is not None else task.branch
        if canonical_same_branch:
            if git.branch_exists(canonical_same_branch):
                if merge_unit is not None:
                    console.print(
                        f"Using merge-unit source branch: [blue]{canonical_same_branch}[/blue]"
                    )
                else:
                    console.print(
                        f"Using existing branch from task {task.id}: [blue]{canonical_same_branch}[/blue]"
                    )
                return canonical_same_branch
            error_message(
                f"Error: Task {task.id} resolved canonical same-branch target "
                f"{canonical_same_branch} but it does not exist"
            )
            return None
        # Use the branch from based_on task (for improve tasks) or depends_on task (fallback).
        # Walk the based_on chain until we find an ancestor with a valid, existing branch.
        source_task = None
        if task.based_on:
            source_task = store.get(task.based_on)
        elif task.depends_on:
            source_task = store.get(task.depends_on)

        resolved_branch: str | None = None
        visited_ids: list[str | None] = []
        seen_ids: set[str | None] = set()
        current = source_task
        while current is not None:
            if current.branch and git.branch_exists(current.branch):
                resolved_branch = current.branch
                if visited_ids:
                    via = " -> ".join(str(i) for i in visited_ids)
                    console.print(
                        f"Using branch from task {current.id} (via {via}): [blue]{resolved_branch}[/blue]"
                    )
                else:
                    console.print(f"Using existing branch from task {current.id}: [blue]{resolved_branch}[/blue]")
                break
            seen_ids.add(current.id)
            visited_ids.append(current.id)
            # Walk up the based_on chain, with cycle detection
            if current.based_on and current.based_on not in seen_ids:
                current = store.get(current.based_on)
            elif current.based_on:
                error_message(f"Error: Cycle detected in based_on chain for task {task.id}")
                return None
            else:
                current = None

        if resolved_branch is None:
            error_message(f"Error: Task {task.id} has same_branch=True but no ancestor has a valid branch")
            return None
        return resolved_branch

    if config.branch_mode == "single":
        return f"{config.project_name}/gza-work"

    # multi branch mode uses branch naming strategy
    assert config.branch_strategy is not None
    assert task.slug is not None
    branch_name = generate_branch_name(
        pattern=config.branch_strategy.pattern,
        project_name=config.project_name,
        task_slug=task.slug,
        prompt=task.prompt,
        default_type=config.branch_strategy.default_type,
        explicit_type=task.task_type_hint,
        task_id=task.id or "",
        project_prefix=config.project_prefix,
    )
    console.print(
        f"Branch strategy: [{_colors.RUNNER_COLORS.label}]{config.branch_strategy.pattern}[/] "
        f"→ [blue]{branch_name}[/blue]"
    )
    return branch_name


def _select_worktree_base_ref(git: Git, default_branch: str) -> str:
    """Select base ref for a new worktree using local/default divergence logic."""
    base_ref = default_branch
    origin_ref = f"origin/{default_branch}"

    # Check if origin ref exists
    origin_exists = git._run("rev-parse", "--verify", origin_ref, check=False).returncode == 0

    if not origin_exists:
        return base_ref

    # Compare local vs origin - use whichever is ahead
    local_ahead = git.count_commits_ahead(default_branch, origin_ref)
    origin_ahead = git.count_commits_ahead(origin_ref, default_branch)

    if origin_ahead > 0 and local_ahead == 0:
        # Origin is strictly ahead, use it
        return origin_ref
    if local_ahead > 0 and origin_ahead == 0:
        # Local is strictly ahead, use it
        return default_branch
    if local_ahead > 0 and origin_ahead > 0:
        # Diverged - prefer local to include unpushed changes
        return default_branch
    # Same commit, use either (default to local)
    return default_branch


def _setup_code_task_worktree(
    task: Task,
    config: Config,
    git: Git,
    *,
    branch_name: str,
    worktree_path: Path,
    default_branch: str,
    resume: bool,
) -> bool:
    """Create or re-create a code-task worktree and check out the target branch."""
    if resume or task.same_branch:
        # Validate branch exists before attempting to check it out
        if not git.branch_exists(branch_name):
            error_message(f"Error: Branch '{branch_name}' no longer exists. Cannot resume.")
            console.print("The branch may have been deleted or merged.")
            return False

        # Check out existing branch in worktree
        try:
            # Remove any existing worktree for this branch (may be at a different path
            # from a previous task run), then also remove worktree at target path if present
            cleanup_worktree_for_branch(git, branch_name, force=True)
            if worktree_path.exists():
                git.worktree_remove(worktree_path, force=True)

            console.print(f"Creating worktree with existing branch: {worktree_path}")
            # For existing branch, use git worktree add <path> <branch>
            git._run("worktree", "add", str(worktree_path), branch_name)
            return True
        except GitError as e:
            error_message(f"Error: Could not check out branch {branch_name} in worktree: {e}")
            return False

    # Delete existing branch if in single mode (worktree_add will recreate it)
    if config.branch_mode == "single" and git.branch_exists(branch_name):
        git._run("branch", "-D", branch_name, check=False)

    try:
        base_ref = task.base_branch or _select_worktree_base_ref(git, default_branch)
        if task.base_branch:
            console.print(f"Creating retry branch from base branch: [blue]{task.base_branch}[/blue]")
        console.print(f"Creating worktree: {worktree_path}")
        git.worktree_add(worktree_path, branch_name, base_ref)
        return True
    except GitError as e:
        error_message(f"Git error: {e}")
        return False


def _filter_stageable_paths(
    candidate_paths: set[str],
    status_paths: set[str],
) -> set[str]:
    """Keep only paths that can be staged without pathspec failures."""
    return {path for path in candidate_paths if path in status_paths}


def _complete_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    worktree_git: Git,
    log_file: Path,
    branch_name: str,
    stats: TaskStats,
    exit_code: int,
    pre_run_status: set[tuple[str, str]],
    worktree_summary_path: Path,
    summary_path: Path,
    summary_dir: Path,
    *,
    target_branch: str | None = None,
    skip_commit: bool = False,
    create_pr: bool = False,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    seeded_paths: set[str] | None = None,
    rebase_diff_baseline: RebaseDiffBaseline | None = None,
) -> int:
    """Handle successful code-task completion (staging, commit, completion state, output).

    Args:
        skip_commit: If True, skip staging/committing changes. Used for rebase
            tasks where the agent handles rebases directly
            and no new commits should be created by the runner.
    """
    if skip_commit:
        has_uncommitted = False
    else:
        seeded_paths = seeded_paths or set()
        # Compute which files changed during the provider run (selective staging)
        post_run_status = worktree_git.status_porcelain()
        new_changes = post_run_status - pre_run_status
        status_paths = {filepath for _, filepath in post_run_status}
        candidate_stage_paths = {filepath for _, filepath in new_changes} | seeded_paths
        files_to_stage = _filter_stageable_paths(
            candidate_stage_paths,
            status_paths,
        )
        has_uncommitted = bool(files_to_stage)

        if not has_uncommitted:
            # Check if branch already has commits from a previous run
            default_branch = worktree_git.default_branch()
            commits_ahead = worktree_git.count_commits_ahead(branch_name, default_branch)
            if commits_ahead == 0:
                # No uncommitted changes and no commits on branch - real failure
                # Note: No need to save WIP here since there are no changes
                failure_reason = _resolve_failure_reason(
                    error_type=None,
                    exit_code=exit_code,
                    log_file=log_file,
                    stats=stats,
                    fallback_to_log=True,
                )
                task_footer(
                    task,
                    stats,
                    status="No changes made",
                    branch=branch_name,
                )
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "outcome",
                        "message": "Outcome: failed (no changes made)",
                        "exit_code": exit_code,
                        "failure_reason": failure_reason,
                    },
                )
                write_log_entry(
                    log_file,
                    {
                        "type": "gza",
                        "subtype": "stats",
                        "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}",
                        "duration_seconds": stats.duration_seconds,
                        "cost_usd": stats.cost_usd,
                        "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0,
                    },
                )
                _mark_task_failed(
                    task=task,
                    config=config,
                    store=store,
                    log_file=log_file,
                    stats=stats,
                    branch=branch_name,
                    explicit_reason=failure_reason,
                    error_type=None,
                    exit_code=exit_code,
                )
                return 0
            # else: branch has commits from a previous run - treat as success without committing

        if has_uncommitted:
            assert task.id is not None, "Task ID must be set before committing"
            # Squash any WIP commits before creating final commit
            _squash_wip_commits(worktree_git, task)

            # Stage only files changed in this run plus extraction-seeded files.
            for f in sorted(files_to_stage):
                worktree_git.add(f)

            review_task_id = None
            if task.task_type == "improve" and task.depends_on:
                review_task = store.get(task.depends_on)
                if review_task and review_task.task_type == "review":
                    review_task_id = review_task.id

            commit_subject = _build_code_task_commit_subject(
                task.prompt,
                worktree_summary_path,
                fallback_subject=_default_code_task_commit_subject(task.slug, task.id),
            )

            commit_message = build_task_commit_message(
                commit_subject,
                task_id=task.id,
                task_slug=task.slug,
                review_task_id=review_task_id,
            )
            worktree_git.commit(commit_message)

    # Copy summary file from worktree to main project directory
    output_content = None
    if worktree_summary_path.exists():
        try:
            summary_content = worktree_summary_path.read_text()
        except (OSError, UnicodeError):
            logger.warning(
                "Failed to read summary file for task completion output at %s; continuing without output_content",
                worktree_summary_path,
                exc_info=True,
            )
        else:
            # Ensure target directory exists
            summary_dir.mkdir(parents=True, exist_ok=True)
            # Copy summary content from worktree to project dir
            summary_path.write_text(summary_content)
            output_content = summary_content

    # Compute diff stats vs. default branch before marking completed
    default_branch = target_branch if target_branch is not None else worktree_git.default_branch()
    numstat_output = worktree_git.get_diff_numstat(f"{default_branch}...{branch_name}")
    diff_files, diff_added, diff_removed = parse_diff_numstat(numstat_output)
    head_sha = worktree_git.rev_parse_if_exists(branch_name)
    base_sha = worktree_git.rev_parse_if_exists(default_branch)

    # Keep branch context on the in-memory task so PR ensure can run before
    # the final completed-state DB transition.
    task.branch = branch_name
    if create_pr and task.task_type != "rebase":
        pr_ready = _ensure_work_pr_for_completed_code_task(task, config, store, worktree_git)
        if not pr_ready:
            print("Error: Task requested PR creation/reuse, aborting before auto-review")
            task.output_content = output_content
            task.diff_files_changed = diff_files
            task.diff_lines_added = diff_added
            task.diff_lines_removed = diff_removed
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                has_commits=True,
                stats=stats,
                branch=branch_name,
                explicit_reason=PR_REQUIRED_FAILURE_REASON,
                error_type=None,
                exit_code=1,
                head_sha=head_sha,
                base_sha=base_sha,
            )
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "message": "Outcome: failed (PR_REQUIRED)",
                    "exit_code": 1,
                },
            )
            _write_stats_entry(log_file, stats)
            return 1

    fix_was_merged_before_run = False
    if task.task_type == "fix":
        root_impl = _resolve_root_implementation_for_fix(task, store)
        if root_impl is not None and root_impl.id is not None:
            root_impl_unit = store.resolve_merge_unit_for_task(root_impl.id)
            fix_was_merged_before_run = (
                (root_impl_unit.state if root_impl_unit is not None else root_impl.merge_status) == "merged"
            )

    task_logger = TaskExecutionLogger(resolve_ops_log_path(config, log_file), echo=True)
    if task.task_type == "rebase":
        return _finalize_rebase_completion(
            task=task,
            config=config,
            store=store,
            worktree_git=worktree_git,
            branch_name=branch_name,
            stats=stats,
            log_file=log_file,
            output_content=output_content,
            diff_files=diff_files,
            diff_added=diff_added,
            diff_removed=diff_removed,
            head_sha=head_sha,
            base_sha=base_sha,
            task_logger=task_logger,
            target_branch=default_branch,
            create_pr=create_pr,
            fix_commits_ahead_before_run=fix_commits_ahead_before_run,
            fix_default_branch=fix_default_branch,
            fix_was_merged_before_run=fix_was_merged_before_run,
            rebase_diff_baseline=rebase_diff_baseline,
        )

    _finalize_completed_code_task(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        branch_name=branch_name,
        output_content=output_content,
        stats=stats,
        diff_files=diff_files,
        diff_added=diff_added,
        diff_removed=diff_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return _post_complete_code_task(
        task,
        config,
        store,
        worktree_git,
        branch_name,
        stats,
        task_logger=task_logger,
        target_branch=default_branch,
        fix_commits_ahead_before_run=fix_commits_ahead_before_run,
        fix_default_branch=fix_default_branch,
        fix_was_merged_before_run=fix_was_merged_before_run,
        rebase_diff_baseline=rebase_diff_baseline,
    )


def _post_complete_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    worktree_git: Git,
    branch_name: str,
    stats: TaskStats,
    *,
    task_logger: TaskExecutionLogger | None = None,
    target_branch: str | None = None,
    fix_commits_ahead_before_run: int | None = None,
    fix_default_branch: str | None = None,
    fix_was_merged_before_run: bool = False,
    rebase_diff_baseline: RebaseDiffBaseline | None = None,
) -> int:
    """Run shared post-completion side effects for completed code tasks."""
    auto_learnings = maybe_auto_regenerate_learnings(store, config)
    improve_follow_up_ready = True
    fix_code_changed = False
    fix_auto_review_ready = True
    impl_ancestor: Task | None = None

    # Clear review state on the root implementation task after improve completes.
    # Improve retries/resumes may chain based_on through previous improves, so
    # resolve the implementation ancestor first. Hold the review-clear handoff
    # until live-PR publication is known safe so later lifecycle automation does
    # not recreate PR-facing review work for unpublished code.
    if task.task_type == "improve":
        impl_ancestor = _resolve_impl_ancestor(store, task)
        if impl_ancestor and impl_ancestor.id is not None:
            # If the implementation was already merged, flip it back to unmerged:
            # improve writes add commits on the shared implementation branch even
            # when publishing those commits still needs operator intervention.
            refreshed_impl = store.get(impl_ancestor.id)
            refreshed_unit = (
                store.resolve_merge_unit_for_task(refreshed_impl.id)
                if refreshed_impl and refreshed_impl.id is not None
                else None
            )
            if refreshed_impl and refreshed_impl.id is not None and (
                refreshed_unit.state if refreshed_unit is not None else refreshed_impl.merge_status
            ) == "merged":
                store.set_merge_status(refreshed_impl.id, "unmerged")
        if task.create_review:
            improve_follow_up_ready = _sync_completed_code_task_branch_for_live_pr(task, store, worktree_git)
        if improve_follow_up_ready and impl_ancestor and impl_ancestor.id is not None:
            store.clear_review_state(impl_ancestor.id)
            store.resolve_comments(
                impl_ancestor.id,
                created_on_or_before=task.created_at,
            )

    # Rebase tasks run provider-side conflict resolution in the worktree.
    # Force-push from the host runner so SSH/auth follows host environment.
    if task.task_type == "rebase":
        publish_rebased_branch(
            worktree_git,
            branch=branch_name,
            baseline=rebase_diff_baseline,
            logger=task_logger,
        )

    rebase_changed_diff: bool | None = None
    # Invalidate review state after rebase completes only when the patch changed
    # or equivalence could not be proven, but only after publication succeeds.
    if task.task_type == "rebase" and task.based_on:
        impl_ancestor = _resolve_impl_ancestor(store, task)
        comparison = compute_rebase_changed_diff(
            worktree_git,
            baseline=(
                rebase_diff_baseline
                if rebase_diff_baseline is not None
                else RebaseDiffBaseline(old_tip=None, target_at_start=None, merge_base_at_start=None, recovered=True)
            ),
            branch=branch_name,
            target=target_branch if target_branch is not None else worktree_git.default_branch(),
        )
        rebase_changed_diff = comparison.changed_diff
        assert task.id is not None
        store.set_rebase_changed_diff(task.id, comparison.changed_diff)
        task.changed_diff = comparison.changed_diff
        if comparison.warning:
            logger.warning(comparison.warning)
            console.print(f"[yellow]Warning: {comparison.warning}[/yellow]")
        rebase_review_target_id = (
            impl_ancestor.id if impl_ancestor and impl_ancestor.id is not None else task.based_on
        )
        if comparison.changed_diff:
            store.invalidate_review_state(rebase_review_target_id)
        parent = store.get(rebase_review_target_id)
        parent_unit = (
            store.resolve_merge_unit_for_task(parent.id) if parent and parent.id is not None else None
        )
        if parent and parent.id is not None and (
            parent_unit.state if parent_unit is not None else parent.merge_status
        ) == "merged":
            store.set_merge_status(parent.id, "unmerged")

    if task.task_type == "fix":
        fix_code_changed = _prepare_fix_follow_up_review(
            task,
            store,
            worktree_git,
            branch_name,
            fix_commits_ahead_before_run=fix_commits_ahead_before_run,
            fix_default_branch=fix_default_branch,
            fix_was_merged_before_run=fix_was_merged_before_run,
        )
        if fix_code_changed:
            if task.create_review:
                fix_auto_review_ready = _sync_completed_code_task_branch_for_live_pr(task, store, worktree_git)
            else:
                _create_fix_follow_up_review_task(task, store)

    console.print("")
    if task.task_type == "rebase" and rebase_changed_diff is not None:
        changed_diff_text = "yes (review must be refreshed)" if rebase_changed_diff else "no (review can be preserved)"
        console.print(f"Changed Diff: {changed_diff_text}")
    task_footer(
        task,
        stats,
        status="Done",
        branch=branch_name,
        learnings=auto_learnings,
        store=store,
    )

    # Auto-create and run review task if requested
    if task.create_review:
        if task.task_type == "improve" and not improve_follow_up_ready:
            review_target = _resolve_impl_ancestor(store, task)
            if review_target and review_target.id is not None:
                print(
                    "Warning: Skipping auto-review until the improve branch is safely published. "
                    f"After resolving the PR sync issue, run `uv run gza review {review_target.id}`."
                )
            else:
                print(
                    "Warning: Skipping auto-review until the improve branch is safely published."
                )
            return 0
        if task.task_type == "fix":
            if not fix_code_changed:
                return 0
            if not fix_auto_review_ready:
                review_target = _resolve_root_implementation_for_fix(task, store)
                if review_target and review_target.id is not None:
                    print(
                        "Warning: Skipping auto-review until the fix branch is safely published. "
                        f"After resolving the PR sync issue, run `uv run gza review {review_target.id}`."
                    )
                else:
                    print(
                        "Warning: Skipping auto-review until the fix branch is safely published."
                    )
                return 0
        if target_branch is None:
            return _create_and_run_review_task(task, config, store)
        return _create_and_run_review_task(task, config, store)

    return 0


def _prepare_fix_follow_up_review(
    task: Task,
    store: SqliteTaskStore,
    worktree_git: Git,
    branch_name: str,
    *,
    fix_commits_ahead_before_run: int | None,
    fix_default_branch: str | None,
    fix_was_merged_before_run: bool = False,
) -> bool:
    """Return True when a completed fix added commits that require follow-up review."""
    root_impl = _resolve_root_implementation_for_fix(task, store)
    if root_impl is None or root_impl.id is None:
        return False
    root_impl_id = root_impl.id
    default_branch = fix_default_branch

    def _restore_prior_merged_state() -> None:
        if not fix_was_merged_before_run:
            return
        refreshed_impl = store.get(root_impl_id)
        if refreshed_impl is not None and refreshed_impl.id is not None:
            store.set_merge_status(refreshed_impl.id, "merged")

    if fix_commits_ahead_before_run is None:
        _restore_prior_merged_state()
        print("Warning: Could not determine fix commit baseline before run")
        print("Warning: Could not determine whether the fix run changed code")
        return False

    if not default_branch:
        try:
            default_branch = worktree_git.default_branch()
        except GitError as exc:
            _restore_prior_merged_state()
            print(f"Warning: Could not determine fix commit delta: {exc}")
            print("Warning: Could not determine whether the fix run changed code")
            return False

    try:
        commits_after = worktree_git.count_commits_ahead(branch_name, default_branch)
    except GitError as exc:
        _restore_prior_merged_state()
        print(f"Warning: Could not determine fix commit delta: {exc}")
        print("Warning: Could not determine whether the fix run changed code")
        return False

    commits_before = fix_commits_ahead_before_run
    if commits_after <= commits_before:
        _restore_prior_merged_state()
        print("Fix completed without new commits; no follow-up review was auto-created.")
        return False

    store.clear_review_state(root_impl_id)
    refreshed_impl = store.get(root_impl_id)
    refreshed_unit = (
        store.resolve_merge_unit_for_task(refreshed_impl.id)
        if refreshed_impl and refreshed_impl.id is not None
        else None
    )
    if refreshed_impl and refreshed_impl.id is not None and (
        refreshed_unit.state if refreshed_unit is not None else refreshed_impl.merge_status
    ) == "merged":
        store.set_merge_status(refreshed_impl.id, "unmerged")

    return True


def _create_fix_follow_up_review_task(task: Task, store: SqliteTaskStore) -> None:
    """Create a pending follow-up review task for a completed fix run."""
    root_impl = _resolve_root_implementation_for_fix(task, store)
    if root_impl is None or root_impl.id is None:
        return

    try:
        review_task = create_review_task(store, root_impl, prompt_mode="auto")
    except DuplicateReviewError as exc:
        active = exc.active_review
        print(
            f"Follow-up review already exists for implementation {root_impl.id}: "
            f"{active.id} ({active.status})."
        )
        return
    except ValueError as exc:
        print(
            f"Warning: Could not auto-create follow-up review for implementation {root_impl.id}: {exc}"
        )
        print(f"Next step: run `uv run gza review {root_impl.id}` after validating task state.")
        return
    print(f"Created follow-up review task {review_task.id} for implementation {root_impl.id}")


def _retry_pr_required_code_task_completion(task: Task, config: Config, store: SqliteTaskStore) -> int:
    """Retry post-code PR/completion steps for tasks blocked on required PR creation."""
    if not task.branch:
        print(f"Error: Task {task.id} has no branch to create/reuse PR")
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=task.log_file,
            has_commits=bool(task.has_commits),
            explicit_reason=PR_REQUIRED_FAILURE_REASON,
            error_type=None,
            exit_code=1,
        )
        return 1

    git = Git(config.project_dir)
    stats = TaskStats(
        duration_seconds=task.duration_seconds,
        num_steps_reported=task.num_steps_reported,
        num_steps_computed=task.num_steps_computed,
        num_turns_reported=task.num_turns_reported,
        num_turns_computed=task.num_turns_computed,
        cost_usd=task.cost_usd,
        input_tokens=task.input_tokens,
        output_tokens=task.output_tokens,
    )

    task.failure_reason = None
    task.completion_reason = None
    target_branch: str | None = git.default_branch() if task.branch and task.has_commits else None
    head_sha = git.rev_parse_if_exists(task.branch) if task.branch and task.has_commits else None
    base_sha = git.rev_parse_if_exists(target_branch) if target_branch and task.has_commits else None
    retry_logger = None
    retry_log_path: Path | None = None
    if task.log_file:
        retry_log_path = config.project_dir / Path(task.log_file)
        retry_log_path.parent.mkdir(parents=True, exist_ok=True)
        retry_logger = TaskExecutionLogger(
            resolve_ops_log_path(config, retry_log_path),
            echo=True,
        )
    if task.task_type == "rebase":
        try:
            return _finalize_already_published_rebase_pr_retry(
                task=task,
                config=config,
                store=store,
                git=git,
                branch_name=task.branch,
                stats=stats,
                log_file=retry_log_path if retry_log_path is not None else config.project_dir / "retry.log",
                output_content=task.output_content,
                diff_files=task.diff_files_changed or 0,
                diff_added=task.diff_lines_added or 0,
                diff_removed=task.diff_lines_removed or 0,
                head_sha=head_sha,
                base_sha=base_sha,
                task_logger=retry_logger
                or TaskExecutionLogger(resolve_ops_log_path(config, config.project_dir / "retry.log"), echo=True),
            )
        except GitError as e:
            error_message(f"Git error: {e}")
            if retry_log_path is not None:
                write_log_entry(
                    retry_log_path,
                    {
                        "type": "gza",
                        "subtype": "outcome",
                        "message": "Outcome: failed (GIT_ERROR)",
                        "exit_code": 1,
                        "failure_reason": "GIT_ERROR",
                    },
                )
                _write_stats_entry(retry_log_path, stats)
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=task.log_file,
                stats=stats,
                branch=task.branch,
                has_commits=bool(task.has_commits),
                explicit_reason="GIT_ERROR",
                error_type=None,
                exit_code=1,
                head_sha=head_sha,
                base_sha=base_sha,
            )
            return 1

    pr_ready = _ensure_work_pr_for_completed_code_task(task, config, store, git)
    if not pr_ready:
        print("Error: PR-required retry still could not create/reuse PR")
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=task.log_file,
            branch=task.branch,
            has_commits=bool(task.has_commits),
            explicit_reason=PR_REQUIRED_FAILURE_REASON,
            error_type=None,
            exit_code=1,
        )
        return 1

    store.mark_completed(
        task,
        branch=task.branch,
        log_file=task.log_file,
        output_content=task.output_content,
        has_commits=bool(task.has_commits),
        stats=stats,
        diff_files_changed=task.diff_files_changed,
        diff_lines_added=task.diff_lines_added,
        diff_lines_removed=task.diff_lines_removed,
        head_sha=head_sha,
        base_sha=base_sha,
    )
    return _post_complete_code_task(
        task,
        config,
        store,
        git,
        task.branch,
        stats,
        task_logger=retry_logger,
        target_branch=target_branch,
    )


def _run_inner(
    task: "Task",
    task_config: Config,
    config: Config,
    store: SqliteTaskStore,
    provider: "Provider",
    git: "Git | None",
    resume: bool = False,
    open_after: bool = False,
    skip_precondition_check: bool = False,
    create_pr: bool = False,
    invocation: RunInvocationContext | None = None,
    interaction_mode: str = "observe_only",
) -> int:
    """Inner task execution logic, split out to allow foreground worker cleanup."""
    # For explore, plan, review, and internal tasks, run without creating a branch.
    # Keep temporary "learn" compatibility for pre-migration rows.
    if task.task_type in ("explore", "plan", "review", "internal", "learn"):
        return _run_non_code_task(
            task,
            task_config,
            store,
            provider,
            git,
            resume=resume,
            open_after=open_after,
            invocation=invocation,
            interaction_mode=interaction_mode,
        )

    # Code tasks (implement/improve) require git
    assert git is not None, "git is required for code tasks"
    default_branch = git.default_branch()

    branch_name = _resolve_code_task_branch_name(task, config, store, git, resume=resume)
    if branch_name is None:
        return 1

    # Create worktree path
    assert task.slug is not None
    worktree_path = config.worktree_path / task.slug

    if not _setup_code_task_worktree(
        task,
        config,
        git,
        branch_name=branch_name,
        worktree_path=worktree_path,
        default_branch=default_branch,
        resume=resume,
    ):
        return 1

    # Create a Git instance for the worktree
    worktree_git = Git(worktree_path)

    # Restore WIP changes if resuming
    if resume:
        # When resuming via a new task (based_on points to the original failed task),
        # the WIP diff file was saved under the original task's task_id.
        original_task_id = None
        if task.based_on:
            original_task = store.get(task.based_on)
            if original_task:
                original_task_id = original_task.slug
        _restore_wip_changes(task, worktree_git, config, branch_name, original_task_id=original_task_id)

    # Persist branch early so it's available if the process is killed before completion
    task.branch = branch_name

    store.update(task)

    # Setup logging using the canonical task log path selected during preflight.
    if task.log_file:
        log_file = config.project_dir / Path(task.log_file)
    else:
        config.log_path.mkdir(parents=True, exist_ok=True)
        log_file = config.log_path / f"{task.slug}.log"
        task.log_file = str(log_file.relative_to(config.project_dir))
        store.update(task)

    # Write orchestration pre-run entries
    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug}"})
    write_log_entry(log_file, {"type": "gza", "subtype": "branch", "message": f"Branch: {branch_name}", "branch": branch_name})
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Provider: {provider.name}, Model: {task_config.model or 'default'}"})
    write_execution_provenance_event(
        log_file,
        invocation=invocation or _resolve_default_invocation_context(),
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )

    if skip_precondition_check and task.depends_on and not task.same_branch:
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "info",
                "message": (
                    f"Skipped dependency merge precondition check (--force) "
                    f"for depends_on task {task.depends_on}"
                ),
            },
        )
    else:
        blocking_dep, target_branch, precondition_error = _check_dependency_merge_precondition(
            task,
            store,
            git,
            default_branch=default_branch,
        )
        if precondition_error is not None:
            error_message(f"Git error: {precondition_error}")
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "message": precondition_error,
                    "failure_reason": "GIT_ERROR",
                },
            )
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                branch=branch_name,
                explicit_reason="GIT_ERROR",
                error_type=None,
                exit_code=1,
            )
            return 1
        if blocking_dep is not None:
            assert blocking_dep.id is not None
            dep_branch = blocking_dep.branch or "<none>"
            failure_message = (
                f"Dependency {blocking_dep.id} on branch '{dep_branch}' is not merged into "
                f"'{target_branch}'. Failing without provider run."
            )
            error_message(f"Error: {failure_message}")
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "message": failure_message,
                    "failure_reason": "PREREQUISITE_UNMERGED",
                    "dependency_task_id": blocking_dep.id,
                    "dependency_branch": dep_branch,
                    "target_branch": target_branch,
                },
            )
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                branch=branch_name,
                explicit_reason="PREREQUISITE_UNMERGED",
                error_type=None,
                exit_code=1,
            )
            return 1

    # Setup summary directory and path for task/implement types
    _, summary_path = get_task_output_paths(task, config.project_dir)
    assert summary_path is not None, f"Code task type '{task.task_type}' must have a summary path"
    summary_dir = summary_path.parent
    summary_dir.mkdir(parents=True, exist_ok=True)

    # Create summary directory structure in worktree
    worktree_summary_dir = worktree_path / summary_dir.relative_to(config.project_dir)
    worktree_summary_dir.mkdir(parents=True, exist_ok=True)
    worktree_summary_path = worktree_path / summary_path.relative_to(config.project_dir)

    # For Docker containers, use /workspace-relative path instead of host worktree path
    # For native mode, use the actual worktree path
    if config.use_docker:
        prompt_summary_path = Path("/workspace") / summary_path.relative_to(config.project_dir)
    else:
        prompt_summary_path = worktree_summary_path

    def _on_session_id(session_id: str) -> None:
        """Persist session_id to the task record as soon as it is first seen.

        This ensures that even if the run is killed mid-stream (e.g. Ctrl+C),
        the session_id is already saved and ``gza resume`` can still work.
        """
        if task.session_id == session_id:
            return
        task.session_id = session_id
        store.update(task)

    def _on_step_count(count: int) -> None:
        """Update task.num_steps_computed in real time during streaming."""
        task.num_steps_computed = count
        store.update(task)

    # Ensure all bundled skills are available in the worktree
    from .skills_utils import ensure_all_skills
    skills_dir = worktree_path / ".claude" / "skills"
    n_installed = ensure_all_skills(skills_dir)
    if n_installed:
        console.print(f"Installed {n_installed} skill(s) into worktree")

    # Copy learnings file into worktree so the agent can read it
    _snapshot_task_db_to_worktree(_resolve_task_db_path(config), worktree_path)
    _copy_learnings_to_worktree(config, worktree_path)

    if not config.use_docker:
        _create_local_dep_symlinks(config, worktree_path)

    seeded_paths: set[str] = set()
    try:
        extraction_seed = _seed_extraction_bundle_if_present(
            task,
            config,
            worktree_path,
            worktree_git,
            log_file,
            resume=resume,
        )
        seeded_paths = set(extraction_seed.seeded_paths)
    except (ExtractionError, GitError) as exc:
        failure_message = f"Extraction preflight/apply failed: {exc}"
        error_message(f"Error: {failure_message}")
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "outcome",
                "message": failure_message,
                "failure_reason": EXTRACTION_PRECHECK_FAILURE_REASON,
            },
        )
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            branch=branch_name,
            explicit_reason=EXTRACTION_PRECHECK_FAILURE_REASON,
            error_type=None,
            exit_code=1,
        )
        return 1

    if extraction_seed.completion_reason:
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "outcome",
                "message": "Outcome: completed without provider execution",
                "completion_reason": extraction_seed.completion_reason,
            },
        )
        store.mark_completed(
            task,
            branch=branch_name,
            log_file=str(log_file.relative_to(config.project_dir)),
            has_commits=False,
            completion_reason=extraction_seed.completion_reason,
        )
        return 0

    # Run provider in the worktree
    if resume:
        prompt = PromptBuilder().resume_prompt()
    else:
        prompt = build_prompt(task, config, store, report_path=None, summary_path=prompt_summary_path, git=git)

    # Snapshot worktree state before provider runs so we can selectively stage only new changes
    pre_run_status = worktree_git.status_porcelain()
    task_logger = TaskExecutionLogger(resolve_ops_log_path(config, log_file), echo=True)
    rebase_validation_state: tuple[str, set[RuffDiagnostic]] | None = None
    rebase_diff_baseline: RebaseDiffBaseline | None = None
    if task.task_type == "rebase":
        try:
            rebase_validation_state = capture_rebase_validation_baseline(worktree_git)
            rebase_diff_baseline = capture_rebase_diff_baseline(
                worktree_git,
                branch=branch_name,
                target=default_branch,
                recovered=_is_recovered_rebase_lineage(task, resume=resume),
            )
        except RuntimeError as exc:
            task_logger.error(f"Pre-rebase ruff validation failed to run: {exc}")
            _save_wip_changes(task, worktree_git, config, branch_name)
            _record_run_failure(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                stats=TaskStats(duration_seconds=0.0, num_steps_reported=0, cost_usd=0.0),
                failure=_rebase_validation_failure(),
                exit_code=1,
                branch=branch_name,
            )
            return 0
    fix_commits_ahead_before_run: int | None = None
    fix_default_branch: str | None = None
    if task.task_type == "fix":
        fix_default_branch = worktree_git.default_branch()
        try:
            fix_commits_ahead_before_run = worktree_git.count_commits_ahead(branch_name, fix_default_branch)
        except GitError:
            fix_commits_ahead_before_run = None

    try:
        provider_run_kwargs: dict[str, Any] = {
            "resume_session_id": task.session_id if resume else None,
            "on_session_id": _on_session_id,
            "on_step_count": _on_step_count,
        }
        if interaction_mode == "interactive":
            provider_run_kwargs["interactive"] = True
        if _provider_accepts_ops_log_file(provider):
            provider_run_kwargs["ops_log_file"] = resolve_ops_log_path(config, log_file)
        provider_prompt = sanitize_provider_prompt(prompt, task_type=task.task_type)
        result = _call_provider_run(
            provider,
            task_config,
            provider_prompt,
            log_file,
            worktree_path,
            provider_run_kwargs=provider_run_kwargs,
        )

        exit_code = result.exit_code
        stats = _run_result_to_stats(result)
        assert task.id is not None
        has_step_events = _persist_run_steps_from_result(store, task.id, provider.name.lower(), result)
        if has_step_events:
            task.log_schema_version = 2

        # Store session_id if available and not already persisted by _on_session_id callback
        if result.session_id and result.session_id != task.session_id:
            task.session_id = result.session_id
            store.update(task)

        resolved_failure = _resolve_run_failure(
            provider_name=provider.name,
            timeout_minutes=config.timeout_minutes,
            step_limit=task_config.max_steps,
            turn_limit=task_config.max_turns,
            error_type=result.error_type,
            exit_code=exit_code,
            log_file=log_file,
            stats=stats,
        )

        if resolved_failure is not None:
            # Save WIP changes before marking failed
            _save_wip_changes(task, worktree_git, config, branch_name)
            _record_run_failure(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                stats=stats,
                failure=resolved_failure,
                exit_code=exit_code,
                branch=branch_name,
            )
            return 0

        if task.task_type == "rebase":
            assert rebase_validation_state is not None
            before_head, pre_existing_diagnostics = rebase_validation_state
            if is_rebase_in_progress(worktree_git.repo_dir):
                task_logger.error("Rebase still in progress after provider success.")
                _save_wip_changes(task, worktree_git, config, branch_name)
                _record_run_failure(
                    task=task,
                    config=config,
                    store=store,
                    log_file=log_file,
                    stats=stats,
                    failure=_rebase_validation_failure(),
                    exit_code=1,
                    branch=branch_name,
                )
                return 0
            if not validate_rebase_resolution_output(
                git=worktree_git,
                before_head=before_head,
                pre_existing_diagnostics=pre_existing_diagnostics,
                task_logger=task_logger,
            ):
                _save_wip_changes(task, worktree_git, config, branch_name)
                _record_run_failure(
                    task=task,
                    config=config,
                    store=store,
                    log_file=log_file,
                    stats=stats,
                    failure=_rebase_validation_failure(),
                    exit_code=1,
                    branch=branch_name,
                )
                return 0

        return _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            branch_name,
            stats,
            exit_code,
            pre_run_status,
            worktree_summary_path,
            summary_path,
            summary_dir,
            target_branch=default_branch,
            skip_commit=task.task_type == "rebase",
            create_pr=create_pr,
            fix_commits_ahead_before_run=fix_commits_ahead_before_run,
            fix_default_branch=fix_default_branch,
            seeded_paths=seeded_paths,
            rebase_diff_baseline=rebase_diff_baseline,
        )

    except GitError as e:
        error_message(f"Git error: {e}")
        _record_run_failure(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            stats=stats,
            branch=branch_name,
            failure=_git_error_failure(),
            exit_code=1,
        )
        return 1
    except KeyboardInterrupt:
        failure_reason = _resolve_failure_reason(
            interrupt_signal=_interrupt_signal_name(),
            interrupted=True,
            error_type=None,
            exit_code=None,
            log_file=None,
        )
        interrupt_metadata = _interruption_metadata()
        # Save WIP changes before returning
        _save_wip_changes(task, worktree_git, config, branch_name)
        write_log_entry(
            log_file,
            {
                "type": "gza",
                "subtype": "interrupt",
                "message": "Task interrupted by signal",
                "failure_reason": failure_reason,
                **interrupt_metadata,
            },
        )
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            branch=branch_name,
            interrupt_signal=_interrupt_signal_name(),
            interrupted=True,
            error_type=None,
            exit_code=None,
        )
        console.print("\nInterrupted")
        return 130


def _run_non_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
    git: Git | None = None,
    resume: bool = False,
    open_after: bool = False,
    invocation: RunInvocationContext | None = None,
    interaction_mode: str = "observe_only",
) -> int:
    """Run a non-code task (explore, plan, review, internal) in a worktree (no branch creation).

    Args:
        task: Task to run
        config: Configuration object
        store: Task store
        provider: AI provider
        git: Git instance for the main repository
        resume: If True, resume from previous session
        open_after: If True, open the report file in $EDITOR after completion
    """
    if resume and task.session_id:
        console.print(f"Resuming with session: [dim]{task.session_id[:12]}...[/dim]")

    # Setup logging using the canonical task log path selected during preflight.
    if task.log_file:
        log_file = config.project_dir / Path(task.log_file)
    else:
        config.log_path.mkdir(parents=True, exist_ok=True)
        log_file = config.log_path / f"{task.slug}.log"
        task.log_file = str(log_file.relative_to(config.project_dir))
        store.update(task)

    # Write orchestration pre-run entries
    write_worker_start_event(log_file, resumed=resume)
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Task: {task.id} {task.slug}"})
    write_log_entry(log_file, {"type": "gza", "subtype": "info", "message": f"Provider: {provider.name}, Model: {config.model or 'default'}"})
    write_execution_provenance_event(
        log_file,
        invocation=invocation or _resolve_default_invocation_context(),
        provider=provider,
        interaction_mode=interaction_mode,
        resumed=resume,
    )

    # Setup report file based on task type
    report_path, _ = get_task_output_paths(task, config.project_dir)
    assert report_path is not None, f"Non-code task type '{task.task_type}' must have a report path"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    report_file_relative = str(report_path.relative_to(config.project_dir))

    # Create worktree in /tmp for Docker compatibility on macOS
    assert task.slug is not None
    worktree_path = config.worktree_path / f"{task.slug}-{task.task_type}"

    try:
        # Get default branch to base worktree on
        default_branch = git.default_branch() if git else "main"

        # Remove existing worktree if it exists
        if worktree_path.exists() and git:
            git.worktree_remove(worktree_path, force=True)

        # For review tasks with depends_on, check if we should run on the implementation branch
        base_ref = None
        if task.task_type == "review" and task.depends_on:
            dep_task = store.get(task.depends_on)
            if dep_task and dep_task.branch and dep_task.status == "completed":
                # Run review on the implementation branch
                base_ref = dep_task.branch
                console.print(f"Running review on implementation branch: [blue]{base_ref}[/blue]")

        # Default to origin/default_branch or local default_branch
        if not base_ref:
            base_ref = f"origin/{default_branch}"
            if git:
                git_result = git._run("rev-parse", "--verify", base_ref, check=False)
                if git_result.returncode != 0:
                    base_ref = default_branch  # Fall back to local branch

        # Create worktree without creating a new branch (use --detach to check out HEAD)
        # This creates a worktree in detached HEAD state based on the specified ref
        console.print(f"Creating worktree: {worktree_path}")
        if git:
            git._run("worktree", "add", "--detach", str(worktree_path), base_ref)

        # Create report directory structure in worktree
        worktree_report_dir = worktree_path / report_path.parent.relative_to(config.project_dir)
        worktree_report_dir.mkdir(parents=True, exist_ok=True)
        worktree_report_path = worktree_path / report_path.relative_to(config.project_dir)

        # For Docker containers, use /workspace-relative path instead of host worktree path
        # The container only has /workspace mounted, so we need to use a path inside that
        # For native mode, use the actual worktree path
        if config.use_docker:
            prompt_report_path = Path("/workspace") / report_path.relative_to(config.project_dir)
        else:
            prompt_report_path = worktree_report_path

        # Ensure all bundled skills are available in the worktree
        from .skills_utils import ensure_all_skills
        skills_dir = worktree_path / ".claude" / "skills"
        n_installed = ensure_all_skills(skills_dir)
        if n_installed:
            console.print(f"Installed {n_installed} skill(s) into worktree")

        _snapshot_task_db_to_worktree(_resolve_task_db_path(config), worktree_path)

        # Internal orchestration tasks do not implicitly consume learnings context.
        if task.task_type not in ("internal", "learn"):
            _copy_learnings_to_worktree(config, worktree_path)

        if not config.use_docker:
            _create_local_dep_symlinks(config, worktree_path)

        # Run provider in the worktree
        if resume:
            prompt = PromptBuilder().resume_prompt(
                task_id=task.id,
                task_slug=task.slug,
                report_path=prompt_report_path,
            )
        else:
            review_verify_result = None
            verify_command = (
                config.verify_command
                if isinstance(config.verify_command, str) and config.verify_command.strip()
                else None
            )
            if task.task_type == "review" and verify_command is not None:
                review_verify_timeout_seconds = getattr(
                    config,
                    "review_verify_timeout_seconds",
                    REVIEW_VERIFY_TIMEOUT_SECONDS,
                )
                if not isinstance(review_verify_timeout_seconds, int) or review_verify_timeout_seconds < 1:
                    review_verify_timeout_seconds = REVIEW_VERIFY_TIMEOUT_SECONDS
                review_verify_result = _run_review_verify_command(
                    verify_command,
                    cwd=worktree_path,
                    timeout_seconds=review_verify_timeout_seconds,
                )
            prompt = build_prompt(
                task,
                config,
                store,
                report_path=prompt_report_path,
                git=git,
                review_verify_result=review_verify_result,
            )

        def _on_session_id_non_code(session_id: str) -> None:
            """Persist session_id as soon as it is first seen during streaming."""
            if task.session_id == session_id:
                return
            task.session_id = session_id
            store.update(task)

        def _on_step_count_non_code(count: int) -> None:
            """Update task.num_steps_computed in real time during streaming."""
            task.num_steps_computed = count
            store.update(task)

        # When running in Docker, the worktree .git file contains a host-specific
        # gitdir path that is invalid inside the container.  Hide it before the
        # provider run and restore it afterwards so the host worktree stays valid.
        host_git_file = worktree_path / ".git"
        hidden_git_file = worktree_path / ".git.gza-host-worktree"
        hide_git = config.use_docker and host_git_file.is_file()
        if hide_git:
            host_git_file.rename(hidden_git_file)

        try:
            provider_run_kwargs: dict[str, Any] = {
                "resume_session_id": task.session_id if resume else None,
                "on_session_id": _on_session_id_non_code,
                "on_step_count": _on_step_count_non_code,
            }
            if interaction_mode == "interactive":
                provider_run_kwargs["interactive"] = True
            if _provider_accepts_ops_log_file(provider):
                provider_run_kwargs["ops_log_file"] = resolve_ops_log_path(config, log_file)
            provider_prompt = sanitize_provider_prompt(prompt, task_type=task.task_type)
            result = _call_provider_run(
                provider,
                config,
                provider_prompt,
                log_file,
                worktree_path,
                provider_run_kwargs=provider_run_kwargs,
            )
        except KeyboardInterrupt:
            failure_reason = _resolve_failure_reason(
                interrupt_signal=_interrupt_signal_name(),
                interrupted=True,
                error_type=None,
                exit_code=None,
                log_file=None,
            )
            interrupt_metadata = _interruption_metadata()
            write_log_entry(
                log_file,
                {
                    "type": "gza",
                    "subtype": "interrupt",
                    "message": "Task interrupted by signal",
                    "failure_reason": failure_reason,
                    **interrupt_metadata,
                },
            )
            _mark_task_failed(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                interrupt_signal=_interrupt_signal_name(),
                interrupted=True,
                error_type=None,
                exit_code=None,
            )
            console.print("\nInterrupted")
            return 130
        finally:
            if hide_git and hidden_git_file.exists():
                hidden_git_file.rename(host_git_file)
        exit_code = result.exit_code
        stats = _run_result_to_stats(result)
        assert task.id is not None
        has_step_events = _persist_run_steps_from_result(store, task.id, provider.name.lower(), result)
        if has_step_events:
            task.log_schema_version = 2

        # Store session_id if available and not already persisted by _on_session_id_non_code callback
        if result.session_id and result.session_id != task.session_id:
            task.session_id = result.session_id
            store.update(task)

        resolved_failure = _resolve_run_failure(
            provider_name=provider.name,
            timeout_minutes=config.timeout_minutes,
            step_limit=config.max_steps,
            turn_limit=config.max_turns,
            error_type=result.error_type,
            exit_code=exit_code,
            log_file=log_file,
            stats=stats,
        )

        if resolved_failure is not None:
            _record_run_failure(
                task=task,
                config=config,
                store=store,
                log_file=log_file,
                stats=stats,
                failure=resolved_failure,
                exit_code=exit_code,
                worktree=worktree_path,
            )
            return 0

        # Copy expected report artifact from worktree to main project directory.
        # For non-code tasks, provider success requires this file contract.
        recovered_from_log = False
        if not worktree_report_path.exists():
            # Before failing, try to recover content from the provider's 'result' log entry.
            # Agents sometimes output the review/report as text rather than writing the file.
            recovered_content = extract_content_from_log(log_file)
            if recovered_content:
                logger.warning(
                    "Task %s: expected report artifact missing; recovering content from provider log",
                    task.slug,
                )
                console.print(
                    "[yellow]Warning: expected report artifact was not created; "
                    "recovering content from provider log[/yellow]"
                )
                # Write the recovered content into the worktree path so the copy-back
                # logic below proceeds as if the agent had written it normally.
                worktree_report_path.write_text(recovered_content)
                recovered_from_log = True
            else:
                expected_relative = str(worktree_report_path.relative_to(worktree_path))
                stale_candidates = sorted(
                    path.relative_to(worktree_path)
                    for path in worktree_report_dir.glob("*.md")
                    if path != worktree_report_path
                )
                if len(stale_candidates) == 1:
                    actual_relative = stale_candidates[0]
                    logger.warning(
                        "Task %s: expected report artifact %s missing; recovering from mismatched file %s",
                        task.slug,
                        expected_relative,
                        actual_relative,
                    )
                    console.print(
                        "[yellow]Warning: expected report artifact "
                        f"{expected_relative} was not created; recovering from mismatched file "
                        f"{actual_relative}[/yellow]"
                    )
                    recovered_content = (worktree_path / actual_relative).read_text()
                    worktree_report_path.write_text(recovered_content)
                else:
                    mismatch_note = (
                        f" (found other report files: {', '.join(str(p) for p in stale_candidates)})"
                        if stale_candidates
                        else ""
                    )
                    failure_message = (
                        f"Outcome: failed (missing report artifact: expected {expected_relative}{mismatch_note})"
                    )
                    console.print(f"Expected report file: [yellow]{report_file_relative}[/yellow]")
                    if stale_candidates:
                        console.print(
                            "Detected report files with other names in worktree "
                            f"(possible stale resume session state): {', '.join(str(p) for p in stale_candidates)}"
                        )
                    console.print(f"See log file for details: {log_file.relative_to(config.project_dir)}")
                    task_footer(
                        task,
                        stats,
                        status="Failed: expected report artifact was not created",
                        worktree=worktree_path,
                        store=store,
                    )
                    write_log_entry(
                        log_file,
                        {
                            "type": "gza",
                            "subtype": "outcome",
                            "message": failure_message,
                            "exit_code": exit_code,
                            "failure_reason": "MISSING_REPORT_ARTIFACT",
                        },
                    )
                    _write_stats_entry(log_file, stats)
                    _mark_task_failed(
                        task=task,
                        config=config,
                        store=store,
                        log_file=log_file,
                        stats=stats,
                        explicit_reason="MISSING_REPORT_ARTIFACT",
                        error_type=None,
                        exit_code=exit_code,
                    )
                    return 0

        console.print(f"Report written to: {report_file_relative}")
        # Ensure target directory exists
        report_path.parent.mkdir(parents=True, exist_ok=True)
        # Copy report content from worktree to project dir
        report_path.write_text(worktree_report_path.read_text())

        # Read output content for storage in DB
        output_content = report_path.read_text()
        if task.task_type == "review":
            task.review_score = compute_review_score(parse_review_template(output_content))
            contract_validation = validate_review_report_contract(output_content)
            if contract_validation.blockers_missing_open_state_citation:
                blocker_ids = ", ".join(contract_validation.blockers_missing_open_state_citation)
                warning_message = (
                    f"Review contract warning: blockers missing open-state citations: {blocker_ids}"
                )
                logger.warning(warning_message)
                console.print(f"[yellow]{warning_message}[/yellow]")
            if contract_validation.blockers_with_malformed_open_state_citation:
                blocker_ids = ", ".join(contract_validation.blockers_with_malformed_open_state_citation)
                warning_message = (
                    "Review contract warning: blockers with malformed open-state citations: "
                    f"{blocker_ids}"
                )
                logger.warning(warning_message)
                console.print(f"[yellow]{warning_message}[/yellow]")

        # Clean up non-code worktree on success — report has been copied back, no further use
        if git:
            try:
                git.worktree_remove(worktree_path, force=True)
                if worktree_path.exists():
                    shutil.rmtree(worktree_path, ignore_errors=True)
            except GitError:
                logger.warning("Failed to remove worktree %s", worktree_path)
                if worktree_path.exists():
                    shutil.rmtree(worktree_path, ignore_errors=True)

        # Write final log entries before marking completed in DB, so that
        # `gza log -f` doesn't break out of the follow loop prematurely.
        outcome_msg = "Outcome: completed (recovered from provider log)" if recovered_from_log else "Outcome: completed"
        write_log_entry(log_file, {"type": "gza", "subtype": "outcome", "message": outcome_msg, "exit_code": 0})
        write_log_entry(log_file, {"type": "gza", "subtype": "stats", "message": f"Stats: {stats.num_steps_computed or stats.num_steps_reported or 0} steps, {stats.duration_seconds or 0.0:.1f}s, ${stats.cost_usd or 0.0:.4f}", "duration_seconds": stats.duration_seconds, "cost_usd": stats.cost_usd, "num_steps": stats.num_steps_computed or stats.num_steps_reported or 0})

        # Mark completed — after log entries are flushed.
        store.mark_completed(
            task,
            branch=None,
            log_file=str(log_file.relative_to(config.project_dir)),
            report_file=report_file_relative,
            output_content=output_content,
            has_commits=False,
            stats=stats,
        )
        auto_learnings = None
        if task.task_type not in ("internal", "learn") and not task.skip_learnings:
            auto_learnings = maybe_auto_regenerate_learnings(store, config)

        # For review tasks, post to PR if applicable
        if task.task_type == "review" and task.depends_on:
            impl_task = store.get(task.depends_on)
            if impl_task:
                post_review_to_pr(task, impl_task, store, config.project_dir, required=False)

        verdict: str | None = None
        if task.task_type == "review":
            verdict = _extract_review_verdict(output_content)

        console.print("")
        task_footer(
            task,
            stats,
            status="Done",
            report=report_file_relative,
            verdict=verdict,
            learnings=auto_learnings,
            store=store,
        )

        # Open review file in $EDITOR if requested
        if open_after and task.task_type == "review" and report_path.exists():
            import os
            import subprocess

            editor = os.environ.get("EDITOR")
            if editor:
                try:
                    console.print(f"\nOpening review in {editor}...")
                    subprocess.run([editor, str(report_path)], check=True)
                except subprocess.CalledProcessError as e:
                    console.print(f"[yellow]Warning: Failed to open editor: {e}[/yellow]")
                except FileNotFoundError:
                    console.print(f"[yellow]Warning: Editor '{editor}' not found[/yellow]")
            else:
                console.print("[yellow]Warning: $EDITOR not set, skipping auto-open[/yellow]")

        return 0

    except GitError as e:
        error_message(f"Git error: {e}")
        _mark_task_failed(
            task=task,
            config=config,
            store=store,
            log_file=log_file,
            explicit_reason="GIT_ERROR",
            error_type=None,
            exit_code=1,
        )
        return 1
