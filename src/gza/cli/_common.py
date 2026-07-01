"""Shared helpers, constants, and lightweight utilities used across CLI sub-modules."""

import argparse
import contextlib
import json
import os
import platform
import re
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn

from rich.markup import escape as rich_escape
from rich.pager import Pager
from rich.panel import Panel

from ..artifact_paths import resolve_artifact_path
from ..artifacts import store_command_output_artifact
from ..branch_resolution import (
    resolve_rebase_base_branch,
    resolve_rebase_target_branch,
    resolve_rebase_target_task,
)
from ..concurrency import (
    LaunchPermit,
    launch_permit,
    reserve_task_launch_permit,
    take_task_launch_permit,
)
from ..config import Config
from ..console import (
    MAX_PROMPT_DISPLAY,
    console,
    truncate,
)
from ..db import (
    TASK_COMMENT_KIND_FEEDBACK,
    DuplicateActiveChildError,
    ManualMigrationRequired,
    SqliteTaskStore,
    StoreOpenMode,
    Task as DbTask,
    merge_unit_membership_role,
    resolve_task_id,
    task_id_numeric_key,
    task_owns_merge_status,
)
from ..derived_tags import resolve_derived_task_tags
from ..failure_policy import is_resumable_failure_reason
from ..failure_reasons import mark_task_failed_from_cause
from ..lineage import resolve_impl_task
from ..log_paths import ops_log_path_for
from ..operator_state import blocked_dependency_error_message, inspect_empty_merge_unit
from ..plan_review_materialization import (
    PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
    PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
    build_plan_review_slice_task_specs,
    inspect_plan_review_materialization_for_repair,
    load_materialized_plan_slice_set,
    plan_review_manifest_digest,
)
from ..plan_review_verdict import (
    PlanReviewManifest,
    PlanReviewValidationError,
    get_plan_review_outcome,
    validate_plan_review_manifest,
)
from ..prompts import PromptBuilder
from ..recovery_engine import FailedRecoveryDecision, classify_recovery_row, decide_failed_task_recovery
from ..review_scope import extract_review_scope_from_prompt
from ..review_tasks import (
    DuplicateReviewError,  # noqa: F401
    create_or_reuse_deferred_blocker_task,
    create_or_reuse_followup_task,
    create_or_reuse_review_blocker_adjudication_task,
    create_review_task,
)
from ..review_verdict import (
    ReviewFinding,
    format_review_outcome as _format_review_outcome,
    get_review_outcome as _get_review_outcome,
    get_review_score as _get_review_score,
    get_review_verdict as _get_review_verdict,
    parse_review_verdict,
)
from ..runner import (
    DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE,
    RunInvocationContext,
    TaskDispatchBlockedError,
    _ensure_task_dispatchable_for_startup,
    get_effective_config_for_task,
    prepare_task_startup_phase,
    remove_task_startup_artifacts,
    run,
    write_ops_entry,
)
from ..status_ops import apply_manual_task_status
from ..task_types import CLI_FILTER_TASK_TYPES
from ..tmux_proxy import get_tmux_session_pid
from ..workers import WorkerMetadata, WorkerRegistry

_REUSE_WORKER_OWNER_ENV = "GZA_REUSE_WORKER_OWNER"
_REUSE_WORKER_OWNER_OUTER = "outer"
_REUSE_WORKER_REENTRY_ENV = "GZA_REUSE_WORKER_REENTRY"
_REUSE_WORKER_SESSION_ENV = "GZA_REUSE_WORKER_SESSION"
_PLAN_REVIEW_OVERRIDE_ARTIFACT_KIND = "plan_review_manifest_override"
PLAN_REVIEW_MATERIALIZATION_AUTO_REPAIR_DROP_REASON = "plan-review-materialization-auto-repair"


def _prepare_startup_phase(
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    *,
    resume_mode: bool = False,
) -> DbTask:
    """Call startup preparation without widening the default monkeypatch signature."""
    if resume_mode:
        return prepare_task_startup_phase(config, store, task, resume_mode=True)
    return prepare_task_startup_phase(config, store, task)


@dataclass(frozen=True)
class PlanReviewMaterializationResult:
    """Materialized or reused implementation slice set for an approved plan review."""

    tasks: list[DbTask]
    created: bool


@dataclass(frozen=True)
class EffectivePlanReviewManifestState:
    """Resolved effective manifest state for a completed plan review."""

    manifest: PlanReviewManifest | None
    source: str
    verdict: str | None
    validation_error: str | None = None


def enable_held_plan_source_auto_implement(plan_task: DbTask) -> bool:
    """Mark a held plan source for automatic implementation follow-up in memory."""
    if plan_task.auto_implement is True:
        return False
    plan_task.auto_implement = True
    return True


def release_held_plan_source(store: SqliteTaskStore, plan_task: DbTask) -> bool:
    """Persist automatic implementation follow-up for a held plan source.

    Returns True only when the stored state changed.
    """
    if not enable_held_plan_source_auto_implement(plan_task):
        return False
    store.update(plan_task)
    return True


def format_task_status_text(task: DbTask) -> str:
    """Return the inline status label used by lineage-oriented displays."""
    if task.status == "failed":
        if task.failure_reason and task.failure_reason != "UNKNOWN":
            return f"failed ({task.failure_reason})"
        return "failed"
    if task.status == "completed" and task.completion_reason:
        return f"completed ({task.completion_reason})"
    return task.status or "unknown"


def format_task_merge_label(task: DbTask) -> str:
    """Return the inline merge label for code-owning tasks."""
    if task.status != "completed":
        return ""
    if not task_owns_merge_status(task):
        return ""
    if task.merge_status == "merged":
        return "merged"
    if task.merge_status == "unmerged":
        return "unmerged"
    return ""


def get_task_status_color(task: DbTask) -> str:
    """Return the shared status color for task state displays."""
    return _colors.LINEAGE_STATUS_COLORS.get(task.status or "", _colors.STATUS_COLORS.unknown)


def _stdout_is_tty() -> bool:
    """Seam for tests: whether stdout is a terminal. Tests patch this, not ``sys.stdout.isatty``."""
    return sys.stdout.isatty()


def _default_interrupt_source(signum: int) -> str:
    """Return a stable source label when no explicit interrupt attribution exists."""
    if signum == signal.SIGTERM:
        return "external_sigterm_unknown"
    if signum == signal.SIGINT:
        return "external_sigint_unknown"
    try:
        signal_name = signal.Signals(signum).name.lower()
    except ValueError:
        signal_name = f"signal_{signum}"
    return f"external_{signal_name}"


def _set_interrupt_env_from_signal(
    *,
    registry: WorkerRegistry,
    pid: int,
    signum: int,
) -> None:
    """Populate interrupt env vars from an explicit request marker when available."""
    try:
        os.environ["GZA_INTERRUPT_SIGNAL"] = signal.Signals(signum).name
    except ValueError:
        os.environ["GZA_INTERRUPT_SIGNAL"] = str(signum)

    request = registry.consume_interrupt_request(pid)
    source = request.get("source") if request else None
    detail = request.get("detail") if request else None

    os.environ["GZA_INTERRUPT_SOURCE"] = source or _default_interrupt_source(signum)
    if detail:
        os.environ["GZA_INTERRUPT_DETAIL"] = detail
    else:
        os.environ.pop("GZA_INTERRUPT_DETAIL", None)


def get_store(config: Config, *, open_mode: StoreOpenMode = "readwrite") -> SqliteTaskStore:
    """Get the SQLite task store.

    Raises:
        ManualMigrationRequired: If the DB needs a manual schema upgrade.
            Callers should run ``gza migrate`` to fix this.
    """
    store = SqliteTaskStore.from_config(config, open_mode=open_mode)
    if open_mode == "query_only":
        for warning in store.startup_warnings():
            print(f"Warning: {warning}", file=sys.stderr)
    return store


def resolve_id(config: Config, arg: str) -> str:
    """Resolve a user-supplied task ID argument to a canonical string ID.

    Wraps :func:`gza.db.resolve_task_id` using the project prefix from config.
    """
    return resolve_task_id(arg, config.project_prefix)


def set_task_urgency(store: SqliteTaskStore, task_id: str, *, urgent: bool) -> bool:
    """Shared urgency update path for queue bump/unbump and add --next."""
    return store.set_urgent(task_id, urgent)


def set_task_queue_position(store: SqliteTaskStore, task_id: str, *, position: int) -> bool:
    """Shared explicit queue ordering path for queue move/next."""
    return store.set_queue_position(task_id, position)


def set_task_queue_position_scoped(
    store: SqliteTaskStore,
    task_id: str,
    *,
    position: int,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> bool:
    """Shared explicit queue ordering path for queue move/next with optional tag scope."""
    return store.set_queue_position(task_id, position, tags=tags, any_tag=any_tag)


def clear_task_queue_position(store: SqliteTaskStore, task_id: str) -> bool:
    """Shared explicit queue ordering clear path."""
    return store.clear_queue_position(task_id)


def clear_task_queue_position_scoped(
    store: SqliteTaskStore,
    task_id: str,
    *,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> bool:
    """Shared explicit queue ordering clear path with optional tag scope."""
    return store.clear_queue_position(task_id, tags=tags, any_tag=any_tag)


def _validate_tag_value(raw: object) -> str:
    """Return raw tag text when non-empty after trim, otherwise raise ValueError."""
    tag = str(raw)
    if not tag.strip():
        raise ValueError("tag must not be empty")
    return tag


def parse_cli_tag_filters(
    args: argparse.Namespace,
    *,
    tags_attr: str = "tags",
    all_tags_attr: str = "all_tags",
) -> tuple[tuple[str, ...] | None, bool]:
    """Parse and validate CLI tag filter flags from argparse args."""
    selected_tags = [_validate_tag_value(raw) for raw in (getattr(args, tags_attr, None) or [])]
    return (tuple(selected_tags) if selected_tags else None, not bool(getattr(args, all_tags_attr, False)))


def validate_cli_tag_values(values: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    """Validate CLI-provided tag values and return them as a tuple."""
    return tuple(_validate_tag_value(raw) for raw in (values or ()))


def format_no_runnable_message_for_tags(
    store: SqliteTaskStore,
    tags: tuple[str, ...],
    *,
    any_tag: bool = False,
    exhausted: bool = False,
) -> str:
    """Render precise tag-filtered empty-pickup messaging.

    Distinguishes between "no matching pending tasks" and
    "matching pending tasks exist but are not runnable", including
    dependency-blocked and internal-only pending matches.
    """
    tag_text = ", ".join(tags)
    matching_pending = store.get_pending(limit=None, tags=tags, any_tag=any_tag)
    if matching_pending:
        matching_non_internal = [task for task in matching_pending if task.task_type != "internal"]
        if matching_non_internal and all(store.is_task_blocked(task)[0] for task in matching_non_internal):
            if exhausted:
                return (
                    f"No more runnable tasks matching tags: {tag_text}. "
                    "Remaining matching pending tasks are blocked by dependencies."
                )
            return (
                f"No runnable tasks found matching tags: {tag_text}. "
                "Matching pending tasks are blocked by dependencies."
            )
        if exhausted:
            return (
                f"No more runnable tasks matching tags: {tag_text}. "
                "Remaining matching pending tasks are not runnable via work (for example internal tasks)."
            )
        return (
            f"No runnable tasks found matching tags: {tag_text}. "
            "Matching pending tasks are not runnable via work (for example internal tasks)."
        )
    if exhausted:
        return f"No more pending tasks matching tags: {tag_text}."
    return f"No pending tasks found matching tags: {tag_text}"


# Matches "{prefix}-{suffix}" where prefix is 1-12 lowercase alphanumeric chars.
# This is tighter than `"-" in arg` (which also matches branch names like "feature-foo").
_TASK_ID_RE = re.compile(r"^[a-z0-9]{1,12}-[0-9]+$")
_FAILURE_MARKER_LINE_RE = re.compile(r"^\s*\[GZA_FAILURE:(?P<reason>[A-Z0-9_]+)\]\s*$")
def _looks_like_task_id(arg: str) -> bool:
    """Return True if *arg* looks like a task ID rather than a branch name.

    Matches only full prefixed decimal IDs, e.g. ``"gza-1234"``.
    """
    return bool(_TASK_ID_RE.match(arg))


def _task_looks_stuck(config: Config, task: DbTask) -> bool:
    """Return True if an in-progress task has not logged recently enough.

    A task is considered stuck when its process has been alive for more than
    the threshold and its log file is either missing, empty, or has not been
    written to within the threshold.  This catches preflight hangs such as a
    CLI blocking on an update/login prompt.
    """
    return _task_is_silent_past_timeout(config, task)


def _candidate_activity_log_paths(config: Config, task: DbTask, worker: WorkerMetadata | None = None) -> list[Path]:
    """Return task/worker log paths that count as startup or execution evidence."""
    paths: list[Path] = []
    seen: set[Path] = set()
    for rel_path in (task.log_file, worker.startup_log_file if worker is not None else None):
        if not rel_path:
            continue
        candidate = config.project_dir / rel_path
        if candidate in seen:
            continue
        seen.add(candidate)
        paths.append(candidate)
    return paths


def _task_started_at(task: DbTask, worker: WorkerMetadata | None = None) -> datetime | None:
    """Return the best available startup timestamp for reconciliation."""
    if task.started_at is not None:
        return task.started_at
    if worker is None:
        return None
    return _normalize_timestamp(worker.started_at)


def _task_is_silent_past_timeout(
    config: Config,
    task: DbTask,
    worker: WorkerMetadata | None = None,
) -> bool:
    """Return whether the task/worker pair has been silent longer than the threshold."""
    threshold = config.watch.no_activity_timeout
    started_at = _task_started_at(task, worker)
    if started_at is None:
        return False
    now = datetime.now(UTC)
    age = (now - started_at).total_seconds()
    if age <= threshold:
        return False
    stats: list[os.stat_result] = []
    latest_mtime = 0.0
    for log_path in _candidate_activity_log_paths(config, task, worker):
        for candidate in (log_path, ops_log_path_for(log_path)):
            try:
                stat = candidate.stat()
            except OSError:
                continue
            stats.append(stat)
            latest_mtime = max(latest_mtime, stat.st_mtime)
    if not stats:
        return True
    total_size = sum(stat.st_size for stat in stats)
    if total_size == 0:
        return True
    mtime_age = now.timestamp() - latest_mtime
    return mtime_age > threshold


def _running_workers_by_task_id(registry: WorkerRegistry) -> dict[str, WorkerMetadata]:
    """Index running-status worker entries by task ID."""
    workers_by_task_id: dict[str, WorkerMetadata] = {}
    for worker in registry.list_all():
        if worker.task_id is None or worker.status != "running":
            continue
        workers_by_task_id.setdefault(str(worker.task_id), worker)
    return workers_by_task_id


def _latest_workers_by_task_id(registry: WorkerRegistry) -> dict[str, WorkerMetadata]:
    """Index the latest worker entry per task ID, including completed/failed rows."""
    workers_by_task_id: dict[str, WorkerMetadata] = {}
    for worker in registry.list_all(include_completed=True):
        if worker.task_id is None:
            continue
        key = str(worker.task_id)
        existing = workers_by_task_id.get(key)
        if existing is None:
            workers_by_task_id[key] = worker
            continue
        existing_time = _normalize_timestamp(existing.completed_at or existing.started_at)
        worker_time = _normalize_timestamp(worker.completed_at or worker.started_at)
        if worker_time is not None and (existing_time is None or worker_time >= existing_time):
            workers_by_task_id[key] = worker
    return workers_by_task_id


def _task_worker_is_dead(task: DbTask, worker: WorkerMetadata | None, registry: WorkerRegistry | None) -> bool:
    """Return whether the claimed worker for a task is dead/stale."""
    if worker is not None and registry is not None:
        return not registry.is_running(worker.worker_id)
    if task.running_pid is None:
        return task.started_at is not None
    if task.running_pid <= 0:
        return True
    try:
        os.kill(task.running_pid, 0)
        return False
    except OSError:
        return True


def _mark_worker_reconciled(
    registry: WorkerRegistry | None,
    worker: WorkerMetadata | None,
    *,
    completion_reason: str,
) -> None:
    """Stop a stale running worker entry from counting toward capacity."""
    if registry is None or worker is None or worker.status != "running":
        return
    registry.mark_completed(
        worker.worker_id,
        exit_code=worker.exit_code if worker.exit_code is not None else 1,
        status="failed",
        completion_reason=completion_reason,
    )


def _signal_name_from_exit_code(exit_code: int | None) -> str | None:
    """Return the terminating signal name for a negative subprocess exit code."""
    if exit_code is None or exit_code >= 0:
        return None
    signum = -exit_code
    try:
        return signal.Signals(signum).name
    except ValueError:
        return f"SIG{signum}"


def _signal_number_from_name(signal_name: str | None) -> int | None:
    """Resolve a signal name like ``SIGKILL`` to its platform signal number."""
    if not signal_name:
        return None
    try:
        return int(getattr(signal, signal_name))
    except (AttributeError, TypeError, ValueError):
        return None


def _load_jsonl_entries(path: Path) -> list[dict[str, Any]]:
    """Best-effort JSONL reader for task conversation or ops logs."""
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    if not content.strip():
        return []

    entries: list[dict[str, Any]] = []
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            entries.append(parsed)
    return entries


def _worker_log_candidates(
    config: Config,
    task: DbTask,
    worker: WorkerMetadata | None,
) -> tuple[Path, ...]:
    """Return candidate task/startup logs that may contain worker lifecycle evidence."""
    candidates: list[Path] = []

    def _append(path_value: str | Path | None) -> None:
        if path_value is None:
            return
        path = Path(path_value)
        if not path.is_absolute():
            path = config.project_dir / path
        if path not in candidates:
            candidates.append(path)

    _append(task.log_file)
    if worker is not None:
        _append(worker.startup_log_file)
    _append(startup_log_path_for_task(config, task))
    return tuple(candidates)


def _sort_entries_by_timestamp(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort structured log entries by timestamp when available while preserving source order."""
    indexed = list(enumerate(entries))
    indexed.sort(key=lambda item: (str(item[1].get("timestamp", "")), item[0]))
    return [entry for _, entry in indexed]


def _load_worker_log_entries(
    config: Config,
    task: DbTask,
    worker: WorkerMetadata | None,
) -> list[dict[str, Any]]:
    """Load merged task/startup structured entries for worker diagnostics."""
    merged: list[dict[str, Any]] = []
    for log_path in _worker_log_candidates(config, task, worker):
        ops_path = ops_log_path_for(log_path)
        source_path = ops_path if ops_path.exists() else log_path
        merged.extend(_load_jsonl_entries(source_path))
    return _sort_entries_by_timestamp(merged)


@dataclass(frozen=True)
class WorkerDeathDiagnostics:
    """Best-effort diagnostics explaining why a worker disappeared."""

    stage: str | None = None
    exit_code: int | None = None
    signal_name: str | None = None
    signal_number: int | None = None
    output_tail: tuple[str, ...] = ()
    os_hint: str | None = None
    source_event: str | None = None
    worker_status: str | None = None
    completion_reason: str | None = None


@dataclass(frozen=True)
class WorkerDeathCaptureResult:
    """Best-effort reconciliation payload for WORKER_DIED terminalization."""

    log_path: Path | None = None
    failure_log_file: str | None = None
    diagnostics: WorkerDeathDiagnostics = WorkerDeathDiagnostics()


def _classify_worker_death_stage(entries: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    """Infer the furthest worker lifecycle stage reached before death."""
    saw_preflight = False
    saw_worker_start = False
    saw_provider_exec = False
    saw_execution_provenance = False
    saw_task_claim = False

    for entry in entries:
        if entry.get("type") != "gza":
            continue
        subtype = entry.get("subtype")
        event = entry.get("event")
        if event == "verify_credentials_docker":
            saw_preflight = True
        if subtype == "worker_lifecycle" and event == "start":
            saw_worker_start = True
        if subtype == "command" and event == "provider_exec_start":
            saw_provider_exec = True
        if subtype == "execution":
            saw_execution_provenance = True
        if subtype == "info":
            message = entry.get("message")
            if isinstance(message, str) and message.startswith("Task: "):
                saw_task_claim = True

    if saw_provider_exec:
        return "provider_exec", "provider_exec_start"
    if saw_worker_start or saw_execution_provenance or saw_task_claim:
        return "after_worker_start_before_provider_exec", "worker_lifecycle/start"
    if saw_preflight:
        return "after_preflight_before_worker_start", "verify_credentials_docker"
    return "before_preflight_or_no_logs", None


def _collect_output_tail_from_entries(entries: list[dict[str, Any]], *, limit: int = 8) -> tuple[str, ...]:
    """Collect a short output tail from structured provider/process entries."""
    tail: list[str] = []
    for entry in reversed(entries):
        message: str | None = None
        subtype = entry.get("subtype")
        if subtype == "process_output":
            raw = entry.get("provider_output") or entry.get("message")
            if isinstance(raw, str) and raw.strip():
                message = raw.strip()
        elif entry.get("type") == "result":
            raw = entry.get("result")
            if isinstance(raw, str) and raw.strip():
                message = raw.strip()
        elif entry.get("type") == "gza" and subtype in {"info", "branch", "outcome", "blocked"}:
            raw = entry.get("message")
            if isinstance(raw, str) and raw.strip():
                message = raw.strip()
        if message:
            tail.append(message)
        if len(tail) >= limit:
            break
    return tuple(reversed(tail))


def _collect_output_tail_from_text(path: Path, *, limit: int = 8) -> tuple[str, ...]:
    """Collect a short raw-text tail from a startup capture file."""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ()
    non_empty = [line.strip() for line in lines if line.strip()]
    return tuple(non_empty[-limit:])


def _resolve_worker_log_path(config: Config, task: DbTask, worker: WorkerMetadata | None) -> Path | None:
    """Pick the best log path to inspect or annotate for worker death diagnostics."""
    if task.log_file:
        task_log = Path(task.log_file)
        return task_log if task_log.is_absolute() else config.project_dir / task_log
    if worker and worker.startup_log_file:
        startup_log = Path(worker.startup_log_file)
        return startup_log if startup_log.is_absolute() else config.project_dir / startup_log
    return startup_log_path_for_task(config, task)


def _darwin_worker_death_hint(
    *,
    pid: int | None,
    reference_time: datetime | None,
) -> str | None:
    """Return a best-effort macOS log hint for worker death root cause."""
    if platform.system() != "Darwin":
        return None
    end = reference_time or datetime.now(UTC)
    start = end - timedelta(minutes=3)
    predicate = (
        'process == "kernel" OR process == "powerd" OR process == "launchd" '
        'OR eventMessage CONTAINS[c] "jetsam" '
        'OR eventMessage CONTAINS[c] "memorystatus" '
        'OR eventMessage CONTAINS[c] "sleep" '
        'OR eventMessage CONTAINS[c] "wake" '
        'OR eventMessage CONTAINS[c] "out of memory"'
    )
    try:
        result = subprocess.run(
            [
                "log",
                "show",
                "--style",
                "compact",
                "--start",
                start.isoformat(),
                "--end",
                end.isoformat(),
                "--predicate",
                predicate,
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if pid is not None:
        pid_token = f"[{pid}]"
        pid_lines = [line for line in lines if pid_token in line or f" pid {pid} " in line or f"pid {pid}" in line]
        if pid_lines:
            return f"darwin log hint (best effort): {truncate(pid_lines[-1], 220)}"

    for line in reversed(lines):
        lowered = line.lower()
        if any(token in lowered for token in ("jetsam", "memorystatus", "out of memory", "sleep", "wake")):
            return f"darwin log hint (best effort): {truncate(line, 220)}"
    return None


def _collect_worker_death_diagnostics(
    config: Config,
    task: DbTask,
    worker: WorkerMetadata | None,
) -> WorkerDeathDiagnostics:
    """Collect best-effort diagnostics for a dead worker without raising."""
    log_path = _resolve_worker_log_path(config, task, worker)
    ops_entries = _load_worker_log_entries(config, task, worker)

    stage, source_event = _classify_worker_death_stage(ops_entries)
    output_tail = _collect_output_tail_from_entries(ops_entries)
    if not output_tail and log_path is not None:
        output_tail = _collect_output_tail_from_text(log_path)
    if not output_tail:
        for candidate in reversed(_worker_log_candidates(config, task, worker)):
            output_tail = _collect_output_tail_from_text(candidate)
            if output_tail:
                break

    event_exit_code: int | None = None
    event_signal_name: str | None = None
    event_signal_number: int | None = None
    event_worker_status: str | None = None
    event_completion_reason: str | None = None
    for entry in reversed(ops_entries):
        if entry.get("type") != "gza" or entry.get("subtype") != "worker_lifecycle":
            continue
        if entry.get("event") not in {
            "death_detected",
            "startup_abort_detected",
            "detached_exit",
            "start_failed",
            "handoff_failed",
        }:
            continue
        exit_code_raw = entry.get("exit_code")
        event_exit_code = exit_code_raw if isinstance(exit_code_raw, int) else None
        event_signal_name = entry.get("signal") if isinstance(entry.get("signal"), str) else None
        signal_number_raw = entry.get("signal_number")
        event_signal_number = (
            signal_number_raw
            if isinstance(signal_number_raw, int)
            else _signal_number_from_name(event_signal_name)
        )
        event_worker_status = entry.get("worker_status") if isinstance(entry.get("worker_status"), str) else None
        event_completion_reason = (
            entry.get("completion_reason") if isinstance(entry.get("completion_reason"), str) else None
        )
        break

    exit_code = worker.exit_code if worker is not None else None
    if exit_code is None:
        exit_code = event_exit_code
    signal_name = _signal_name_from_exit_code(exit_code) if exit_code is not None else None
    if signal_name is None:
        signal_name = event_signal_name
    signal_number = _signal_number_from_name(signal_name)
    if signal_number is None:
        signal_number = event_signal_number

    reference_time = None
    if worker is not None and worker.completed_at:
        reference_time = _normalize_timestamp(worker.completed_at)
    if reference_time is None and task.completed_at:
        reference_time = _normalize_timestamp(task.completed_at)

    os_hint = _darwin_worker_death_hint(
        pid=worker.pid if worker is not None else task.running_pid,
        reference_time=reference_time,
    )

    return WorkerDeathDiagnostics(
        stage=stage,
        exit_code=exit_code,
        signal_name=signal_name,
        signal_number=signal_number,
        output_tail=output_tail,
        os_hint=os_hint,
        source_event=source_event,
        worker_status=(worker.status if worker is not None else None) or event_worker_status,
        completion_reason=(worker.completion_reason if worker is not None else None) or event_completion_reason,
    )


def _worker_death_failure_log_file(
    config: Config,
    task: DbTask,
    worker: WorkerMetadata | None,
    log_path: Path | None,
) -> str | None:
    """Choose the log file path that should remain visible on the failed task."""
    if task.log_file:
        return task.log_file
    if worker is not None and worker.startup_log_file:
        return worker.startup_log_file
    if log_path is None:
        return None
    try:
        return str(log_path.relative_to(config.project_dir))
    except ValueError:
        return str(log_path)


def _fallback_worker_death_diagnostics(worker: WorkerMetadata | None) -> WorkerDeathDiagnostics:
    """Minimal diagnostics used when best-effort capture fails."""
    exit_code = worker.exit_code if worker is not None else None
    signal_name = _signal_name_from_exit_code(exit_code)
    return WorkerDeathDiagnostics(
        exit_code=exit_code,
        signal_name=signal_name,
        signal_number=_signal_number_from_name(signal_name),
        worker_status=worker.status if worker is not None else None,
        completion_reason=worker.completion_reason if worker is not None else None,
    )


def _capture_worker_death_best_effort(
    *,
    config: Config,
    task: DbTask,
    worker: WorkerMetadata | None,
    event: str,
    warning_context: str,
) -> WorkerDeathCaptureResult:
    """Capture worker-death diagnostics without letting capture failure block terminalization."""
    log_path: Path | None = None
    diagnostics = _fallback_worker_death_diagnostics(worker)

    try:
        log_path = _resolve_worker_log_path(config, task, worker)
    except Exception as exc:
        print(
            f"Warning: Failed to resolve worker-death log path for {warning_context}: {exc}",
            file=sys.stderr,
        )

    try:
        diagnostics = _collect_worker_death_diagnostics(config, task, worker)
    except Exception as exc:
        print(
            f"Warning: Failed to collect worker-death diagnostics for {warning_context}: {exc}",
            file=sys.stderr,
        )

    if log_path is not None:
        try:
            _write_worker_death_ops_event(log_path, task=task, worker=worker, diagnostics=diagnostics, event=event)
        except Exception as exc:
            print(
                f"Warning: Failed to write worker-death diagnostics for {warning_context}: {exc}",
                file=sys.stderr,
            )

    return WorkerDeathCaptureResult(
        log_path=log_path,
        failure_log_file=_worker_death_failure_log_file(config, task, worker, log_path),
        diagnostics=diagnostics,
    )


def _worker_lifecycle_entry_indicates_death(entry: Mapping[str, object]) -> bool:
    """Return whether a worker_lifecycle entry records concrete worker-death evidence."""
    if entry.get("type") != "gza" or entry.get("subtype") != "worker_lifecycle":
        return False

    event = entry.get("event")
    if event in {"death_detected", "startup_abort_detected", "start_failed", "handoff_failed"}:
        return True
    if event != "detached_exit":
        return False

    if entry.get("reason") == "WORKER_DIED":
        return True

    exit_code = entry.get("exit_code")
    if isinstance(exit_code, int) and exit_code != 0:
        return True

    if isinstance(entry.get("signal"), str) and entry["signal"]:
        return True

    if entry.get("worker_status") == "failed":
        return True

    return entry.get("completion_reason") == "startup failure before task claim"


def _worker_has_death_exit_evidence(
    *,
    config: Config,
    task: DbTask,
    worker: WorkerMetadata | None,
    warning_context: str,
) -> bool:
    """Return whether a dead pending worker has concrete exit/startup-abort evidence."""
    if worker is None:
        return False
    if worker.exit_code not in (None, 0):
        return True
    if worker.completion_reason == "startup failure before task claim":
        return True

    try:
        for entry in reversed(_load_worker_log_entries(config, task, worker)):
            if _worker_lifecycle_entry_indicates_death(entry):
                return True
    except Exception as exc:
        print(
            f"Warning: Failed to inspect worker-death evidence for {warning_context}: {exc}",
            file=sys.stderr,
        )
    return False


def _mark_pending_worker_failed(
    *,
    config: Config,
    store: SqliteTaskStore,
    registry: WorkerRegistry,
    task: DbTask,
    worker: WorkerMetadata,
    explicit_reason: str,
    log_file: str | None,
    exit_code: int | None,
    completion_reason: str,
) -> None:
    """Terminalize a pending task and retire its registered worker."""
    has_commits = _branch_has_commits(config, task.branch)
    mark_task_failed_from_cause(
        task=task,
        config=config,
        store=store,
        log_file=log_file,
        branch=task.branch,
        explicit_reason=explicit_reason,
        has_commits=has_commits,
        error_type=None,
        exit_code=exit_code,
    )
    registry.mark_completed(
        worker.worker_id,
        exit_code=exit_code if exit_code is not None else 1,
        status="failed",
        completion_reason=completion_reason,
    )


def _write_worker_death_ops_event(
    log_path: Path,
    *,
    task: DbTask,
    worker: WorkerMetadata | None,
    diagnostics: WorkerDeathDiagnostics,
    event: str = "death_detected",
) -> None:
    """Persist a structured worker-death breadcrumb for later diagnostics."""
    payload: dict[str, Any] = {
        "subtype": "worker_lifecycle",
        "event": event,
        "reason": "WORKER_DIED",
        "task_id": task.id,
        "worker_id": worker.worker_id if worker is not None else None,
        "worker_status": diagnostics.worker_status,
        "completion_reason": diagnostics.completion_reason,
        "exit_code": diagnostics.exit_code,
        "signal": diagnostics.signal_name,
        "signal_number": diagnostics.signal_number,
        "stage": diagnostics.stage,
        "stage_source_event": diagnostics.source_event,
        "output_tail": list(diagnostics.output_tail),
        "os_hint": diagnostics.os_hint,
    }
    if diagnostics.signal_name:
        payload["message"] = (
            f"Worker died with signal {diagnostics.signal_name}"
            + (f" during {diagnostics.stage}" if diagnostics.stage else "")
        )
    elif diagnostics.exit_code is not None:
        payload["message"] = (
            f"Worker exited with code {diagnostics.exit_code}"
            + (f" during {diagnostics.stage}" if diagnostics.stage else "")
        )
    else:
        payload["message"] = (
            "Worker died without an observed exit code"
            + (f" during {diagnostics.stage}" if diagnostics.stage else "")
        )
    write_ops_entry(ops_log_path_for(log_path), payload, raise_on_error=True)


def _normalize_timestamp(value: datetime | str | None) -> datetime | None:
    """Parse an ISO timestamp into UTC when present."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _branch_has_commits(config: Config, branch: str | None) -> bool:
    """Check if a branch has commits beyond the default branch.

    Used during reconciliation to detect whether a WORKER_DIED task
    actually produced work before the process vanished.
    """
    if not branch:
        return False
    try:
        from ..git import Git  # lazy import to avoid circular: _common → git → config → _common
        git = Git(config.project_dir)
        default_branch = git.default_branch()
        count = git.count_commits_ahead(branch, default_branch)
        return count > 0
    except (subprocess.CalledProcessError, OSError, ValueError) as exc:
        print(f"Warning: Could not check commits on branch '{branch}': {exc}", file=sys.stderr)
        return False


def reconcile_in_progress_tasks(config: Config) -> None:
    """Best-effort reconciliation for orphaned/timed-out in-progress tasks."""
    try:
        store = get_store(config)
        registry = WorkerRegistry(config.workers_path)
    except ManualMigrationRequired:
        # DB needs gza migrate — skip reconciliation silently
        return
    except (sqlite3.Error, OSError, ValueError) as exc:
        print(f"Warning: Skipping task reconciliation due to setup error: {exc}", file=sys.stderr)
        return
    except Exception as exc:
        print(f"Warning: Skipping task reconciliation due to unexpected error: {exc}", file=sys.stderr)
        return

    running_workers_by_task_id = _running_workers_by_task_id(registry)
    latest_workers_by_task_id = _latest_workers_by_task_id(registry)

    for task in store.get_in_progress():
        task_label = f"{task.id}" if task.id is not None else "<unknown>"
        try:
            running_worker = running_workers_by_task_id.get(str(task.id)) if task.id is not None else None
            worker = latest_workers_by_task_id.get(str(task.id)) if task.id is not None else None
            if _task_worker_is_dead(task, running_worker, registry):
                has_commits = _branch_has_commits(config, task.branch)
                capture = _capture_worker_death_best_effort(
                    config=config,
                    task=task,
                    worker=worker,
                    event="death_detected",
                    warning_context=f"task {task_label}",
                )
                mark_task_failed_from_cause(
                    task=task,
                    config=config,
                    store=store,
                    log_file=capture.failure_log_file,
                    branch=task.branch,
                    explicit_reason="WORKER_DIED",
                    has_commits=has_commits,
                    error_type=None,
                    exit_code=capture.diagnostics.exit_code,
                )
                _mark_worker_reconciled(
                    registry,
                    running_worker,
                    completion_reason="watch reconciliation detected dead in-progress worker",
                )
                continue

            # PID is alive — check for a stuck worker that hasn't logged anything.
            if _task_looks_stuck(config, task):
                # Attempt to stop the wedged worker before marking failed so it
                # cannot later overwrite the outcome.
                if task.running_pid is not None and task.running_pid > 0:
                    try:
                        registry = WorkerRegistry(config.workers_path)
                        registry.record_interrupt_request(
                            task.running_pid,
                            signal_name="SIGTERM",
                            source="watch_reconcile_no_activity",
                            task_id=str(task.id) if task.id is not None else None,
                            detail="watch reconciliation detected no recent task log activity",
                        )
                        os.kill(task.running_pid, signal.SIGTERM)
                    except OSError:
                        pass
                has_commits = _branch_has_commits(config, task.branch)
                mark_task_failed_from_cause(
                    task=task,
                    config=config,
                    store=store,
                    log_file=task.log_file,
                    branch=task.branch,
                    explicit_reason="NO_ACTIVITY",
                    has_commits=has_commits,
                    error_type=None,
                    exit_code=None,
                )
                _mark_worker_reconciled(
                    registry,
                    running_worker,
                    completion_reason="watch reconciliation marked in-progress worker NO_ACTIVITY",
                )
        except (sqlite3.Error, OSError, ValueError) as exc:
            print(f"Warning: Failed to reconcile task {task_label}: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"Warning: Unexpected reconciliation error for task {task_label}: {exc}", file=sys.stderr)

    for task_id, worker in latest_workers_by_task_id.items():
        task_label = task_id
        try:
            pending_task = store.get(task_id)
            if pending_task is None or pending_task.status != "pending":
                continue
            if registry.is_running(worker.worker_id):
                continue

            if _worker_has_death_exit_evidence(
                config=config,
                task=pending_task,
                worker=worker,
                warning_context=f"pending task {task_label}",
            ):
                capture = _capture_worker_death_best_effort(
                    config=config,
                    task=pending_task,
                    worker=worker,
                    event="startup_abort_detected",
                    warning_context=f"pending task {task_label}",
                )
                _mark_pending_worker_failed(
                    config=config,
                    store=store,
                    registry=registry,
                    task=pending_task,
                    worker=worker,
                    explicit_reason="WORKER_DIED",
                    log_file=capture.failure_log_file,
                    exit_code=capture.diagnostics.exit_code,
                    completion_reason="startup failure before task claim",
                )
                continue

            if not _task_is_silent_past_timeout(config, pending_task, worker):
                continue
            _mark_pending_worker_failed(
                config=config,
                store=store,
                registry=registry,
                task=pending_task,
                worker=worker,
                explicit_reason="NO_ACTIVITY",
                log_file=pending_task.log_file or worker.startup_log_file,
                exit_code=worker.exit_code,
                completion_reason="watch reconciliation detected dead pending worker with no activity",
            )
        except (sqlite3.Error, OSError, ValueError) as exc:
            print(f"Warning: Failed to reconcile task {task_label}: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"Warning: Unexpected reconciliation error for task {task_label}: {exc}", file=sys.stderr)


def reconcile_dead_pending_recovery_tasks(config: Config) -> None:
    """Fail prepared recovery rows whose worker already recorded a pre-claim startup failure."""
    try:
        store = get_store(config)
        registry = WorkerRegistry(config.workers_path)
    except ManualMigrationRequired:
        return
    except (sqlite3.Error, OSError, ValueError) as exc:
        print(f"Warning: Skipping pending recovery reconciliation due to setup error: {exc}", file=sys.stderr)
        return
    except Exception as exc:
        print(f"Warning: Skipping pending recovery reconciliation due to unexpected error: {exc}", file=sys.stderr)
        return

    for worker in registry.list_all(include_completed=True):
        if worker.task_id is None:
            continue
        if worker.status != "failed":
            continue

        task_label = worker.task_id
        try:
            task = store.get(worker.task_id)
            if task is None or task.status != "pending":
                continue
            if classify_recovery_row(store, task) not in {"resume", "retry"}:
                continue

            capture = _capture_worker_death_best_effort(
                config=config,
                task=task,
                worker=worker,
                event="startup_abort_detected",
                warning_context=f"pending recovery task {task_label}",
            )
            _mark_pending_worker_failed(
                config=config,
                store=store,
                registry=registry,
                task=task,
                worker=worker,
                explicit_reason="WORKER_DIED",
                log_file=capture.failure_log_file,
                exit_code=capture.diagnostics.exit_code,
                completion_reason="startup failure before task claim",
            )
        except (sqlite3.Error, OSError, ValueError) as exc:
            print(
                f"Warning: Failed to reconcile pending recovery task {task_label}: {exc}",
                file=sys.stderr,
            )
        except Exception as exc:
            print(
                f"Warning: Unexpected pending recovery reconciliation error for task {task_label}: {exc}",
                file=sys.stderr,
            )


def prune_terminal_dead_workers(config: Config) -> None:
    """Remove worker registry entries for terminal tasks once the worker PID is dead."""
    try:
        store = get_store(config, open_mode="query_only")
        registry = WorkerRegistry(config.workers_path)
    except (sqlite3.Error, OSError, ValueError) as exc:
        print(f"Warning: Skipping worker prune due to setup error: {exc}", file=sys.stderr)
        return
    except Exception as exc:
        print(f"Warning: Skipping worker prune due to unexpected error: {exc}", file=sys.stderr)
        return

    terminal_statuses = {"completed", "failed", "dropped", "unmerged"}
    for worker in registry.list_all(include_completed=True):
        task_label = f"{worker.task_id}" if worker.task_id is not None else "<unknown>"
        try:
            if worker.task_id is None:
                # Only prune stale workers (registered as running but PID is dead).
                # Keep failed/completed workers so startup failures remain visible in ps.
                if worker.status == "running" and not registry.is_running(worker.worker_id):
                    registry.remove(worker.worker_id)
                continue
            task = store.get(worker.task_id)
            if task is None:
                print(
                    f"Warning: Worker {worker.worker_id} references task {task_label} not found in DB; "
                    f"possible registry/DB desynchronization",
                    file=sys.stderr,
                )
                continue
            if task.status not in terminal_statuses:
                continue
            if not registry.is_running(worker.worker_id):
                registry.remove(worker.worker_id)
        except (sqlite3.Error, OSError, ValueError) as exc:
            print(
                f"Warning: Failed to prune worker {worker.worker_id} for task {task_label}: {exc}",
                file=sys.stderr,
            )
        except Exception as exc:
            print(
                f"Warning: Unexpected worker prune error for worker {worker.worker_id} "
                f"(task {task_label}): {exc}",
                file=sys.stderr,
            )


# Shared color palette for history and stats output — defined in gza.colors.
import gza.colors as _colors  # noqa: E402
from gza.colors import TASK_COLORS_DICT as TASK_COLORS  # noqa: E402, F401


def startup_log_path_for_task(config: Config, task: DbTask) -> Path | None:
    """Return deterministic startup log path for a task."""
    if not task.slug:
        return None
    startup_log_path = config.workers_path / f"{task.slug}.startup.log"
    startup_log_path.parent.mkdir(parents=True, exist_ok=True)
    return startup_log_path


def _startup_capture_path_for_worker(
    config: Config,
    registry: WorkerRegistry,
    worker_id: str | None,
) -> Path | None:
    """Return the raw detached-worker startup capture path, if available."""
    if worker_id is None:
        return None
    meta = registry.get(worker_id)
    if meta is None or not meta.startup_log_file:
        return None
    startup_path = Path(meta.startup_log_file)
    if startup_path.is_absolute():
        return startup_path
    return config.project_dir / startup_path


def _record_preclaim_startup_failure(
    *,
    config: Config,
    registry: WorkerRegistry,
    worker_id: str | None,
    task: DbTask | None,
    exit_code: int,
) -> None:
    """Mirror detached pre-claim failures into the task-visible startup log."""
    capture_path = _startup_capture_path_for_worker(config, registry, worker_id)
    startup_log_path = startup_log_path_for_task(config, task) if task is not None else None
    if startup_log_path is None:
        startup_log_path = capture_path
    if startup_log_path is None:
        return
    mirror_paths: list[Path] = [startup_log_path]
    if task is not None and task.log_file:
        main_log_path = Path(task.log_file)
        if not main_log_path.is_absolute():
            main_log_path = config.project_dir / main_log_path
        if main_log_path not in mirror_paths:
            mirror_paths.append(main_log_path)
    capture_text = ""
    if capture_path is not None and capture_path.exists():
        capture_text = capture_path.read_text(errors="replace").strip()
    message = capture_text.splitlines()[-1].strip() if capture_text else (
        f"Worker exited before claiming the task (exit code {exit_code})."
    )
    for mirror_path in mirror_paths:
        if capture_text and mirror_path != capture_path:
            with mirror_path.open("a", encoding="utf-8") as handle:
                handle.write(capture_text)
                if not capture_text.endswith("\n"):
                    handle.write("\n")
        elif not capture_text:
            with mirror_path.open("a", encoding="utf-8") as handle:
                handle.write(f"[{datetime.now(UTC).isoformat()}] {message}\n")
    entry = {
        "subtype": "worker_lifecycle",
        "event": "start_failed",
        "worker_id": worker_id,
        "task_id": task.id if task is not None else None,
        "task_status": task.status if task is not None else None,
        "exit_code": exit_code,
        "message": message,
    }
    ops_paths = {ops_log_path_for(path) for path in mirror_paths}
    if capture_path is not None:
        ops_paths.add(ops_log_path_for(capture_path))
    for ops_path in ops_paths:
        write_ops_entry(ops_path, entry)


def _print_work_message(message: str, *, color: str | None = None) -> None:
    """Print a themed work/worker message via the shared Rich console."""
    wc = _colors.WORK_COLORS
    message_color = color or wc.default
    console.print(f"[{message_color}]{rich_escape(message)}[/{message_color}]")


def _phase1_uses_stderr(args: argparse.Namespace) -> bool:
    """Return whether this parent-side startup path should write diagnostics to stderr."""
    return bool(getattr(args, "background", False) or getattr(args, "new", False))


def print_phase1_message(args: argparse.Namespace, message: str) -> None:
    """Print a Phase 1 parent-side diagnostic to the caller's shell."""
    print(message, file=sys.stderr if _phase1_uses_stderr(args) else sys.stdout)


def phase1_error(args: argparse.Namespace, message: str) -> int:
    """Print a parent-side startup error on the correct stream and return failure."""
    print_phase1_message(args, f"Error: {message}")
    return 1


def _print_background_phase1_error(message: str) -> None:
    """Print detached-worker startup errors to the caller's stderr."""
    print(f"Error: {message}", file=sys.stderr)


def _print_background_worker_started(
    task: DbTask,
    *,
    pid: int,
    quiet: bool,
    resume: bool = False,
    startup_quiet: bool = False,
) -> None:
    """Print shared themed startup output for background worker launches."""
    if startup_quiet:
        return
    wc = _colors.WORK_COLORS
    task_id_color = _colors.TASK_COLORS.task_id
    task_id = rich_escape(str(task.id) if task.id is not None else "<unknown>")
    status_text = (
        f"in background (resuming, PID {pid})"
        if resume
        else f"in background (PID {pid})"
    )

    console.print(
        f"[{wc.default}]Started task [/{wc.default}]"
        f"[{task_id_color}]{task_id}[/{task_id_color}]"
        f"[{wc.default}] {status_text}[/{wc.default}]"
    )
    if quiet:
        return

    if task.prompt:
        prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY)
        console.print(
            f"  [{wc.default}]Prompt:[/{wc.default}] "
            f"[{_colors.pink}]{rich_escape(prompt_display)}[/{_colors.pink}]"
        )
    console.print("")
    console.print(
        f"[{wc.default}]Use 'gza log [/{wc.default}]"
        f"[{task_id_color}]{task_id}[/{task_id_color}]"
        f"[{wc.default}] -f' to follow progress[/{wc.default}]"
    )


def _spawn_detached_worker_process(
    cmd: list[str],
    config: Config,
    worker_id: str,
) -> tuple[subprocess.Popen, str]:
    """Spawn detached worker process and capture early output."""
    startup_log_path = config.workers_path / f"{worker_id}-startup.log"
    startup_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(startup_log_path, "ab") as startup_log:
        proc = subprocess.Popen(
            cmd,
            stdout=startup_log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            cwd=config.project_dir,
        )
    startup_log_rel = str(startup_log_path.relative_to(config.project_dir))
    return proc, startup_log_rel


def _start_detached_process_reaper(
    proc: subprocess.Popen,
    *,
    config: Config,
    worker_id: str,
    startup_log_rel: str,
) -> None:
    """Reap short-lived detached children so the parent does not leave zombies behind."""

    def _warn_reaper_failure(action: str, exc: Exception) -> None:
        print(
            f"Warning: Detached worker reaper failed to {action} for worker {worker_id}: {exc}",
            file=sys.stderr,
        )

    def _wait_for_exit() -> None:
        try:
            returncode = proc.wait()
            startup_log = config.project_dir / startup_log_rel
            signal_name = _signal_name_from_exit_code(returncode)
        except Exception as exc:
            _warn_reaper_failure("wait for process exit", exc)
            return

        try:
            write_ops_entry(
                ops_log_path_for(startup_log),
                {
                    "subtype": "worker_lifecycle",
                    "event": "detached_exit",
                    "reason": "WORKER_DIED" if returncode != 0 else None,
                    "worker_id": worker_id,
                    "pid": proc.pid,
                    "exit_code": returncode,
                    "signal": signal_name,
                    "message": (
                        f"Detached worker {worker_id} exited via {signal_name}"
                        if signal_name
                        else f"Detached worker {worker_id} exited with code {returncode}"
                    ),
                },
                raise_on_error=True,
            )
        except Exception as exc:
            _warn_reaper_failure("record detached exit diagnostics", exc)

        try:
            registry = WorkerRegistry(config.workers_path)
            worker = registry.get(worker_id)
        except Exception as exc:
            _warn_reaper_failure("load worker registry state", exc)
            return

        if worker is None or worker.status != "running":
            return

        try:
            registry.mark_completed(
                worker_id,
                exit_code=returncode if returncode is not None else 1,
                status="completed" if returncode == 0 else "failed",
                completion_reason=(
                    f"detached worker exited with signal {signal_name}"
                    if signal_name
                    else None
                ),
            )
        except Exception as exc:
            _warn_reaper_failure("mark the worker completed", exc)

    threading.Thread(
        target=_wait_for_exit,
        name=f"gza-worker-reaper-{proc.pid}",
        daemon=True,
    ).start()


def _rollback_background_worker_launch(
    *,
    registry: WorkerRegistry,
    worker_id: str,
    proc: subprocess.Popen | None = None,
    tmux_session: str | None = None,
) -> None:
    """Best-effort cleanup when a detached worker launch fails mid-handoff."""
    if tmux_session:
        with contextlib.suppress(Exception):
            subprocess.run(
                ["tmux", "kill-session", "-t", tmux_session],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
    if proc is not None:
        with contextlib.suppress(Exception):
            proc.terminate()
        with contextlib.suppress(Exception):
            proc.wait(timeout=1)
        if proc.poll() is None:
            with contextlib.suppress(Exception):
                proc.kill()
            with contextlib.suppress(Exception):
                proc.wait(timeout=1)
    try:
        registry.remove(worker_id)
    except Exception as cleanup_exc:
        try:
            registry.mark_completed(
                worker_id,
                exit_code=1,
                status="failed",
                completion_reason="background launch rollback after registry cleanup failure",
            )
        except Exception as mark_exc:
            print(
                f"Warning: failed to clean up background worker {worker_id} after launch failure: "
                f"remove failed with {cleanup_exc}; terminal fallback failed with {mark_exc}",
                file=sys.stderr,
            )


def _prepare_task_for_immediate_execution(
    config: Config,
    task: DbTask,
    *,
    rollback_on_failure: bool,
    resume_mode: bool = False,
    rollback_cleanup: Callable[[], None] | None = None,
) -> DbTask | None:
    """Run the synchronous creator phase on the caller's stdout/stderr."""
    store = get_store(config)
    original_slug = task.slug
    original_log_file = task.log_file
    try:
        prepared = _prepare_startup_phase(config, store, task, resume_mode=resume_mode)
    except Exception as exc:
        if rollback_on_failure and task.id is not None:
            remove_task_startup_artifacts(config, task)
            if rollback_cleanup is not None:
                rollback_cleanup()
            store.delete(task.id)
        elif task.id is not None:
            remove_task_startup_artifacts(config, task)
            restored_task = store.get(task.id) or task
            restored_task.slug = original_slug
            restored_task.log_file = original_log_file
            store.update(restored_task)
            task.slug = original_slug
            task.log_file = original_log_file
        print(f"Error: {exc}", file=sys.stderr)
        return None
    return prepared


def _reserve_task_launch_after_prepare(
    task: DbTask | None,
    permit: LaunchPermit | None,
) -> DbTask | None:
    if task is None or permit is None or task.id is None:
        return task
    reserve_task_launch_permit(str(task.id), permit)
    return task


def _prepare_task_for_reserved_launch(
    config: Config,
    task: DbTask,
    *,
    permit: LaunchPermit,
    rollback_on_failure: bool,
    resume_mode: bool = False,
    rollback_cleanup: Callable[[], None] | None = None,
) -> DbTask | None:
    prepared = _prepare_task_for_immediate_execution(
        config,
        task,
        rollback_on_failure=rollback_on_failure,
        resume_mode=resume_mode,
        rollback_cleanup=rollback_cleanup,
    )
    if prepared is None:
        permit.release()
        return None
    return _reserve_task_launch_after_prepare(prepared, permit)


def _prepare_task_for_launch(
    config: Config,
    task: DbTask,
    *,
    rollback_on_failure: bool,
    resume_mode: bool = False,
    rollback_cleanup: Callable[[], None] | None = None,
    allow_same_pid_reentry: bool = False,
    permit: LaunchPermit | None = None,
) -> tuple[DbTask, LaunchPermit] | None:
    store = get_store(config)
    original_slug = task.slug
    original_log_file = task.log_file
    acquired_permit = permit
    try:
        if acquired_permit is None:
            acquired_permit = launch_permit(
                config,
                store,
                current_pid=os.getpid() if allow_same_pid_reentry else None,
            )
        prepared = _prepare_startup_phase(config, store, task, resume_mode=resume_mode)
    except Exception as exc:
        if acquired_permit is not None:
            acquired_permit.release()
        if rollback_on_failure and task.id is not None:
            remove_task_startup_artifacts(config, task)
            if rollback_cleanup is not None:
                rollback_cleanup()
            store.delete(task.id)
        elif task.id is not None:
            remove_task_startup_artifacts(config, task)
            restored_task = store.get(task.id) or task
            restored_task.slug = original_slug
            restored_task.log_file = original_log_file
            store.update(restored_task)
        print(f"Error: {exc}", file=sys.stderr)
        return None
    assert acquired_permit is not None
    return prepared, acquired_permit


def _acquire_background_launch_permit(config: Config, store: SqliteTaskStore) -> LaunchPermit | None:
    """Acquire a launch permit while preserving detached phase-1 error handling."""
    try:
        return launch_permit(config, store)
    except Exception as exc:
        _print_background_phase1_error(str(exc))
        return None


def _run_foreground(
    config: Config,
    task_id: str | None,
    resume: bool = False,
    open_after: bool = False,
    force: bool = False,
    create_pr: bool = False,
    phase1_args: argparse.Namespace | None = None,
    invocation: RunInvocationContext | None = None,
    prepared_task: DbTask | None = None,
) -> int:
    """Run a task in the foreground with worker registration.

    Wraps run() with foreground worker registration so that gza ps/next/history
    can correctly identify actively running foreground tasks.

    Args:
        config: Configuration object
        task_id: Task ID to run
        resume: Whether this is a resume run
        open_after: Whether to open the output after completion
        force: Skip runner precondition checks
        invocation: Optional runner invocation context.
    """
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)
    worker_id = os.environ.get("GZA_WORKER_ID")
    worker_mode = os.environ.get("GZA_WORKER_MODE")
    worker = None
    reuse_existing_worker = False
    worker_registered = False
    if worker_id and worker_mode == "1":
        reuse_existing_worker = True
        existing_worker = registry.get(worker_id)
        if existing_worker is not None:
            worker = registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=task_id,
                    pid=os.getpid(),
                    is_background=False,
                )
            )
            worker_registered = True
    allow_registered_worker_reentry = worker_registered and os.environ.get(_REUSE_WORKER_REENTRY_ENV, "1") == "1"
    outer_worker_owns_completion = (
        os.environ.get(_REUSE_WORKER_SESSION_ENV) == "1"
        and os.environ.get(_REUSE_WORKER_OWNER_ENV) == _REUSE_WORKER_OWNER_OUTER
    )
    allow_registered_worker_reentry = allow_registered_worker_reentry or (
        outer_worker_owns_completion and os.environ.get(_REUSE_WORKER_REENTRY_ENV, "1") == "1"
    )
    if worker is None and worker_id and worker_mode == "1":
        worker = WorkerMetadata(
            worker_id=worker_id,
            task_id=task_id,
            pid=os.getpid(),
            is_background=False,
        )
    elif worker is None:
        worker_id = registry.generate_worker_id()
        worker = WorkerMetadata(
            worker_id=worker_id,
            task_id=task_id,
            pid=os.getpid(),
            is_background=False,
        )

    # Save original signal handlers so we can restore them in the finally block
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)
    previous_interrupt_signal = os.environ.get("GZA_INTERRUPT_SIGNAL")
    previous_interrupt_source = os.environ.get("GZA_INTERRUPT_SOURCE")
    previous_interrupt_detail = os.environ.get("GZA_INTERRUPT_DETAIL")

    def _cleanup(signum, frame):
        del frame
        # Restore original handlers and re-raise so the except block handles cleanup
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        _set_interrupt_env_from_signal(registry=registry, pid=os.getpid(), signum=signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    permit: LaunchPermit | None = None
    try:
        if resume and task_id is not None:
            task = store.get(task_id)
            if task is None:
                if phase1_args is not None:
                    return phase1_error(phase1_args, f"Task {task_id} not found")
                print(f"Error: Task {task_id} not found", file=sys.stderr)
                return 1
            try:
                _ensure_task_dispatchable_for_startup(task, store, resume_mode=True)
            except TaskDispatchBlockedError as exc:
                if phase1_args is not None:
                    print_phase1_message(phase1_args, f"Error: {exc}")
                else:
                    print(f"Error: {exc}", file=sys.stderr)
                return DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        if resume and task_id is not None:
            rebase_exit_code = _auto_rebase_before_resume(config, task_id)
            if rebase_exit_code != 0:
                if not worker_registered and not reuse_existing_worker:
                    registry.register(worker)
                    worker_registered = True
                if worker_registered and (not reuse_existing_worker or not outer_worker_owns_completion):
                    registry.mark_completed(worker.worker_id, exit_code=rebase_exit_code, status="failed")
                return rebase_exit_code
        if prepared_task is not None and prepared_task.id is not None:
            permit = take_task_launch_permit(str(prepared_task.id))
            task_id = str(prepared_task.id)
        elif task_id is not None:
            permit = take_task_launch_permit(task_id)
            if permit is not None:
                task = store.get(task_id)
                if task is None:
                    if phase1_args is not None:
                        return phase1_error(phase1_args, f"Task {task_id} not found")
                    print(f"Error: Task {task_id} not found", file=sys.stderr)
                    permit.release()
                    return 1
            else:
                task = store.get(task_id)
                if task is None:
                    if phase1_args is not None:
                        return phase1_error(phase1_args, f"Task {task_id} not found")
                    print(f"Error: Task {task_id} not found", file=sys.stderr)
                    return 1
                prepared_launch = _prepare_task_for_launch(
                    config,
                    task,
                    rollback_on_failure=False,
                    resume_mode=resume,
                    allow_same_pid_reentry=allow_registered_worker_reentry,
                )
                if prepared_launch is None:
                    return 1
                _prepared_task, permit = prepared_launch
        else:
            try:
                permit = launch_permit(
                    config,
                    store,
                    current_pid=os.getpid() if allow_registered_worker_reentry else None,
                )
            except Exception as exc:
                if phase1_args is not None:
                    return phase1_error(phase1_args, str(exc))
                print(f"Error: {exc}", file=sys.stderr)
                return 1

        if not worker_registered:
            registry.register(worker)
            worker_registered = True

        def _on_task_claimed(_claimed_task: DbTask) -> None:
            if permit is not None:
                permit.release()

        if invocation is None:
            exit_code = run(
                config,
                task_id=task_id,
                resume=resume,
                open_after=open_after,
                skip_precondition_check=force,
                create_pr=create_pr,
                on_task_claimed=_on_task_claimed,
            )
        else:
            exit_code = run(
                config,
                task_id=task_id,
                resume=resume,
                open_after=open_after,
                skip_precondition_check=force,
                create_pr=create_pr,
                on_task_claimed=_on_task_claimed,
                invocation=invocation,
            )
        if permit is not None:
            permit.release()
        status = "completed" if exit_code == 0 else "failed"
        if not reuse_existing_worker or not outer_worker_owns_completion:
            registry.mark_completed(worker.worker_id, exit_code=exit_code, status=status)
        return exit_code
    except KeyboardInterrupt:
        if permit is not None:
            permit.release()
        if worker_registered and (not reuse_existing_worker or not outer_worker_owns_completion):
            registry.mark_completed(worker.worker_id, exit_code=130, status="failed")
        return 130
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        if previous_interrupt_signal is None:
            os.environ.pop("GZA_INTERRUPT_SIGNAL", None)
        else:
            os.environ["GZA_INTERRUPT_SIGNAL"] = previous_interrupt_signal
        if previous_interrupt_source is None:
            os.environ.pop("GZA_INTERRUPT_SOURCE", None)
        else:
            os.environ["GZA_INTERRUPT_SOURCE"] = previous_interrupt_source
        if previous_interrupt_detail is None:
            os.environ.pop("GZA_INTERRUPT_DETAIL", None)
        else:
            os.environ["GZA_INTERRUPT_DETAIL"] = previous_interrupt_detail


def _spawn_background_worker(
    args: argparse.Namespace,
    config: Config,
    task_id: str | None = None,
    quiet: bool = False,
    prepared_task: DbTask | None = None,
    startup_quiet: bool = False,
) -> int:
    """Spawn a background worker process.

    Args:
        args: Command-line arguments
        config: Configuration object
        task_id: Specific task ID to run (optional)
        quiet: If True, suppress verbose output (prompt, gza ps/attach hints).
    """
    # Initialize worker registry
    registry = WorkerRegistry(config.workers_path)

    # Get task to run (either specific or next pending)
    store = get_store(config)
    explicit_task_id = task_id
    selected_task: DbTask | None = None
    resume_mode = bool(getattr(args, "resume", False))
    selected_tags, any_tag = parse_cli_tag_filters(args)

    if explicit_task_id is not None:
        task = prepared_task or store.get(explicit_task_id)
        if not task:
            _print_background_phase1_error(f"Task {explicit_task_id} not found")
            return 1

        if resume_mode:
            if task.status not in ("pending", "failed"):
                _print_background_phase1_error(
                    f"Task {explicit_task_id} is not resumable (status: {task.status})"
                )
                return 1
            if not task.session_id:
                _print_background_phase1_error(
                    f"Task {explicit_task_id} has no session ID (cannot resume)"
                )
                return 1
            try:
                _ensure_task_dispatchable_for_startup(task, store, resume_mode=True)
            except TaskDispatchBlockedError as exc:
                _print_background_phase1_error(str(exc))
                return 1
        else:
            allow_pr_retry = _allow_pr_required_retry(args, task)
            if task.status != "pending" and not allow_pr_retry:
                _print_background_phase1_error(
                    f"Task {explicit_task_id} is not pending (status: {task.status})"
                )
                return 1

            # Check if task is blocked
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                del blocking_id, blocking_status
                _print_background_phase1_error(blocked_dependency_error_message(store, task))
                return 1
        selected_task = task
    else:
        if resume_mode:
            _print_background_phase1_error("Cannot resume without specifying a task ID")
            return 1
        # Select a candidate for UX; actual claim happens in the child runner.
        selected_task = store.get_next_pending(
            tags=selected_tags,
            any_tag=any_tag,
            quiet_seconds=config.quiet_period_seconds,
        )
        if not selected_task:
            if selected_tags:
                _print_work_message(
                    format_no_runnable_message_for_tags(store, selected_tags, any_tag=any_tag),
                    color=_colors.WORK_COLORS.waiting,
                )
            else:
                _print_work_message("No pending tasks found", color=_colors.WORK_COLORS.waiting)
            return 0

    assert selected_task is not None

    permit: LaunchPermit | None = None
    if prepared_task is None:
        reserved_task_id = str(explicit_task_id) if explicit_task_id is not None else (
            str(selected_task.id) if selected_task.id is not None else None
        )
        permit = take_task_launch_permit(reserved_task_id)
        prepared_launch = _prepare_task_for_launch(
            config,
            selected_task,
            rollback_on_failure=False,
            resume_mode=resume_mode,
            permit=permit,
        )
        if prepared_launch is None:
            return 1
        prepared_task, permit = prepared_launch
    else:
        reserved_task_id = str(prepared_task.id) if prepared_task.id is not None else explicit_task_id
        permit = take_task_launch_permit(reserved_task_id)
        if permit is None:
            permit = _acquire_background_launch_permit(config, store)
        if permit is None:
            return 1
    selected_task = prepared_task
    task_id_for_child = explicit_task_id or selected_task.id

    # Build inner command for the worker subprocess
    inner_cmd = [
        sys.executable, "-m", "gza",
        "work",
        "--worker-mode",
    ]
    if resume_mode:
        inner_cmd.append("--resume")

    if task_id_for_child is not None:
        inner_cmd.append(str(task_id_for_child))
    elif selected_tags:
        for tag in selected_tags:
            inner_cmd.extend(["--tag", tag])
        if not any_tag:
            inner_cmd.append("--all-tags")

    if args.no_docker:
        inner_cmd.append("--no-docker")

    if hasattr(args, 'max_turns') and args.max_turns is not None:
        inner_cmd.extend(["--max-turns", str(args.max_turns)])
    if getattr(args, "force", False):
        inner_cmd.append("--force")
    if getattr(args, "create_pr", False):
        inner_cmd.append("--pr")

    # Add project directory
    inner_cmd.extend(["--project", str(config.project_dir.absolute())])

    # Resolve provider the same way the runner will at execution time. This respects
    # task-type routing (task_providers.<type>) so the worker mode (tmux/attach) matches
    # the provider that will actually run the task — e.g. a fix routed through
    # task_providers.fix must not use claude-specific worker plumbing when routed to
    # another provider.
    _, effective_provider, _ = get_effective_config_for_task(selected_task, config)
    provider_name = (effective_provider or "claude").lower()
    # The proxy-based tmux auto-accept flow is superseded for Claude attach.
    # Keep a compatibility escape hatch for testing or emergency fallback.
    legacy_tmux_proxy = os.environ.get("GZA_ENABLE_TMUX_PROXY", "").strip() == "1"
    use_tmux = config.tmux.enabled
    if use_tmux and provider_name == "claude" and not legacy_tmux_proxy:
        use_tmux = False

    if use_tmux:
        # Verify tmux binary is present; fall back to bare subprocess if not available
        if shutil.which("tmux") is None:
            print(
                "Warning: tmux not found; falling back to non-tmux execution. "
                "Install tmux to enable interactive task attachment.",
                file=sys.stderr,
            )
            use_tmux = False

    tmux_session: str | None = None

    if use_tmux:
        # Use explicit task ID for session name when available; fall back to worker-based name
        # (worker_id is generated below, so we use a placeholder key derived from the task)
        session_task_id = task_id_for_child if task_id_for_child is not None else selected_task.id
        tmux_session = f"gza-{session_task_id}"
        inner_cmd.extend(["--tmux-session", tmux_session])

    # Spawn detached process
    worker_id = registry.generate_worker_id()
    proc: subprocess.Popen | None = None
    try:
        inner_cmd.extend(["--worker-id", worker_id])

        startup_log_rel: str | None = None
        if use_tmux:
            assert tmux_session is not None
            cols, rows = config.tmux.terminal_size

            # Write the task prompt to a temporary file so the proxy can deliver
            # it to Claude via the PTY (simulating typing), avoiding shell argument
            # size limits and matching the spec's stdin-based delivery design.
            prompt_file_path: str | None = None
            if selected_task.prompt:
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w",
                        suffix="-gza-prompt.txt",
                        delete=False,
                    ) as tf:
                        tf.write(selected_task.prompt)
                        prompt_file_path = tf.name
                except OSError as e:
                    print(f"Warning: Failed to write prompt to temp file: {e}", file=sys.stderr)

            proxy_cmd = [
                sys.executable, "-m", "gza.tmux_proxy",
                "--session", tmux_session,
                "--auto-accept-timeout", str(config.tmux.auto_accept_timeout),
                "--max-idle-timeout", str(config.tmux.max_idle_timeout),
                "--detach-grace", str(config.tmux.detach_grace),
            ]
            if prompt_file_path:
                proxy_cmd.extend(["--prompt-file", prompt_file_path])
            proxy_cmd.extend(["--", *inner_cmd])
            # Kill any existing session with this name to avoid "session already exists" error
            subprocess.run(
                ["tmux", "kill-session", "-t", tmux_session],
                stderr=subprocess.DEVNULL,
            )
            tmux_cmd = [
                "tmux", "new-session", "-d",
                "-s", tmux_session,
                "-x", str(cols), "-y", str(rows),
                "--", *proxy_cmd,
            ]
            subprocess.run(tmux_cmd, check=True)
            # Ensure the session is destroyed when the command exits,
            # even if the user has remain-on-exit on globally.
            roi_result = subprocess.run(
                ["tmux", "set-option", "-t", tmux_session, "remain-on-exit", "off"],
                capture_output=True,
            )
            if roi_result.returncode != 0:
                print(
                    f"Warning: could not set remain-on-exit off on {tmux_session}. "
                    "Session may persist after task ends.",
                    file=sys.stderr,
                )

            # Get PID of the proxy process from tmux
            pid = get_tmux_session_pid(tmux_session) or 0
        else:
            proc, _startup_log_rel = _spawn_detached_worker_process(inner_cmd, config, worker_id)
            pid = proc.pid
            startup_log_rel = _startup_log_rel

        # Register worker
        worker_metadata = WorkerMetadata(
            worker_id=worker_id,
            task_id=task_id_for_child,
            pid=pid,
            task_slug=selected_task.slug,
            log_file=selected_task.log_file,
            startup_log_file=startup_log_rel,
            tmux_session=tmux_session,
        )
        registry.ensure_running(worker_metadata)
        if proc is not None and startup_log_rel is not None:
            _start_detached_process_reaper(
                proc,
                config=config,
                worker_id=worker_id,
                startup_log_rel=startup_log_rel,
            )
        if permit is not None:
            permit.release()

        _print_background_worker_started(
            selected_task,
            pid=pid,
            quiet=quiet,
            resume=resume_mode,
            startup_quiet=startup_quiet,
        )

        return 0

    except Exception as e:
        if permit is not None:
            permit.release()
        _rollback_background_worker_launch(
            registry=registry,
            worker_id=worker_id,
            proc=proc,
            tmux_session=tmux_session,
        )
        _print_background_phase1_error(f"spawning background worker: {e}")
        return 1


def _run_as_worker(args: argparse.Namespace, config: Config) -> int:
    """Run in worker mode (called internally by background workers)."""
    registry = WorkerRegistry(config.workers_path)
    worker_id = None

    # Use explicit worker ID if passed by parent, otherwise fall back to PID matching
    if hasattr(args, 'worker_id') and args.worker_id:
        worker_id = args.worker_id
    else:
        my_pid = os.getpid()
        workers = registry.list_all(include_completed=False)
        for w in workers:
            if w.pid == my_pid:
                worker_id = w.worker_id
                break

    store = get_store(config)
    task_claimed = False

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print("\nReceived shutdown signal, cleaning up...")
        if worker_id:
            registry.mark_completed(
                worker_id,
                exit_code=1,
                status="failed",
                completion_reason="startup failure before task claim" if not task_claimed else None,
            )
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Run the task normally
    exit_code = 1
    startup_log_path: Path | None = None
    startup_task: DbTask | None = None
    startup_header_written = False
    explicit_task_id = args.task_ids[0] if hasattr(args, "task_ids") and args.task_ids else None
    if worker_id:
        registry.ensure_running(
            WorkerMetadata(
                worker_id=worker_id,
                task_id=explicit_task_id,
                pid=os.getpid(),
            )
        )
    if explicit_task_id is None and worker_id:
        meta = registry.get(worker_id)
        if meta and meta.task_id is not None:
            explicit_task_id = meta.task_id
    if explicit_task_id is not None:
        startup_task = store.get(explicit_task_id)
        if startup_task:
            startup_log_path = startup_log_path_for_task(config, startup_task)
            if worker_id:
                startup_meta = registry.get(worker_id)
                registry.ensure_running(
                    WorkerMetadata(
                        worker_id=worker_id,
                        task_id=startup_task.id,
                        task_slug=startup_task.slug,
                        pid=os.getpid(),
                        log_file=startup_task.log_file,
                        startup_log_file=startup_meta.startup_log_file if startup_meta else None,
                    )
                )

    def _on_task_claimed(claimed_task: DbTask) -> None:
        nonlocal startup_task
        nonlocal startup_log_path
        nonlocal startup_header_written
        nonlocal task_claimed

        startup_task = claimed_task
        task_claimed = True
        if startup_log_path is None:
            startup_log_path = startup_log_path_for_task(config, claimed_task)

        if worker_id:
            meta = registry.get(worker_id)
            registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=claimed_task.id,
                    task_slug=claimed_task.slug,
                    pid=os.getpid(),
                    log_file=claimed_task.log_file,
                    startup_log_file=meta.startup_log_file if meta else None,
                )
            )

        if startup_log_path and not startup_header_written:
            startup_log_path.write_text(
                f"[{datetime.now(UTC).isoformat()}] worker starting pid={os.getpid()}\n"
            )
            startup_header_written = True

    # Propagate tmux session name to config so the provider can dispatch to
    # interactive mode when running inside a tmux session.
    if hasattr(args, "tmux_session") and args.tmux_session:
        config.tmux.session_name = args.tmux_session

    previous_worker_id = os.environ.get("GZA_WORKER_ID")
    previous_worker_mode = os.environ.get("GZA_WORKER_MODE")
    if worker_id:
        os.environ["GZA_WORKER_ID"] = worker_id
        os.environ["GZA_WORKER_MODE"] = "1"

    def _startup_failed_without_running() -> bool:
        if not task_claimed:
            return True
        if startup_task is None or startup_task.id is None:
            return False
        refreshed = store.get(startup_task.id)
        return refreshed is not None and refreshed.status == "pending"

    try:
        if startup_log_path:
            startup_log_path.write_text(
                f"[{datetime.now(UTC).isoformat()}] worker starting pid={os.getpid()}\n"
            )
            startup_header_written = True
        resume = hasattr(args, 'resume') and args.resume
        explicit_run_task_id = args.task_ids[0] if hasattr(args, 'task_ids') and args.task_ids else None
        if resume and explicit_run_task_id is not None:
            rebase_exit_code = _auto_rebase_before_resume(config, explicit_run_task_id)
            if rebase_exit_code != 0:
                if worker_id:
                    registry.mark_completed(
                        worker_id,
                        exit_code=rebase_exit_code,
                        status="failed",
                        completion_reason="startup failure before task claim",
                    )
                return rebase_exit_code
        run_kwargs: dict[str, Any] = {
            "resume": resume,
            "skip_precondition_check": getattr(args, "force", False),
            "on_task_claimed": _on_task_claimed,
        }
        if getattr(args, "create_pr", False):
            run_kwargs["create_pr"] = True
        if hasattr(args, 'task_ids') and args.task_ids:
            # Worker mode only runs one task at a time
            run_kwargs["task_id"] = args.task_ids[0]
        exit_code = run(config, **run_kwargs)
        startup_failed_without_running = exit_code != 0 and _startup_failed_without_running()
        if startup_failed_without_running:
            _record_preclaim_startup_failure(
                config=config,
                registry=registry,
                worker_id=worker_id,
                task=startup_task,
                exit_code=exit_code,
            )

        # Update worker status on completion
        if worker_id:
            status = "completed" if exit_code == 0 else "failed"
            registry.mark_completed(
                worker_id,
                exit_code=exit_code,
                status=status,
                completion_reason="startup failure before task claim" if status == "failed" and startup_failed_without_running else None,
            )

        return exit_code

    except Exception as e:
        print(f"Worker error: {e}")
        in_progress = [t for t in store.get_in_progress() if t.running_pid == os.getpid()]
        if not in_progress and startup_task and startup_task.id is not None:
            refreshed = store.get(startup_task.id)
            if refreshed and refreshed.status == "in_progress":
                in_progress = [refreshed]
        for task in in_progress:
            has_commits = _branch_has_commits(config, task.branch)
            mark_task_failed_from_cause(
                task=task,
                config=config,
                store=store,
                log_file=task.log_file,
                branch=task.branch,
                explicit_reason="WORKER_DIED",
                has_commits=has_commits,
                error_type=None,
                exit_code=None,
            )
        if startup_log_path:
            with open(startup_log_path, "a") as f:
                f.write(f"[{datetime.now(UTC).isoformat()}] worker crashed: {e}\n")
        if worker_id:
            registry.mark_completed(
                worker_id,
                exit_code=1,
                status="failed",
                completion_reason=None if task_claimed else "startup failure before task claim",
            )
        return 1
    finally:
        if previous_worker_id is None:
            os.environ.pop("GZA_WORKER_ID", None)
        else:
            os.environ["GZA_WORKER_ID"] = previous_worker_id
        if previous_worker_mode is None:
            os.environ.pop("GZA_WORKER_MODE", None)
        else:
            os.environ["GZA_WORKER_MODE"] = previous_worker_mode


def _spawn_background_resume_worker(
    args: argparse.Namespace,
    config: Config,
    new_task_id: str,
    quiet: bool = False,
    prepared_task: DbTask | None = None,
    startup_quiet: bool = False,
) -> int:
    """Spawn a background worker to run a resume task.

    Args:
        args: Command-line arguments
        config: Configuration object
        new_task_id: ID of the new resume task (created by cmd_resume)

    Returns:
        0 on success, 1 on error
    """
    # Initialize worker registry
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)

    # Get the new resume task
    task = prepared_task or store.get(new_task_id)
    if not task:
        _print_background_phase1_error(f"Task {new_task_id} not found")
        return 1
    permit: LaunchPermit | None = None
    if prepared_task is None:
        prepared_launch = _prepare_task_for_launch(
            config,
            task,
            rollback_on_failure=False,
            resume_mode=True,
        )
        if prepared_launch is None:
            return 1
        task, permit = prepared_launch
    else:
        permit = take_task_launch_permit(str(prepared_task.id) if prepared_task.id is not None else new_task_id)
        if permit is None:
            permit = _acquire_background_launch_permit(config, store)
        if permit is None:
            return 1

    # Build command for worker subprocess
    cmd = [
        sys.executable, "-m", "gza",
        "work",
        "--worker-mode",
        "--resume",
        str(new_task_id),
    ]

    if args.no_docker:
        cmd.append("--no-docker")

    if hasattr(args, 'max_turns') and args.max_turns is not None:
        cmd.extend(["--max-turns", str(args.max_turns)])
    if getattr(args, "force", False):
        cmd.append("--force")

    # Add project directory
    cmd.extend(["--project", str(config.project_dir.absolute())])

    # Spawn detached process
    worker_id = registry.generate_worker_id()
    proc: subprocess.Popen | None = None
    try:
        cmd.extend(["--worker-id", worker_id])
        proc, startup_log_rel = _spawn_detached_worker_process(cmd, config, worker_id)

        # Register worker
        worker = WorkerMetadata(
            worker_id=worker_id,
            task_id=task.id,
            pid=proc.pid,
            task_slug=task.slug,
            log_file=task.log_file,
            startup_log_file=startup_log_rel,
        )
        registry.ensure_running(worker)
        _start_detached_process_reaper(
            proc,
            config=config,
            worker_id=worker_id,
            startup_log_rel=startup_log_rel,
        )
        if permit is not None:
            permit.release()

        _print_background_worker_started(
            task,
            pid=proc.pid,
            quiet=quiet,
            resume=True,
            startup_quiet=startup_quiet,
        )

        return 0

    except Exception as e:
        if permit is not None:
            permit.release()
        _rollback_background_worker_launch(
            registry=registry,
            worker_id=worker_id,
            proc=proc,
        )
        _print_background_phase1_error(f"spawning background worker: {e}")
        return 1


def _spawn_background_iterate_worker(
    args: argparse.Namespace,
    config: Config,
    impl_task: DbTask,
    *,
    max_iterations: int,
    resume: bool = False,
    retry: bool = False,
    auto_iterate: bool = False,
    quiet: bool = False,
    dry_run: bool = False,
    prepared_task_id: str | None = None,
    prepared_resume: bool = False,
    prepared_phase: str | None = None,
    prepared_action_type: str | None = None,
    prepared_review_task_id: str | None = None,
    startup_quiet: bool = False,
) -> int:
    """Spawn the iterate loop as a detached background process."""
    registry = WorkerRegistry(config.workers_path)
    display_task = impl_task
    if prepared_task_id is not None:
        prepared_task = get_store(config).get(prepared_task_id)
        if prepared_task is not None:
            display_task = prepared_task

    inner_cmd = [
        sys.executable, "-m", "gza",
        "iterate",
        str(impl_task.id),
        "--max-iterations", str(max_iterations),
    ]

    if getattr(args, "no_docker", False):
        inner_cmd.append("--no-docker")
    if getattr(args, "force", False):
        inner_cmd.append("--force")
    if resume:
        inner_cmd.append("--resume")
    if retry:
        inner_cmd.append("--retry")
    if auto_iterate:
        inner_cmd.append("--auto-iterate")
    if prepared_task_id:
        inner_cmd.extend(["--prepared-task-id", prepared_task_id])
    if prepared_resume:
        inner_cmd.append("--prepared-resume")
    if prepared_phase:
        inner_cmd.extend(["--prepared-phase", prepared_phase])
    if prepared_action_type:
        inner_cmd.extend(["--prepared-action-type", prepared_action_type])
    if prepared_review_task_id:
        inner_cmd.extend(["--prepared-review-task-id", prepared_review_task_id])

    inner_cmd.extend(["--project", str(config.project_dir.absolute())])

    if dry_run:
        print(f"[dry-run] Would spawn background iterate worker: {shlex.join(inner_cmd)}")
        return 0

    permit = take_task_launch_permit(prepared_task_id)
    if permit is None:
        permit = _acquire_background_launch_permit(config, get_store(config))
    if permit is None:
        return 1

    worker_id = registry.generate_worker_id()
    inner_cmd.extend(["--worker-id", worker_id])
    proc: subprocess.Popen | None = None
    try:
        proc, startup_log_rel = _spawn_detached_worker_process(inner_cmd, config, worker_id)
        worker = WorkerMetadata(
            worker_id=worker_id,
            task_id=display_task.id,
            pid=proc.pid,
            task_slug=display_task.slug,
            log_file=display_task.log_file,
            startup_log_file=startup_log_rel,
        )
        registry.ensure_running(worker)
        _start_detached_process_reaper(
            proc,
            config=config,
            worker_id=worker_id,
            startup_log_rel=startup_log_rel,
        )
        permit.release()
        _print_background_worker_started(
            display_task,
            pid=proc.pid,
            quiet=quiet,
            startup_quiet=startup_quiet,
        )
        return 0
    except Exception as e:
        permit.release()
        _rollback_background_worker_launch(
            registry=registry,
            worker_id=worker_id,
            proc=proc,
        )
        _print_background_phase1_error(f"spawning background iterate worker: {e}")
        return 1


def _create_rebase_task(
    store: SqliteTaskStore,
    parent_task_id: str,
    branch: str,
    target_branch: str,
    *,
    trigger_source: str,
) -> DbTask:
    """Create a rebase task for resolving merge conflicts.

    Used by both ``gza rebase --background`` and ``gza advance`` so that
    rebases always go through the standard runner.
    """
    parent_task = store.get(parent_task_id)
    return store.add(
        prompt=(
            f"Rebase branch '{branch}' onto the local branch '{target_branch}' and resolve "
            f"any conflicts. Use /gza-rebase --auto to perform the rebase. "
            "Do not fetch from origin or any other remote, do not run git ls-remote, "
            "do not use HTTPS fallback, and do not modify git remotes or git config. "
            "Use only local refs already present in this repository. "
            "If the local target branch is missing, stop and report the failure."
        ),
        task_type="rebase",
        based_on=parent_task_id,
        enforce_single_active_sibling=True,
        same_branch=True,
        base_branch=target_branch,
        review_scope=(
            _resolved_review_scope_metadata(parent_task)
            if parent_task is not None
            else None
        ),
        tags=resolve_derived_task_tags(parent_task) if parent_task is not None else (),
        skip_learnings=True,
        trigger_source=trigger_source,
    )


def format_duplicate_rebase_message(exc: DuplicateActiveChildError, *, parent_task_id: str | None = None) -> str:
    """Render a stable operator-facing message for duplicate active rebase children."""
    return format_duplicate_active_child_message(
        exc,
        parent_task_id=parent_task_id,
        task=exc.active_child,
    )


def _spawn_background_workers(
    args: argparse.Namespace,
    config: Config,
    *,
    prepared_tasks: dict[str, DbTask] | None = None,
) -> int:
    """Spawn N background workers in parallel.

    Args:
        args: Command-line arguments including count and task_ids
        config: Configuration object

    Returns:
        0 on success, 1 on error
    """
    # Determine how many workers to spawn
    count = args.count if args.count is not None else 1
    selected_tags, any_tag = parse_cli_tag_filters(args)
    store = get_store(config)

    # If specific task_ids are provided, spawn one worker per task ID
    if hasattr(args, 'task_ids') and args.task_ids:
        if count > 1:
            print("Warning: --count is ignored when specific task IDs are provided")

        # Spawn one worker per task ID
        spawned_count = 0
        had_error = False
        for task_id in args.task_ids:
            result = _spawn_background_worker(
                args,
                config,
                task_id=task_id,
                prepared_task=(prepared_tasks or {}).get(str(task_id)),
            )
            if result == 0:
                spawned_count += 1
            else:
                had_error = True

        if len(args.task_ids) > 1:
            print(f"\n=== Spawned {spawned_count} background worker(s) for {len(args.task_ids)} task(s) ===")

        return 1 if had_error else 0

    if selected_tags:
        pending_tasks = store.get_pending_pickup(
            limit=count,
            tags=selected_tags,
            any_tag=any_tag,
            quiet_seconds=config.quiet_period_seconds,
        )
        if not pending_tasks:
            print(format_no_runnable_message_for_tags(store, selected_tags, any_tag=any_tag))
            return 0
        spawned_count = 0
        had_error = False
        for task in pending_tasks:
            if task.id is None:
                continue
            result = _spawn_background_worker(args, config, task_id=task.id)
            if result == 0:
                spawned_count += 1
            else:
                had_error = True
        if count > 1:
            print(
                f"\n=== Attempted to spawn {count} background worker(s) "
                f"for tags '{', '.join(selected_tags)}' ==="
            )
        return 1 if had_error else 0

    pending_tasks = store.get_pending_pickup(limit=count, quiet_seconds=config.quiet_period_seconds)
    if not pending_tasks:
        print("No pending tasks found")
        return 0

    spawned_count = 0
    had_error = False

    for task in pending_tasks:
        if task.id is None:
            continue
        result = _spawn_background_worker(args, config, task_id=task.id)
        if result == 0:
            spawned_count += 1
        else:
            had_error = True

    # Since _spawn_background_worker prints its own output for each worker,
    # we just print a summary if multiple workers were requested
    if count > 1:
        print(f"\n=== Attempted to spawn {count} background worker(s) ===")

    return 1 if had_error else 0


def _allow_pr_required_retry(args: argparse.Namespace, task: DbTask) -> bool:
    """Return whether work may retry a failed PR_REQUIRED task."""
    return bool(
        (getattr(args, "create_pr", False) or task.create_pr)
        and task.status == "failed"
        and task.failure_reason == "PR_REQUIRED"
        and bool(task.branch)
    )


def format_stats(task: DbTask) -> str:
    """Format task stats as a compact string."""
    parts = []
    if task.duration_seconds is not None:
        if task.duration_seconds < 60:
            parts.append(f"{task.duration_seconds:.0f}s")
        else:
            mins = int(task.duration_seconds // 60)
            secs = int(task.duration_seconds % 60)
            parts.append(f"{mins}m{secs}s")
    if task.started_at is not None:
        parts.append(task.started_at.strftime("%Y-%m-%d"))
    if task.attach_count:
        attach_part = f"{task.attach_count} attach"
        if task.attach_count != 1:
            attach_part += "es"
        if task.attach_duration_seconds:
            attach_secs = task.attach_duration_seconds
            if attach_secs < 60:
                attach_part += f" ({attach_secs:.0f}s)"
            else:
                mins = int(attach_secs // 60)
                secs = int(attach_secs % 60)
                attach_part += f" ({mins}m{secs}s)"
        parts.append(attach_part)
    return " | ".join(parts) if parts else ""


def get_task_step_count(task: DbTask) -> int | None:
    """Return a task's canonical step count using step-first fallback."""
    if task.num_steps_reported is not None:
        return task.num_steps_reported
    if task.num_steps_computed is not None:
        return task.num_steps_computed
    if task.num_turns_reported is not None:
        return task.num_turns_reported
    return None


def get_review_verdict(config: Config, review_task: DbTask) -> str | None:
    """Extract verdict from a review file.

    Args:
        config: Configuration object
        review_task: Review task

    Returns:
        Verdict string ('APPROVED', 'APPROVED_WITH_FOLLOWUPS', 'CHANGES_REQUESTED', 'NEEDS_DISCUSSION') or None if not found
    """
    return _get_review_verdict(config.project_dir, review_task)


def format_review_outcome(config: Config, review_task: DbTask, *, unknown_label: str = "UNKNOWN") -> str:
    """Format verdict plus parsed follow-up IDs for a completed review task."""
    return _format_review_outcome(
        _get_review_outcome(config.project_dir, review_task),
        unknown_label=unknown_label,
    )


def get_review_score(config: Config, review_task: DbTask) -> int | None:
    """Compute deterministic review score from review output content/report file."""
    return _get_review_score(config.project_dir, review_task)


def _create_review_task(
    store: SqliteTaskStore,
    impl_task: DbTask,
    *,
    trigger_source: str,
    model: str | None = None,
    provider: str | None = None,
) -> DbTask:
    """Create a review task for an implementation task.

    Shared wrapper used by CLI commands so patching ``gza.cli._create_review_task``
    in tests continues to work after centralizing review creation logic.
    """
    return create_review_task(
        store,
        impl_task,
        trigger_source=trigger_source,
        prompt_mode="cli",
        model=model,
        provider=provider,
    )


def _create_review_adjudication_task(
    store: SqliteTaskStore,
    impl_task: DbTask,
    review_task: DbTask,
    finding: ReviewFinding,
    *,
    dispute_metadata: dict[str, Any],
    trigger_source: str,
) -> DbTask:
    """Create or reuse an internal adjudication task for one disputed blocker."""
    task, _created_now = create_or_reuse_review_blocker_adjudication_task(
        store,
        review_task=review_task,
        impl_task=impl_task,
        finding=finding,
        dispute_metadata=dispute_metadata,
        trigger_source=trigger_source,
    )
    return task


def _create_or_reuse_followup_tasks(
    store: SqliteTaskStore,
    *,
    review_task: DbTask,
    impl_task: DbTask,
    findings: tuple[ReviewFinding, ...],
    trigger_source: str,
) -> tuple[list[DbTask], list[DbTask]]:
    """Create/reuse follow-up implement tasks for parsed FOLLOWUP findings.

    Returns:
        (created_tasks, reused_tasks)
    """
    created: list[DbTask] = []
    reused: list[DbTask] = []
    for finding in findings:
        task, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source=trigger_source,
        )
        if created_now:
            created.append(task)
        else:
            reused.append(task)
    return created, reused


def _create_or_reuse_deferred_blocker_tasks(
    store: SqliteTaskStore,
    *,
    review_task: DbTask,
    impl_task: DbTask,
    findings: tuple[ReviewFinding, ...],
    trigger_source: str,
) -> tuple[list[DbTask], list[DbTask]]:
    """Create/reuse deferred-blocker implement tasks for parsed BLOCKER findings."""
    created: list[DbTask] = []
    reused: list[DbTask] = []
    for finding in findings:
        task, created_now = create_or_reuse_deferred_blocker_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source=trigger_source,
        )
        if created_now:
            created.append(task)
        else:
            reused.append(task)
    return created, reused


def resolve_improve_action(
    store: SqliteTaskStore,
    impl_task_id: str,
    review_task_id: str,
    max_resume_attempts: int | None = None,
) -> tuple[str, DbTask | None, FailedRecoveryDecision | None]:
    """Determine the right improve action for an impl+review pair.

    Returns:
        ("new", None, None) — no existing improve, create fresh
        ("resume", failed_task, decision) — shared policy chose resume
        ("retry", failed_task, decision) — shared policy chose retry
        ("give_up", failed_task, decision) — automatic recovery disabled
        ("manual_review", failed_task, decision) — latest failure requires human review
    """
    existing = store.get_improve_tasks_for(impl_task_id, review_task_id)
    failed_improves = [t for t in existing if t.status == "failed"]
    if not failed_improves:
        return ("new", None, None)

    latest_failed = max(failed_improves, key=lambda t: t.created_at or datetime.min)
    decision = decide_failed_task_recovery(
        store,
        latest_failed,
        max_recovery_attempts=0 if max_resume_attempts is None else max_resume_attempts,
    )
    if decision.action in {"resume", "retry"}:
        return (decision.action, latest_failed, decision)
    if decision.reason_code == "automatic_recovery_disabled":
        return ("give_up", latest_failed, decision)
    return ("manual_review", latest_failed, decision)


def resolve_comments_improve_action(
    store: SqliteTaskStore,
    impl_task_id: str,
    max_resume_attempts: int | None = None,
) -> tuple[str, DbTask | None, FailedRecoveryDecision | None]:
    """Determine improve action for comments-only (no-review) improve flows.

    Returns:
        ("new", None, None) — create a fresh comments-only improve
        ("reuse_pending", pending_task, None) — reuse existing pending comments-only improve
        ("wait_in_progress", in_progress_task, None) — existing in-progress comments-only improve is still running
        ("resume", failed_task, decision) — shared policy chose resume
        ("retry", failed_task, decision) — shared policy chose retry
        ("give_up", failed_task, decision) — automatic recovery disabled
        ("manual_review", failed_task, decision) — latest failure requires human review
    """
    def _normalize_time(value: datetime | None) -> datetime:
        if value is None:
            return datetime.min
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value

    def _time_key(task: DbTask) -> tuple[datetime, int]:
        return (_normalize_time(task.created_at), task_id_numeric_key(task.id))

    unresolved_comments = store.get_comments(
        impl_task_id,
        unresolved_only=True,
        kinds=(TASK_COMMENT_KIND_FEEDBACK,),
    )
    latest_unresolved_comment_time: datetime | None = None
    if unresolved_comments:
        latest_unresolved_comment_time = max(
            _normalize_time(comment.created_at)
            for comment in unresolved_comments
        )

    def _candidate_is_fresh(task: DbTask) -> bool:
        # Improves consume a comment snapshot as-of improve.created_at.
        # If a newer unresolved comment exists, this candidate is stale.
        if latest_unresolved_comment_time is None:
            return True
        return _normalize_time(task.created_at) >= latest_unresolved_comment_time

    existing = [
        task for task in store.get_improve_tasks_by_root(impl_task_id)
        if task.depends_on is None
    ]
    if not existing:
        return ("new", None, None)

    in_progress = [
        task for task in existing
        if task.status == "in_progress" and _candidate_is_fresh(task)
    ]
    if in_progress:
        return ("wait_in_progress", max(in_progress, key=_time_key), None)

    pending = [
        task for task in existing
        if task.status == "pending" and _candidate_is_fresh(task)
    ]
    if pending:
        return ("reuse_pending", max(pending, key=_time_key), None)

    failed = [
        task for task in existing
        if task.status == "failed" and _candidate_is_fresh(task)
    ]
    if not failed:
        return ("new", None, None)

    latest_failed = max(failed, key=_time_key)
    decision = decide_failed_task_recovery(
        store,
        latest_failed,
        max_recovery_attempts=0 if max_resume_attempts is None else max_resume_attempts,
    )
    if decision.action in {"resume", "retry"}:
        return (decision.action, latest_failed, decision)
    if decision.reason_code == "automatic_recovery_disabled":
        return ("give_up", latest_failed, decision)
    return ("manual_review", latest_failed, decision)


def _create_improve_task(
    store: SqliteTaskStore,
    impl_task: DbTask,
    review_task: DbTask | None,
    *,
    trigger_source: str,
    create_review: bool = False,
    create_pr: bool = False,
    model: str | None = None,
    provider: str | None = None,
) -> DbTask:
    """Create an improve task for an implementation task.

    Uses review feedback when a review task is supplied; otherwise falls back
    to unresolved task comments.
    Validates that no duplicate improve task already exists for review-backed
    improves. Comments-only improve lifecycle selection is handled by
    ``resolve_comments_improve_action``.
    Returns the created improve task, or raises ValueError with an error message.
    """
    assert impl_task.id is not None
    has_comments = bool(
        store.get_comments(
            impl_task.id,
            unresolved_only=True,
            kinds=(TASK_COMMENT_KIND_FEEDBACK,),
        )
    )
    based_on_id = impl_task.id
    if review_task is not None:
        assert review_task.id is not None
        existing = store.get_improve_tasks_for(impl_task.id, review_task.id)
        if existing:
            latest_existing = existing[0]
            if latest_existing.status == "completed" and latest_existing.changed_diff is False:
                assert latest_existing.id is not None
                based_on_id = latest_existing.id
            else:
                raise ValueError(
                    f"An improve task already exists for implementation {impl_task.id} "
                    f"and review {review_task.id}: {latest_existing.id} (status: {latest_existing.status})"
                )

    prompt = PromptBuilder().improve_task_prompt(
        impl_task.id,
        review_task.id if review_task is not None else None,
        has_comments=has_comments,
    )
    return store.add(
        prompt=prompt,
        task_type="improve",
        depends_on=review_task.id if review_task is not None else None,
        based_on=based_on_id,
        enforce_single_active_sibling=review_task is not None,
        single_active_sibling_scope="review_backed_improve" if review_task is not None else None,
        same_branch=True,
        tags=resolve_derived_task_tags(impl_task),
        review_scope=_resolved_review_scope_metadata(impl_task),
        create_review=create_review,
        create_pr=create_pr,
        model=model,
        provider=provider,
        trigger_source=trigger_source,
    )


def _create_plan_review_task(
    store: SqliteTaskStore,
    plan_task: DbTask,
    *,
    trigger_source: str,
    model: str | None = None,
    provider: str | None = None,
) -> DbTask:
    """Create a plan_review task for a plan or revised-plan source."""
    assert plan_task.id is not None
    if plan_task.task_type not in {"plan", "plan_improve"}:
        raise ValueError("plan_review source must be a plan or plan_improve task")
    return store.add(
        prompt=f"Review plan source {plan_task.id}",
        task_type="plan_review",
        depends_on=plan_task.id,
        based_on=None,
        tags=resolve_derived_task_tags(plan_task),
        model=model,
        provider=provider,
        trigger_source=trigger_source,
    )


def _create_plan_improve_task(
    store: SqliteTaskStore,
    plan_task: DbTask,
    review_task: DbTask,
    *,
    trigger_source: str,
    model: str | None = None,
    provider: str | None = None,
) -> DbTask:
    """Create a plan_improve task for a plan review that requested changes."""
    assert plan_task.id is not None
    assert review_task.id is not None
    if plan_task.task_type not in {"plan", "plan_improve"}:
        raise ValueError("plan_improve source must be a plan or plan_improve task")
    if review_task.task_type != "plan_review":
        raise ValueError("plan_improve dependency must be a plan_review task")
    return store.add(
        prompt=f"Revise plan source {plan_task.id} based on plan review {review_task.id}",
        task_type="plan_improve",
        depends_on=review_task.id,
        based_on=plan_task.id,
        tags=resolve_derived_task_tags(plan_task),
        model=model,
        provider=provider,
        trigger_source=trigger_source,
    )


from ..query import _LINEAGE_REL_LABELS, TaskLineageNode as _TaskLineageNode  # noqa: E402


def _format_lineage(
    lineage_tree: _TaskLineageNode,
    task_id_color: str | None = None,
    *,
    annotate: bool = False,
    show_status: bool = False,
    status_color_resolver: Callable[[DbTask], str] | None = None,
    review_verdict_resolver: Callable[[DbTask], str | None] | None = None,
) -> str:
    """Format a lineage tree as a multi-line branch rendering."""
    lc = _colors.LINEAGE_COLORS
    # Allow callers to override task_id color (e.g. unmerged passes its own)
    _task_id_color = task_id_color if task_id_color is not None else lc.task_id

    def _normalize_time(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)

    def _lineage_time(task: DbTask) -> datetime:
        return task.completed_at or task.created_at or datetime.min

    latest_review_task_id: str | None = None
    if annotate:
        review_tasks: list[DbTask] = []

        def _collect_reviews(node: _TaskLineageNode) -> None:
            if node.task.task_type == "review":
                review_tasks.append(node.task)
            for child_node in node.children:
                _collect_reviews(child_node)

        _collect_reviews(lineage_tree)
        if review_tasks:
            latest_review = max(
                review_tasks,
                key=lambda task: (
                    _normalize_time(_lineage_time(task)),
                    task_id_numeric_key(task.id),
                ),
            )
            latest_review_task_id = latest_review.id

    def _annotation(task: DbTask) -> str:
        if not annotate:
            return ""

        completed_label = (
            task.completed_at.strftime("%Y-%m-%d %H:%M")
            if task.completed_at
            else "n/a"
        )
        status_label = task.status or "unknown"
        verdict_label = "-"

        if task.task_type == "review":
            verdict = (
                review_verdict_resolver(task)
                if review_verdict_resolver is not None
                else parse_review_verdict(task.output_content)
            )
            verdict_map = {
                "APPROVED": "approved",
                "APPROVED_WITH_FOLLOWUPS": "approved_with_followups",
                "CHANGES_REQUESTED": "changes_requested",
                "NEEDS_DISCUSSION": "needs_discussion",
            }
            verdict_label = verdict_map.get(verdict, "unknown") if verdict else "unknown"
            if latest_review_task_id is not None and task.id == latest_review_task_id:
                verdict_label = f"{verdict_label} \u2190 latest"

        return f" [{lc.annotation}]({completed_label} | {status_label} | {verdict_label})[/{lc.annotation}]"

    def _node_label(task: DbTask, relationship: str = "root") -> str:
        rel_suffix = ""
        rel_label = _LINEAGE_REL_LABELS.get(relationship, "")
        if rel_label and rel_label != task.task_type:
            rel_suffix = f" [{lc.task_type}]\\[{rel_label}][/{lc.task_type}]"
        status_suffix = ""
        if show_status:
            status_text = format_task_status_text(task)
            status_color = (
                status_color_resolver(task)
                if status_color_resolver is not None
                else get_task_status_color(task)
            )
            status_suffix = f" [{status_color}]{rich_escape(status_text)}[/{status_color}]"
            merge_label = format_task_merge_label(task)
            if merge_label:
                merge_color = _colors.STATUS_COLORS.completed if merge_label == "merged" else _colors.STATUS_COLORS.unmerged
                status_suffix += f" ([{merge_color}]{merge_label}[/{merge_color}])"
        if task.id is None:
            return f"[{lc.task_type}]\\[{task.task_type}][/{lc.task_type}]{rel_suffix}{status_suffix}{_annotation(task)}"
        return (
            f"[{_task_id_color}]{task.id}[/{_task_id_color}]"
            f"[{lc.task_type}]\\[{task.task_type}][/{lc.task_type}]"
            f"{rel_suffix}"
            f"{status_suffix}"
            f"{_annotation(task)}"
        )

    lines: list[str] = [_node_label(lineage_tree.task)]

    def _walk(node: _TaskLineageNode, ancestors_last: tuple[bool, ...] = ()) -> None:
        for index, child in enumerate(node.children):
            is_last = index == (len(node.children) - 1)
            prefix = _lineage_tree_prefix((*ancestors_last, is_last))
            lines.append(f"{prefix}{_node_label(child.task, child.relationship)}")
            _walk(child, (*ancestors_last, is_last))

    _walk(lineage_tree)
    return "\n".join(lines)


def _lineage_tree_prefix(ancestors_last: tuple[bool, ...]) -> str:
    """Render a tree connector with guide widths that match child connectors."""
    if not ancestors_last:
        return ""
    prefix = "".join("    " if flag else "│   " for flag in ancestors_last[:-1])
    prefix += "└── " if ancestors_last[-1] else "├── "
    return prefix


def _resolved_review_scope_metadata(task: DbTask) -> str | None:
    """Resolve the authoritative review scope carried by a source task."""
    if task.review_scope:
        normalized = task.review_scope.strip()
        if normalized:
            return normalized
    return extract_review_scope_from_prompt(task.prompt)


def _create_implementation_task_from_source(
    store: SqliteTaskStore,
    source_task: DbTask,
    *,
    prompt: str,
    trigger_source: str,
    tags: tuple[str, ...] | list[str] | None = None,
    review_scope: str | None = None,
    create_review: bool = False,
    create_pr: bool = False,
    same_branch: bool = False,
    task_type_hint: str | None = None,
    model: str | None = None,
    model_is_explicit: bool | None = None,
    provider: str | None = None,
    provider_is_explicit: bool | None = None,
    skip_learnings: bool = False,
) -> DbTask:
    """Create an implementation task using shared review-scope inheritance rules."""
    assert source_task.id is not None
    return store.add(
        prompt=prompt,
        task_type="implement",
        depends_on=source_task.id,
        tags=resolve_derived_task_tags(source_task, explicit_tags=tags),
        review_scope=review_scope if review_scope is not None else _resolved_review_scope_metadata(source_task),
        create_review=create_review,
        create_pr=create_pr,
        same_branch=same_branch,
        task_type_hint=task_type_hint,
        model=model,
        model_is_explicit=model_is_explicit,
        provider=provider,
        provider_is_explicit=provider_is_explicit,
        skip_learnings=skip_learnings,
        trigger_source=trigger_source,
    )


def _materialize_plan_review_slices(
    config: Config,
    store: SqliteTaskStore,
    plan_source_task: DbTask,
    review_task: DbTask,
    manifest: PlanReviewManifest,
    *,
    trigger_source: str,
    require_review_before_merge: bool,
) -> PlanReviewMaterializationResult:
    """Create implementation tasks from an approved plan-review manifest exactly once."""
    assert plan_source_task.id is not None
    assert review_task.id is not None
    manifest = validate_plan_review_manifest(
        json.loads(json.dumps(asdict(manifest))),
        markdown_verdict="APPROVED",
        source_task_id=plan_source_task.id,
        source_task_type=plan_source_task.task_type,
        max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
        max_plan_slices=getattr(config, "max_plan_slices", None),
    )
    reused_tasks = load_materialized_plan_slice_set(
        store,
        review_task=review_task,
        plan_source_task=plan_source_task,
        manifest=manifest,
    )
    if reused_tasks is not None:
        return PlanReviewMaterializationResult(tasks=reused_tasks, created=False)

    manifest_digest = plan_review_manifest_digest(manifest)
    task_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan_source_task,
        review_task=review_task,
        manifest=manifest,
        trigger_source=trigger_source,
        require_review_before_merge=require_review_before_merge,
    )

    artifact_path = (
        Path(".gza")
        / "artifacts"
        / review_task.id
        / f"plan-review-materialization-{manifest_digest[:16]}.txt"
    ).as_posix()
    created_tasks = store.add_tasks_with_artifact_atomic(
        tasks=task_specs,
        artifact_task_id=review_task.id,
        artifact_kind=PLAN_REVIEW_MATERIALIZATION_ARTIFACT_KIND,
        artifact_label="plan_review_materialization",
        artifact_path=artifact_path,
        artifact_byte_size=0,
        artifact_sha256="e3b0c44298fc1c149afbf4c8996fb924"
        "27ae41e4649b934ca495991b7852b855",
        artifact_status="materialized",
        artifact_producer="gza.cli.plan-review",
        artifact_metadata_builder=lambda tasks: {
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review_task.id,
            "source_task_id": plan_source_task.id,
            "source_task_type": plan_source_task.task_type,
            "manifest_digest": manifest_digest,
            "trigger_source": trigger_source,
            "create_review": require_review_before_merge,
            "task_ids": [task.id for task in tasks if task.id is not None],
        },
    )
    return PlanReviewMaterializationResult(tasks=created_tasks, created=True)


@dataclass(frozen=True)
class PlanReviewMaterializationRepairResult:
    """Outcome of dropping stale partial slice rows and recreating the full manifest."""

    dropped_tasks: tuple[DbTask, ...]
    materialization: PlanReviewMaterializationResult


@dataclass(frozen=True)
class PlanReviewMaterializationRepairBlocked(ValueError):
    """Non-mutating stale/ambiguous repair failure that should surface as attention."""

    message: str
    reason: str = "plan-review-materialization-repair-needed"

    def __str__(self) -> str:
        return self.message


def _repair_plan_review_slice_materialization(
    config: Config,
    store: SqliteTaskStore,
    plan_source_task: DbTask,
    review_task: DbTask,
    manifest: PlanReviewManifest,
    *,
    partial_task_ids: tuple[str, ...],
    trigger_source: str,
    require_review_before_merge: bool,
) -> PlanReviewMaterializationRepairResult:
    """Drop safe pending partial slices, then recreate the full validated manifest."""
    assert plan_source_task.id is not None
    assert review_task.id is not None

    expected_manifest_digest = plan_review_manifest_digest(manifest)
    current_plan_source, current_review, current_manifest = _revalidate_plan_review_slice_repair_rows(
        config=config,
        store=store,
        plan_source_task=plan_source_task,
        review_task=review_task,
        expected_manifest_digest=expected_manifest_digest,
    )
    inspection = inspect_plan_review_materialization_for_repair(
        store,
        review_task=current_review,
        plan_source_task=current_plan_source,
        manifest=current_manifest,
    )
    if inspection.blocked_reason is not None:
        raise PlanReviewMaterializationRepairBlocked(inspection.blocked_reason)
    partial_tasks = _resolve_pending_partial_plan_review_slice_tasks_for_repair(
        store=store,
        plan_source_task=current_plan_source,
        review_task=current_review,
        manifest=current_manifest,
        partial_task_ids=partial_task_ids,
        reusable_tasks=inspection.reusable_tasks,
        trigger_source=trigger_source,
        require_review_before_merge=require_review_before_merge,
    )

    for partial_task in partial_tasks:
        apply_manual_task_status(
            config=config,
            store=store,
            task=partial_task,
            status="dropped",
            reason=PLAN_REVIEW_MATERIALIZATION_AUTO_REPAIR_DROP_REASON,
        )

    if inspection.reusable_tasks is not None:
        return PlanReviewMaterializationRepairResult(
            dropped_tasks=tuple(store.get(task.id) or task for task in partial_tasks if task.id is not None),
            materialization=PlanReviewMaterializationResult(tasks=inspection.reusable_tasks, created=False),
        )

    materialization = _materialize_plan_review_slices(
        config,
        store,
        current_plan_source,
        current_review,
        current_manifest,
        trigger_source=trigger_source,
        require_review_before_merge=require_review_before_merge,
    )
    return PlanReviewMaterializationRepairResult(
        dropped_tasks=tuple(store.get(task.id) or task for task in partial_tasks if task.id is not None),
        materialization=materialization,
    )


def _revalidate_plan_review_slice_repair_rows(
    *,
    config: Config,
    store: SqliteTaskStore,
    plan_source_task: DbTask,
    review_task: DbTask,
    expected_manifest_digest: str,
) -> tuple[DbTask, DbTask, PlanReviewManifest]:
    assert plan_source_task.id is not None
    assert review_task.id is not None

    current_plan_source = store.get(plan_source_task.id)
    if current_plan_source is None:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan source {plan_source_task.id} no longer exists; repair candidate is stale"
        )
    if current_plan_source.task_type != plan_source_task.task_type:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan source {plan_source_task.id} changed task_type; repair candidate is stale"
        )
    if current_plan_source.status != plan_source_task.status:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan source {plan_source_task.id} changed status to {current_plan_source.status}; repair candidate is stale"
        )

    current_review = store.get(review_task.id)
    if current_review is None:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan review {review_task.id} no longer exists; repair candidate is stale"
        )
    if current_review.task_type != review_task.task_type:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan review {review_task.id} changed task_type; repair candidate is stale"
        )
    if current_review.status != review_task.status:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan review {review_task.id} changed status to {current_review.status}; repair candidate is stale"
        )
    if current_review.depends_on != current_plan_source.id:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan review {review_task.id} no longer depends on {current_plan_source.id}; repair candidate is stale"
        )

    assert current_plan_source.id is not None
    outcome = get_plan_review_outcome(
        config.project_dir,
        current_review,
        source_task_id=current_plan_source.id,
        source_task_type=current_plan_source.task_type,
        max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
        max_plan_slices=getattr(config, "max_plan_slices", None),
    )
    if outcome.verdict != "APPROVED" or outcome.manifest is None:
        detail = outcome.validation_error or outcome.verdict or "review is no longer approved"
        raise PlanReviewMaterializationRepairBlocked(
            f"plan review {review_task.id} no longer validates for auto-repair ({detail})"
        )
    current_manifest = outcome.manifest
    if plan_review_manifest_digest(current_manifest) != expected_manifest_digest:
        raise PlanReviewMaterializationRepairBlocked(
            f"plan review {review_task.id} manifest changed; repair candidate is stale"
        )

    return current_plan_source, current_review, current_manifest


def _resolve_pending_partial_plan_review_slice_tasks_for_repair(
    *,
    store: SqliteTaskStore,
    plan_source_task: DbTask,
    review_task: DbTask,
    manifest: PlanReviewManifest,
    partial_task_ids: tuple[str, ...],
    reusable_tasks: list[DbTask] | None,
    trigger_source: str,
    require_review_before_merge: bool,
) -> list[DbTask]:
    if plan_source_task.id is None or review_task.id is None:
        raise ValueError("missing plan-review repair inputs")
    if not partial_task_ids:
        raise ValueError("missing partial slice task ids")
    if len(set(partial_task_ids)) != len(partial_task_ids):
        raise ValueError("duplicate partial slice task ids")

    expected_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan_source_task,
        review_task=review_task,
        manifest=manifest,
        trigger_source=trigger_source,
        require_review_before_merge=require_review_before_merge,
    )
    prompt_to_index: dict[str, int] = {}
    for index, spec in enumerate(expected_specs):
        if spec.prompt in prompt_to_index:
            raise ValueError("repair requires unique slice prompts")
        prompt_to_index[spec.prompt] = index

    partial_indexed_tasks: dict[int, DbTask] = {}
    partial_tasks: list[DbTask] = []
    for task_id in partial_task_ids:
        partial_task = store.get(task_id)
        if partial_task is None:
            raise ValueError(f"partial slice task {task_id} no longer exists")
        if partial_task.status != "pending":
            raise ValueError(f"partial slice task {task_id} is no longer pending")
        if partial_task.branch:
            raise ValueError(f"partial slice task {task_id} already has branch state")
        if partial_task.prompt not in prompt_to_index:
            raise ValueError(f"partial slice task {task_id} does not match the validated manifest")
        spec_index = prompt_to_index[partial_task.prompt]
        if spec_index in partial_indexed_tasks:
            raise ValueError(f"multiple partial slice tasks map to manifest slice {partial_task.prompt!r}")
        partial_indexed_tasks[spec_index] = partial_task
        partial_tasks.append(partial_task)

    current_descendants = _list_non_dropped_implement_descendants_for_plan_source(store, plan_source_task.id)
    current_descendant_ids = {task.id for task in current_descendants if task.id is not None}
    if any(task.id not in current_descendant_ids for task in partial_tasks if task.id is not None):
        raise PlanReviewMaterializationRepairBlocked(
            "current partial slice set no longer matches the repair candidate; re-evaluate lifecycle state"
        )

    reusable_task_ids = {
        task.id for task in reusable_tasks or [] if task.id is not None
    }
    overlapping_reusable_partial_ids = tuple(
        task_id for task_id in partial_task_ids if task_id in reusable_task_ids
    )
    if overlapping_reusable_partial_ids:
        overlap_list = ", ".join(overlapping_reusable_partial_ids)
        raise PlanReviewMaterializationRepairBlocked(
            "repair candidate is stale because reusable materialization already references "
            f"partial slice task id(s): {overlap_list}"
        )
    allowed_descendant_ids = set(partial_task_ids) | reusable_task_ids
    for task in current_descendants:
        if task.id is None or task.id not in allowed_descendant_ids:
            raise PlanReviewMaterializationRepairBlocked(
                f"implement descendant {task.id or 'unknown'} no longer matches the validated manifest"
            )
        if task.prompt not in prompt_to_index:
            raise PlanReviewMaterializationRepairBlocked(
                f"implement descendant {task.id or 'unknown'} no longer matches the validated manifest"
            )

    current_partial_ids = tuple(
        task_id for task_id in partial_task_ids if task_id in current_descendant_ids
    )
    if current_partial_ids != partial_task_ids:
        raise PlanReviewMaterializationRepairBlocked(
            "current partial slice set no longer matches the repair candidate; re-evaluate lifecycle state"
        )

    for spec_index, task in partial_indexed_tasks.items():
        expected_spec = expected_specs[spec_index]
        if task.task_type != expected_spec.task_type:
            raise ValueError(f"partial slice task {task.id} has unexpected task type")
        if task.trigger_source != expected_spec.trigger_source:
            raise ValueError(f"partial slice task {task.id} has unexpected trigger source")
        if task.same_branch != expected_spec.same_branch:
            raise ValueError(f"partial slice task {task.id} has unexpected same_branch wiring")
        if task.review_scope != expected_spec.review_scope:
            raise ValueError(f"partial slice task {task.id} has unexpected review scope")
        if task.tags != expected_spec.tags:
            raise ValueError(f"partial slice task {task.id} has unexpected tags")
        if task.create_review != expected_spec.create_review:
            raise ValueError(f"partial slice task {task.id} has unexpected review creation flag")
        if task.based_on != _resolve_partial_materialized_task_ref(expected_spec.based_on, partial_indexed_tasks):
            raise ValueError(f"partial slice task {task.id} has unexpected based_on wiring")
        if task.depends_on != _resolve_partial_materialized_task_ref(expected_spec.depends_on, partial_indexed_tasks):
            raise ValueError(f"partial slice task {task.id} has unexpected depends_on wiring")

    return partial_tasks


def _list_non_dropped_implement_descendants_for_plan_source(
    store: SqliteTaskStore,
    source_task_id: str,
) -> list[DbTask]:
    descendants: list[DbTask] = []
    for task in store.get_all():
        if (
            task.task_type == "implement"
            and task.status != "dropped"
            and _is_based_on_descendant_of_source(store, task, source_task_id)
        ):
            descendants.append(task)
    return descendants


def _is_based_on_descendant_of_source(
    store: SqliteTaskStore,
    task: DbTask,
    source_task_id: str,
) -> bool:
    current: DbTask | None = task
    seen: set[str] = set()

    while current is not None and current.based_on is not None:
        parent_id = current.based_on
        if parent_id == source_task_id:
            return True
        if parent_id in seen:
            return False
        seen.add(parent_id)
        current = store.get(parent_id)

    return False


def _resolve_partial_materialized_task_ref(
    value: str | None,
    indexed_tasks: Mapping[int, DbTask],
) -> str | None:
    if value is None:
        return None
    if not value.startswith("__new_task_idx__:"):
        return value
    index = int(value.split(":", 1)[1])
    resolved = indexed_tasks.get(index)
    if resolved is None or resolved.id is None:
        raise ValueError(
            f"partial slice repair cannot validate unresolved slice dependency at manifest index {index}"
        )
    return resolved.id


def _plan_review_timeout_budget_minutes(config: Config) -> int:
    return int(config.get_plan_slice_target_timeout_minutes())


def _serialize_plan_review_manifest(manifest: PlanReviewManifest) -> str:
    return json.dumps(asdict(manifest), indent=2, sort_keys=True) + "\n"

def _latest_plan_review_override_artifact(
    store: SqliteTaskStore,
    review_task: DbTask,
) -> Any | None:
    if review_task.id is None:
        return None
    artifacts = store.list_artifacts(review_task.id, kind=_PLAN_REVIEW_OVERRIDE_ARTIFACT_KIND)
    if not artifacts:
        return None
    return max(artifacts, key=lambda artifact: artifact.created_at)


def _read_plan_review_override_manifest(
    store: SqliteTaskStore,
    config: Config,
    *,
    review_task: DbTask,
    plan_source_task: DbTask,
) -> PlanReviewManifest | None:
    artifact = _latest_plan_review_override_artifact(store, review_task)
    if artifact is None:
        return None
    artifact_path = resolve_artifact_path(Path(config.project_dir), artifact.path)
    raw_manifest = json.loads(artifact_path.read_text())
    if not isinstance(raw_manifest, dict):
        raise PlanReviewValidationError("stored plan review override is not a JSON object")
    return validate_plan_review_manifest(
        raw_manifest,
        markdown_verdict="APPROVED",
        source_task_id=plan_source_task.id or "",
        source_task_type=plan_source_task.task_type,
        max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
        max_plan_slices=getattr(config, "max_plan_slices", None),
    )


def resolve_effective_plan_review_manifest(
    store: SqliteTaskStore,
    config: Config,
    *,
    review_task: DbTask,
    plan_source_task: DbTask,
) -> tuple[PlanReviewManifest | None, str]:
    """Return the manifest that should drive manual materialization for a review."""
    state = resolve_effective_plan_review_manifest_state(
        store,
        config,
        review_task=review_task,
        plan_source_task=plan_source_task,
    )
    return state.manifest, state.source


def resolve_effective_plan_review_manifest_state(
    store: SqliteTaskStore,
    config: Config,
    *,
    review_task: DbTask,
    plan_source_task: DbTask,
) -> EffectivePlanReviewManifestState:
    """Return the effective manual-materialization manifest plus validation details."""
    override_artifact = _latest_plan_review_override_artifact(store, review_task)
    if override_artifact is not None:
        try:
            override_manifest = _read_plan_review_override_manifest(
                store,
                config,
                review_task=review_task,
                plan_source_task=plan_source_task,
            )
        except (OSError, ValueError, PlanReviewValidationError) as exc:
            return EffectivePlanReviewManifestState(
                manifest=None,
                source="override",
                verdict="APPROVED",
                validation_error=str(exc),
            )
        if override_manifest is not None:
            return EffectivePlanReviewManifestState(
                manifest=override_manifest,
                source="override",
                verdict="APPROVED",
            )

    outcome = get_plan_review_outcome(
        Path(config.project_dir),
        review_task,
        source_task_id=plan_source_task.id or "",
        source_task_type=plan_source_task.task_type,
        max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
        max_plan_slices=getattr(config, "max_plan_slices", None),
    )
    return EffectivePlanReviewManifestState(
        manifest=outcome.manifest,
        source="review",
        verdict=outcome.verdict,
        validation_error=outcome.validation_error,
    )


def persist_plan_review_override_manifest(
    store: SqliteTaskStore,
    config: Config,
    *,
    review_task: DbTask,
    plan_source_task: DbTask,
    manifest: PlanReviewManifest,
) -> None:
    if review_task.id is None or plan_source_task.id is None:
        raise ValueError("review_task.id and plan_source_task.id are required")
    store_command_output_artifact(
        store,
        review_task,
        config,
        kind=_PLAN_REVIEW_OVERRIDE_ARTIFACT_KIND,
        producer="gza.cli.plan-review",
        label="plan_review_manifest_override",
        output=_serialize_plan_review_manifest(manifest),
        status="validated",
        metadata={
            "schema_version": PLAN_REVIEW_ARTIFACT_SCHEMA_VERSION,
            "review_task_id": review_task.id,
            "source_task_id": plan_source_task.id,
            "source_task_type": plan_source_task.task_type,
            "manifest_digest": plan_review_manifest_digest(manifest),
        },
    )


def _create_resume_task(
    store: SqliteTaskStore,
    original_task: DbTask,
    *,
    trigger_source: str,
) -> DbTask:
    """Create a new resume task pointing to the original failed task.

    Copies prompt, task_type, tags, session_id, branch, model, etc.
    Preserves provider across resumes:
      - When the original task had an explicit provider override, it carries over.
      - When the resume will reuse a backend session_id, the originally resolved
        provider is frozen as an explicit override so the resumed run cannot
        switch backends under the same session_id even if task-type routing
        changed between attempts.
    Sets based_on to original_task.id to track resume lineage.
    """
    assert original_task.id is not None
    carry_session = original_task.session_id is not None
    preserve_provider = bool(
        original_task.provider and (original_task.provider_is_explicit or carry_session)
    )
    new_task = store.add(
        prompt=original_task.prompt,
        task_type=original_task.task_type,
        tags=resolve_derived_task_tags(original_task),
        spec=original_task.spec,
        review_scope=original_task.review_scope,
        depends_on=original_task.depends_on,
        create_review=original_task.create_review,
        auto_implement=original_task.auto_implement is not False,
        create_pr=original_task.create_pr,
        same_branch=original_task.same_branch,
        task_type_hint=original_task.task_type_hint,
        based_on=original_task.id,  # Track resume lineage (points to failed task)
        model=original_task.model,
        model_is_explicit=original_task.model_is_explicit,
        provider=original_task.provider if preserve_provider else None,
        provider_is_explicit=preserve_provider,
        recovery_origin="resume",
        trigger_source=trigger_source,
        enforce_single_active_sibling=_recovery_task_requires_singleton_guard(original_task),
    )
    # Copy session_id and branch from original task so the resumed run
    # continues the Claude Code session and uses the same branch.
    assert new_task.id is not None
    new_task.session_id = original_task.session_id
    new_task.branch = original_task.branch
    store.update(new_task)
    return new_task


def _create_retry_task(
    store: SqliteTaskStore,
    original_task: DbTask,
    *,
    trigger_source: str,
    automatic_recovery: bool = False,
) -> DbTask:
    """Create a fresh retry task pointing to the original task.

    Implement retries keep the historical fresh-branch semantics: when the
    failed task was same-branch and already had a branch, retry from that prior
    branch via ``base_branch``. All other task types preserve same-branch
    targeting against the original merge-unit branch across both manual retry
    and automatic recovery.
    """
    assert original_task.id is not None
    retry_same_branch = original_task.same_branch
    retry_base_branch: str | None = None
    should_fork_retry_branch = (
        original_task.task_type == "implement" and original_task.same_branch and original_task.branch
    )
    if should_fork_retry_branch:
        retry_same_branch = False
        retry_base_branch = original_task.branch
    elif original_task.task_type == "rebase":
        retry_base_branch = resolve_rebase_base_branch(original_task)

    retry_task = store.add(
        prompt=original_task.prompt,
        task_type=original_task.task_type,
        tags=resolve_derived_task_tags(original_task),
        spec=original_task.spec,
        review_scope=original_task.review_scope,
        depends_on=original_task.depends_on,
        create_review=original_task.create_review,
        auto_implement=original_task.auto_implement is not False,
        create_pr=original_task.create_pr,
        same_branch=retry_same_branch,
        task_type_hint=original_task.task_type_hint,
        based_on=original_task.id,
        model=original_task.model,
        model_is_explicit=original_task.model_is_explicit,
        provider=original_task.provider if original_task.provider_is_explicit else None,
        provider_is_explicit=original_task.provider_is_explicit,
        base_branch=retry_base_branch,
        recovery_origin="retry",
        trigger_source=trigger_source,
        enforce_single_active_sibling=_recovery_task_requires_singleton_guard(original_task),
    )
    updates_needed = False
    if retry_task.same_branch and original_task.branch and retry_task.branch != original_task.branch:
        retry_task.branch = original_task.branch
        updates_needed = True

    if (
        original_task.task_type != "implement"
        and store.supports_merge_units()
        and retry_task.id is not None
    ):
        original_unit = _resolve_retry_merge_unit(store, original_task)
        if original_unit is not None:
            store.attach_task_to_merge_unit(retry_task.id, original_unit.id, merge_unit_membership_role(retry_task))
            if retry_task.same_branch and original_unit.source_branch and retry_task.branch != original_unit.source_branch:
                retry_task.branch = original_unit.source_branch
                updates_needed = True
        elif automatic_recovery and retry_task.same_branch and original_task.task_type == "rebase":
            target_branch = resolve_rebase_target_branch(store, original_task)
            if target_branch and retry_task.branch != target_branch:
                retry_task.branch = target_branch
                updates_needed = True

    if updates_needed:
        store.update(retry_task)
    return retry_task


def _recovery_task_requires_singleton_guard(task: DbTask) -> bool:
    """Return whether retry/resume cloning should enforce singleton derived-child rules."""
    if task.task_type in {"review", "rebase"}:
        return True
    return task.task_type == "improve" and task.depends_on is not None


def _duplicate_active_child_label(task: DbTask) -> str:
    if task.task_type == "improve" and task.depends_on is not None:
        return "review-backed improve"
    return task.task_type or "task"


def format_duplicate_active_child_message(
    exc: DuplicateActiveChildError,
    *,
    parent_task_id: str | None = None,
    task: DbTask | None = None,
) -> str:
    """Render a stable operator-facing message for duplicate active singleton children."""
    active_child = exc.active_child
    display_task = task or active_child
    parent_id = parent_task_id or active_child.based_on
    label = _duplicate_active_child_label(display_task)
    if parent_id and active_child.id:
        return f"{label} already pending/in progress for {parent_id}: {active_child.id}"
    if active_child.id:
        return f"{label} already pending/in progress: {active_child.id}"
    return f"{label} already pending/in progress"


def _resolve_retry_merge_unit(store: SqliteTaskStore, original_task: DbTask):
    """Resolve the canonical merge unit for a non-implement retry."""
    assert original_task.id is not None
    if original_task.task_type == "rebase":
        canonical_task = resolve_rebase_target_task(store, original_task)
        if canonical_task is not None:
            canonical_unit = (
                store.resolve_merge_unit_for_task(canonical_task.id)
                if canonical_task.id is not None
                else None
            ) or (store.get_or_create_merge_unit_for_task(canonical_task) if canonical_task.branch else None)
            if canonical_unit is not None:
                return canonical_unit

    attached_unit = store.resolve_merge_unit_for_task(original_task.id)
    if attached_unit is not None:
        return attached_unit

    if original_task.task_type in {"improve", "fix", "review"}:
        impl_task, err = resolve_impl_task(store, original_task.id)
        if err is None and impl_task is not None:
            return (
                store.resolve_merge_unit_for_task(impl_task.id)
                if impl_task.id is not None
                else None
            ) or store.get_or_create_merge_unit_for_task(impl_task)

    if original_task.task_type == "rebase" and original_task.based_on:
        parent = store.get(original_task.based_on)
        if parent is not None:
            return (
                store.resolve_merge_unit_for_task(parent.id)
                if parent.id is not None
                else None
            ) or (store.get_or_create_merge_unit_for_task(parent) if parent.branch else None)

    return None


def _auto_rebase_before_resume(config: Config, task_id: str) -> int:
    """Rebase resumable code-task branches onto their canonical local target before resuming."""
    store = get_store(config)
    task = store.get(task_id)
    if task is None or not task.branch or task.task_type not in {"task", "implement", "improve"}:
        return 0

    merge_unit = _resolve_retry_merge_unit(store, task)
    target_branch = (
        merge_unit.target_branch if merge_unit is not None else store.default_merge_target(strict=True)
    )
    try:
        rebase_task = _create_rebase_task(
            store,
            task.id or task_id,
            task.branch,
            target_branch,
            trigger_source="manual",
        )
    except DuplicateActiveChildError as exc:
        print(format_duplicate_rebase_message(exc, parent_task_id=task.id or task_id), file=sys.stderr)
        return 1
    assert rebase_task.id is not None
    rebase_task.branch = task.branch
    store.update(rebase_task)
    from .git_ops import _run_task_backed_rebase

    print(f"Auto-rebasing task {task.id} onto '{target_branch}' before resume...")
    return _run_task_backed_rebase(
        config=config,
        store=store,
        rebase_task=rebase_task,
        branch=task.branch,
        target_branch=target_branch,
        remote=False,
        parent_task_id=task.id,
        failure_hint_lines=[
            "Use 'gza retry' to create a new retry attempt or run 'gza rebase' manually.",
        ],
    )


def run_with_recovery(
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    *,
    run_task: Callable[[DbTask, bool], int],
    max_resume_attempts: int | None = None,
    on_recovery: Callable[[DbTask, DbTask, FailedRecoveryDecision], None] | None = None,
    on_terminal_skip: Callable[[DbTask, FailedRecoveryDecision, int], None] | None = None,
) -> tuple[DbTask, int]:
    """Execute a task and apply the shared automatic recovery policy.

    Args:
        config: Loaded project configuration.
        store: Task store for reloading state and creating resume tasks.
        task: Task to execute.
        run_task: Callback that executes a task and returns exit code.
            Signature: ``run_task(task, resume)`` where ``resume`` indicates
            whether this invocation is resuming an existing session.
        max_resume_attempts: Recovery-policy override. ``0`` disables automatic
            recovery. Any positive value enables the bounded shared policy.
        on_recovery: Optional callback invoked when a recovery child is created.
            Signature: ``on_recovery(failed_task, recovery_task, decision)``.
        on_terminal_skip: Optional callback invoked when recovery stops without
            creating a child. Signature:
            ``on_terminal_skip(failed_task, decision, exit_code)``.

    Returns:
        Tuple of ``(final_task, exit_code)``.
    """
    def _failure_exit_code(raw_rc: int) -> int:
        return raw_rc if raw_rc != 0 else 1

    effective_limit = config.max_resume_attempts if max_resume_attempts is None else max_resume_attempts
    if not isinstance(effective_limit, int) or effective_limit < 0:
        effective_limit = 0

    current_task = task
    resume_mode = False

    while True:
        rc = run_task(current_task, resume_mode)

        if current_task.id is not None:
            refreshed = store.get(current_task.id) or current_task
        else:
            refreshed = current_task

        # The runner reports handled task outcomes through task state. A timed-out
        # or max-steps task can still return process exit 0 after marking itself failed.
        if refreshed.status != "failed":
            return refreshed, 0

        decision = decide_failed_task_recovery(
            store,
            refreshed,
            max_recovery_attempts=effective_limit,
        )
        if decision.action == "skip":
            if on_terminal_skip is not None:
                on_terminal_skip(refreshed, decision, _failure_exit_code(rc))
            return refreshed, _failure_exit_code(rc)

        if decision.action == "resume":
            if decision.reuse_existing and decision.recovery_task_id is not None:
                resume_task = store.get(decision.recovery_task_id)
                assert resume_task is not None
            else:
                resume_task = _create_resume_task(store, refreshed, trigger_source="auto-recovery")
            if on_recovery is not None:
                on_recovery(refreshed, resume_task, decision)
            current_task = resume_task
            resume_mode = True
            continue
        if decision.action == "reconcile":
            if not refreshed.branch:
                if on_terminal_skip is not None:
                    on_terminal_skip(refreshed, decision, _failure_exit_code(rc))
                return refreshed, _failure_exit_code(rc)
            from ..git import Git
            from .git_ops import _reconcile_diverged_branch_with_origin, complete_branch_unpushable_after_reconcile

            git = Git(config.project_dir)
            reconcile_outcome = _reconcile_diverged_branch_with_origin(
                config,
                git,
                refreshed,
                target_branch=git.default_branch(),
            )
            if reconcile_outcome.status != "reconciled":
                if on_terminal_skip is not None:
                    on_terminal_skip(refreshed, decision, _failure_exit_code(rc))
                return refreshed, _failure_exit_code(rc)
            completion_rc = complete_branch_unpushable_after_reconcile(
                config=config,
                store=store,
                git=git,
                task=refreshed,
            )
            if refreshed.id is not None:
                refreshed = store.get(refreshed.id) or refreshed
            if completion_rc != 0 or refreshed.status == "failed":
                return refreshed, _failure_exit_code(completion_rc)
            return refreshed, 0

        if decision.reuse_existing and decision.recovery_task_id is not None:
            retry_task = store.get(decision.recovery_task_id)
            assert retry_task is not None
        else:
            retry_task = _create_retry_task(
                store,
                refreshed,
                trigger_source="auto-recovery",
                automatic_recovery=True,
            )
        if on_recovery is not None:
            on_recovery(refreshed, retry_task, decision)
        current_task = retry_task
        resume_mode = False


def run_with_resume(
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    *,
    run_task: Callable[[DbTask, bool], int],
    max_resume_attempts: int | None = None,
    on_resume: Callable[[DbTask, DbTask, int, int], None] | None = None,
) -> tuple[DbTask, int]:
    """Backward-compatible wrapper for callers/tests using the older helper name."""

    def _on_recovery(
        failed_task: DbTask,
        recovery_task: DbTask,
        decision: FailedRecoveryDecision,
    ) -> None:
        if decision.action != "resume" or on_resume is None:
            return
        on_resume(
            failed_task,
            recovery_task,
            decision.attempt_index,
            decision.attempt_limit,
        )

    return run_with_recovery(
        config,
        store,
        task,
        run_task=run_task,
        max_resume_attempts=max_resume_attempts,
        on_recovery=_on_recovery if on_resume is not None else None,
    )


def _resolve_task_log_path(config: Config, task: DbTask) -> Path | None:
    """Resolve best log path for a task from explicit and inferred candidates."""
    from .log import _task_log_candidates

    candidates = _task_log_candidates(config, task)
    if not candidates:
        return None
    for candidate in candidates:
        if _existing_log_source_path(candidate) is not None:
            return candidate
    return candidates[0]


def _looks_like_verify_command(command: str, verify_command: str | None) -> bool:
    """Heuristic match for verification-related command invocations."""
    normalized = command.lower()
    if verify_command and verify_command.strip() and verify_command.strip().lower() in normalized:
        return True

    verify_tokens = [
        "pytest",
        "mypy",
        "ruff",
        "uv run pytest",
        "uv run mypy",
        "npm test",
        "pnpm test",
        "yarn test",
        "go test",
        "cargo test",
    ]
    return any(token in normalized for token in verify_tokens)


def _looks_like_failure_output(text: str) -> bool:
    """Heuristic match for command output that indicates failure."""
    lowered = text.lower()
    markers = ["failed", "error", "traceback", "assertionerror", "exit code", "exception"]
    return any(marker in lowered for marker in markers)


def _extract_failure_log_context(log_path: Path, verify_command: str | None) -> tuple[str | None, str | None]:
    """Extract last failing verify snippet and last result context from log."""
    from .log import _load_log_file_entries, _message_content_items

    try:
        log_data, entries, content = _load_log_file_entries(log_path)
    except OSError:
        return None, None

    if not entries and content:
        last_line = content.splitlines()[-1].strip()
        return None, truncate(last_line, 180) if last_line else None

    tool_calls: dict[str, str] = {}
    last_verify_failure: str | None = None
    last_result_context: str | None = None

    def _result_snippet(value: Any, limit: int = 160) -> str:
        if isinstance(value, str):
            return truncate(value.replace("\\n", "\n"), limit)
        return truncate(json.dumps(value, ensure_ascii=True), limit)

    def _record_verify_failure(command: str, output: Any, *, is_error: bool) -> None:
        nonlocal last_verify_failure
        snippet = _result_snippet(output)
        if _looks_like_verify_command(command, verify_command) and (is_error or _looks_like_failure_output(snippet)):
            last_verify_failure = f"{truncate(command, 120)} => {snippet}"

    def _store_tool_command(tool_id: str, command: str) -> None:
        if tool_id and command:
            tool_calls[tool_id] = command

    def _extract_command(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            command = value.get("command")
            if isinstance(command, str):
                return command.strip()
        return ""

    def _resolve_tool_command(entry: dict) -> str:
        tool_id = str(entry.get("id") or entry.get("call_id") or entry.get("tool_use_id") or "")
        command = tool_calls.get(tool_id, "")
        if command:
            return command
        return _extract_command(entry.get("tool_input"))

    for entry in entries:
        entry_type = entry.get("type")
        if entry_type == "assistant":
            for item in _message_content_items(entry):
                if item.get("type") != "tool_use":
                    continue
                if str(item.get("name")) != "Bash":
                    continue
                tool_id = str(item.get("id") or "")
                tool_input = item.get("input", {})
                if not isinstance(tool_input, dict):
                    continue
                command = str(tool_input.get("command") or "").strip()
                _store_tool_command(tool_id, command)
        elif entry_type == "user":
            for item in _message_content_items(entry):
                if item.get("type") != "tool_result":
                    continue
                tool_id = str(item.get("tool_use_id") or "")
                command = tool_calls.get(tool_id, "")
                if not command:
                    continue
                is_error = bool(item.get("is_error", False))
                _record_verify_failure(command, item.get("content", ""), is_error=is_error)
        elif entry_type == "tool_use":
            tool_name = str(entry.get("tool_name") or "")
            if tool_name != "Bash":
                continue
            command = _resolve_tool_command(entry)
            tool_id = str(entry.get("id") or entry.get("call_id") or "")
            _store_tool_command(tool_id, command)
        elif entry_type in {"tool_output", "tool_error"}:
            command = _resolve_tool_command(entry)
            if not command:
                continue
            is_error = entry_type == "tool_error"
            if not is_error:
                is_error = bool(entry.get("is_error"))
            if not is_error:
                exit_code = entry.get("exit_code")
                is_error = isinstance(exit_code, int) and exit_code != 0
            output = entry.get("content")
            if output is None:
                output = entry.get("output")
            if output is None:
                payload = {k: v for k, v in entry.items() if k not in {"type", "id", "call_id", "tool_use_id"}}
                output = payload
            _record_verify_failure(command, output, is_error=is_error)
        elif entry_type == "item.completed":
            item = entry.get("item", {})
            if not isinstance(item, dict):
                continue
            if item.get("type") != "command_execution":
                continue
            command = str(item.get("command") or "").strip()
            if not command:
                continue
            exit_code = item.get("exit_code")
            is_error = isinstance(exit_code, int) and exit_code != 0
            output = item.get("aggregated_output")
            if output is None:
                output = item.get("output")
            _record_verify_failure(command, output or "", is_error=is_error)
        elif entry_type == "result":
            subtype = str(entry.get("subtype") or "unknown")
            if subtype == "success":
                continue
            result_text = entry.get("result", "")
            if isinstance(result_text, str) and result_text.strip():
                detail = truncate(result_text.replace("\\n", "\n"), 180)
            elif entry.get("errors"):
                detail = truncate(json.dumps(entry.get("errors"), ensure_ascii=True), 180)
            else:
                detail = ""
            if detail:
                last_result_context = f"{subtype}: {detail}"
            else:
                last_result_context = subtype

    if last_result_context is None and log_data:
        subtype = str(log_data.get("subtype") or "")
        if subtype and subtype != "success":
            last_result_context = subtype

    return last_verify_failure, last_result_context


def _failure_summary(
    task: DbTask,
    reason: str,
    *,
    store: SqliteTaskStore | None = None,
) -> str:
    """Build short human-readable failure summary."""
    if reason == "PREREQUISITE_UNMERGED":
        empty_lookup = inspect_empty_merge_unit(store, task)
        if empty_lookup.warning is not None:
            return (
                "Dependency-ordering failure guidance unavailable; "
                f"{empty_lookup.warning}. Inspect merge-unit state before retrying."
            )
        if empty_lookup.is_empty:
            return (
                "Historical dependency-ordering failure produced no work, but the failed "
                "prerequisite lineage still requires recovery or manual resolution."
            )
    summaries = {
        "AGENT_FORFEIT": "Agent forfeited: could not complete the task.",
        "MAX_STEPS": "Stopped due to max steps limit.",
        "MAX_TURNS": "Stopped due to max turns limit.",
        "REBASE_CONFLICT": "Stopped because the rebase still requires manual conflict resolution.",
        "TERMINATED": "Stopped by an external termination signal.",
        "TERMINAL_NO_WORK": "Task produced no publishable work; the branch resolved as terminal no-work.",
        "PREREQUISITE_UNMERGED": "Dependency is not yet merged to main.",
        "TEST_FAILURE": "Stopped due to verification/test failure.",
        "UNKNOWN": "Task failed; inspect log output for details.",
    }
    return summaries.get(reason, f"Task failed: {reason}")


def _extract_last_agent_message_text(log_path: Path) -> str | None:
    """Extract the most recent ``item.completed`` agent_message text from a log file."""
    from .log import _load_log_file_entries

    try:
        _log_data, entries, _content = _load_log_file_entries(log_path)
    except OSError:
        return None

    last_message: str | None = None
    for entry in entries:
        if entry.get("type") != "item.completed":
            continue
        item = entry.get("item", {})
        if not isinstance(item, dict):
            continue
        if item.get("type") != "agent_message":
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            last_message = text
    return last_message


def _extract_agent_failure_marker_reason(log_path: Path) -> str | None:
    """Return the last ``[GZA_FAILURE:REASON]`` marker from the final agent message."""
    text = _extract_last_agent_message_text(log_path)
    if not text:
        return None
    marker_reason: str | None = None
    for line in text.splitlines():
        match = _FAILURE_MARKER_LINE_RE.match(line)
        if match:
            marker_reason = match.group("reason")
    return marker_reason


def _extract_last_agent_message_for_failure(log_path: Path) -> str | None:
    """Return final agent message text with failure marker lines removed."""
    text = _extract_last_agent_message_text(log_path)
    if not text:
        return None

    cleaned_lines = [
        line for line in text.splitlines()
        if _FAILURE_MARKER_LINE_RE.match(line) is None
    ]
    cleaned = "\n".join(cleaned_lines).strip()
    return cleaned or None


@dataclass(frozen=True)
class FailureDiagnostics:
    """Canonical failed-task diagnostics extracted from task/log context."""

    reason: str
    marker_reason: str | None
    summary: str
    interrupt_source: str | None
    worker_stage: str | None
    worker_exit_code: int | None
    worker_signal: str | None
    worker_output_tail: tuple[str, ...]
    worker_os_hint: str | None
    explanation: str | None
    verify_context: str | None
    result_context: str | None


def _extract_interrupt_source(log_path: Path) -> str | None:
    """Extract the latest structured interrupt source label from the task log."""
    from .log import _load_log_file_entries

    source_path = _existing_log_source_path(log_path)
    if source_path is None:
        return None
    try:
        entries = _load_log_file_entries(source_path)[1]
    except OSError:
        return None

    for entry in reversed(entries):
        if entry.get("type") != "gza" or entry.get("subtype") != "interrupt":
            continue
        source = entry.get("source")
        if isinstance(source, str) and source:
            detail = entry.get("detail")
            if isinstance(detail, str) and detail:
                return f"{source} ({detail})"
            return source
    return None


def _extract_worker_death_diagnostics(log_path: Path) -> WorkerDeathDiagnostics:
    """Extract the latest structured worker-death details from a task log."""
    from .log import _load_log_file_entries

    source_path = _existing_log_source_path(log_path)
    if source_path is None:
        return WorkerDeathDiagnostics()
    try:
        entries = _load_log_file_entries(source_path)[1]
    except OSError:
        return WorkerDeathDiagnostics()

    for entry in reversed(entries):
        if entry.get("type") != "gza" or entry.get("subtype") != "worker_lifecycle":
            continue
        if entry.get("event") not in {
            "death_detected",
            "startup_abort_detected",
            "detached_exit",
            "start_failed",
            "handoff_failed",
        }:
            continue
        exit_code_raw = entry.get("exit_code")
        exit_code = exit_code_raw if isinstance(exit_code_raw, int) else None
        signal_name = entry.get("signal") if isinstance(entry.get("signal"), str) else None
        signal_number_raw = entry.get("signal_number")
        signal_number = signal_number_raw if isinstance(signal_number_raw, int) else _signal_number_from_name(signal_name)
        output_tail_raw = entry.get("output_tail")
        output_tail = tuple(
            str(line).strip() for line in output_tail_raw
            if isinstance(line, str) and line.strip()
        ) if isinstance(output_tail_raw, list) else ()
        return WorkerDeathDiagnostics(
            stage=entry.get("stage") if isinstance(entry.get("stage"), str) else None,
            exit_code=exit_code,
            signal_name=signal_name or _signal_name_from_exit_code(exit_code),
            signal_number=signal_number,
            output_tail=output_tail,
            os_hint=entry.get("os_hint") if isinstance(entry.get("os_hint"), str) else None,
            source_event=entry.get("stage_source_event") if isinstance(entry.get("stage_source_event"), str) else None,
            worker_status=entry.get("worker_status") if isinstance(entry.get("worker_status"), str) else None,
            completion_reason=entry.get("completion_reason") if isinstance(entry.get("completion_reason"), str) else None,
        )
    return WorkerDeathDiagnostics()


def _existing_log_source_path(log_path: Path | None) -> Path | None:
    """Return an existing task log source, preferring structured ops siblings."""
    if log_path is None:
        return None
    ops_path = ops_log_path_for(log_path)
    if ops_path.exists():
        return ops_path
    if log_path.exists():
        return log_path
    return None


def _build_failure_diagnostics(
    task: DbTask,
    log_path: Path | None,
    verify_command: str | None,
    *,
    store: SqliteTaskStore | None = None,
) -> FailureDiagnostics:
    """Build canonical failure diagnostics for CLI rendering."""
    reason = task.failure_reason or "UNKNOWN"
    marker_reason: str | None = None
    interrupt_source: str | None = None
    worker_diagnostics = WorkerDeathDiagnostics()
    explanation: str | None = None
    verify_context: str | None = None
    result_context: str | None = None

    if log_path is not None and _existing_log_source_path(log_path) is not None:
        marker_reason = _extract_agent_failure_marker_reason(log_path)
        interrupt_source = _extract_interrupt_source(log_path)
        explanation = _extract_last_agent_message_for_failure(log_path)
        verify_context, result_context = _extract_failure_log_context(log_path, verify_command)
        if reason == "WORKER_DIED":
            worker_diagnostics = _extract_worker_death_diagnostics(log_path)

    return FailureDiagnostics(
        reason=reason,
        marker_reason=marker_reason,
        summary=_failure_summary(task, reason, store=store),
        interrupt_source=interrupt_source,
        worker_stage=worker_diagnostics.stage,
        worker_exit_code=worker_diagnostics.exit_code,
        worker_signal=worker_diagnostics.signal_name,
        worker_output_tail=worker_diagnostics.output_tail,
        worker_os_hint=worker_diagnostics.os_hint,
        explanation=explanation,
        verify_context=verify_context,
        result_context=result_context,
    )


def _render_failure_diagnostics(
    diagnostics: FailureDiagnostics,
    *,
    label_color: str,
    value_color: str,
    status_failed_color: str,
    soft_wrap: bool = False,
    include_explanation: bool = True,
) -> None:
    """Render canonical failed-task diagnostics for CLI output."""
    marker_text = ""
    if diagnostics.marker_reason:
        marker_label = rich_escape(f"[GZA_FAILURE:{diagnostics.marker_reason}]")
        marker_text = f" [{status_failed_color}]{marker_label}[/{status_failed_color}]"

    console.print(
        f"[{label_color}]Failure Reason:[/{label_color}] "
        f"[{status_failed_color}]{diagnostics.reason}[/{status_failed_color}]"
        f"{marker_text}",
        soft_wrap=soft_wrap,
    )
    console.print(
        f"[{label_color}]Failure Summary:[/{label_color}] "
        f"[{value_color}]{diagnostics.summary}[/{value_color}]",
        soft_wrap=soft_wrap,
    )
    if diagnostics.interrupt_source:
        console.print(
            f"[{label_color}]Termination Source:[/{label_color}] "
            f"[{value_color}]{rich_escape(diagnostics.interrupt_source)}[/{value_color}]",
            soft_wrap=soft_wrap,
        )
    if diagnostics.worker_signal or diagnostics.worker_exit_code is not None:
        exit_parts: list[str] = []
        if diagnostics.worker_signal:
            exit_parts.append(diagnostics.worker_signal)
        if diagnostics.worker_exit_code is not None:
            exit_parts.append(f"exit code {diagnostics.worker_exit_code}")
        console.print(
            f"[{label_color}]Worker Exit:[/{label_color}] "
            f"[{value_color}]{rich_escape(', '.join(exit_parts))}[/{value_color}]",
            soft_wrap=soft_wrap,
        )
    if diagnostics.worker_stage:
        console.print(
            f"[{label_color}]Worker Death Stage:[/{label_color}] "
            f"[{value_color}]{rich_escape(diagnostics.worker_stage)}[/{value_color}]",
            soft_wrap=soft_wrap,
        )
    if diagnostics.worker_output_tail:
        console.print(f"[{label_color}]Worker Output Tail:[/{label_color}]", soft_wrap=soft_wrap)
        console.print(
            Panel(
                "\n".join(rich_escape(line) for line in diagnostics.worker_output_tail),
                border_style=status_failed_color,
                padding=(0, 1),
                expand=False,
            )
        )
    if diagnostics.worker_os_hint:
        console.print(
            f"[{label_color}]Worker OS Hint:[/{label_color}] "
            f"[{value_color}]{rich_escape(diagnostics.worker_os_hint)}[/{value_color}]",
            soft_wrap=soft_wrap,
        )

    if include_explanation:
        console.print(f"[{label_color}]Agent Explanation:[/{label_color}]", soft_wrap=soft_wrap)
        if diagnostics.explanation:
            console.print(
                Panel(
                    rich_escape(diagnostics.explanation),
                    border_style=status_failed_color,
                    padding=(0, 1),
                    expand=False,
                )
            )
        else:
            console.print(f"[{value_color}]  (not found in log)[/{value_color}]", soft_wrap=soft_wrap)

    if diagnostics.verify_context:
        console.print(
            f"[{label_color}]Last Verify Failure:[/{label_color}] "
            f"[{value_color}]{rich_escape(diagnostics.verify_context)}[/{value_color}]",
            soft_wrap=soft_wrap,
        )
    if diagnostics.result_context:
        console.print(
            f"[{label_color}]Last Result Context:[/{label_color}] "
            f"[{value_color}]{rich_escape(diagnostics.result_context)}[/{value_color}]",
            soft_wrap=soft_wrap,
        )


def _precondition_blocking_dependency_id(task: DbTask, config: Config | None) -> str | None:
    """Extract dependency_task_id from dependency-merge precondition log entries."""
    if config is None or not task.log_file:
        return None
    from .log import _load_log_file_entries

    log_path = config.project_dir / task.log_file
    source_path = _existing_log_source_path(log_path)
    if source_path is None:
        return None
    try:
        entries = _load_log_file_entries(source_path)[1]
    except OSError:
        return None

    for entry in reversed(entries):
        if entry.get("type") != "gza" or entry.get("subtype") not in {"outcome", "blocked"}:
            continue
        if entry.get("failure_reason") != "PREREQUISITE_UNMERGED" and entry.get("reason") != "dependency_merge_precondition":
            continue
        dep_id = entry.get("dependency_task_id")
        if isinstance(dep_id, str) and dep_id:
            return dep_id
    return None


def _failure_next_steps(
    task: DbTask,
    reason: str,
    *,
    config: Config | None = None,
    store: SqliteTaskStore | None = None,
) -> list[str]:
    """Return concrete next-step commands for a failed task."""
    if task.id is None:
        return []

    steps = [f"gza log -t {task.id} --steps-verbose"]
    if reason == "AGENT_FORFEIT":
        steps.append(f"gza retry {task.id}")
        return steps
    if reason == "PREREQUISITE_UNMERGED":
        empty_lookup = inspect_empty_merge_unit(store, task)
        if empty_lookup.warning is not None or empty_lookup.is_empty:
            return steps
        blocking_dep_id = _precondition_blocking_dependency_id(task, config) or task.depends_on
        if blocking_dep_id:
            steps.append(f"gza merge {blocking_dep_id}")
        return steps
    if is_resumable_failure_reason(reason):
        if task.session_id:
            steps.append(f"gza resume {task.id}")
        steps.append(f"gza retry {task.id}")
        return steps

    if reason == "TEST_FAILURE":
        steps.append(f"gza retry {task.id}")
        return steps

    if task.session_id:
        steps.append(f"gza resume {task.id}")
    steps.append(f"gza retry {task.id}")
    return steps


class GzaArgumentParser(argparse.ArgumentParser):
    """ArgumentParser with compact unknown-command errors and retired-flag fallbacks."""

    _INVALID_CHOICE_RE = re.compile(
        r"argument (?P<argument>[\w-]+|\{[^}]+\}): invalid choice: ['\"]?(?P<cmd>[^'\"\s]+)['\"]?"
    )
    _RETIRED_INVALID_CHOICE_FLAGS = frozenset({
        "--continue",
        "--depends-on",
        "--execution-mode",
        "--group",
        "--preset",
        "--view",
    })

    @classmethod
    def _retired_invalid_choice_message(cls, cmd: str, argv: list[str]) -> str | None:
        """Map retired flag parse fallthroughs back to `unrecognized arguments` errors."""
        if cmd not in argv:
            return None

        cmd_index = argv.index(cmd)
        if cmd in cls._RETIRED_INVALID_CHOICE_FLAGS:
            trailing_value = ""
            if cmd_index + 1 < len(argv) and not argv[cmd_index + 1].startswith("-"):
                trailing_value = f" {argv[cmd_index + 1]}"
            return f"unrecognized arguments: {cmd}{trailing_value}"

        if cmd_index == 0:
            return None

        previous = argv[cmd_index - 1]
        if previous not in cls._RETIRED_INVALID_CHOICE_FLAGS:
            return None
        return f"unrecognized arguments: {previous} {cmd}"

    @staticmethod
    def _is_command_choice_argument(argument: str) -> bool:
        """Return whether an invalid-choice error came from command selection."""
        return argument == "command" or argument.startswith("{")

    def error(self, message: str) -> NoReturn:
        match = self._INVALID_CHOICE_RE.match(message)
        if match:
            argument = match.group("argument")
            cmd = match.group("cmd")
            retired_message = self._retired_invalid_choice_message(cmd, sys.argv)
            if retired_message is not None:
                super().error(retired_message)
            if not self._is_command_choice_argument(argument):
                super().error(message)
            # Plain-word invalid commands keep argparse's native wording so the
            # user sees the full choice list. Hyphenated pseudo-commands get
            # the terse git-style message below.
            if cmd.isalpha():
                super().error(message)
            self.exit(
                2,
                f"{self.prog}: '{cmd}' is not a gza command. See 'gza --help'.\n",
            )
        super().error(message)


class SortingHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Custom help formatter that sorts subcommands alphabetically."""

    def _iter_indented_subactions(self, action):
        """Override to sort subactions alphabetically by their command name."""
        try:
            # Get the subactions (subcommands)
            subactions = action._get_subactions()
        except AttributeError:
            # If no _get_subactions, fall back to default behavior
            subactions = super()._iter_indented_subactions(action)
        else:
            # Sort subcommands alphabetically by their metavar (command name)
            subactions = sorted(subactions, key=lambda x: x.metavar if x.metavar else "")

        # Yield sorted subactions with indentation
        yield from subactions

    def _metavar_formatter(self, action, default_metavar):
        """Override to sort choices alphabetically in usage string."""
        if action.metavar is not None:
            result = action.metavar
        elif action.choices is not None:
            # Sort choices alphabetically
            choice_strs = sorted(str(choice) for choice in action.choices)
            result = '{{{}}}'.format(','.join(choice_strs))
        else:
            result = default_metavar

        def format(tuple_size):
            if isinstance(result, tuple):
                return result
            else:
                return (result, ) * tuple_size
        return format


def _add_skills_install_args(
    parser: argparse.ArgumentParser,
) -> None:
    """Add common arguments for skills install commands."""
    parser.add_argument(
        "skills",
        nargs="*",
        help="Specific skills to install (installs all public skills if not specified)",
    )
    parser.add_argument(
        "--target",
        choices=["claude", "codex", "gemini", "all"],
        action="append",
        help="Install target(s): claude, codex, gemini, or all (default depends on command)",
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Overwrite existing skills",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available skills without installing",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Include dev (non-public) skills",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update skills that have a newer bundled version",
    )


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO timestamp safely."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to a subparser."""
    parser.add_argument(
        "--project",
        "-C",
        dest="project_dir",
        default=None,
        help="Target project directory (default: current directory)",
    )


def _add_query_filter_args(parser: argparse.ArgumentParser) -> None:
    """Add shared query/filter arguments to a subparser (history, stats, etc.)."""
    parser.add_argument(
        "--last",
        "-n",
        type=int,
        metavar="N",
        help="Show last N tasks",
    )
    parser.add_argument(
        "--type",
        type=str,
        choices=list(CLI_FILTER_TASK_TYPES),
        help="Filter tasks by task_type",
    )
    parser.add_argument(
        "--type-not",
        type=str,
        choices=list(CLI_FILTER_TASK_TYPES),
        help="Exclude tasks by task_type",
    )
    parser.add_argument(
        "--days",
        type=int,
        metavar="N",
        help="Show only tasks from the last N days",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        metavar="YYYY-MM-DD",
        help="Show only tasks on or after this date",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        metavar="YYYY-MM-DD",
        help="Show only tasks on or before this date",
    )
    parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Filter by tag (repeatable; matches any requested tag by default)",
    )
    parser.add_argument(
        "--tag-not",
        action="append",
        dest="tags_not",
        metavar="TAG",
        help="Exclude by tag (repeatable, same matching mode as --tag)",
    )
    parser.add_argument(
        "--all-tags",
        action="store_true",
        dest="all_tags",
        help="With repeated --tag/--tag-not values, require all requested tags instead of the default any-tag matching",
    )


def _get_pager(repo_dir: Path) -> str:
    """Determine which pager to use for output.

    Checks in order:
    1. $GIT_PAGER environment variable
    2. git config core.pager
    3. $PAGER environment variable
    4. Falls back to 'less -R'

    Args:
        repo_dir: Path to git repository (used for git config lookup)

    Returns:
        The pager command to use
    """
    git_pager = os.environ.get('GIT_PAGER')
    if git_pager:
        return git_pager

    try:
        result = subprocess.run(
            ["git", "config", "core.pager"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        import logging
        logging.getLogger(__name__).debug("Failed to read git core.pager config", exc_info=True)

    pager = os.environ.get('PAGER')
    if pager:
        return pager

    return 'less -R'


class _GzaPager(Pager):
    """Rich Pager implementation that pipes content through the configured pager command."""

    def __init__(self, pager_cmd: str) -> None:
        self._pager_cmd = pager_cmd

    def show(self, content: str) -> None:
        pager_proc = subprocess.Popen(
            self._pager_cmd,
            stdin=subprocess.PIPE,
            shell=True,
        )
        pager_proc.communicate(content.encode('utf-8', errors='replace'))


def pager_context(use_page: bool, project_dir: Path) -> contextlib.AbstractContextManager:
    """Return a context manager that pipes Rich console output through the configured pager.

    When ``use_page`` is False or stdout is not a TTY, returns a no-op context.

    Args:
        use_page: Whether paging was requested (e.g. ``args.page``).
        project_dir: Project root used to resolve the pager command.

    Returns:
        A context manager; use it as ``with pager_context(...): ...``.
    """
    if use_page and _stdout_is_tty():
        from ..console import console
        pager_cmd = _get_pager(project_dir)
        return console.pager(pager=_GzaPager(pager_cmd), styles=True)
    return nullcontext()
