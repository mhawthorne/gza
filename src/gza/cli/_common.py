"""Shared helpers, constants, and lightweight utilities used across CLI sub-modules."""

import argparse
import contextlib
import json
import os
import re
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn

from rich.markup import escape as rich_escape
from rich.pager import Pager
from rich.panel import Panel

from ..config import Config
from ..console import (
    MAX_PROMPT_DISPLAY,
    console,
    truncate,
)
from ..db import (
    ManualMigrationRequired,
    SqliteTaskStore,
    StoreOpenMode,
    Task as DbTask,
    resolve_task_id,
    task_id_numeric_key,
)
from ..failure_policy import is_resumable_failure_reason
from ..prompts import PromptBuilder
from ..resume_policy import is_resumable_failure
from ..review_tasks import (
    DuplicateReviewError,  # noqa: F401
    create_or_reuse_followup_task,
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
from ..runner import RunInvocationContext, get_effective_config_for_task, run
from ..tmux_proxy import get_tmux_session_pid
from ..workers import WorkerMetadata, WorkerRegistry


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
    group_attr: str = "group",
    any_tag_attr: str = "any_tag",
    warn_on_group_alias: bool = True,
) -> tuple[tuple[str, ...] | None, bool]:
    """Parse and validate tag/group filter flags from argparse args."""
    selected_tags = [_validate_tag_value(raw) for raw in (getattr(args, tags_attr, None) or [])]
    legacy_group = getattr(args, group_attr, None) if hasattr(args, group_attr) else None
    if legacy_group is not None:
        selected_tags.append(_validate_tag_value(legacy_group))
        if warn_on_group_alias:
            print("Warning: --group is deprecated; use --tag instead.", file=sys.stderr)
    return (tuple(selected_tags) if selected_tags else None, bool(getattr(args, any_tag_attr, False)))


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


NO_ACTIVITY_THRESHOLD_SECONDS = 60


def _task_looks_stuck(config: Config, task: DbTask) -> bool:
    """Return True if an in-progress task has not logged anything in NO_ACTIVITY_THRESHOLD_SECONDS.

    A task is considered stuck when its process has been alive for more than
    the threshold and its log file is either missing, empty, or has not been
    written to within the threshold.  This catches preflight hangs such as a
    CLI blocking on an update/login prompt.
    """
    if task.started_at is None:
        return False
    now = datetime.now(UTC)
    age = (now - task.started_at).total_seconds()
    if age <= NO_ACTIVITY_THRESHOLD_SECONDS:
        return False
    if not task.log_file:
        return True
    log_path = config.project_dir / task.log_file
    try:
        stat = log_path.stat()
    except OSError:
        return True
    if stat.st_size == 0:
        return True
    mtime_age = now.timestamp() - stat.st_mtime
    return mtime_age > NO_ACTIVITY_THRESHOLD_SECONDS


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
    except ManualMigrationRequired:
        # DB needs gza migrate — skip reconciliation silently
        return
    except (sqlite3.Error, OSError, ValueError) as exc:
        print(f"Warning: Skipping task reconciliation due to setup error: {exc}", file=sys.stderr)
        return
    except Exception as exc:
        print(f"Warning: Skipping task reconciliation due to unexpected error: {exc}", file=sys.stderr)
        return

    for task in store.get_in_progress():
        task_label = f"{task.id}" if task.id is not None else "<unknown>"
        try:
            is_dead = False
            if task.running_pid is None:
                # No PID tracked — mark as orphaned if the task was actually started.
                if task.started_at is not None:
                    is_dead = True
                else:
                    continue
            elif task.running_pid <= 0:
                is_dead = True
            else:
                try:
                    os.kill(task.running_pid, 0)
                except OSError:
                    is_dead = True

            if is_dead:
                has_commits = _branch_has_commits(config, task.branch)
                store.mark_failed(task, log_file=task.log_file, branch=task.branch, failure_reason="WORKER_DIED", has_commits=has_commits)
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
                store.mark_failed(
                    task,
                    log_file=task.log_file,
                    branch=task.branch,
                    failure_reason="NO_ACTIVITY",
                    has_commits=has_commits,
                )
        except (sqlite3.Error, OSError, ValueError) as exc:
            print(f"Warning: Failed to reconcile task {task_label}: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"Warning: Unexpected reconciliation error for task {task_label}: {exc}", file=sys.stderr)


def prune_terminal_dead_workers(config: Config) -> None:
    """Remove worker registry entries for terminal tasks when the worker PID is dead."""
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
            if registry.is_running(worker.worker_id):
                continue
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


def _print_work_message(message: str, *, color: str | None = None) -> None:
    """Print a themed work/worker message via the shared Rich console."""
    wc = _colors.WORK_COLORS
    message_color = color or wc.default
    console.print(f"[{message_color}]{rich_escape(message)}[/{message_color}]")


def _print_background_worker_started(
    task: DbTask,
    *,
    pid: int,
    quiet: bool,
    resume: bool = False,
) -> None:
    """Print shared themed startup output for background worker launches."""
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
    return proc, str(startup_log_path.relative_to(config.project_dir))


def _run_foreground(
    config: Config,
    task_id: str | None,
    resume: bool = False,
    open_after: bool = False,
    force: bool = False,
    invocation: RunInvocationContext | None = None,
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
    worker_id = registry.generate_worker_id()

    worker = WorkerMetadata(
        worker_id=worker_id,
        task_id=task_id,
        pid=os.getpid(),
        is_background=False,
    )
    registry.register(worker)

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

    try:
        if resume and task_id is not None:
            rebase_exit_code = _auto_rebase_before_resume(config, task_id)
            if rebase_exit_code != 0:
                registry.mark_completed(worker_id, exit_code=rebase_exit_code, status="failed")
                return rebase_exit_code
        if invocation is None:
            exit_code = run(
                config,
                task_id=task_id,
                resume=resume,
                open_after=open_after,
                skip_precondition_check=force,
            )
        else:
            exit_code = run(
                config,
                task_id=task_id,
                resume=resume,
                open_after=open_after,
                skip_precondition_check=force,
                invocation=invocation,
            )
        status = "completed" if exit_code == 0 else "failed"
        registry.mark_completed(worker_id, exit_code=exit_code, status=status)
        return exit_code
    except KeyboardInterrupt:
        registry.mark_completed(worker_id, exit_code=130, status="failed")
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


def _spawn_background_worker(args: argparse.Namespace, config: Config, task_id: str | None = None, quiet: bool = False) -> int:
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
        task = store.get(explicit_task_id)
        if not task:
            _print_work_message(f"Error: Task {explicit_task_id} not found", color=_colors.WORK_COLORS.error)
            return 1

        if resume_mode:
            if task.status not in ("pending", "failed"):
                _print_work_message(
                    f"Error: Task {explicit_task_id} is not resumable (status: {task.status})",
                    color=_colors.WORK_COLORS.error,
                )
                return 1
            if not task.session_id:
                _print_work_message(
                    f"Error: Task {explicit_task_id} has no session ID (cannot resume)",
                    color=_colors.WORK_COLORS.error,
                )
                return 1
        else:
            allow_pr_retry = _allow_pr_required_retry(args, task)
            if task.status != "pending" and not allow_pr_retry:
                _print_work_message(
                    f"Error: Task {explicit_task_id} is not pending (status: {task.status})",
                    color=_colors.WORK_COLORS.error,
                )
                return 1

            # Check if task is blocked
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                _print_work_message(
                    f"Error: Task {explicit_task_id} is blocked by task {blocking_id} ({blocking_status})",
                    color=_colors.WORK_COLORS.error,
                )
                return 1
        selected_task = task
    else:
        if resume_mode:
            _print_work_message("Error: Cannot resume without specifying a task ID", color=_colors.WORK_COLORS.error)
            return 1
        # Select a candidate for UX; actual claim happens in the child runner.
        selected_task = store.get_next_pending(tags=selected_tags, any_tag=any_tag)
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

    # Build inner command for the worker subprocess
    inner_cmd = [
        sys.executable, "-m", "gza",
        "work",
        "--worker-mode",
    ]
    if resume_mode:
        inner_cmd.append("--resume")

    if explicit_task_id is not None:
        inner_cmd.append(str(explicit_task_id))
    elif selected_tags:
        for tag in selected_tags:
            inner_cmd.extend(["--tag", tag])
        if any_tag:
            inner_cmd.append("--any-tag")

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
        session_task_id = explicit_task_id if explicit_task_id is not None else selected_task.id
        tmux_session = f"gza-{session_task_id}"
        inner_cmd.extend(["--tmux-session", tmux_session])

    # Spawn detached process
    try:
        worker_id = registry.generate_worker_id()
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
            task_id=explicit_task_id,  # None when no explicit task; child runner claims the task
            pid=pid,
            startup_log_file=startup_log_rel,
            tmux_session=tmux_session,
        )
        registry.register(worker_metadata)

        _print_background_worker_started(selected_task, pid=pid, quiet=quiet, resume=resume_mode)

        return 0

    except Exception as e:
        _print_work_message(f"Error spawning background worker: {e}", color=_colors.WORK_COLORS.error)
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

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print("\nReceived shutdown signal, cleaning up...")
        if worker_id:
            registry.mark_completed(worker_id, exit_code=1, status="failed")
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Run the task normally
    exit_code = 1
    startup_log_path: Path | None = None
    startup_task: DbTask | None = None
    startup_header_written = False
    explicit_task_id = args.task_ids[0] if hasattr(args, "task_ids") and args.task_ids else None
    if explicit_task_id is None and worker_id:
        meta = registry.get(worker_id)
        if meta and meta.task_id is not None:
            explicit_task_id = meta.task_id
    if explicit_task_id is not None:
        startup_task = store.get(explicit_task_id)
        if startup_task:
            startup_log_path = startup_log_path_for_task(config, startup_task)

    def _on_task_claimed(claimed_task: DbTask) -> None:
        nonlocal startup_task
        nonlocal startup_log_path
        nonlocal startup_header_written

        startup_task = claimed_task
        if startup_log_path is None:
            startup_log_path = startup_log_path_for_task(config, claimed_task)

        if worker_id and claimed_task.id is not None:
            meta = registry.get(worker_id)
            if meta and meta.task_id != claimed_task.id:
                meta.task_id = claimed_task.id
                registry.update(meta)

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
                    registry.mark_completed(worker_id, exit_code=rebase_exit_code, status="failed")
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

        # Update worker status on completion
        if worker_id:
            status = "completed" if exit_code == 0 else "failed"
            registry.mark_completed(worker_id, exit_code=exit_code, status=status)

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
            store.mark_failed(task, log_file=task.log_file, branch=task.branch, failure_reason="WORKER_DIED", has_commits=has_commits)
        if startup_log_path:
            with open(startup_log_path, "a") as f:
                f.write(f"[{datetime.now(UTC).isoformat()}] worker crashed: {e}\n")
        if worker_id:
            registry.mark_completed(worker_id, exit_code=1, status="failed")
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


def _spawn_background_resume_worker(args: argparse.Namespace, config: Config, new_task_id: str, quiet: bool = False) -> int:
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
    task = store.get(new_task_id)
    if not task:
        _print_work_message(f"Error: Task {new_task_id} not found", color=_colors.WORK_COLORS.error)
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
    try:
        # Generate worker ID
        worker_id = registry.generate_worker_id()
        cmd.extend(["--worker-id", worker_id])
        proc, startup_log_rel = _spawn_detached_worker_process(cmd, config, worker_id)

        # Register worker
        worker = WorkerMetadata(
            worker_id=worker_id,
            task_id=task.id,
            pid=proc.pid,
            startup_log_file=startup_log_rel,
        )
        registry.register(worker)

        _print_background_worker_started(task, pid=proc.pid, quiet=quiet, resume=True)

        return 0

    except Exception as e:
        _print_work_message(f"Error spawning background worker: {e}", color=_colors.WORK_COLORS.error)
        return 1


def _spawn_background_iterate_worker(
    args: argparse.Namespace,
    config: Config,
    impl_task: DbTask,
    *,
    max_iterations: int,
    resume: bool = False,
    retry: bool = False,
    quiet: bool = False,
    dry_run: bool = False,
) -> int:
    """Spawn the iterate loop as a detached background process."""
    registry = WorkerRegistry(config.workers_path)

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

    inner_cmd.extend(["--project", str(config.project_dir.absolute())])

    if dry_run:
        print(f"[dry-run] Would spawn background iterate worker: {shlex.join(inner_cmd)}")
        return 0

    worker_id = registry.generate_worker_id()
    inner_cmd.extend(["--worker-id", worker_id])

    try:
        proc, startup_log_rel = _spawn_detached_worker_process(inner_cmd, config, worker_id)
        worker = WorkerMetadata(
            worker_id=worker_id,
            task_id=impl_task.id,
            pid=proc.pid,
            startup_log_file=startup_log_rel,
        )
        registry.register(worker)
        _print_background_worker_started(impl_task, pid=proc.pid, quiet=quiet)
        return 0
    except Exception as e:
        _print_work_message(f"Error spawning background iterate worker: {e}", color=_colors.WORK_COLORS.error)
        return 1


def _create_rebase_task(
    store: SqliteTaskStore,
    parent_task_id: str,
    branch: str,
    target_branch: str,
) -> DbTask:
    """Create a rebase task for resolving merge conflicts.

    Used by both ``gza rebase --background`` and ``gza advance`` so that
    rebases always go through the standard runner.
    """
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
        same_branch=True,
        skip_learnings=True,
    )


def _spawn_background_workers(args: argparse.Namespace, config: Config) -> int:
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
        for task_id in args.task_ids:
            result = _spawn_background_worker(args, config, task_id=task_id)
            if result == 0:
                spawned_count += 1

        if len(args.task_ids) > 1:
            print(f"\n=== Spawned {spawned_count} background worker(s) for {len(args.task_ids)} task(s) ===")

        return 0

    if selected_tags:
        pending_tasks = store.get_pending_pickup(limit=count, tags=selected_tags, any_tag=any_tag)
        if not pending_tasks:
            print(format_no_runnable_message_for_tags(store, selected_tags, any_tag=any_tag))
            return 0
        spawned_count = 0
        for task in pending_tasks:
            if task.id is None:
                continue
            result = _spawn_background_worker(args, config, task_id=task.id)
            if result == 0:
                spawned_count += 1
        if count > 1:
            print(
                f"\n=== Attempted to spawn {count} background worker(s) "
                f"for tags '{', '.join(selected_tags)}' ==="
            )
        return 0

    # Spawn N workers - each will atomically claim a pending task
    # If there are fewer pending tasks than requested, some spawns will
    # find no tasks and exit gracefully
    spawned_count = 0

    for i in range(count):
        # _spawn_background_worker will atomically claim next pending task
        # It returns 0 if successful OR if no tasks are available
        # It returns 1 only on actual errors
        result = _spawn_background_worker(args, config)
        if result == 0:
            spawned_count += 1

    # Since _spawn_background_worker prints its own output for each worker,
    # we just print a summary if multiple workers were requested
    if count > 1:
        print(f"\n=== Attempted to spawn {count} background worker(s) ===")

    return 0


def _allow_pr_required_retry(args: argparse.Namespace, task: DbTask) -> bool:
    """Return whether work may retry a failed PR_REQUIRED task."""
    return bool(
        (getattr(args, "create_pr", False) or task.create_pr)
        and task.status == "failed"
        and task.failure_reason == "PR_REQUIRED"
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
        prompt_mode="cli",
        model=model,
        provider=provider,
    )


def _create_or_reuse_followup_tasks(
    store: SqliteTaskStore,
    *,
    review_task: DbTask,
    impl_task: DbTask,
    findings: tuple[ReviewFinding, ...],
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
) -> tuple[str, DbTask | None]:
    """Determine the right improve action for an impl+review pair.

    Returns:
        ("new", None) — no existing improve, create fresh
        ("resume", failed_task) — resumable failed improve exists
        ("retry", failed_task) — non-resumable failed improve exists
        ("give_up", failed_task) — retry/resume cap exceeded; stop and surface failure

    The cap counts failed attempts for this (impl, review) pair. When the number
    of prior failed attempts reaches ``max_resume_attempts``, further resume/retry
    is suppressed to prevent unbounded loops (e.g. a stale branch that keeps
    timing out on the same slow test).
    """
    from ..resume_policy import is_resumable_failed_task

    existing = store.get_improve_tasks_for(impl_task_id, review_task_id)
    failed_improves = [t for t in existing if t.status == "failed"]
    if not failed_improves:
        return ("new", None)

    latest_failed = max(failed_improves, key=lambda t: t.created_at or datetime.min)

    # Count of resume/retry attempts so far = failures beyond the original one.
    # If this already meets or exceeds the cap, don't spawn another attempt.
    if max_resume_attempts is not None and (len(failed_improves) - 1) >= max_resume_attempts:
        return ("give_up", latest_failed)

    if is_resumable_failed_task(latest_failed):
        return ("resume", latest_failed)
    return ("retry", latest_failed)


def resolve_comments_improve_action(
    store: SqliteTaskStore,
    impl_task_id: str,
    max_resume_attempts: int | None = None,
) -> tuple[str, DbTask | None]:
    """Determine improve action for comments-only (no-review) improve flows.

    Returns:
        ("new", None) — create a fresh comments-only improve
        ("reuse_pending", pending_task) — reuse existing pending comments-only improve
        ("wait_in_progress", in_progress_task) — existing in-progress comments-only improve is still running
        ("resume", failed_task) — resumable failed comments-only improve exists
        ("retry", failed_task) — non-resumable failed comments-only improve exists
        ("give_up", failed_task) — retry/resume cap exceeded
    """
    from ..resume_policy import is_resumable_failed_task

    def _normalize_time(value: datetime | None) -> datetime:
        if value is None:
            return datetime.min
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value

    def _time_key(task: DbTask) -> tuple[datetime, int]:
        return (_normalize_time(task.created_at), task_id_numeric_key(task.id))

    unresolved_comments = store.get_comments(impl_task_id, unresolved_only=True)
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
        return ("new", None)

    in_progress = [
        task for task in existing
        if task.status == "in_progress" and _candidate_is_fresh(task)
    ]
    if in_progress:
        return ("wait_in_progress", max(in_progress, key=_time_key))

    pending = [
        task for task in existing
        if task.status == "pending" and _candidate_is_fresh(task)
    ]
    if pending:
        return ("reuse_pending", max(pending, key=_time_key))

    failed = [
        task for task in existing
        if task.status == "failed" and _candidate_is_fresh(task)
    ]
    if not failed:
        return ("new", None)

    latest_failed = max(failed, key=_time_key)
    if max_resume_attempts is not None and (len(failed) - 1) >= max_resume_attempts:
        return ("give_up", latest_failed)

    if is_resumable_failed_task(latest_failed):
        return ("resume", latest_failed)
    return ("retry", latest_failed)


def _create_improve_task(
    store: SqliteTaskStore,
    impl_task: DbTask,
    review_task: DbTask | None,
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
    has_comments = bool(store.get_comments(impl_task.id, unresolved_only=True))
    if review_task is not None:
        assert review_task.id is not None
        existing = store.get_improve_tasks_for(impl_task.id, review_task.id)
        if existing:
            existing_task = existing[0]
            raise ValueError(
                f"An improve task already exists for implementation {impl_task.id} "
                f"and review {review_task.id}: {existing_task.id} (status: {existing_task.status})"
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
        based_on=impl_task.id,
        same_branch=True,
        tags=impl_task.tags,
        create_review=create_review,
        create_pr=create_pr,
        model=model,
        provider=provider,
    )


from ..query import _LINEAGE_REL_LABELS, TaskLineageNode as _TaskLineageNode  # noqa: E402


def _format_lineage(
    lineage_tree: _TaskLineageNode,
    task_id_color: str | None = None,
    *,
    annotate: bool = False,
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
        if task.id is None:
            return f"[{lc.task_type}]\\[{task.task_type}][/{lc.task_type}]{rel_suffix}{_annotation(task)}"
        return (
            f"[{_task_id_color}]{task.id}[/{_task_id_color}]"
            f"[{lc.task_type}]\\[{task.task_type}][/{lc.task_type}]"
            f"{rel_suffix}"
            f"{_annotation(task)}"
        )

    lines: list[str] = [_node_label(lineage_tree.task)]

    def _walk(node: _TaskLineageNode, prefix: str) -> None:
        for index, child in enumerate(node.children):
            is_last = index == (len(node.children) - 1)
            branch = "└── " if is_last else "├── "
            lines.append(f"{prefix}{branch}{_node_label(child.task, child.relationship)}")
            next_prefix = f"{prefix}{'    ' if is_last else '│   '}"
            _walk(child, next_prefix)

    _walk(lineage_tree, "")
    return "\n".join(lines)


def _create_resume_task(store: SqliteTaskStore, original_task: DbTask) -> DbTask:
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
        tags=original_task.tags,
        spec=original_task.spec,
        depends_on=original_task.depends_on,
        create_review=original_task.create_review,
        create_pr=original_task.create_pr,
        same_branch=original_task.same_branch,
        task_type_hint=original_task.task_type_hint,
        based_on=original_task.id,  # Track resume lineage (points to failed task)
        model=original_task.model,
        provider=original_task.provider if preserve_provider else None,
        provider_is_explicit=preserve_provider,
    )
    # Copy session_id and branch from original task so the resumed run
    # continues the Claude Code session and uses the same branch.
    assert new_task.id is not None
    new_task.session_id = original_task.session_id
    new_task.branch = original_task.branch
    store.update(new_task)
    return new_task


def _create_retry_task(store: SqliteTaskStore, original_task: DbTask) -> DbTask:
    """Create a fresh retry task pointing to the original task.

    For same-branch tasks that already ran on a branch, retries fork a fresh
    branch from that prior branch via ``base_branch``.
    """
    assert original_task.id is not None
    retry_same_branch = original_task.same_branch
    retry_base_branch: str | None = None
    if original_task.same_branch and original_task.branch:
        retry_same_branch = False
        retry_base_branch = original_task.branch

    return store.add(
        prompt=original_task.prompt,
        task_type=original_task.task_type,
        tags=original_task.tags,
        spec=original_task.spec,
        depends_on=original_task.depends_on,
        create_review=original_task.create_review,
        create_pr=original_task.create_pr,
        same_branch=retry_same_branch,
        task_type_hint=original_task.task_type_hint,
        based_on=original_task.id,
        model=original_task.model,
        provider=original_task.provider if original_task.provider_is_explicit else None,
        provider_is_explicit=original_task.provider_is_explicit,
        base_branch=retry_base_branch,
    )


def _auto_rebase_before_resume(config: Config, task_id: str) -> int:
    """Rebase resumable code-task branches onto the default branch before resuming."""
    from ..git import Git

    task = get_store(config).get(task_id)
    if task is None or not task.branch or task.task_type not in {"task", "implement", "improve"}:
        return 0

    git = Git(config.project_dir)
    default_branch = git.default_branch()
    store = get_store(config)
    rebase_task = _create_rebase_task(store, task.id or task_id, task.branch, default_branch)
    assert rebase_task.id is not None
    rebase_task.branch = task.branch
    store.update(rebase_task)
    from .git_ops import _run_task_backed_rebase

    print(f"Auto-rebasing task {task.id} onto '{default_branch}' before resume...")
    return _run_task_backed_rebase(
        config=config,
        store=store,
        rebase_task=rebase_task,
        branch=task.branch,
        target_branch=default_branch,
        remote=False,
        parent_task_id=task.id,
        failure_hint_lines=[
            "Use 'gza retry' to start fresh or run 'gza rebase' manually.",
        ],
    )


def run_with_resume(
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    *,
    run_task: Callable[[DbTask, bool], int],
    max_resume_attempts: int | None = None,
    on_resume: Callable[[DbTask, DbTask, int, int], None] | None = None,
) -> tuple[DbTask, int]:
    """Execute a task and auto-resume eligible failed tasks.

    Args:
        config: Loaded project configuration.
        store: Task store for reloading state and creating resume tasks.
        task: Task to execute.
        run_task: Callback that executes a task and returns exit code.
            Signature: ``run_task(task, resume)`` where ``resume`` indicates
            whether this invocation is resuming an existing session.
        max_resume_attempts: Maximum number of resume retries after the
            initial run. Defaults to ``config.max_resume_attempts``.
        on_resume: Optional callback invoked when a resume child is created.
            Signature: ``on_resume(failed_task, resume_task, attempt, max_attempts)``.

    Returns:
        Tuple of ``(final_task, exit_code)``.
    """
    def _failure_exit_code(raw_rc: int) -> int:
        return raw_rc if raw_rc != 0 else 1

    effective_limit = config.max_resume_attempts if max_resume_attempts is None else max_resume_attempts
    if not isinstance(effective_limit, int):
        effective_limit = 0
    if effective_limit < 0:
        effective_limit = 0

    current_task = task
    resume_attempt = 0
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

        resumable_failure = is_resumable_failure(
            status=refreshed.status,
            failure_reason=refreshed.failure_reason,
            session_id=refreshed.session_id,
        )
        if not resumable_failure:
            return refreshed, _failure_exit_code(rc)

        if resume_attempt >= effective_limit:
            return refreshed, _failure_exit_code(rc)

        resume_attempt += 1
        resume_task = _create_resume_task(store, refreshed)
        if on_resume is not None:
            on_resume(refreshed, resume_task, resume_attempt, effective_limit)
        current_task = resume_task
        resume_mode = True


def _resolve_task_log_path(config: Config, task: DbTask) -> Path | None:
    """Resolve best log path for a task from explicit and inferred candidates."""
    from .log import _task_log_candidates

    candidates = _task_log_candidates(config, task)
    if not candidates:
        return None
    for candidate in candidates:
        if candidate.exists():
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


def _failure_summary(reason: str) -> str:
    """Build short human-readable failure summary."""
    summaries = {
        "MAX_STEPS": "Stopped due to max steps limit.",
        "MAX_TURNS": "Stopped due to max steps limit.",
        "TERMINATED": "Stopped by an external termination signal.",
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
    explanation: str | None
    verify_context: str | None
    result_context: str | None


def _extract_interrupt_source(log_path: Path) -> str | None:
    """Extract the latest structured interrupt source label from the task log."""
    from .log import _load_log_file_entries

    try:
        entries = _load_log_file_entries(log_path)[1]
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


def _build_failure_diagnostics(task: DbTask, log_path: Path | None, verify_command: str | None) -> FailureDiagnostics:
    """Build canonical failure diagnostics for CLI rendering."""
    reason = task.failure_reason or "UNKNOWN"
    marker_reason: str | None = None
    interrupt_source: str | None = None
    explanation: str | None = None
    verify_context: str | None = None
    result_context: str | None = None

    if log_path and log_path.exists():
        marker_reason = _extract_agent_failure_marker_reason(log_path)
        interrupt_source = _extract_interrupt_source(log_path)
        explanation = _extract_last_agent_message_for_failure(log_path)
        verify_context, result_context = _extract_failure_log_context(log_path, verify_command)

    return FailureDiagnostics(
        reason=reason,
        marker_reason=marker_reason,
        summary=_failure_summary(reason),
        interrupt_source=interrupt_source,
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
    """Extract dependency_task_id from PREREQUISITE_UNMERGED outcome log entry."""
    if config is None or not task.log_file:
        return None
    from .log import _load_log_file_entries

    log_path = config.project_dir / task.log_file
    if not log_path.exists():
        return None
    try:
        entries = _load_log_file_entries(log_path)[1]
    except OSError:
        return None

    for entry in reversed(entries):
        if entry.get("type") != "gza" or entry.get("subtype") != "outcome":
            continue
        if entry.get("failure_reason") != "PREREQUISITE_UNMERGED":
            continue
        dep_id = entry.get("dependency_task_id")
        if isinstance(dep_id, str) and dep_id:
            return dep_id
    return None


def _failure_next_steps(task: DbTask, reason: str, *, config: Config | None = None) -> list[str]:
    """Return concrete next-step commands for a failed task."""
    if task.id is None:
        return []

    steps = [f"gza log -t {task.id} --steps-verbose"]
    if reason == "PREREQUISITE_UNMERGED":
        blocking_dep_id = _precondition_blocking_dependency_id(task, config) or task.depends_on
        if blocking_dep_id:
            steps.append(f"gza merge {blocking_dep_id}")
        steps.append(f"gza retry {task.id}")
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
    """ArgumentParser that prints a terse git-style error for unknown subcommands."""

    _INVALID_CHOICE_RE = re.compile(
        r"argument (?:command|\{[^}]+\}): invalid choice: ['\"]?(?P<cmd>[^'\"\s]+)['\"]?"
    )

    def error(self, message: str) -> NoReturn:
        match = self._INVALID_CHOICE_RE.match(message)
        if match:
            cmd = match.group("cmd")
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
        choices=["explore", "plan", "implement", "review", "improve", "fix", "rebase", "internal"],
        help="Filter tasks by task_type",
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
        help="Filter by tag (repeatable, AND semantics by default)",
    )
    parser.add_argument(
        "--any-tag",
        action="store_true",
        dest="any_tag",
        help="With repeated --tag values, match any tag instead of all tags",
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
