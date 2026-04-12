"""Shared helpers, constants, and lightweight utilities used across CLI sub-modules."""

import argparse
import contextlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich.pager import Pager

from ..config import Config
from ..console import (
    MAX_PROMPT_DISPLAY,
    truncate,
)
from ..db import ManualMigrationRequired, SqliteTaskStore, Task as DbTask, resolve_task_id, task_id_numeric_key
from ..prompts import PromptBuilder
from ..review_tasks import (
    DuplicateReviewError,  # noqa: F401
    create_review_task,
)
from ..review_verdict import parse_review_verdict
from ..runner import run
from ..tmux_proxy import get_tmux_session_pid
from ..workers import WorkerMetadata, WorkerRegistry


def get_store(config: Config) -> SqliteTaskStore:
    """Get the SQLite task store.

    Raises:
        ManualMigrationRequired: If the DB needs a manual schema upgrade.
            Callers should run ``gza migrate`` to fix this.
    """
    return SqliteTaskStore(config.db_path, prefix=config.project_prefix)


def resolve_id(config: Config, arg: str) -> str:
    """Resolve a user-supplied task ID argument to a canonical string ID.

    Wraps :func:`gza.db.resolve_task_id` using the project prefix from config.
    """
    return resolve_task_id(arg, config.project_prefix)


# Matches "{prefix}-{suffix}" where prefix is 1-12 lowercase alphanumeric chars.
# This is tighter than `"-" in arg` (which also matches branch names like "feature-foo").
_TASK_ID_RE = re.compile(r"^[a-z0-9]{1,12}-[0-9]+$")


def _looks_like_task_id(arg: str) -> bool:
    """Return True if *arg* looks like a task ID rather than a branch name.

    Matches only full prefixed decimal IDs, e.g. ``"gza-1234"``.
    """
    return bool(_TASK_ID_RE.match(arg))


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

            # PID is alive — leave timeout handling to the runner process.
        except (sqlite3.Error, OSError, ValueError) as exc:
            print(f"Warning: Failed to reconcile task {task_label}: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"Warning: Unexpected reconciliation error for task {task_label}: {exc}", file=sys.stderr)


def prune_terminal_dead_workers(config: Config) -> None:
    """Remove worker registry entries for terminal tasks when the worker PID is dead."""
    try:
        store = get_store(config)
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

    def _cleanup(signum, frame):
        # Restore original handlers and re-raise so the except block handles cleanup
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    try:
        exit_code = run(
            config,
            task_id=task_id,
            resume=resume,
            open_after=open_after,
            skip_precondition_check=force,
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

    if explicit_task_id is not None:
        task = store.get(explicit_task_id)
        if not task:
            print(f"Error: Task {explicit_task_id} not found")
            return 1

        if resume_mode:
            if task.status not in ("pending", "failed"):
                print(
                    f"Error: Task {explicit_task_id} is not resumable "
                    f"(status: {task.status})"
                )
                return 1
            if not task.session_id:
                print(f"Error: Task {explicit_task_id} has no session ID (cannot resume)")
                return 1
        else:
            if task.status != "pending":
                print(f"Error: Task {explicit_task_id} is not pending (status: {task.status})")
                return 1

            # Check if task is blocked
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                print(f"Error: Task {explicit_task_id} is blocked by task {blocking_id} ({blocking_status})")
                return 1
        selected_task = task
    else:
        if resume_mode:
            print("Error: Cannot resume without specifying a task ID")
            return 1
        # Select a candidate for UX; actual claim happens in the child runner.
        selected_task = store.get_next_pending()
        if not selected_task:
            print("No pending tasks found")
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

    if args.no_docker:
        inner_cmd.append("--no-docker")

    if hasattr(args, 'max_turns') and args.max_turns is not None:
        inner_cmd.extend(["--max-turns", str(args.max_turns)])
    if getattr(args, "force", False):
        inner_cmd.append("--force")

    # Add project directory
    inner_cmd.extend(["--project", str(config.project_dir.absolute())])

    provider_name = (selected_task.provider or config.provider or "claude").lower()
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

        if quiet:
            print(f"Started worker {worker_id} (PID {pid}) for task {selected_task.id}")
        else:
            print(f"Started worker {worker_id} (PID {pid})")
            print(f"  Task: {selected_task.id}")
            if selected_task.prompt:
                prompt_display = truncate(selected_task.prompt, MAX_PROMPT_DISPLAY)
                print(f"  Prompt: {prompt_display}")
            print()
            print("Use 'gza ps' to view running workers")
            print(f"Use 'gza log -w {worker_id} -f' to follow output")

        return 0

    except Exception as e:
        print(f"Error spawning background worker: {e}")
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
        if hasattr(args, 'task_ids') and args.task_ids:
            # Worker mode only runs one task at a time
            exit_code = run(
                config,
                task_id=args.task_ids[0],
                resume=resume,
                skip_precondition_check=getattr(args, "force", False),
                on_task_claimed=_on_task_claimed,
            )
        else:
            exit_code = run(
                config,
                resume=resume,
                skip_precondition_check=getattr(args, "force", False),
                on_task_claimed=_on_task_claimed,
            )

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
        print(f"Error: Task {new_task_id} not found")
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

        if quiet:
            print(f"Started worker {worker_id} (PID {proc.pid}) for task {task.id} (resuming)")
        else:
            print(f"Started worker {worker_id} (PID {proc.pid})")
            print(f"  Task: {task.id} (resuming)")
            if task.prompt:
                prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY)
                print(f"  Prompt: {prompt_display}")
            print()
            print("Use 'gza ps' to view running workers")
            print(f"Use 'gza log -w {worker_id} -f' to follow output")

        return 0

    except Exception as e:
        print(f"Error spawning background worker: {e}")
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
) -> int:
    """Spawn the iterate loop as a detached background process."""
    registry = WorkerRegistry(config.workers_path)
    worker_id = registry.generate_worker_id()

    inner_cmd = [
        sys.executable, "-m", "gza",
        "iterate",
        str(impl_task.id),
        "--max-iterations", str(max_iterations),
    ]

    if getattr(args, "no_docker", False):
        inner_cmd.append("--no-docker")
    if resume:
        inner_cmd.append("--resume")
    if retry:
        inner_cmd.append("--retry")

    inner_cmd.extend(["--project", str(config.project_dir.absolute())])

    try:
        proc, startup_log_rel = _spawn_detached_worker_process(inner_cmd, config, worker_id)
        worker = WorkerMetadata(
            worker_id=worker_id,
            task_id=impl_task.id,
            pid=proc.pid,
            startup_log_file=startup_log_rel,
        )
        registry.register(worker)
        if quiet:
            print(f"Started iterate worker {worker_id} (PID {proc.pid}) for task {impl_task.id}")
        else:
            print(f"Started iterate worker {worker_id} (PID {proc.pid})")
            print(f"  Task: {impl_task.id}")
            print()
            print("Use 'gza ps' to view running workers")
            print(f"Use 'gza log -w {worker_id} -f' to follow output")
        return 0
    except Exception as e:
        print(f"Error spawning background iterate worker: {e}")
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
            f"Rebase branch '{branch}' onto '{target_branch}' and resolve "
            f"any conflicts. Use /gza-rebase --auto to perform the rebase."
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
    resolved_steps = get_task_step_count(task)
    if resolved_steps is not None:
        parts.append(f"{resolved_steps} steps")
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
    if task.cost_usd is not None:
        parts.append(f"${task.cost_usd:.4f}")
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
        Verdict string ('APPROVED', 'CHANGES_REQUESTED', 'NEEDS_DISCUSSION') or None if not found
    """
    # First try output_content (cached in DB)
    if review_task.output_content:
        content = review_task.output_content
    # Then try reading from report_file
    elif review_task.report_file:
        review_path = config.project_dir / review_task.report_file
        if not review_path.exists():
            return None
        content = review_path.read_text()
    else:
        return None

    return parse_review_verdict(content)


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


def _create_improve_task(
    store: SqliteTaskStore,
    impl_task: DbTask,
    review_task: DbTask,
    create_review: bool = False,
    model: str | None = None,
    provider: str | None = None,
) -> DbTask:
    """Create an improve task for an implementation task based on a review.

    Validates that no duplicate improve task already exists.
    Returns the created improve task, or raises ValueError with an error message.
    """
    assert impl_task.id is not None
    assert review_task.id is not None

    existing = store.get_improve_tasks_for(impl_task.id, review_task.id)
    if existing:
        existing_task = existing[0]
        raise ValueError(
            f"An improve task already exists for implementation {impl_task.id} "
            f"and review {review_task.id}: {existing_task.id} (status: {existing_task.status})"
        )

    prompt = PromptBuilder().improve_task_prompt(impl_task.id, review_task.id)
    return store.add(
        prompt=prompt,
        task_type="improve",
        depends_on=review_task.id,
        based_on=impl_task.id,
        same_branch=True,
        group=impl_task.group,
        create_review=create_review,
        model=model,
        provider=provider,
    )


from ..query import TaskLineageNode as _TaskLineageNode  # noqa: E402


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
                "CHANGES_REQUESTED": "changes_requested",
                "NEEDS_DISCUSSION": "needs_discussion",
            }
            verdict_label = verdict_map.get(verdict, "unknown") if verdict else "unknown"
            if latest_review_task_id is not None and task.id == latest_review_task_id:
                verdict_label = f"{verdict_label} \u2190 latest"

        return f" [{lc.annotation}]({completed_label} | {status_label} | {verdict_label})[/{lc.annotation}]"

    def _node_label(task: DbTask) -> str:
        if task.id is None:
            return f"[{lc.task_type}]\\[{task.task_type}][/{lc.task_type}]{_annotation(task)}"
        return (
            f"[{_task_id_color}]{task.id}[/{_task_id_color}]"
            f"[{lc.task_type}]\\[{task.task_type}][/{lc.task_type}]"
            f"{_annotation(task)}"
        )

    lines: list[str] = [_node_label(lineage_tree.task)]

    def _walk(node: _TaskLineageNode, prefix: str) -> None:
        for index, child in enumerate(node.children):
            is_last = index == (len(node.children) - 1)
            branch = "└── " if is_last else "├── "
            lines.append(f"{prefix}{branch}{_node_label(child.task)}")
            next_prefix = f"{prefix}{'    ' if is_last else '│   '}"
            _walk(child, next_prefix)

    _walk(lineage_tree, "")
    return "\n".join(lines)


def _create_resume_task(store: SqliteTaskStore, original_task: DbTask) -> DbTask:
    """Create a new resume task pointing to the original failed task.

    Copies prompt, task_type, group, session_id, branch, model, etc.
    Preserves provider only when the original task had an explicit override.
    Sets based_on to original_task.id to track resume lineage.
    """
    assert original_task.id is not None
    new_task = store.add(
        prompt=original_task.prompt,
        task_type=original_task.task_type,
        group=original_task.group,
        spec=original_task.spec,
        depends_on=original_task.depends_on,
        create_review=original_task.create_review,
        same_branch=original_task.same_branch,
        task_type_hint=original_task.task_type_hint,
        based_on=original_task.id,  # Track resume lineage (points to failed task)
        model=original_task.model,
        provider=original_task.provider if original_task.provider_is_explicit else None,
        provider_is_explicit=original_task.provider_is_explicit,
    )
    # Copy session_id and branch from original task so the resumed run
    # continues the Claude Code session and uses the same branch.
    assert new_task.id is not None
    new_task.session_id = original_task.session_id
    new_task.branch = original_task.branch
    store.update(new_task)
    return new_task


def run_with_resume(
    config: Config,
    store: SqliteTaskStore,
    task: DbTask,
    *,
    run_task: Callable[[DbTask, bool], int],
    max_resume_attempts: int | None = None,
    on_resume: Callable[[DbTask, DbTask, int, int], None] | None = None,
) -> tuple[DbTask, int]:
    """Execute a task and auto-resume MAX_STEPS/MAX_TURNS failures.

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

        if rc == 0:
            return refreshed, 0

        resumable_failure = (
            refreshed.status == "failed"
            and refreshed.failure_reason in {"MAX_STEPS", "MAX_TURNS"}
            and refreshed.session_id is not None
        )
        if not resumable_failure:
            return refreshed, rc

        if resume_attempt >= effective_limit:
            return refreshed, rc

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
        "PREREQUISITE_UNMERGED": "Dependency is not yet merged to main.",
        "TEST_FAILURE": "Stopped due to verification/test failure.",
        "UNKNOWN": "Task failed; inspect log output for details.",
    }
    return summaries.get(reason, f"Task failed: {reason}")


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
    if reason in {"MAX_STEPS", "MAX_TURNS"}:
        if task.session_id:
            steps.append(f"gza resume {task.id}")
        steps.append(f"gza retry {task.id}")
        return steps

    if reason == "PREREQUISITE_UNMERGED":
        blocking_dep_id = _precondition_blocking_dependency_id(task, config) or task.depends_on
        if blocking_dep_id:
            steps.append(f"gza merge {blocking_dep_id}")
        steps.append(f"gza retry {task.id}")
        return steps

    if reason == "TEST_FAILURE":
        if task.session_id:
            steps.append(f"gza resume {task.id}")
        steps.append(f"gza retry {task.id}")
        return steps

    if task.session_id:
        steps.append(f"gza resume {task.id}")
    steps.append(f"gza retry {task.id}")
    return steps


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
        default=".",
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
        choices=["explore", "plan", "implement", "review", "improve", "rebase", "internal"],
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
    if use_page and sys.stdout.isatty():
        from ..console import console
        pager_cmd = _get_pager(project_dir)
        return console.pager(pager=_GzaPager(pager_cmd), styles=True)
    return nullcontext()
