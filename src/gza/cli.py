"""Command-line interface for Gza."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

from .config import Config, ConfigError
from .db import SqliteTaskStore, add_task_interactive, edit_task_interactive, Task as DbTask
from .git import Git, GitError
from .github import GitHub, GitHubError
from .importer import parse_import_file, validate_import, import_tasks
from .runner import run
from .tasks import YamlTaskStore, Task as YamlTask
from .workers import WorkerMetadata, WorkerRegistry


def get_store(config: Config) -> SqliteTaskStore:
    """Get the SQLite task store."""
    return SqliteTaskStore(config.db_path)


def _spawn_background_worker(args: argparse.Namespace, config: Config, task_id: int = None) -> int:
    """Spawn a background worker process.

    Args:
        args: Command-line arguments
        config: Configuration object
        task_id: Specific task ID to run (optional)
    """
    # Initialize worker registry
    registry = WorkerRegistry(config.workers_path)

    # Get task to run (either specific or next pending)
    store = get_store(config)
    if task_id:
        task = store.get(task_id)
        if not task:
            print(f"Error: Task #{task_id} not found")
            return 1

        if task.status != "pending":
            print(f"Error: Task #{task_id} is not pending (status: {task.status})")
            return 1

        # Check if task is blocked
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
        if is_blocked:
            print(f"Error: Task #{task_id} is blocked by task #{blocking_id} ({blocking_status})")
            return 1
    else:
        # Atomically claim next task
        task = store.claim_next_pending_task()
        if not task:
            print("No pending tasks found")
            return 0
        task_id = task.id

    # Build command for worker subprocess
    cmd = [
        sys.executable, "-m", "gza",
        "work",
        "--worker-mode",
    ]

    if task_id:
        cmd.append(str(task_id))

    if args.no_docker:
        cmd.append("--no-docker")

    # Add project directory
    cmd.append(str(config.project_dir.absolute()))

    # Spawn detached process
    try:
        # Use nohup to detach from terminal
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            cwd=config.project_dir,
        )

        # Generate worker ID
        worker_id = registry.generate_worker_id()

        # Register worker
        worker = WorkerMetadata(
            worker_id=worker_id,
            pid=proc.pid,
            task_id=task.id,
            task_slug=None,  # Will be set when runner starts
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,  # Will be set when runner starts
            worktree=None,  # Will be set when runner starts
        )
        registry.register(worker)

        print(f"Started worker {worker_id} (PID {proc.pid})")
        print(f"  Task: #{task.id}")
        if task.prompt:
            prompt_display = task.prompt[:60] + "..." if len(task.prompt) > 60 else task.prompt
            print(f"  Prompt: {prompt_display}")
        print()
        print(f"Use 'gza ps' to view running workers")
        print(f"Use 'gza log -w {worker_id} -f' to follow output")

        return 0

    except Exception as e:
        print(f"Error spawning background worker: {e}")
        return 1


def _run_as_worker(args: argparse.Namespace, config: Config) -> int:
    """Run in worker mode (called internally by background workers)."""
    registry = WorkerRegistry(config.workers_path)
    worker_id = None

    # Find our worker entry by PID
    my_pid = os.getpid()
    workers = registry.list_all(include_completed=False)
    for w in workers:
        if w.pid == my_pid:
            worker_id = w.worker_id
            break

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
    try:
        resume = hasattr(args, 'resume') and args.resume
        if hasattr(args, 'task_ids') and args.task_ids:
            # Worker mode only runs one task at a time
            exit_code = run(config, task_id=args.task_ids[0], resume=resume)
        else:
            exit_code = run(config, resume=resume)

        # Update worker status on completion
        if worker_id:
            status = "completed" if exit_code == 0 else "failed"
            registry.mark_completed(worker_id, exit_code=exit_code, status=status)

        return exit_code

    except Exception as e:
        print(f"Worker error: {e}")
        if worker_id:
            registry.mark_completed(worker_id, exit_code=1, status="failed")
        return 1


def _spawn_background_resume_worker(args: argparse.Namespace, config: Config, task_id: int) -> int:
    """Spawn a background worker to resume a failed task.

    Args:
        args: Command-line arguments
        config: Configuration object
        task_id: Task ID to resume

    Returns:
        0 on success, 1 on error
    """
    # Initialize worker registry
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)

    # Get task (validation already done in cmd_resume)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task #{task_id} not found")
        return 1

    # Build command for worker subprocess
    cmd = [
        sys.executable, "-m", "gza",
        "work",
        "--worker-mode",
        "--resume",
        str(task_id),
    ]

    if args.no_docker:
        cmd.append("--no-docker")

    # Add project directory
    cmd.append(str(config.project_dir.absolute()))

    # Spawn detached process
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            cwd=config.project_dir,
        )

        # Generate worker ID
        worker_id = registry.generate_worker_id()

        # Register worker
        worker = WorkerMetadata(
            worker_id=worker_id,
            pid=proc.pid,
            task_id=task.id,
            task_slug=None,  # Will be set when runner starts
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,  # Will be set when runner starts
            worktree=None,  # Will be set when runner starts
        )
        registry.register(worker)

        print(f"Started worker {worker_id} (PID {proc.pid})")
        print(f"  Task: #{task.id} (resuming)")
        if task.prompt:
            prompt_display = task.prompt[:60] + "..." if len(task.prompt) > 60 else task.prompt
            print(f"  Prompt: {prompt_display}")
        print()
        print(f"Use 'gza ps' to view running workers")
        print(f"Use 'gza log -w {worker_id} -f' to follow output")

        return 0

    except Exception as e:
        print(f"Error spawning background worker: {e}")
        return 1


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


def cmd_run(args: argparse.Namespace) -> int:
    """Run the next pending task(s) or specific tasks."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    # Handle background mode
    if args.background:
        return _spawn_background_workers(args, config)

    # Handle worker mode (internal)
    if args.worker_mode:
        return _run_as_worker(args, config)

    # Register as a foreground worker
    registry = WorkerRegistry(config.workers_path)
    worker_id = registry.generate_worker_id()

    # Get task info for registration
    store = get_store(config)
    task_id_for_registration = None

    # Check if specific task IDs were provided
    if hasattr(args, 'task_ids') and args.task_ids:
        # Validate all task IDs first
        for task_id in args.task_ids:
            task = store.get(task_id)
            if not task:
                print(f"Error: Task #{task_id} not found")
                return 1

            if task.status != "pending":
                print(f"Error: Task #{task_id} is not pending (status: {task.status})")
                return 1

            # Check if task is blocked by a dependency
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                print(f"Error: Task #{task_id} is blocked by task #{blocking_id} ({blocking_status})")
                return 1

        task_id_for_registration = args.task_ids[0]
    else:
        # For loop mode, we'll register with the first task we're about to run
        next_task = store.get_next_pending()
        if next_task:
            task_id_for_registration = next_task.id

    # Register foreground worker
    worker = WorkerMetadata(
        worker_id=worker_id,
        pid=os.getpid(),
        task_id=task_id_for_registration,
        task_slug=None,
        started_at=datetime.now(timezone.utc).isoformat(),
        status="running",
        log_file=None,
        worktree=None,
        is_background=False,
    )
    registry.register(worker)

    # Set up signal handlers for cleanup
    def cleanup_handler(signum, frame):
        """Clean up worker registration on interrupt."""
        registry.mark_completed(worker_id, exit_code=130, status="failed")
        sys.exit(130)

    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)

    try:
        # Run the task(s)
        if hasattr(args, 'task_ids') and args.task_ids:
            # Run the specific tasks
            tasks_completed = 0
            for task_id in args.task_ids:
                result = run(config, task_id=task_id)
                if result != 0:
                    if tasks_completed == 0:
                        # First task failed
                        registry.mark_completed(worker_id, exit_code=result, status="failed")
                        return result
                    else:
                        # We completed some tasks before failure
                        print(f"\nCompleted {tasks_completed} task(s) before task #{task_id} failed")
                        registry.mark_completed(worker_id, exit_code=result, status="failed")
                        return result
                tasks_completed += 1

            # All tasks completed successfully
            if tasks_completed > 1:
                print(f"\n=== Completed {tasks_completed} tasks ===")
            registry.mark_completed(worker_id, exit_code=0, status="completed")
            return 0

        # Determine how many tasks to run
        count = args.count if args.count is not None else config.work_count

        # Run tasks in a loop
        tasks_completed = 0
        for i in range(count):
            result = run(config)

            # If run returns non-zero, it means something went wrong or no tasks left
            if result != 0:
                if tasks_completed == 0:
                    # First task failed or no tasks available, return the error code
                    registry.mark_completed(worker_id, exit_code=result,
                                           status="failed" if result != 0 else "completed")
                    return result
                else:
                    # We completed some tasks before stopping, consider it success
                    break

            tasks_completed += 1

            # Check if there are more pending tasks
            if i < count - 1:  # Not the last iteration
                from .db import SqliteTaskStore
                store = SqliteTaskStore(config.db_path)
                if not store.get_next_pending():
                    print(f"\nCompleted {tasks_completed} task(s). No more pending tasks.")
                    break

        if tasks_completed > 1:
            print(f"\n=== Completed {tasks_completed} tasks ===")

        # Clean up worker registration on normal exit
        registry.mark_completed(worker_id, exit_code=0, status="completed")
        return 0

    except Exception as e:
        # Clean up worker registration on exception
        registry.mark_completed(worker_id, exit_code=1, status="failed")
        raise


def cmd_next(args: argparse.Namespace) -> int:
    """List upcoming pending tasks in order."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    pending = store.get_pending()

    if not pending:
        print("No pending tasks")
        return 0

    # Filter blocked tasks unless --all is specified
    show_all = args.all if hasattr(args, 'all') else False

    runnable = []
    blocked = []

    for task in pending:
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
        if is_blocked:
            blocked.append((task, blocking_id))
        else:
            runnable.append(task)

    # Show runnable tasks
    if runnable:
        for i, task in enumerate(runnable, 1):
            type_label = f"[{task.task_type}] " if task.task_type != "task" else ""
            # Get first line only, then truncate
            first_line = task.prompt.split('\n')[0].strip()
            prompt_display = first_line[:60] + "..." if len(first_line) > 60 else first_line
            print(f"{i}. (#{task.id}) {type_label}{prompt_display}")
    else:
        if not show_all:
            print("No runnable tasks")

    # Show blocked tasks if --all is specified
    if show_all and blocked:
        if runnable:
            print()
        for i, (task, blocking_id) in enumerate(blocked, len(runnable) + 1):
            type_label = f"[{task.task_type}] " if task.task_type != "task" else ""
            first_line = task.prompt.split('\n')[0].strip()
            prompt_display = first_line[:60] + "..." if len(first_line) > 60 else first_line
            print(f"{i}. (#{task.id}) {type_label}{prompt_display} (blocked by #{blocking_id})")

    # Show blocked count at the bottom (only if not showing all)
    if not show_all and blocked:
        print()
        count = len(blocked)
        plural = "tasks" if count != 1 else "task"
        print(f"({count} {plural} blocked by dependencies)")

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
    if task.num_turns is not None:
        parts.append(f"{task.num_turns} turns")
    if task.cost_usd is not None:
        parts.append(f"${task.cost_usd:.4f}")
    return " | ".join(parts) if parts else ""


def cmd_history(args: argparse.Namespace) -> int:
    """List recent completed/failed tasks."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    recent = store.get_history(limit=10)
    if not recent:
        print("No completed or failed tasks")
        return 0

    for task in recent:
        status_icon = "✓" if task.status == "completed" else "✗"
        date_str = f"({task.completed_at.strftime('%Y-%m-%d %H:%M')})" if task.completed_at else ""
        type_label = f" [{task.task_type}]" if task.task_type != "task" else ""
        prompt_display = task.prompt[:50] + "..." if len(task.prompt) > 50 else task.prompt
        print(f"{status_icon} [#{task.id}] {date_str} {prompt_display}{type_label}")
        if task.branch:
            print(f"    branch: {task.branch}")
        if task.report_file:
            print(f"    report: {task.report_file}")
        stats_str = format_stats(task)
        if stats_str:
            print(f"    stats: {stats_str}")
    return 0


def cmd_unmerged(args: argparse.Namespace) -> int:
    """List tasks with unmerged work on branches."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    default_branch = git.default_branch()

    # Get completed tasks with branches and check if merged
    history = store.get_history(limit=100)
    unmerged = []
    for task in history:
        if task.status == "completed" and task.branch and task.has_commits:
            if not git.is_merged(task.branch, default_branch):
                unmerged.append(task)

    if not unmerged:
        print("No unmerged tasks")
        return 0

    for task in unmerged:
        date_str = f"({task.completed_at.strftime('%Y-%m-%d %H:%M')})" if task.completed_at else ""
        type_label = f" [{task.task_type}]" if task.task_type != "task" else ""
        prompt_display = task.prompt[:50] + "..." if len(task.prompt) > 50 else task.prompt
        print(f"⚡ [#{task.id}] {date_str} {prompt_display}{type_label}")
        if task.branch:
            print(f"    branch: {task.branch}")
        if task.report_file:
            print(f"    report: {task.report_file}")
        stats_str = format_stats(task)
        if stats_str:
            print(f"    stats: {stats_str}")
    return 0


def cmd_merge(args: argparse.Namespace) -> int:
    """Merge a task's branch into the current branch."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Get the task
    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged"):
        print(f"Error: Task #{task.id} is not completed or unmerged (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task #{task.id} has no branch")
        return 1

    # Get current and default branches
    current_branch = git.current_branch()
    default_branch = git.default_branch()

    # Check if branch already merged
    if git.is_merged(task.branch, current_branch):
        print(f"Error: Branch '{task.branch}' is already merged into {current_branch}")
        return 1

    # Check for uncommitted changes (untracked files are OK, they won't conflict with merge)
    if git.has_changes(include_untracked=False):
        print("Error: You have uncommitted changes. Please commit or stash them first.")
        return 1

    # Check for conflicting flags
    if args.rebase and args.squash:
        print("Error: Cannot use --rebase and --squash together")
        return 1

    # Perform the merge or rebase
    try:
        if args.rebase:
            # For rebase: checkout the task branch, rebase onto current, then fast-forward merge
            print(f"Rebasing '{task.branch}' onto '{current_branch}'...")
            git.checkout(task.branch)
            git.rebase(current_branch)
            print(f"✓ Successfully rebased {task.branch}")

            # Switch back and fast-forward merge
            git.checkout(current_branch)
            git.merge(task.branch, squash=False)
            print(f"✓ Fast-forwarded {current_branch} to {task.branch}")
        else:
            # Regular merge or squash merge
            merge_type = "squash merging" if args.squash else "merging"
            print(f"Merging '{task.branch}' into '{current_branch}'...")
            git.merge(task.branch, squash=args.squash)
            print(f"✓ Successfully merged {task.branch}")

        # Delete branch if requested
        if args.delete:
            try:
                git.delete_branch(task.branch)
                print(f"✓ Deleted branch {task.branch}")
            except GitError as e:
                print(f"Warning: Could not delete branch: {e}")

        return 0

    except GitError as e:
        operation = "rebase" if args.rebase else "merge"
        print(f"Error during {operation}: {e}")
        print(f"\nAborting {operation} and restoring clean state...")
        try:
            if args.rebase:
                git.rebase_abort()
                # Try to switch back to original branch
                try:
                    git.checkout(current_branch)
                except GitError:
                    pass  # Best effort to return to original branch
                print("✓ Rebase aborted, working directory restored")
            else:
                git.merge_abort()
                print("✓ Merge aborted, working directory restored")
        except GitError as abort_error:
            print(f"Warning: Could not abort {operation}: {abort_error}")
        return 1


def _generate_pr_content(
    task: DbTask,
    commit_log: str,
    diff_stat: str,
) -> tuple[str, str]:
    """Generate PR title and body using Claude.

    Args:
        task: The task to create a PR for
        commit_log: Git log output for the branch
        diff_stat: Git diff --stat output

    Returns:
        Tuple of (title, body)
    """
    import subprocess

    # Build a prompt for Claude
    prompt = f"""Generate a GitHub pull request title and description for this completed task.

Task prompt:
{task.prompt}

Commits on branch:
{commit_log}

Files changed:
{diff_stat}

Format your response EXACTLY like this (no markdown code fences):
TITLE: <concise PR title, max 72 chars>

BODY:
## Summary
<2-3 sentences describing what was done and why>

## Changes
<bullet points of key changes>

Output ONLY in the format above, nothing else."""

    try:
        result = subprocess.run(
            ["claude", "--print"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0 and result.stdout.strip():
            return _parse_pr_response(result.stdout.strip(), task)
        elif result.returncode != 0 and result.stderr:
            print(f"Warning: claude failed: {result.stderr.strip()}", file=sys.stderr)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Fallback: generate simple title/body from task
    return _fallback_pr_content(task, commit_log)


def _parse_pr_response(response: str, task: DbTask) -> tuple[str, str]:
    """Parse Claude's response into title and body."""
    lines = response.split("\n")
    title = ""
    body_lines = []
    in_body = False

    for line in lines:
        if line.startswith("TITLE:"):
            title = line[6:].strip()
        elif line.strip() == "BODY:":
            in_body = True
        elif in_body:
            body_lines.append(line)

    if not title:
        # Use task_id or first line of prompt
        title = task.task_id or task.prompt.split("\n")[0][:72]

    body = "\n".join(body_lines).strip()
    if not body:
        body = f"Task: {task.prompt[:500]}"

    return title, body


def _fallback_pr_content(task: DbTask, commit_log: str) -> tuple[str, str]:
    """Generate simple PR content without AI."""
    # Title from task_id or prompt
    if task.task_id:
        # Convert slug like "20240106-add-feature" to "Add feature"
        parts = task.task_id.split("-")[1:]  # Remove date prefix
        title = " ".join(parts).capitalize()
    else:
        title = task.prompt.split("\n")[0][:72]

    body = f"""## Task Prompt

> {task.prompt[:500].replace(chr(10), chr(10) + '> ')}

## Commits
```
{commit_log}
```
"""
    return title, body


def cmd_pr(args: argparse.Namespace) -> int:
    """Create a GitHub PR from a completed task."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    gh = GitHub()

    # Check gh CLI is available
    if not gh.is_available():
        print("Error: GitHub CLI (gh) is not installed or not authenticated")
        print("Install: https://cli.github.com/")
        print("Auth: gh auth login")
        return 1

    # Get the task
    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged"):
        print(f"Error: Task #{task.id} is not completed (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task #{task.id} has no branch")
        return 1

    if not task.has_commits:
        print(f"Error: Task #{task.id} has no commits")
        return 1

    default_branch = git.default_branch()

    # Check branch is not already merged
    if git.is_merged(task.branch, default_branch):
        print(f"Error: Branch '{task.branch}' is already merged into {default_branch}")
        return 1

    # Check if PR already exists
    existing_pr = gh.pr_exists(task.branch)
    if existing_pr:
        print(f"PR already exists: {existing_pr}")
        return 0

    # Ensure branch is pushed to remote (push if remote doesn't exist or is behind)
    try:
        if git.needs_push(task.branch):
            print(f"Pushing branch '{task.branch}' to origin...")
            git.push_branch(task.branch)
    except GitError as e:
        print(f"Error pushing branch: {e}")
        return 1

    # Get commit log and diff stat for context
    commit_log = git.get_log(f"{default_branch}..{task.branch}")
    diff_stat = git.get_diff_stat(f"{default_branch}...{task.branch}")

    # Generate or use provided title/body
    if args.title:
        title = args.title
        body = f"## Summary\n{task.prompt[:500]}"
    else:
        print("Generating PR description...")
        title, body = _generate_pr_content(task, commit_log, diff_stat)

    # Create the PR
    try:
        pr = gh.create_pr(
            head=task.branch,
            base=default_branch,
            title=title,
            body=body,
            draft=args.draft,
        )
        print(f"✓ Created PR: {pr.url}")
        return 0
    except GitHubError as e:
        print(f"Error creating PR:\n{e}")
        return 1


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def cmd_stats(args: argparse.Namespace) -> int:
    """Show cost and usage statistics."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    stats = store.get_stats()
    if stats["completed"] == 0 and stats["failed"] == 0:
        print("No completed or failed tasks")
        return 0

    tasks_with_cost = stats["completed"] + stats["failed"]
    avg_cost = stats["total_cost"] / tasks_with_cost if tasks_with_cost else 0

    # Print summary
    print("Summary")
    print("=" * 50)
    print(f"  Tasks:        {stats['completed']} completed, {stats['failed']} failed")
    print(f"  Total cost:   ${stats['total_cost']:.2f}")
    print(f"  Total time:   {format_duration(stats['total_duration'])}")
    print(f"  Total turns:  {stats['total_turns']}")
    if tasks_with_cost:
        print(f"  Avg cost:     ${avg_cost:.2f}/task")
    print()

    # Print recent tasks
    limit = args.last
    recent = store.get_history(limit=limit)

    print(f"Recent Tasks (last {len(recent)})")
    print("=" * 50)

    # Table header
    print(f"{'Status':<8} {'Cost':>8} {'Turns':>6} {'Time':>8}  Description")
    print("-" * 50)

    for task in recent:
        status = "✓" if task.status == "completed" else "✗"
        cost_str = f"${task.cost_usd:.4f}" if task.cost_usd is not None else "-"
        turns_str = str(task.num_turns) if task.num_turns is not None else "-"
        time_str = format_duration(task.duration_seconds) if task.duration_seconds else "-"

        # Truncate description to fit
        desc = task.prompt
        if len(desc) > 40:
            desc = desc[:37] + "..."

        print(f"{status:<8} {cost_str:>8} {turns_str:>6} {time_str:>8}  {desc}")

    print()
    print(f"Total for shown: ${sum(t.cost_usd or 0 for t in recent):.2f}")

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate the gza.yaml configuration file."""
    is_valid, errors, warnings = Config.validate(args.project_dir)

    # Print warnings first
    for warning in warnings:
        print(f"⚠ Warning: {warning}")

    if is_valid:
        print("✓ Configuration is valid")
        return 0
    else:
        print("✗ Configuration validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1


def cmd_init(args: argparse.Namespace) -> int:
    """Generate a new gza.yaml configuration file with defaults."""
    from .config import (
        CONFIG_FILENAME,
        DEFAULT_TASKS_FILE,
        DEFAULT_LOG_DIR,
        DEFAULT_TIMEOUT_MINUTES,
        DEFAULT_USE_DOCKER,
        DEFAULT_BRANCH_MODE,
        DEFAULT_MAX_TURNS,
        DEFAULT_CLAUDE_ARGS,
        DEFAULT_WORKTREE_DIR,
        DEFAULT_WORK_COUNT,
    )

    # Derive project name from directory name
    default_project_name = args.project_dir.name

    config_path = args.project_dir / CONFIG_FILENAME

    if config_path.exists() and not args.force:
        print(f"Error: {CONFIG_FILENAME} already exists at {config_path}")
        print("Use --force to overwrite")
        return 1

    # Check if running interactively (stdin is a TTY)
    is_interactive = sys.stdin.isatty()

    if is_interactive:
        # Prompt for branch strategy
        print("Branch naming strategy:")
        print("  1. monorepo    - {project}/{task_id} (e.g., myproj/20260107-add-feature)")
        print("  2. conventional - {type}/{slug} (e.g., feature/add-feature, fix/login-bug)")
        print("  3. simple      - {slug} (e.g., add-feature)")
        print("  4. custom      - Define your own pattern")

        while True:
            choice = input("Choose strategy [1-4, default=1]: ").strip() or "1"
            if choice in ("1", "2", "3", "4"):
                break
            print("Invalid choice. Please enter 1, 2, 3, or 4.")
    else:
        # Non-interactive mode: use default (monorepo)
        choice = "1"

    # Determine branch_strategy value
    if choice == "1":
        branch_strategy_line = "# branch_strategy: monorepo  # Default: {project}/{task_id}"
    elif choice == "2":
        branch_strategy_line = "branch_strategy: conventional  # {type}/{slug}"
    elif choice == "3":
        branch_strategy_line = "branch_strategy: simple  # {slug}"
    else:  # custom
        print("\nCustom pattern variables:")
        print("  {project}  - Project name")
        print("  {task_id}  - Full task ID (YYYYMMDD-slug)")
        print("  {date}     - Date portion (YYYYMMDD)")
        print("  {slug}     - Slug portion")
        print("  {type}     - Inferred/default type (feature, fix, etc.)")

        while True:
            pattern = input("Enter custom pattern: ").strip()
            if pattern:
                break
            print("Pattern cannot be empty.")

        default_type = input("Default type [default=feature]: ").strip() or "feature"
        branch_strategy_line = f"""branch_strategy:
  pattern: "{pattern}"
  default_type: {default_type}"""

    # Generate config file with project_name required and other defaults commented out
    config_content = f"""# Gza Configuration

# Project name (required) - used for branch prefixes and Docker image naming
project_name: {default_project_name}

# Branch naming strategy (default: monorepo)
# Options: monorepo, conventional, simple, or custom pattern dict
{branch_strategy_line}

# All settings below show default values and are commented out.
# Uncomment and modify any setting you want to change.

# Path to tasks file (relative to project directory) - deprecated, using SQLite now
# tasks_file: {DEFAULT_TASKS_FILE}

# Directory for log files (relative to project directory)
# log_dir: {DEFAULT_LOG_DIR}

# Whether to run Claude in Docker container
# use_docker: {str(DEFAULT_USE_DOCKER).lower()}

# Custom Docker image name (defaults to <project_name>-gza)
# docker_image: ""

# Maximum time per task in minutes
# timeout_minutes: {DEFAULT_TIMEOUT_MINUTES}

# Branch mode: "single" (reuse one branch) or "multi" (create branch per task)
# branch_mode: {DEFAULT_BRANCH_MODE}

# Maximum conversation turns per task
# max_turns: {DEFAULT_MAX_TURNS}

# Directory for git worktrees (isolates task execution from main checkout)
# worktree_dir: {DEFAULT_WORKTREE_DIR}

# Number of tasks to run in a single work session (default: 1)
# work_count: {DEFAULT_WORK_COUNT}

# Arguments passed to Claude Code
# claude_args:
"""

    # Add commented claude_args list
    for arg in DEFAULT_CLAUDE_ARGS:
        config_content += f"#   - {arg}\n"

    config_path.write_text(config_content)
    print(f"✓ Created {config_path}")

    # Initialize the database (Config.load will now work since we have project_name)
    config = Config.load(args.project_dir)
    store = get_store(config)
    print(f"✓ Initialized database at {config.db_path}")

    return 0


def _format_log_entry(entry: dict) -> str | None:
    """Format a single JSON log entry for display.

    Returns formatted string or None to skip the entry.
    """
    entry_type = entry.get("type")

    if entry_type == "system":
        subtype = entry.get("subtype", "")
        if subtype == "init":
            model = entry.get("model", "unknown")
            return f"[system] Session initialized (model: {model})"
        return None  # Skip other system messages

    elif entry_type == "user":
        # User messages contain tool results
        message = entry.get("message", {})
        content = message.get("content", [])
        parts = []
        for item in content:
            if item.get("type") == "tool_result":
                tool_id = item.get("tool_use_id", "")[:8]
                result = item.get("content", "")
                if isinstance(result, str):
                    # Unescape literal \n from double-escaped JSON (Claude Code logging artifact)
                    result = result.replace("\\n", "\n").replace("\\t", "\t")
                    if len(result) > 200:
                        result = result[:200] + "..."
                parts.append(f"[tool result {tool_id}] {result}")
        if parts:
            return "\n".join(parts)
        return None

    elif entry_type == "assistant":
        message = entry.get("message", {})
        content = message.get("content", [])
        parts = []
        for item in content:
            if item.get("type") == "text":
                text = item.get("text", "")
                parts.append(text)
            elif item.get("type") == "tool_use":
                name = item.get("name", "unknown")
                tool_input = item.get("input", {})
                # Show condensed tool use info
                if name == "Bash":
                    cmd = tool_input.get("command", "")
                    if len(cmd) > 100:
                        cmd = cmd[:100] + "..."
                    parts.append(f"[tool: {name}] {cmd}")
                elif name == "Read":
                    path = tool_input.get("file_path", "")
                    parts.append(f"[tool: {name}] {path}")
                elif name == "Edit":
                    path = tool_input.get("file_path", "")
                    parts.append(f"[tool: {name}] {path}")
                elif name == "Write":
                    path = tool_input.get("file_path", "")
                    parts.append(f"[tool: {name}] {path}")
                elif name == "Grep":
                    pattern = tool_input.get("pattern", "")
                    parts.append(f"[tool: {name}] {pattern}")
                elif name == "Glob":
                    pattern = tool_input.get("pattern", "")
                    parts.append(f"[tool: {name}] {pattern}")
                elif name == "TodoWrite":
                    todos = tool_input.get("todos", [])
                    in_progress = [t for t in todos if t.get("status") == "in_progress"]
                    if in_progress:
                        parts.append(f"[tool: {name}] {in_progress[0].get('activeForm', '')}")
                    else:
                        parts.append(f"[tool: {name}]")
                else:
                    parts.append(f"[tool: {name}]")
        if parts:
            return "\n".join(parts)
        return None

    elif entry_type == "result":
        result = entry.get("result", "")
        is_error = entry.get("is_error", False)
        if is_error:
            return f"[result] ERROR: {result}"
        # For success, show summary if available
        duration = entry.get("duration_ms", 0)
        num_turns = entry.get("num_turns", 0)
        cost = entry.get("total_cost_usd", 0)
        return f"[result] Completed in {num_turns} turns, {duration/1000:.1f}s, ${cost:.4f}"

    return None


def cmd_log(args: argparse.Namespace) -> int:
    """Display the log for a task or worker."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    registry = WorkerRegistry(config.workers_path)

    query = args.identifier
    task = None
    worker = None
    log_path = None
    is_running = False

    if args.worker:
        # Look up by worker ID
        worker = registry.get(query)
        if not worker:
            print(f"Error: Worker '{query}' not found")
            return 1
        is_running = registry.is_running(query)
        if worker.task_id:
            task = store.get(worker.task_id)
        if task and task.log_file:
            log_path = config.project_dir / task.log_file
        elif task and task.task_id:
            log_path = config.log_path / f"{task.task_id}.log"

    elif args.task:
        # Look up by numeric task ID
        try:
            task_id = int(query)
            task = store.get(task_id)
        except ValueError:
            print(f"Error: '{query}' is not a valid task ID (must be numeric)")
            return 1
        if not task:
            print(f"Error: Task {query} not found")
            return 1
        if task.log_file:
            log_path = config.project_dir / task.log_file

    elif args.slug:
        # Look up by slug (exact or partial match)
        task = store.get_by_task_id(query)
        if not task:
            # Try partial match
            all_tasks = store.get_all()
            for t in all_tasks:
                if t.task_id and query in t.task_id:
                    task = t
                    break
        if not task:
            print(f"Error: No task found matching slug '{query}'")
            return 1
        if task.log_file:
            log_path = config.project_dir / task.log_file

    if not log_path:
        print(f"Error: No log file found")
        return 1

    if not log_path.exists():
        if is_running:
            print(f"Log file not yet created: {log_path}")
            print("Worker is still starting up...")
        else:
            print(f"Error: Log file not found at {log_path}")
        return 1

    # Determine mode: follow (live tail) vs static display
    follow = hasattr(args, 'follow') and args.follow
    if follow and not is_running:
        follow = False  # Can't follow a completed task

    # Check for raw mode
    raw_mode = hasattr(args, 'raw') and args.raw

    if follow or raw_mode:
        # Live streaming mode - use the formatted streaming output
        return _tail_log_file(log_path, args, registry, query if worker else None)

    # Static display mode - show summary or full turns
    log_data = None
    entries = []
    try:
        with open(log_path) as f:
            content = f.read().strip()

        # Try parsing as single JSON first (old format)
        try:
            log_data = json.loads(content)
        except json.JSONDecodeError:
            # Try parsing as JSONL (new format)
            for line in content.split('\n'):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    entries.append(entry)
                    if entry.get("type") == "result":
                        log_data = entry
                except json.JSONDecodeError:
                    continue

        if log_data is None and not entries:
            # If we have content but couldn't parse any JSON, it's likely a startup error
            if content:
                print("Task failed during startup (no Claude session):")
                # Display the raw error message, indented for clarity
                for line in content.split('\n'):
                    print(f"  {line}")
                return 1
            else:
                print("Error: No log entries found in log file")
                return 1
    except Exception as e:
        print(f"Error: Failed to read log file: {e}")
        return 1

    # Display header
    print("=" * 70)
    if task:
        prompt_display = task.prompt[:100] if task.prompt else "(no prompt)"
        print(f"Task: {prompt_display}")
        print(f"ID: {task.id} | Slug: {task.task_id}")
        print(f"Status: {task.status}")
        if task.branch:
            print(f"Branch: {task.branch}")
    elif worker:
        print(f"Worker: {worker.worker_id}")
        print(f"Status: {'running' if is_running else 'completed'}")
    print("=" * 70)
    print()

    if args.turns and entries:
        # Show the full conversation turns
        _display_conversation_turns(entries)
    elif log_data:
        # Extract and display the result field (which contains markdown)
        if "result" in log_data:
            print(log_data["result"])
        else:
            # No result - show the subtype (e.g., error_max_turns)
            subtype = log_data.get("subtype", "unknown")
            print(f"Run ended with: {subtype}")
            if log_data.get("errors"):
                print(f"Errors: {log_data['errors']}")
    else:
        # No result entry yet - show formatted entries
        for entry in entries:
            output = _format_log_entry(entry)
            if output:
                print(output)

    print()
    print("=" * 70)

    # Display summary stats if available
    if log_data:
        if "duration_ms" in log_data:
            duration_sec = log_data["duration_ms"] / 1000
            print(f"Duration: {format_duration(duration_sec)}")
        if "num_turns" in log_data:
            print(f"Turns: {log_data['num_turns']}")
        if "total_cost_usd" in log_data:
            print(f"Cost: ${log_data['total_cost_usd']:.4f}")

    return 0


def _tail_log_file(log_path: Path, args: argparse.Namespace, registry: WorkerRegistry, worker_id: str | None) -> int:
    """Tail a log file with optional follow mode."""
    raw_mode = hasattr(args, 'raw') and args.raw
    follow = hasattr(args, 'follow') and args.follow

    if raw_mode:
        # Use tail directly for raw JSON output
        try:
            cmd = ["tail"]
            if hasattr(args, 'tail') and args.tail:
                cmd.extend(["-n", str(args.tail)])
            if follow:
                cmd.append("-f")
            cmd.append(str(log_path))
            subprocess.run(cmd)
            return 0
        except KeyboardInterrupt:
            return 0
        except Exception as e:
            print(f"Error tailing log: {e}")
            return 1

    # Formatted output mode
    try:
        tail_lines = args.tail if hasattr(args, 'tail') and args.tail else None

        def read_and_format_lines(file_path: Path, num_lines: int | None = None) -> list[str]:
            """Read lines from file and return formatted output."""
            with open(file_path, 'r') as f:
                lines = f.readlines()

            if num_lines:
                lines = lines[-num_lines:]

            formatted = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    output = _format_log_entry(entry)
                    if output:
                        formatted.append(output)
                except json.JSONDecodeError:
                    formatted.append(line)
            return formatted

        # Initial read
        formatted = read_and_format_lines(log_path, tail_lines)
        for line in formatted:
            print(line)

        if not follow:
            return 0

        # Follow mode - watch for new lines
        last_size = log_path.stat().st_size
        last_line_count = sum(1 for _ in open(log_path))

        while True:
            time.sleep(0.5)

            current_size = log_path.stat().st_size
            if current_size > last_size:
                with open(log_path, 'r') as f:
                    lines = f.readlines()

                new_lines = lines[last_line_count:]
                last_line_count = len(lines)
                last_size = current_size

                for line in new_lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        output = _format_log_entry(entry)
                        if output:
                            print(output)
                    except json.JSONDecodeError:
                        print(line)

            # Check if worker is still running
            if worker_id and not registry.is_running(worker_id):
                time.sleep(0.5)
                with open(log_path, 'r') as f:
                    lines = f.readlines()
                new_lines = lines[last_line_count:]
                for line in new_lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        output = _format_log_entry(entry)
                        if output:
                            print(output)
                    except json.JSONDecodeError:
                        print(line)
                break

        return 0

    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error tailing log: {e}")
        return 1


def _display_conversation_turns(entries: list[dict]) -> None:
    """Display the conversation turns from JSONL log entries."""
    turn_num = 0
    for entry in entries:
        entry_type = entry.get("type")

        if entry_type == "system":
            # Show init info briefly
            model = entry.get("model", "unknown")
            print(f"[System] Model: {model}")
            print("-" * 40)
            continue

        if entry_type == "assistant":
            message = entry.get("message", {})
            content = message.get("content", [])

            for item in content:
                if item.get("type") == "text":
                    text = item.get("text", "")
                    if text:
                        turn_num += 1
                        print(f"\n[Assistant - Turn {turn_num}]")
                        print(text)
                        print()
                elif item.get("type") == "tool_use":
                    tool_name = item.get("name", "unknown")
                    tool_input = item.get("input", {})
                    print(f"  -> Tool: {tool_name}")
                    # Show brief summary of tool input
                    if tool_name == "Read":
                        print(f"     File: {tool_input.get('file_path', 'unknown')}")
                    elif tool_name == "Edit":
                        print(f"     File: {tool_input.get('file_path', 'unknown')}")
                    elif tool_name == "Bash":
                        cmd = tool_input.get('command', '')
                        if len(cmd) > 80:
                            cmd = cmd[:77] + "..."
                        print(f"     Command: {cmd}")
                    elif tool_name == "Grep":
                        print(f"     Pattern: {tool_input.get('pattern', 'unknown')}")
                    elif tool_name == "Glob":
                        print(f"     Pattern: {tool_input.get('pattern', 'unknown')}")
                    elif tool_name == "Write":
                        print(f"     File: {tool_input.get('file_path', 'unknown')}")
                    elif tool_name == "TodoWrite":
                        todos = tool_input.get('todos', [])
                        print(f"     Todos: {len(todos)} items")
                    else:
                        # Show first key-value for unknown tools
                        for k, v in list(tool_input.items())[:1]:
                            v_str = str(v)
                            if len(v_str) > 60:
                                v_str = v_str[:57] + "..."
                            print(f"     {k}: {v_str}")

        elif entry_type == "user":
            # User entries are tool results - show brief summary
            message = entry.get("message", {})
            content = message.get("content", [])
            for item in content:
                if item.get("type") == "tool_result":
                    is_error = item.get("is_error", False)
                    result_content = item.get("content", "")
                    if is_error:
                        print(f"  <- Error: {result_content[:100]}")
                    # Don't print successful tool results - too verbose

        elif entry_type == "result":
            # Final result - already shown in summary
            pass


def cmd_add(args: argparse.Namespace) -> int:
    """Add a new task."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    # Determine task type
    if args.type:
        task_type = args.type
    elif args.explore:
        task_type = "explore"
    else:
        task_type = "task"

    # Validate task type
    valid_types = ["task", "explore", "plan", "implement", "review"]
    if task_type not in valid_types:
        print(f"Error: Invalid task type '{task_type}'. Must be one of: {', '.join(valid_types)}")
        return 1

    # Get optional parameters
    group = args.group if hasattr(args, 'group') and args.group else None
    depends_on = args.depends_on if hasattr(args, 'depends_on') and args.depends_on else None
    based_on = args.based_on if hasattr(args, 'based_on') and args.based_on else None
    create_review = args.review if hasattr(args, 'review') and args.review else False
    same_branch = args.same_branch if hasattr(args, 'same_branch') and args.same_branch else False
    spec = args.spec if hasattr(args, 'spec') and args.spec else None
    branch_type = args.branch_type if hasattr(args, 'branch_type') and args.branch_type else None

    # Validation: --spec must reference an existing file
    if spec:
        spec_path = config.project_dir / spec
        if not spec_path.exists():
            print(f"Error: Spec file not found: {spec}")
            return 1

    # Validation: --same-branch requires --based-on or --depends-on
    if same_branch and not based_on and not depends_on:
        print("Error: --same-branch requires --based-on or --depends-on")
        return 1

    # Validation: --based-on must reference an existing task
    if based_on:
        dep_task = store.get(based_on)
        if not dep_task:
            print(f"Error: Task #{based_on} not found")
            return 1

    # Validation: --depends-on must reference an existing task
    if depends_on:
        dep_task = store.get(depends_on)
        if not dep_task:
            print(f"Error: Task #{depends_on} not found")
            return 1

    if args.edit or not args.prompt:
        # Interactive mode with $EDITOR
        task = add_task_interactive(
            store,
            task_type=task_type,
            based_on=based_on,
            spec=spec,
            group=group,
            depends_on=depends_on,
            create_review=create_review,
            same_branch=same_branch,
            task_type_hint=branch_type,
        )
        if not task:
            return 1
        print(f"✓ Added task #{task.id}")
        return 0
    else:
        # Inline prompt
        task = store.add(
            args.prompt,
            task_type=task_type,
            based_on=based_on,
            group=group,
            depends_on=depends_on,
            create_review=create_review,
            same_branch=same_branch,
            spec=spec,
            task_type_hint=branch_type,
        )
        print(f"✓ Added task #{task.id}")
        return 0


def cmd_edit(args: argparse.Namespace) -> int:
    """Edit a task's prompt or metadata."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    if task.status != "pending":
        print(f"Error: Can only edit pending tasks (task is {task.status})")
        return 1

    # Handle --group flag
    if hasattr(args, 'group_flag') and args.group_flag is not None:
        # Empty string removes from group
        if args.group_flag == "":
            task.group = None
            store.update(task)
            print(f"✓ Removed task #{task.id} from group")
            return 0
        else:
            task.group = args.group_flag
            store.update(task)
            print(f"✓ Moved task #{task.id} to group '{args.group_flag}'")
            return 0

    # Handle --based-on flag
    if hasattr(args, 'based_on_flag') and args.based_on_flag is not None:
        dep_task = store.get(args.based_on_flag)
        if not dep_task:
            print(f"Error: Task #{args.based_on_flag} not found")
            return 1
        task.depends_on = args.based_on_flag
        store.update(task)
        print(f"✓ Set task #{task.id} to depend on task #{args.based_on_flag}")
        return 0

    # Handle --review flag
    if hasattr(args, 'review') and args.review:
        task.create_review = True
        store.update(task)
        print(f"✓ Enabled automatic review task creation for task #{task.id}")
        return 0

    if args.explore and args.task:
        print("Error: Cannot use both --explore and --task")
        return 1

    # Handle type conversion without opening editor
    if args.explore or args.task:
        new_type = "explore" if args.explore else "task"
        if task.task_type == new_type:
            print(f"Task #{task.id} is already a {new_type}")
            return 0
        task.task_type = new_type
        store.update(task)
        print(f"✓ Converted task #{task.id} to {new_type}")
        return 0

    if edit_task_interactive(store, task):
        print(f"✓ Updated task #{task.id}")
        return 0
    return 1


def cmd_groups(args: argparse.Namespace) -> int:
    """List all groups with task counts."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    groups = store.get_groups()

    # Count ungrouped tasks
    all_tasks = store.get_all()
    ungrouped_counts: dict[str, int] = {}
    for task in all_tasks:
        if task.group is None:
            status = task.status
            ungrouped_counts[status] = ungrouped_counts.get(status, 0) + 1

    if not groups and not ungrouped_counts:
        print("No tasks found")
        return 0

    # Sort groups by name
    for group_name in sorted(groups.keys()):
        status_counts = groups[group_name]
        total = sum(status_counts.values())

        # Build status summary
        parts = []
        for status in ["pending", "in_progress", "completed", "failed", "unmerged"]:
            if status in status_counts and status_counts[status] > 0:
                parts.append(f"{status_counts[status]} {status}")

        status_str = ", ".join(parts) if parts else "0 tasks"
        print(f"{group_name:<20} {total} tasks ({status_str})")

    # Show ungrouped tasks
    if ungrouped_counts:
        total = sum(ungrouped_counts.values())
        parts = []
        for status in ["pending", "in_progress", "completed", "failed", "unmerged"]:
            if status in ungrouped_counts and ungrouped_counts[status] > 0:
                parts.append(f"{ungrouped_counts[status]} {status}")

        status_str = ", ".join(parts) if parts else "0 tasks"
        print(f"{'(ungrouped)':<20} {total} tasks ({status_str})")

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show tasks in a group."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    group_name = args.group
    tasks = store.get_by_group(group_name)

    if not tasks:
        print(f"No tasks found in group '{group_name}'")
        return 0

    print(f"Group: {group_name}")
    print()

    for task in tasks:
        # Status icon
        if task.status == "completed":
            icon = "✓"
        elif task.status == "in_progress":
            icon = "→"
        elif task.status == "failed":
            icon = "✗"
        else:
            icon = "○"

        # Task type label
        type_label = f"[{task.task_type}] " if task.task_type != "task" else ""

        # Get first line of prompt
        first_line = task.prompt.split('\n')[0].strip()
        prompt_display = first_line[:50] + "..." if len(first_line) > 50 else first_line

        # Status display
        status_display = task.status

        # Check if blocked
        blocked_info = ""
        if task.status == "pending":
            is_blocked, blocking_id, _ = store.is_task_blocked(task)
            if is_blocked:
                blocked_info = f" (blocked by #{blocking_id})"

        # Date info for completed tasks
        date_info = ""
        if task.completed_at:
            date_info = f"  {task.completed_at.strftime('%m/%d')}"

        print(f"  {icon} {task.id}. {type_label}{prompt_display:<50} {status_display}{date_info}{blocked_info}")

    return 0


def cmd_ps(args: argparse.Namespace) -> int:
    """List running and completed workers."""
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)

    workers = registry.list_all(include_completed=args.all if hasattr(args, 'all') else False)

    if not workers:
        if hasattr(args, 'all') and args.all:
            print("No workers found")
        else:
            print("No running workers (use --all to see completed)")
        return 0

    if hasattr(args, 'quiet') and args.quiet:
        # Just print worker IDs
        for worker in workers:
            print(worker.worker_id)
        return 0

    if hasattr(args, 'json') and args.json:
        # JSON output
        import json as json_lib
        print(json_lib.dumps([w.to_dict() for w in workers], indent=2))
        return 0

    # Table output
    print(f"{'WORKER ID':<20} {'PID':<8} {'TYPE':<6} {'TASK ID':<10} {'STATUS':<12} {'TASK':<30} {'DURATION':<10}")
    print("-" * 106)

    for worker in workers:
        # Check if still running
        if worker.status == "running" and not registry.is_running(worker.worker_id):
            worker.status = "stale"

        # Get task info
        task_display = ""
        task_id_display = ""
        if worker.task_id:
            task_id_display = f"#{worker.task_id}"
            task = store.get(worker.task_id)
            if task:
                if task.task_id:
                    task_display = task.task_id
                else:
                    prompt = task.prompt[:25] + "..." if len(task.prompt) > 25 else task.prompt
                    task_display = prompt

        # Calculate duration
        started = datetime.fromisoformat(worker.started_at)
        if worker.completed_at:
            ended = datetime.fromisoformat(worker.completed_at)
            duration_sec = (ended - started).total_seconds()
        else:
            duration_sec = (datetime.now(timezone.utc) - started).total_seconds()

        if duration_sec < 60:
            duration = f"{duration_sec:.0f}s"
        else:
            minutes = int(duration_sec // 60)
            seconds = int(duration_sec % 60)
            duration = f"{minutes}m {seconds}s"

        # Determine worker type (default to background for old workers without is_background field)
        worker_type = "fg" if hasattr(worker, 'is_background') and not worker.is_background else "bg"

        print(f"{worker.worker_id:<20} {worker.pid:<8} {worker_type:<6} {task_id_display:<10} {worker.status:<12} {task_display:<30} {duration:<10}")

    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    """Stop a running worker."""
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)

    # Validate arguments
    if not hasattr(args, 'worker_id') or (not args.worker_id and not (hasattr(args, 'all') and args.all)):
        print("Error: Must specify worker_id or use --all")
        return 1

    if hasattr(args, 'all') and args.all:
        # Stop all running workers
        workers = registry.list_all(include_completed=False)
        running_workers = [w for w in workers if w.status == "running" and registry.is_running(w.worker_id)]

        if not running_workers:
            print("No running workers to stop")
            return 0

        for worker in running_workers:
            print(f"Stopping worker {worker.worker_id} (PID {worker.pid})...")
            if registry.stop(worker.worker_id, force=args.force if hasattr(args, 'force') else False):
                print(f"  ✓ Sent stop signal")
            else:
                print(f"  ✗ Failed to stop worker")

        return 0

    # Stop specific worker
    worker = registry.get(args.worker_id)
    if not worker:
        print(f"Error: Worker {args.worker_id} not found")
        return 1

    if worker.status != "running":
        print(f"Worker {args.worker_id} is not running (status: {worker.status})")
        return 1

    if not registry.is_running(args.worker_id):
        print(f"Worker {args.worker_id} is not running (process not found)")
        registry.mark_completed(args.worker_id, exit_code=1, status="stale")
        return 1

    print(f"Stopping worker {args.worker_id} (PID {worker.pid})...")
    if registry.stop(args.worker_id, force=args.force if hasattr(args, 'force') else False):
        print("✓ Sent stop signal")

        # Wait a moment and check if it stopped
        time.sleep(1)
        if not registry.is_running(args.worker_id):
            print("✓ Worker stopped")
            registry.mark_completed(args.worker_id, exit_code=1, status="failed")
        else:
            print("Worker is still running, may take a moment to shut down")

        return 0
    else:
        print("✗ Failed to stop worker")
        return 1


def cmd_delete(args: argparse.Namespace) -> int:
    """Delete a task."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    if task.status == "in_progress":
        print(f"Error: Cannot delete in-progress task")
        return 1

    if not args.force:
        prompt_display = task.prompt[:60] + "..." if len(task.prompt) > 60 else task.prompt
        confirm = input(f"Delete task #{task.id}: {prompt_display}? [y/N] ")
        if confirm.lower() != 'y':
            print("Cancelled")
            return 0

    if store.delete(args.task_id):
        print(f"✓ Deleted task #{args.task_id}")
        return 0
    else:
        print(f"Error: Failed to delete task")
        return 1


def cmd_retry(args: argparse.Namespace) -> int:
    """Retry a failed or completed task by creating a new pending task."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    store = get_store(config)

    # Get the original task
    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    # Validate status
    if task.status not in ("completed", "failed"):
        print(f"Error: Can only retry completed or failed tasks (task is {task.status})")
        return 1

    # Create new task copying relevant fields
    new_task = store.add(
        prompt=task.prompt,
        task_type=task.task_type,
        group=task.group,
        spec=task.spec,
        create_review=task.create_review,
        task_type_hint=task.task_type_hint,
        based_on=args.task_id,  # Track retry lineage
    )

    print(f"✓ Created task #{new_task.id} (retry of #{args.task_id})")

    # Handle background mode - spawn worker to run the new task
    if args.background:
        # Create a temporary args object for the worker with the new task_id
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [new_task.id]
        return _spawn_background_worker(worker_args, config, task_id=new_task.id)

    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    """Resume a failed task from where it left off."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    if task.status != "failed":
        print(f"Error: Can only resume failed tasks (task is {task.status})")
        return 1

    if not task.session_id:
        print(f"Error: Task #{args.task_id} has no session ID (cannot resume)")
        print("Use 'gza retry' to start fresh instead")
        return 1

    # Handle background mode
    if args.background:
        return _spawn_background_resume_worker(args, config, args.task_id)

    # Resume the task
    print(f"=== Resuming Task #{args.task_id} ===")
    return run(config, task_id=args.task_id, resume=True)


def cmd_show(args: argparse.Namespace) -> int:
    """Show details of a specific task."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    print(f"Task #{task.id}")
    print("=" * 50)
    print(f"Status: {task.status}")
    print(f"Type: {task.task_type}")
    if task.task_id:
        print(f"Slug: {task.task_id}")
    if task.based_on:
        print(f"Based on: task #{task.based_on}")
    if task.depends_on:
        print(f"Depends on: task #{task.depends_on}")
    if task.group:
        print(f"Group: {task.group}")
    if task.spec:
        print(f"Spec: {task.spec}")
    if task.branch:
        print(f"Branch: {task.branch}")
    if task.log_file:
        print(f"Log: {task.log_file}")
    if task.report_file:
        print(f"Report: {task.report_file}")
    if task.session_id:
        print(f"Session ID: {task.session_id}")
    print()
    print("Prompt:")
    print("-" * 50)
    print(task.prompt)
    print("-" * 50)
    print()
    if task.created_at:
        print(f"Created: {task.created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if task.started_at:
        print(f"Started: {task.started_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    if task.completed_at:
        print(f"Completed: {task.completed_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    stats_str = format_stats(task)
    if stats_str:
        print(f"Stats: {stats_str}")

    return 0


def cmd_import(args: argparse.Namespace) -> int:
    """Import tasks from a YAML file."""
    # Handle legacy usage: gza import <project_dir>
    # If the file argument is a directory, treat it as project_dir
    if args.file and Path(args.file).is_dir():
        args.project_dir = Path(args.file).resolve()
        args.file = None

    config = Config.load(args.project_dir)
    store = get_store(config)

    # Determine which file to import
    if args.file:
        import_path = Path(args.file)
        if not import_path.is_absolute():
            import_path = config.project_dir / import_path
    else:
        # Legacy: import from tasks.yaml
        import_path = config.tasks_path
        if not import_path.exists():
            print(f"Error: No file specified and {import_path} not found")
            print("Usage: gza import <file> [--dry-run] [--force]")
            return 1
        return _cmd_import_legacy(config, store)

    # Parse the import file
    tasks, default_group, default_spec, parse_errors = parse_import_file(import_path)

    if parse_errors:
        print("Error: Failed to parse import file:")
        for error in parse_errors:
            if error.task_index:
                print(f"  Task {error.task_index}: {error.message}")
            else:
                print(f"  {error.message}")
        return 1

    # Validate the tasks
    validation_errors = validate_import(tasks, config.project_dir, default_spec)

    if validation_errors:
        print("Error: Validation failed:")
        for error in validation_errors:
            if error.task_index:
                print(f"  Task {error.task_index}: {error.message}")
            else:
                print(f"  {error.message}")
        return 1

    # Import the tasks
    if args.dry_run:
        print(f"Would import {len(tasks)} tasks:")
    else:
        print(f"Importing {len(tasks)} tasks...")

    results, messages = import_tasks(
        store=store,
        tasks=tasks,
        project_dir=config.project_dir,
        dry_run=args.dry_run,
        force=args.force,
    )

    for message in messages:
        print(message)

    # Summary
    if args.dry_run:
        return 0

    created = sum(1 for r in results if not r.skipped)
    skipped = sum(1 for r in results if r.skipped)

    if skipped:
        print(f"Imported {created} tasks ({skipped} skipped)")
    else:
        print(f"Imported {created} tasks")

    return 0


def _cmd_import_legacy(config: Config, store: SqliteTaskStore) -> int:
    """Legacy import from tasks.yaml (old format)."""
    yaml_store = YamlTaskStore(config.tasks_path)
    imported = 0
    skipped = 0

    for yaml_task in yaml_store._tasks:
        # Check if already imported (by task_id)
        if yaml_task.task_id:
            existing = store.get_by_task_id(yaml_task.task_id)
            if existing:
                skipped += 1
                continue

        # Create task in SQLite (Task class uses 'prompt' and 'task_type')
        task = store.add(yaml_task.prompt, task_type=yaml_task.task_type)

        # Copy over fields - need to convert TaskStatus enum to string for status
        status_value = yaml_task.status.value if hasattr(yaml_task.status, 'value') else yaml_task.status
        task.status = status_value
        task.task_id = yaml_task.task_id
        task.branch = yaml_task.branch
        task.log_file = yaml_task.log_file
        task.report_file = yaml_task.report_file
        task.has_commits = yaml_task.has_commits
        task.duration_seconds = yaml_task.duration_seconds
        task.num_turns = yaml_task.num_turns
        task.cost_usd = yaml_task.cost_usd
        if yaml_task.completed_at:
            if isinstance(yaml_task.completed_at, datetime):
                task.completed_at = yaml_task.completed_at
            else:
                task.completed_at = datetime.combine(yaml_task.completed_at, datetime.min.time())

        store.update(task)
        imported += 1

    print(f"✓ Imported {imported} tasks")
    if skipped:
        print(f"  Skipped {skipped} already imported tasks")

    return 0


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
        for subaction in subactions:
            yield subaction

    def _metavar_formatter(self, action, default_metavar):
        """Override to sort choices alphabetically in usage string."""
        if action.metavar is not None:
            result = action.metavar
        elif action.choices is not None:
            # Sort choices alphabetically
            choice_strs = sorted(str(choice) for choice in action.choices)
            result = '{%s}' % ','.join(choice_strs)
        else:
            result = default_metavar

        def format(tuple_size):
            if isinstance(result, tuple):
                return result
            else:
                return (result, ) * tuple_size
        return format


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to a subparser."""
    parser.add_argument(
        "--project",
        "-C",
        dest="project_dir",
        default=".",
        help="Target project directory (default: current directory)",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gza - AI agent task runner",
        formatter_class=SortingHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # work command
    work_parser = subparsers.add_parser("work", help="Run the next pending task or specific tasks")
    work_parser.add_argument(
        "task_ids",
        nargs="*",
        type=int,
        help="Specific task ID(s) to run (optional, can specify multiple)",
    )
    add_common_args(work_parser)
    work_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker",
    )
    work_parser.add_argument(
        "--count", "-c",
        type=int,
        metavar="N",
        help="Number of tasks to run before stopping (overrides config default)",
    )
    work_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    work_parser.add_argument(
        "--worker-mode",
        action="store_true",
        help=argparse.SUPPRESS,  # Internal flag for background workers
    )
    work_parser.add_argument(
        "--resume",
        action="store_true",
        help=argparse.SUPPRESS,  # Internal flag for resume mode
    )

    # next command
    next_parser = subparsers.add_parser("next", help="List upcoming pending tasks")
    add_common_args(next_parser)
    next_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all pending tasks including blocked ones",
    )

    # history command
    history_parser = subparsers.add_parser("history", help="List recent completed/failed tasks")
    add_common_args(history_parser)

    # unmerged command
    unmerged_parser = subparsers.add_parser("unmerged", help="List tasks with unmerged work")
    add_common_args(unmerged_parser)

    # merge command
    merge_parser = subparsers.add_parser("merge", help="Merge a task's branch into current branch")
    merge_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to merge",
    )
    merge_parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the branch after successful merge",
    )
    merge_parser.add_argument(
        "--squash",
        action="store_true",
        help="Perform a squash merge instead of a regular merge",
    )
    merge_parser.add_argument(
        "--rebase",
        action="store_true",
        help="Rebase the task's branch onto current branch instead of merging",
    )
    add_common_args(merge_parser)

    # pr command
    pr_parser = subparsers.add_parser("pr", help="Create GitHub PR from completed task")
    pr_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to create PR from",
    )
    pr_parser.add_argument(
        "--title",
        help="Override auto-generated PR title",
    )
    pr_parser.add_argument(
        "--draft",
        action="store_true",
        help="Create as draft PR",
    )
    add_common_args(pr_parser)

    # stats command
    stats_parser = subparsers.add_parser("stats", help="Show cost and usage statistics")
    add_common_args(stats_parser)
    stats_parser.add_argument(
        "--last",
        type=int,
        default=5,
        metavar="N",
        help="Show last N tasks (default: 5)",
    )

    # validate command
    validate_parser = subparsers.add_parser("validate", help="Validate gza.yaml configuration")
    add_common_args(validate_parser)

    # init command
    init_parser = subparsers.add_parser("init", help="Generate new gza.yaml with defaults")
    add_common_args(init_parser)
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing gza.yaml file",
    )

    # log command
    log_parser = subparsers.add_parser("log", help="Display log for a task or worker")
    log_parser.add_argument(
        "identifier",
        help="Task ID, slug, or worker ID",
    )
    log_type_group = log_parser.add_mutually_exclusive_group(required=True)
    log_type_group.add_argument(
        "--task", "-t",
        action="store_true",
        help="Look up by task ID (numeric)",
    )
    log_type_group.add_argument(
        "--slug", "-s",
        action="store_true",
        help="Look up by task slug (supports partial match)",
    )
    log_type_group.add_argument(
        "--worker", "-w",
        action="store_true",
        help="Look up by worker ID",
    )
    log_parser.add_argument(
        "--turns",
        action="store_true",
        help="Show the full conversation turns instead of just the summary",
    )
    log_parser.add_argument(
        "--follow", "-f",
        action="store_true",
        help="Follow the log in real-time (for running workers)",
    )
    log_parser.add_argument(
        "--tail",
        type=int,
        metavar="N",
        help="Show last N lines only (used with --follow or --raw)",
    )
    log_parser.add_argument(
        "--raw",
        action="store_true",
        help="Show raw JSON lines instead of formatted output",
    )
    add_common_args(log_parser)

    # add command
    add_parser = subparsers.add_parser("add", help="Add a new task")
    add_parser.add_argument(
        "prompt",
        nargs="?",
        help="Task prompt (opens $EDITOR if not provided)",
    )
    add_parser.add_argument(
        "--edit", "-e",
        action="store_true",
        help="Open $EDITOR to write the prompt",
    )
    add_parser.add_argument(
        "--type",
        choices=["task", "explore", "plan", "implement", "review"],
        help="Set task type (default: task)",
    )
    add_parser.add_argument(
        "--branch-type",
        metavar="TYPE",
        help="Set branch type hint for branch naming (e.g., fix, feature, chore)",
    )
    add_parser.add_argument(
        "--explore",
        action="store_true",
        help="Create an explore task (shorthand for --type explore)",
    )
    add_parser.add_argument(
        "--group",
        metavar="NAME",
        help="Set task group",
    )
    add_parser.add_argument(
        "--based-on",
        type=int,
        metavar="ID",
        help="Base this task on a previous task's output (sets depends_on field)",
    )
    add_parser.add_argument(
        "--depends-on",
        type=int,
        metavar="ID",
        help="Set dependency on another task",
    )
    add_parser.add_argument(
        "--review",
        action="store_true",
        help="Auto-create review task on completion (for implement tasks)",
    )
    add_parser.add_argument(
        "--same-branch",
        action="store_true",
        help="Continue on depends_on task's branch instead of creating new",
    )
    add_parser.add_argument(
        "--spec",
        metavar="FILE",
        help="Path to spec file for task context",
    )
    add_common_args(add_parser)

    # edit command
    edit_parser = subparsers.add_parser("edit", help="Edit a pending task's prompt or metadata")
    edit_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to edit",
    )
    edit_parser.add_argument(
        "--group",
        dest="group_flag",
        metavar="NAME",
        help="Move task to group (use empty string \"\" to remove from group)",
    )
    edit_parser.add_argument(
        "--based-on",
        dest="based_on_flag",
        type=int,
        metavar="ID",
        help="Set dependency on another task",
    )
    edit_parser.add_argument(
        "--explore",
        action="store_true",
        help="Convert to an explore task",
    )
    edit_parser.add_argument(
        "--task",
        action="store_true",
        help="Convert to a regular task",
    )
    edit_parser.add_argument(
        "--review",
        action="store_true",
        help="Enable automatic review task creation on completion",
    )
    add_common_args(edit_parser)

    # delete command
    delete_parser = subparsers.add_parser("delete", help="Delete a task")
    delete_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to delete",
    )
    delete_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Skip confirmation prompt",
    )
    add_common_args(delete_parser)

    # retry command
    retry_parser = subparsers.add_parser("retry", help="Retry a failed or completed task")
    retry_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to retry",
    )
    retry_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker (only with --background)",
    )
    retry_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    add_common_args(retry_parser)

    # resume command
    resume_parser = subparsers.add_parser("resume", help="Resume a failed task from where it left off")
    resume_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to resume",
    )
    resume_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker",
    )
    resume_parser.add_argument(
        "--background", "-b",
        action="store_true",
        help="Run worker in background (detached mode)",
    )
    add_common_args(resume_parser)

    # show command
    show_parser = subparsers.add_parser("show", help="Show details of a specific task")
    show_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to show",
    )
    add_common_args(show_parser)

    # import command
    import_parser = subparsers.add_parser("import", help="Import tasks from a YAML file")
    import_parser.add_argument(
        "file",
        nargs="?",
        help="YAML file to import tasks from",
    )
    import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be imported without creating tasks",
    )
    import_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Skip duplicate detection and import all tasks",
    )
    add_common_args(import_parser)

    # groups command
    groups_parser = subparsers.add_parser("groups", help="List all groups with task counts")
    add_common_args(groups_parser)

    # status command
    status_parser = subparsers.add_parser("status", help="Show tasks in a group")
    status_parser.add_argument(
        "group",
        help="Group name to show tasks for",
    )
    add_common_args(status_parser)

    # ps command
    ps_parser = subparsers.add_parser("ps", help="List running workers")
    ps_parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Include completed/failed workers",
    )
    ps_parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Only show worker IDs",
    )
    ps_parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON",
    )
    add_common_args(ps_parser)

    # stop command
    stop_parser = subparsers.add_parser("stop", help="Stop a running worker")
    stop_parser.add_argument(
        "worker_id",
        nargs="?",
        help="Worker ID to stop (optional if --all is used)",
    )
    stop_parser.add_argument(
        "--all",
        action="store_true",
        help="Stop all running workers",
    )
    stop_parser.add_argument(
        "--force",
        action="store_true",
        help="Force kill (SIGKILL instead of SIGTERM)",
    )
    add_common_args(stop_parser)

    args = parser.parse_args()

    # Validate and resolve project_dir
    if hasattr(args, 'project_dir'):
        args.project_dir = Path(args.project_dir).resolve()
        if not args.project_dir.is_dir():
            print(f"Error: {args.project_dir} is not a directory")
            return 1

    try:
        if args.command == "work":
            return cmd_run(args)
        elif args.command == "next":
            return cmd_next(args)
        elif args.command == "history":
            return cmd_history(args)
        elif args.command == "unmerged":
            return cmd_unmerged(args)
        elif args.command == "merge":
            return cmd_merge(args)
        elif args.command == "pr":
            return cmd_pr(args)
        elif args.command == "stats":
            return cmd_stats(args)
        elif args.command == "validate":
            return cmd_validate(args)
        elif args.command == "init":
            return cmd_init(args)
        elif args.command == "log":
            return cmd_log(args)
        elif args.command == "add":
            return cmd_add(args)
        elif args.command == "edit":
            return cmd_edit(args)
        elif args.command == "delete":
            return cmd_delete(args)
        elif args.command == "retry":
            return cmd_retry(args)
        elif args.command == "resume":
            return cmd_resume(args)
        elif args.command == "show":
            return cmd_show(args)
        elif args.command == "import":
            return cmd_import(args)
        elif args.command == "groups":
            return cmd_groups(args)
        elif args.command == "status":
            return cmd_status(args)
        elif args.command == "ps":
            return cmd_ps(args)
        elif args.command == "stop":
            return cmd_stop(args)
    except ConfigError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
