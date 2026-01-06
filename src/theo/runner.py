"""Main Theo runner orchestration."""

import os
import re
from datetime import datetime
from pathlib import Path

from .config import Config
from .db import SqliteTaskStore, Task, TaskStats
from .git import Git, GitError
from .providers import get_provider, Provider, RunResult


DEFAULT_REPORT_DIR = ".theo/explorations"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.0f}s"


def print_stats(stats: TaskStats, has_commits: bool | None = None) -> None:
    """Print task statistics."""
    parts = []
    if stats.duration_seconds is not None:
        parts.append(f"Runtime: {format_duration(stats.duration_seconds)}")
    if stats.num_turns is not None:
        parts.append(f"Turns: {stats.num_turns}")
    if stats.cost_usd is not None:
        parts.append(f"Cost: ${stats.cost_usd:.4f}")
    if has_commits is not None:
        parts.append(f"Commits: {'yes' if has_commits else 'no'}")
    if parts:
        print(f"Stats: {' | '.join(parts)}")


def load_dotenv(project_dir: Path) -> None:
    """Load .env files from home directory and project directory.

    Home directory .env (~/.theo/.env) is loaded first, then project directory .env,
    so project-specific values override home directory values.
    """
    # Load from home directory first (~/.theo/.env)
    home_env = Path.home() / ".theo" / ".env"
    if home_env.exists():
        with open(home_env) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())

    # Load from project directory (overrides home directory values)
    project_env = project_dir / ".env"
    if project_env.exists():
        with open(project_env) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    # Use setdefault for home dir, but set directly for project to allow overrides
                    os.environ[key.strip()] = value.strip()


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


def generate_task_id(
    prompt: str,
    existing_id: str | None = None,
    log_path: Path | None = None,
    git: Git | None = None,
    project_name: str | None = None,
) -> str:
    """Generate a task ID in YYYYMMDD-slug format, with suffix for retries."""
    if existing_id:
        # This is a retry - strip any existing suffix to get base
        base_id = re.sub(r'-\d+$', '', existing_id)
    else:
        # Fresh task - generate base ID
        date_prefix = datetime.now().strftime("%Y%m%d")
        slug = slugify(prompt)
        base_id = f"{date_prefix}-{slug}"

    # Check if base ID is available
    if not _task_id_exists(base_id, log_path, git, project_name):
        return base_id

    # Find next available suffix
    suffix = 2
    new_id = f"{base_id}-{suffix}"
    while _task_id_exists(new_id, log_path, git, project_name):
        suffix += 1
        new_id = f"{base_id}-{suffix}"
    return new_id


def _task_id_exists(task_id: str, log_path: Path | None, git: Git | None, project_name: str | None) -> bool:
    """Check if a task_id is already in use (log file or branch exists)."""
    # Check log file
    if log_path and (log_path / f"{task_id}.log").exists():
        return True
    # Check branch
    if git and project_name:
        branch_name = f"{project_name}/{task_id}"
        exists = git.branch_exists(branch_name)
        if exists:
            return True
    return False


def build_prompt(task: Task, config: Config, store: SqliteTaskStore, report_path: Path | None = None) -> str:
    """Build the prompt for Claude."""
    base_prompt = f"Complete this task: {task.prompt}"

    # Add context from referenced parent task if task has based_on
    if task.based_on:
        parent_task = store.get(task.based_on)
        if parent_task and parent_task.report_file:
            base_prompt += f"\n\nThis task is based on the findings in: {parent_task.report_file}"
            base_prompt += "\nRead and review that report for context before implementing."
        elif parent_task:
            base_prompt += f"\n\nThis task is a follow-up to task #{parent_task.id}: {parent_task.prompt[:100]}"

    # For explore tasks, instruct to write findings to report file
    if task.is_explore() and report_path:
        base_prompt += f"""

This is an exploration/research task. Write your findings and recommendations to:
  {report_path}

Structure the report with clear sections and actionable recommendations."""
    else:
        base_prompt += "\n\nWhen you are done, report what you accomplished."

    return base_prompt


def _run_result_to_stats(result: RunResult) -> TaskStats:
    """Convert a provider RunResult to TaskStats for storage."""
    return TaskStats(
        duration_seconds=result.duration_seconds,
        num_turns=result.num_turns,
        cost_usd=result.cost_usd,
    )


def run(config: Config) -> int:
    """Run Theo on the next pending task.

    Uses git worktrees to isolate task execution from the main working directory.
    This allows concurrent work in the main checkout while theo runs.
    """
    load_dotenv(config.project_dir)

    # Get the configured provider
    provider = get_provider(config)

    if not provider.check_credentials():
        print(f"Error: No {provider.name} credentials found")
        return 1

    # Verify credentials work before proceeding
    print(f"Verifying {provider.name} credentials...")
    if not provider.verify_credentials(config):
        return 1
    print("Credentials verified ✓")

    # Load tasks from SQLite
    store = SqliteTaskStore(config.db_path)
    task = store.get_next_pending()

    if not task:
        print("No pending tasks found")
        return 0

    # Setup git on the main repo (for worktree operations)
    git = Git(config.project_dir)
    default_branch = git.default_branch()

    # Pull latest on default branch (without switching away from user's current branch)
    # We do this by fetching and then basing the worktree on origin/default_branch
    try:
        git._run("fetch", "origin", default_branch)
    except GitError:
        pass  # May fail if offline, continue anyway

    # Generate task_id - checks for collisions with existing branches/logs
    task.task_id = generate_task_id(
        task.prompt,
        existing_id=task.task_id,  # None for fresh tasks, set for retries
        log_path=config.log_path,
        git=git,
        project_name=config.project_name,
    )

    prompt_display = task.prompt[:80] + "..." if len(task.prompt) > 80 else task.prompt
    print(f"=== Task: {prompt_display} ===")
    print(f"    ID: {task.task_id}")
    if task.is_explore():
        print(f"    Type: explore (no code changes required)")

    # For explore tasks, run in project dir without creating a branch
    if task.is_explore():
        return _run_explore_task(task, config, store, provider)

    # Determine branch name based on branch_mode
    if config.branch_mode == "single":
        branch_name = f"{config.project_name}/theo-work"
    else:  # multi
        branch_name = f"{config.project_name}/{task.task_id}"

    # Create worktree path
    worktree_path = config.worktree_path / task.task_id

    # Delete existing branch if in single mode (worktree_add will recreate it)
    if config.branch_mode == "single" and git.branch_exists(branch_name):
        git._run("branch", "-D", branch_name, check=False)

    try:
        # Create worktree with new branch based on origin/default_branch (or local if fetch failed)
        base_ref = f"origin/{default_branch}"
        result = git._run("rev-parse", "--verify", base_ref, check=False)
        if result.returncode != 0:
            base_ref = default_branch  # Fall back to local branch

        print(f"Creating worktree: {worktree_path}")
        git.worktree_add(worktree_path, branch_name, base_ref)

        # Create a Git instance for the worktree
        worktree_git = Git(worktree_path)

        # Mark task in progress
        store.mark_in_progress(task)

        # Setup logging - use task_id for naming (logs stay in main project)
        config.log_path.mkdir(parents=True, exist_ok=True)
        log_file = config.log_path / f"{task.task_id}.log"

        # Run provider in the worktree
        prompt = build_prompt(task, config, store, report_path=None)
        result = provider.run(config, prompt, log_file, worktree_path)

        exit_code = result.exit_code
        stats = _run_result_to_stats(result)

        # Handle failures - check error_type first, then exit codes
        if result.error_type == "max_turns":
            print(f"Task failed: max turns of {config.max_turns} exceeded")
            print_stats(stats, has_commits=False)
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0
        elif exit_code == 124:
            print(f"Task failed: {provider.name} timed out after {config.timeout_minutes} minutes")
            print_stats(stats, has_commits=False)
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0
        elif exit_code != 0:
            print(f"Task failed: {provider.name} exited with code {exit_code}")
            print_stats(stats, has_commits=False)
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0

        # For regular tasks: require code changes
        if not worktree_git.has_changes("."):
            # Check exit code - if Claude succeeded but made no changes, that's a failure
            print("No changes made")
            print_stats(stats, has_commits=False)
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0

        # Commit changes in worktree
        worktree_git.add(".")
        worktree_git.commit(f"Theo: {task.prompt[:50]}\n\nTask ID: {task.task_id}")

        # Mark completed
        store.mark_completed(
            task,
            branch=branch_name,
            log_file=str(log_file.relative_to(config.project_dir)),
            has_commits=True,
            stats=stats,
        )

        print("")
        print("=== Done ===")
        print_stats(stats, has_commits=True)
        print(f"changes committed to branch: {branch_name}")
        print("")
        print("to view logs:")
        print(f"theo log {task.task_id}")
        print("")
        print("to review changes:")
        print(f"git diff {default_branch}...{branch_name} --")
        print("")
        print("to merge:")
        print(f"git merge --squash{branch_name}")

        _cleanup_worktree(git, worktree_path)

        return 0

    except GitError as e:
        print(f"Git error: {e}")
        _cleanup_worktree(git, worktree_path)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted")
        _cleanup_worktree(git, worktree_path)
        return 130


def _run_explore_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
) -> int:
    """Run an explore task in the project directory (no branch/worktree)."""
    # Mark task in progress
    store.mark_in_progress(task)

    # Setup logging
    config.log_path.mkdir(parents=True, exist_ok=True)
    log_file = config.log_path / f"{task.task_id}.log"

    # Setup report file
    report_dir = config.project_dir / DEFAULT_REPORT_DIR
    report_dir.mkdir(parents=True, exist_ok=True)
    report_filename = f"{task.task_id}.md"
    report_path = report_dir / report_filename
    report_file_relative = f"{DEFAULT_REPORT_DIR}/{report_filename}"

    # Run provider in the project directory
    prompt = build_prompt(task, config, store, report_path)
    try:
        result = provider.run(config, prompt, log_file, config.project_dir)
    except KeyboardInterrupt:
        print("\nInterrupted")
        return 130

    exit_code = result.exit_code
    stats = _run_result_to_stats(result)

    # Handle failures - check error_type first, then exit codes
    if result.error_type == "max_turns":
        print(f"Task failed: max turns of {config.max_turns} exceeded")
        print_stats(stats, has_commits=False)
        store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
        return 0
    elif exit_code == 124:
        print(f"Task failed: {provider.name} timed out after {config.timeout_minutes} minutes")
        print_stats(stats, has_commits=False)
        store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
        return 0
    elif exit_code != 0:
        print(f"Task failed: {provider.name} exited with code {exit_code}")
        print_stats(stats, has_commits=False)
        store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
        return 0

    # Check if report was created
    if report_path.exists():
        print(f"Report written to: {report_file_relative}")
    else:
        # Copy log to report if provider didn't create the report file
        print(f"Note: Report file not created, copying log output")
        with open(log_file) as lf:
            with open(report_path, 'w') as rf:
                rf.write(f"# Exploration: {task.prompt}\n\n")
                rf.write(lf.read())

    # Mark completed with report file reference (no branch, no commits)
    store.mark_completed(
        task,
        branch=None,
        log_file=str(log_file.relative_to(config.project_dir)),
        report_file=report_file_relative,
        has_commits=False,
        stats=stats,
    )

    print("")
    print("=== Exploration Complete ===")
    print_stats(stats, has_commits=False)
    print(f"Report: {report_file_relative}")
    print("")
    print("To implement based on this exploration, add a task with:")
    print(f"  theo add --based-on {task.id}")

    return 0


def _cleanup_worktree(git: Git, worktree_path: Path) -> None:
    """Clean up a worktree, ignoring errors."""
    try:
        git.worktree_remove(worktree_path, force=True)
    except GitError:
        pass
