"""Main Gza runner orchestration."""

import os
import re
from datetime import datetime
from pathlib import Path

from .config import APP_NAME, Config
from .db import SqliteTaskStore, Task, TaskStats
from .git import Git, GitError
from .providers import get_provider, Provider, RunResult


DEFAULT_REPORT_DIR = f".{APP_NAME}/explorations"
PLAN_DIR = f".{APP_NAME}/plans"
REVIEW_DIR = f".{APP_NAME}/reviews"


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

    Home directory .env (~/.{APP_NAME}/.env) is loaded first, then project directory .env,
    so project-specific values override home directory values.
    """
    # Load from home directory first (~/.{APP_NAME}/.env)
    home_env = Path.home() / f".{APP_NAME}" / ".env"
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


def build_prompt(task: Task, config: Config, store: SqliteTaskStore, report_path: Path | None = None, git: Git | None = None) -> str:
    """Build the prompt for Claude."""
    base_prompt = f"Complete this task: {task.prompt}"

    # Include spec file content if specified
    if task.spec:
        spec_path = config.project_dir / task.spec
        if spec_path.exists():
            spec_content = spec_path.read_text()
            base_prompt += f"\n\n## Specification\n\nThe following specification file ({task.spec}) provides context for this task:\n\n{spec_content}"

    # Add context from based_on chain (walk up the chain to find plan tasks)
    if task.based_on or task.task_type in ("implement", "review"):
        context = _build_context_from_chain(task, store, config.project_dir, git)
        if context:
            base_prompt += "\n\n" + context

    # Task type-specific instructions
    if task.task_type == "explore":
        if report_path:
            base_prompt += f"""

This is an exploration/research task. Write your findings and recommendations to:
  {report_path}

Structure the report with clear sections and actionable recommendations."""
    elif task.task_type == "plan":
        if report_path:
            base_prompt += f"""

This is a planning task. Write your design/architecture plan to:
  {report_path}

Structure the plan with clear sections covering:
- Overview of the approach
- Key design decisions
- Implementation steps
- Potential risks or considerations"""
    elif task.task_type == "review":
        # Check for REVIEW.md in project root for custom review guidelines
        review_md_path = config.project_dir / "REVIEW.md"
        if review_md_path.exists():
            review_guidelines = review_md_path.read_text()
            base_prompt += f"\n\n## Review Guidelines\n\n{review_guidelines}"

        if report_path:
            base_prompt += f"""

This is a review task. Write your review to:
  {report_path}

Your review should include:
- Assessment of whether the implementation matches the plan
- Code quality observations
- Potential issues or improvements
- Final verdict: APPROVED, CHANGES_REQUESTED, or NEEDS_DISCUSSION

End your review with a clear verdict line like:
Verdict: APPROVED"""
    else:
        base_prompt += "\n\nWhen you are done, report what you accomplished."

    return base_prompt


def _get_task_output(task: Task, project_dir: Path) -> str | None:
    """Get task output content, preferring DB over filesystem."""
    # Prefer DB content (works in distributed mode)
    if task.output_content:
        return task.output_content

    # Fall back to file (local mode, backward compat)
    if task.report_file:
        path = project_dir / task.report_file
        if path.exists():
            return path.read_text()

    return None


def _build_context_from_chain(task: Task, store: SqliteTaskStore, project_dir: Path, git: Git | None) -> str:
    """Build context by walking the depends_on and based_on chain."""
    context_parts = []

    # For implement tasks, include plan from based_on chain
    if task.task_type == "implement" and task.based_on:
        plan_task = _find_task_of_type_in_chain(task.based_on, "plan", store)
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
                # Get diff if we have a branch
                if impl_task.branch and git:
                    try:
                        default_branch = git.default_branch()
                        diff_stat = git.get_diff_stat(f"{default_branch}...{impl_task.branch}")
                        if diff_stat:
                            context_parts.append(f"Implementation branch: {impl_task.branch}")
                            context_parts.append(f"\nDiff summary:\n{diff_stat}")
                    except GitError:
                        pass  # Ignore git errors

                # Find plan task from impl_task's chain
                if impl_task.based_on:
                    plan_task = _find_task_of_type_in_chain(impl_task.based_on, "plan", store)
                    if plan_task:
                        plan_content = _get_task_output(plan_task, project_dir)
                        if plan_content:
                            context_parts.append("\n## Original plan:\n")
                            context_parts.append(plan_content)

    # Fallback for generic based_on references
    if task.based_on and not context_parts:
        parent_task = store.get(task.based_on)
        if parent_task and parent_task.report_file:
            context_parts.append(f"This task is based on the findings in: {parent_task.report_file}")
            context_parts.append("Read and review that report for context before implementing.")
        elif parent_task:
            context_parts.append(f"This task is a follow-up to task #{parent_task.id}: {parent_task.prompt[:100]}")

    return "\n".join(context_parts) if context_parts else ""


def _find_task_of_type_in_chain(task_id: int, task_type: str, store: SqliteTaskStore, visited: set[int] | None = None) -> Task | None:
    """Walk up the based_on chain to find a task of the given type."""
    if visited is None:
        visited = set()

    if task_id in visited:
        return None  # Avoid cycles
    visited.add(task_id)

    task = store.get(task_id)
    if not task:
        return None

    if task.task_type == task_type:
        return task

    if task.based_on:
        return _find_task_of_type_in_chain(task.based_on, task_type, store, visited)

    return None


def _run_result_to_stats(result: RunResult) -> TaskStats:
    """Convert a provider RunResult to TaskStats for storage."""
    return TaskStats(
        duration_seconds=result.duration_seconds,
        num_turns=result.num_turns,
        cost_usd=result.cost_usd,
    )


def _create_and_run_review_task(completed_task: Task, config: Config, store: SqliteTaskStore) -> int:
    """Create and immediately execute a review task for a completed implementation.

    Returns:
        Exit code from running the review task.
    """
    # Create review task
    review_prompt = f"Review the implementation from task #{completed_task.id}"
    if completed_task.prompt:
        review_prompt += f": {completed_task.prompt[:100]}"

    review_task = store.add(
        prompt=review_prompt,
        task_type="review",
        depends_on=completed_task.id,
        group=completed_task.group,
        based_on=completed_task.based_on,  # Inherit based_on to find plan
    )

    print(f"\n=== Auto-created review task #{review_task.id} ===")
    print(f"Running review task...")

    # Run the review task immediately
    return run(config, task_id=review_task.id)


def run(config: Config, task_id: int | None = None, resume: bool = False) -> int:
    """Run Gza on the next pending task or a specific task.

    Uses git worktrees to isolate task execution from the main working directory.
    This allows concurrent work in the main checkout while gza runs.

    Args:
        config: Configuration object
        task_id: Optional specific task ID to run. If None, runs next pending task.
        resume: If True, resume from previous session using stored session_id.
    """
    load_dotenv(config.project_dir)

    # Get the configured provider
    provider = get_provider(config)

    if not provider.check_credentials():
        home_env = Path.home() / f".{APP_NAME}" / ".env"
        print(f"Error: No {provider.name} credentials found")
        print(f"  Set ANTHROPIC_API_KEY in {home_env} or {config.project_dir}/.env")
        print("  Or run 'claude login' to authenticate via OAuth")
        return 1

    # Verify credentials work before proceeding
    print(f"Verifying {provider.name} credentials...")
    if not provider.verify_credentials(config):
        return 1
    print("Credentials verified ✓")

    # Load tasks from SQLite
    store = SqliteTaskStore(config.db_path)

    if task_id:
        task = store.get(task_id)
        if not task:
            print(f"Error: Task #{task_id} not found")
            return 1

        # Resume mode validation
        if resume:
            if task.status != "failed":
                print(f"Error: Can only resume failed tasks (task is {task.status})")
                return 1
            if not task.session_id:
                print(f"Error: Task #{task_id} has no session ID (cannot resume)")
                print("Use 'gza retry' to start fresh instead")
                return 1
            # Reset task to in_progress
            store.mark_in_progress(task)
        else:
            # Check if task is blocked by dependencies
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                print(f"Error: Task #{task_id} is blocked by task #{blocking_id} ({blocking_status})")
                return 1
    else:
        if resume:
            print("Error: Cannot resume without specifying a task ID")
            return 1
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
    print(f"    Type: {task.task_type}")

    # For explore, plan, and review tasks, run in project dir without creating a branch
    if task.task_type in ("explore", "plan", "review"):
        return _run_non_code_task(task, config, store, provider, git, resume=resume)

    # Determine branch name based on resume, same_branch, and branch_mode
    if resume and task.branch:
        # Resume uses the existing branch from the failed task
        branch_name = task.branch
        print(f"    Resuming on existing branch: {branch_name}")
    elif task.same_branch and task.depends_on:
        # Use the branch from the dependency task
        dep_task = store.get(task.depends_on)
        if dep_task and dep_task.branch:
            branch_name = dep_task.branch
            print(f"    Using existing branch from task #{dep_task.id}: {branch_name}")
        else:
            print(f"Error: Task #{task.id} has same_branch=True but dependency task has no branch")
            return 1
    elif config.branch_mode == "single":
        branch_name = f"{config.project_name}/gza-work"
    else:  # multi
        # Use branch naming strategy
        from gza.branch_naming import generate_branch_name
        branch_name = generate_branch_name(
            pattern=config.branch_strategy.pattern,
            project_name=config.project_name,
            task_id=task.task_id,
            prompt=task.prompt,
            default_type=config.branch_strategy.default_type,
            explicit_type=task.task_type_hint,
        )

    # Create worktree path
    worktree_path = config.worktree_path / task.task_id

    # Handle branch and worktree creation
    if resume or task.same_branch:
        # Validate branch exists before attempting to check it out
        if not git.branch_exists(branch_name):
            print(f"Error: Branch '{branch_name}' no longer exists. Cannot resume.")
            print("The branch may have been deleted or merged.")
            return 1

        # Check out existing branch in worktree
        try:
            # Remove existing worktree if it exists
            if worktree_path.exists():
                git.worktree_remove(worktree_path, force=True)

            print(f"Creating worktree with existing branch: {worktree_path}")
            # For existing branch, use git worktree add <path> <branch>
            git._run("worktree", "add", str(worktree_path), branch_name)
        except GitError as e:
            print(f"Error: Could not check out branch {branch_name} in worktree: {e}")
            return 1
    else:
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
        except GitError as e:
            print(f"Git error: {e}")
            return 1

    # Create a Git instance for the worktree
    worktree_git = Git(worktree_path)

    # Mark task in progress (unless resuming, in which case already set)
    if not resume:
        store.mark_in_progress(task)

    # Setup logging - use task_id for naming (logs stay in main project)
    config.log_path.mkdir(parents=True, exist_ok=True)
    log_file = config.log_path / f"{task.task_id}.log"

    # Run provider in the worktree
    if resume:
        prompt = "Continue from where you left off. The task was interrupted due to max_turns limit."
    else:
        prompt = build_prompt(task, config, store, report_path=None, git=git)

    try:
        result = provider.run(config, prompt, log_file, worktree_path, resume_session_id=task.session_id if resume else None)

        exit_code = result.exit_code
        stats = _run_result_to_stats(result)

        # Store session_id if available
        if result.session_id:
            task.session_id = result.session_id
            store.update(task)

        # Handle failures - check error_type first, then exit codes
        if result.error_type == "max_turns":
            print(f"Task failed: max turns of {config.max_turns} exceeded")
            print_stats(stats, has_commits=False)
            print(f"Task ID: {task.id}")
            print("")
            print("Next steps:")
            print(f"  gza retry {task.id}           # retry from scratch")
            print(f"  gza resume {task.id}          # resume from where it left off")
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0
        elif exit_code == 124:
            print(f"Task failed: {provider.name} timed out after {config.timeout_minutes} minutes")
            print_stats(stats, has_commits=False)
            print(f"Task ID: {task.id}")
            print("")
            print("Next steps:")
            print(f"  gza retry {task.id}           # retry from scratch")
            print(f"  gza resume {task.id}          # resume from where it left off")
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0
        elif exit_code != 0:
            print(f"Task failed: {provider.name} exited with code {exit_code}")
            print_stats(stats, has_commits=False)
            print(f"Task ID: {task.id}")
            print("")
            print("Next steps:")
            print(f"  gza retry {task.id}           # retry from scratch")
            print(f"  gza resume {task.id}          # resume from where it left off")
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0

        # For regular tasks: require code changes
        if not worktree_git.has_changes("."):
            # Check exit code - if Claude succeeded but made no changes, that's a failure
            print("No changes made")
            print_stats(stats, has_commits=False)
            print(f"Task ID: {task.id}")
            print("")
            print("Next steps:")
            print(f"  gza retry {task.id}           # retry from scratch")
            print(f"  gza resume {task.id}          # resume from where it left off")
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0

        # Commit changes in worktree
        worktree_git.add(".")
        worktree_git.commit(f"Gza: {task.prompt[:50]}\n\nTask ID: {task.task_id}")

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
        print(f"Task ID: {task.id}")
        print(f"Branch: {branch_name}")
        print("")
        print("Next steps:")
        print(f"  gza merge {task.id}           # merge branch for task")
        print(f"  gza pr {task.id}              # create a PR")
        print(f"  gza retry {task.id}           # retry from scratch")
        print(f"  gza resume {task.id}          # resume from where it left off")
        print("")
        print("To review changes:")
        print(f"  git diff {default_branch}...{branch_name} --")
        print("")
        print("To merge:")
        print(f"  git merge --squash {branch_name}")

        _cleanup_worktree(git, worktree_path)

        # Auto-create and run review task if requested
        if task.create_review:
            return _create_and_run_review_task(task, config, store)

        return 0

    except GitError as e:
        print(f"Git error: {e}")
        _cleanup_worktree(git, worktree_path)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted")
        _cleanup_worktree(git, worktree_path)
        return 130


def _run_non_code_task(
    task: Task,
    config: Config,
    store: SqliteTaskStore,
    provider: Provider,
    git: Git | None = None,
    resume: bool = False,
) -> int:
    """Run a non-code task (explore, plan, review) in a worktree (no branch creation)."""
    if resume:
        print(f"    Resuming with session: {task.session_id[:12]}...")

    # Mark task in progress
    store.mark_in_progress(task)

    # Setup logging
    config.log_path.mkdir(parents=True, exist_ok=True)
    log_file = config.log_path / f"{task.task_id}.log"

    # Setup report file based on task type
    if task.task_type == "explore":
        report_dir = config.project_dir / DEFAULT_REPORT_DIR
        task_type_display = "Exploration"
    elif task.task_type == "plan":
        report_dir = config.project_dir / PLAN_DIR
        task_type_display = "Plan"
    elif task.task_type == "review":
        report_dir = config.project_dir / REVIEW_DIR
        task_type_display = "Review"
    else:
        report_dir = config.project_dir / DEFAULT_REPORT_DIR
        task_type_display = "Report"

    report_dir.mkdir(parents=True, exist_ok=True)
    report_filename = f"{task.task_id}.md"
    report_path = report_dir / report_filename
    report_file_relative = str(report_path.relative_to(config.project_dir))

    # Create worktree in /tmp for Docker compatibility on macOS
    worktree_path = config.worktree_path / f"{task.task_id}-{task.task_type}"

    try:
        # Get default branch to base worktree on
        default_branch = git.default_branch() if git else "main"

        # Remove existing worktree if it exists
        if worktree_path.exists():
            git.worktree_remove(worktree_path, force=True)

        # For review tasks with depends_on, check if we should run on the implementation branch
        base_ref = None
        if task.task_type == "review" and task.depends_on:
            dep_task = store.get(task.depends_on)
            if dep_task and dep_task.branch and dep_task.status == "completed":
                # Run review on the implementation branch
                base_ref = dep_task.branch
                print(f"Running review on implementation branch: {base_ref}")

        # Default to origin/default_branch or local default_branch
        if not base_ref:
            base_ref = f"origin/{default_branch}"
            result = git._run("rev-parse", "--verify", base_ref, check=False)
            if result.returncode != 0:
                base_ref = default_branch  # Fall back to local branch

        # Create worktree without creating a new branch (use --detach to check out HEAD)
        # This creates a worktree in detached HEAD state based on the specified ref
        print(f"Creating worktree: {worktree_path}")
        git._run("worktree", "add", "--detach", str(worktree_path), base_ref)

        # Create report directory structure in worktree
        worktree_report_dir = worktree_path / report_dir.relative_to(config.project_dir)
        worktree_report_dir.mkdir(parents=True, exist_ok=True)
        worktree_report_path = worktree_path / report_path.relative_to(config.project_dir)

        # For Docker containers, use /workspace-relative path instead of host worktree path
        # The container only has /workspace mounted, so we need to use a path inside that
        container_report_path = Path("/workspace") / report_path.relative_to(config.project_dir)

        # Run provider in the worktree
        prompt = build_prompt(task, config, store, container_report_path, git)
        resume_session_id = task.session_id if resume else None
        try:
            result = provider.run(config, prompt, log_file, worktree_path, resume_session_id=resume_session_id)
        except KeyboardInterrupt:
            print("\nInterrupted")
            _cleanup_worktree(git, worktree_path)
            return 130

        exit_code = result.exit_code
        stats = _run_result_to_stats(result)

        # Store session_id if available
        if result.session_id:
            task.session_id = result.session_id
            store.update(task)

        # Handle failures - check error_type first, then exit codes
        if result.error_type == "max_turns":
            print(f"Task failed: max turns of {config.max_turns} exceeded")
            print_stats(stats, has_commits=False)
            print(f"Task ID: {task.id}")
            print("")
            print("Next steps:")
            print(f"  gza retry {task.id}           # retry from scratch")
            print(f"  gza resume {task.id}          # resume from where it left off")
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0
        elif exit_code == 124:
            print(f"Task failed: {provider.name} timed out after {config.timeout_minutes} minutes")
            print_stats(stats, has_commits=False)
            print(f"Task ID: {task.id}")
            print("")
            print("Next steps:")
            print(f"  gza retry {task.id}           # retry from scratch")
            print(f"  gza resume {task.id}          # resume from where it left off")
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0
        elif exit_code != 0:
            print(f"Task failed: {provider.name} exited with code {exit_code}")
            print_stats(stats, has_commits=False)
            print(f"Task ID: {task.id}")
            print("")
            print("Next steps:")
            print(f"  gza retry {task.id}           # retry from scratch")
            print(f"  gza resume {task.id}          # resume from where it left off")
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats)
            _cleanup_worktree(git, worktree_path)
            return 0

        # Copy report file from worktree to main project directory
        if worktree_report_path.exists():
            print(f"Report written to: {report_file_relative}")
            # Ensure target directory exists
            report_dir.mkdir(parents=True, exist_ok=True)
            # Copy report content from worktree to project dir
            report_path.write_text(worktree_report_path.read_text())
        else:
            # Report file was not created - task likely failed to write output
            print(f"Warning: Report file not created by provider")
            print(f"See log file for details: {log_file.relative_to(config.project_dir)}")

        # Read output content for storage in DB
        output_content = None
        if report_path.exists():
            output_content = report_path.read_text()

        # Mark completed with report file reference (no branch, no commits)
        store.mark_completed(
            task,
            branch=None,
            log_file=str(log_file.relative_to(config.project_dir)),
            report_file=report_file_relative,
            output_content=output_content,
            has_commits=False,
            stats=stats,
        )

        print("")
        print(f"=== {task_type_display} Complete ===")
        print_stats(stats, has_commits=False)
        print(f"Task ID: {task.id}")
        print(f"Report: {report_file_relative}")
        print("")

        if task.task_type == "explore":
            print("To implement based on this exploration, add a task with:")
            print(f"  gza add --based-on {task.id}")
        elif task.task_type == "plan":
            print("To implement this plan, add a task with:")
            print(f"  gza add --type implement --based-on {task.id}")

        print("")
        print("Next steps:")
        print(f"  gza retry {task.id}           # retry from scratch")
        print(f"  gza resume {task.id}          # resume from where it left off")

        # Cleanup worktree
        _cleanup_worktree(git, worktree_path)
        return 0

    except GitError as e:
        print(f"Git error: {e}")
        _cleanup_worktree(git, worktree_path)
        return 1


def _cleanup_worktree(git: Git, worktree_path: Path) -> None:
    """Clean up a worktree, ignoring errors."""
    try:
        git.worktree_remove(worktree_path, force=True)
    except GitError:
        pass
