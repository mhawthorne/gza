"""Main Gza runner orchestration."""

import os
import re
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .config import APP_NAME, Config
from .console import console, task_header, stats_line, success_message, error_message, info_line, next_steps
from .db import SqliteTaskStore, Task, TaskStats, extract_failure_reason
from .git import Git, GitError, cleanup_worktree_for_branch, parse_diff_numstat
from .github import GitHub, GitHubError
from .learnings import maybe_auto_regenerate_learnings
from .prompts import PromptBuilder
from .providers import get_provider, Provider, RunResult


def get_effective_config_for_task(task: Task, config: Config) -> tuple[str | None, str, int]:
    """Get the effective model, provider, and max_steps for a task.

    Priority order for provider selection:
    1. Task-specific provider (task.provider)
    2. Config default (config.provider, already env-merged in Config.load)

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
    provider = task.provider if task.provider else config.provider
    model = task.model if task.model else config.get_model_for_task(task.task_type, provider)
    max_steps = config.get_max_steps_for_task(task.task_type, provider)
    return model, provider, max_steps


DEFAULT_REPORT_DIR = f".{APP_NAME}/explorations"
PLAN_DIR = f".{APP_NAME}/plans"
REVIEW_DIR = f".{APP_NAME}/reviews"
SUMMARY_DIR = f".{APP_NAME}/summaries"
WIP_DIR = f".{APP_NAME}/wip"
BACKUP_DIR = f".{APP_NAME}/backups"

# Diff size thresholds for tiered diff strategy in review prompts
DIFF_SMALL_THRESHOLD = 500   # lines: pass verbatim
DIFF_MEDIUM_THRESHOLD = 2000  # lines: prepend --stat summary above full diff
REVIEW_CONTEXT_FILE_LIMIT = 12


def _extract_review_verdict(content: str | None) -> str | None:
    """Extract review verdict from markdown content."""
    if not content:
        return None
    match = re.search(
        r"\*{0,2}Verdict:\s*(APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)\*{0,2}",
        content,
        re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).upper()


def backup_database(db_path: Path, project_dir: Path) -> None:
    """Create an hourly backup of the SQLite database if one doesn't exist yet.

    Checks if a backup for the current hour already exists. If not, creates
    a timestamped backup using SQLite's backup API (safe for concurrent access).

    Backup filename format: gza-YYYYMMDDHH.db (e.g., gza-2026021414.db)

    Args:
        db_path: Path to the source SQLite database
        project_dir: Project directory (used to locate the backups folder)
    """
    if not db_path.exists():
        return

    backup_dir = project_dir / BACKUP_DIR
    hour_stamp = datetime.now().strftime("%Y%m%d%H")
    backup_path = backup_dir / f"gza-{hour_stamp}.db"

    if backup_path.exists():
        return

    backup_dir.mkdir(parents=True, exist_ok=True)

    source = sqlite3.connect(str(db_path))
    try:
        dest = sqlite3.connect(str(backup_path))
        try:
            source.backup(dest)
        finally:
            dest.close()
    finally:
        source.close()


# format_duration logic is now in console.stats_line
# stats_line function is now imported from console module


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


def build_prompt(task: Task, config: Config, store: SqliteTaskStore, report_path: Path | None = None, summary_path: Path | None = None, git: Git | None = None) -> str:
    """Build the prompt for Claude."""
    return PromptBuilder().build(task, config, store, report_path=report_path, summary_path=summary_path, git=git)


def _get_task_output(task: Task, project_dir: Path) -> str | None:
    """Get task output content, preferring DB over filesystem.

    Auto-sync: If report_file exists and is newer than completed_at,
    read from disk instead of DB (allows users to edit plans).
    """
    # Check if file has been modified after task completion
    if task.report_file and task.completed_at:
        path = project_dir / task.report_file
        if path.exists():
            file_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
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

    return None


def _parse_changed_files_from_numstat(numstat_output: str) -> list[str]:
    """Extract changed file paths from git diff --numstat output."""
    changed_files: list[str] = []
    for line in numstat_output.splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        changed_files.append(parts[2].strip())
    return changed_files


def _build_review_diff_context(git: Git, revision_range: str, branch_name: str) -> str:
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

    if total_lines < DIFF_SMALL_THRESHOLD:
        diff_content = git.get_diff(revision_range)
        if not isinstance(diff_content, str):
            diff_content = ""
        if diff_content:
            parts.append("")
            parts.append("Full diff:")
            parts.append(diff_content)
        return "\n".join(parts)

    if total_lines < DIFF_MEDIUM_THRESHOLD:
        diff_content = git.get_diff(revision_range)
        if not isinstance(diff_content, str):
            diff_content = ""
        if diff_content:
            parts.append("")
            parts.append("Full diff:")
            parts.append(diff_content)
        return "\n".join(parts)

    # Large diff: include targeted per-file diff excerpts for the most relevant files.
    selected_files = changed_files[:REVIEW_CONTEXT_FILE_LIMIT]
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


def _build_context_from_chain(task: Task, store: SqliteTaskStore, project_dir: Path, git: Git | None) -> str:
    """Build context by walking the depends_on and based_on chain."""
    context_parts = []

    # For improve tasks, include review feedback and original plan
    if task.task_type == "improve":
        # Get the review we're addressing
        if task.depends_on:
            review_task = store.get(task.depends_on)
            if review_task and review_task.task_type == "review":
                review_content = _get_task_output(review_task, project_dir)
                if review_content:
                    context_parts.append("## Review feedback to address:\n")
                    context_parts.append(review_content)

        # Get the original plan (via based_on chain)
        if task.based_on:
            impl_task = store.get(task.based_on)
            if impl_task and impl_task.based_on:
                plan_task = _find_task_of_type_in_chain(impl_task.based_on, "plan", store)
                if plan_task:
                    plan_content = _get_task_output(plan_task, project_dir)
                    if plan_content:
                        context_parts.append("\n## Original plan:\n")
                        context_parts.append(plan_content)

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
                # Include spec file content if the implementation task has a spec field
                if impl_task.spec:
                    spec_path = project_dir / impl_task.spec
                    if spec_path.exists():
                        spec_content = spec_path.read_text()
                        context_parts.append(f"## Specification\n\nThe following specification file ({impl_task.spec}) provides context for this implementation:\n\n{spec_content}")

                # Get diff if we have a branch (tiered strategy based on diff size)
                if impl_task.branch and git:
                    try:
                        default_branch = git.default_branch()
                        revision_range = f"{default_branch}...{impl_task.branch}"
                        context_parts.append(
                            _build_review_diff_context(git, revision_range, impl_task.branch)
                        )
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
        num_steps_reported=result.num_steps_reported,
        num_steps_computed=result.num_steps_computed,
        num_turns_reported=result.num_turns_reported,
        num_turns_computed=result.num_turns_computed,
        cost_usd=result.cost_usd,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
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

    # Get the diff for backup
    worktree_git.add(".")
    diff = worktree_git._run("diff", "--cached", check=False).stdout

    # Save diff to backup file
    if task.task_id and diff:
        wip_file = wip_dir / f"{task.task_id}.diff"
        wip_file.write_text(diff)
        console.print(f"[yellow]Saved WIP diff to: {wip_file.relative_to(config.project_dir)}[/yellow]")

    # Commit changes with --no-verify
    try:
        worktree_git._run("commit", "--no-verify", "-m", f"WIP: gza task interrupted\n\nTask ID: {task.task_id}")
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
    if not task.task_id and not original_task_id:
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
    for candidate_id in filter(None, [original_task_id, task.task_id]):
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
                    worktree_git._run("commit", "--no-verify", "-m", f"WIP: restored from diff\n\nTask ID: {task.task_id}")
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

    # Find PR number
    pr_number = None

    # Try cached pr_number first
    if impl_task.pr_number:
        pr_number = impl_task.pr_number
        print(f"Found PR #{pr_number} (cached)")
    elif impl_task.branch:
        # Try to discover PR via branch
        pr_number = gh.get_pr_number(impl_task.branch)
        if pr_number:
            print(f"Found PR #{pr_number} for branch {impl_task.branch}")
            # Cache it for future use
            impl_task.pr_number = pr_number
            store.update(impl_task)

    if not pr_number:
        if required:
            print(f"Error: No PR found for task #{impl_task.id}")
            if impl_task.branch:
                print(f"Branch '{impl_task.branch}' has no associated PR")
            else:
                print("Task has no branch")
            return
        else:
            print(f"Info: No PR found for task #{impl_task.id}, skipping PR comment")
            return

    # Get review content
    review_content = _get_task_output(review_task, project_dir)
    if not review_content:
        print(f"Warning: Review task #{review_task.id} has no output content")
        return

    # Format as PR comment
    comment_body = f"""## 🤖 Automated Code Review

**Review Task**: #{review_task.id}
**Implementation Task**: #{impl_task.id}

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


def _create_and_run_review_task(completed_task: Task, config: Config, store: SqliteTaskStore) -> int:
    """Create and immediately execute a review task for a completed implementation.

    Returns:
        Exit code from running the review task.
    """
    # Create review task with slug derived from implementation task
    # Extract slug from the completed task's task_id (format: YYYYMMDD-slug or YYYYMMDD-slug-N)
    if completed_task.task_id:
        # Remove date prefix (YYYYMMDD-) and any retry suffix (-N)
        parts = completed_task.task_id.split('-', 1)
        if len(parts) == 2:
            slug = parts[1]  # Everything after the date
            # Remove retry suffix if present
            slug = re.sub(r'-\d+$', '', slug)
            review_prompt = f"review {slug}"
        else:
            # Fallback if task_id format is unexpected
            review_prompt = f"Review the implementation from task #{completed_task.id}"
    else:
        # Fallback if task_id is not set
        review_prompt = f"Review the implementation from task #{completed_task.id}"

    if not review_prompt.startswith("review ") and completed_task.prompt:
        review_prompt += f": {completed_task.prompt[:100]}"

    review_task = store.add(
        prompt=review_prompt,
        task_type="review",
        depends_on=completed_task.id,
        group=completed_task.group,
        based_on=completed_task.based_on,  # Inherit based_on to find plan
    )

    console.print(f"\n[bold cyan]=== Auto-created review task #{review_task.id} ===[/bold cyan]")
    console.print(f"Running review task...")

    # Run the review task immediately
    # Note: PR posting happens in _run_non_code_task, no need to do it here
    return run(config, task_id=review_task.id)


def _hide_invalid_worktree_git_metadata_for_docker(task: Task, config: Config, worktree_path: Path) -> Path | None:
    """Temporarily hide invalid worktree .git metadata for Docker review runs.

    Worktree .git files can reference host-only absolute gitdir paths that do not
    exist inside the container. Hiding the file prevents provider-side git probes
    from failing with path-mismatch errors.

    Returns:
        Backup path of the hidden .git file if one was moved, otherwise None.
    """
    if not config.use_docker or task.task_type != "review":
        return None

    git_file = worktree_path / ".git"
    if not git_file.is_file():
        return None

    try:
        first_line = git_file.read_text().splitlines()[0].strip()
    except (OSError, IndexError):
        return None

    if not first_line.startswith("gitdir:"):
        return None

    gitdir_path = Path(first_line.split(":", 1)[1].strip())
    if gitdir_path.exists():
        return None

    backup_path = worktree_path / ".git.gza-host-worktree"
    if backup_path.exists():
        if backup_path.is_dir():
            shutil.rmtree(backup_path)
        else:
            backup_path.unlink()

    git_file.rename(backup_path)
    return backup_path


def _restore_worktree_git_metadata(worktree_path: Path, backup_path: Path | None) -> None:
    """Restore original worktree .git metadata after provider execution."""
    if not backup_path or not backup_path.exists():
        return

    git_path = worktree_path / ".git"
    if git_path.exists():
        if git_path.is_dir():
            shutil.rmtree(git_path)
        else:
            git_path.unlink()

    backup_path.rename(git_path)


def run(config: Config, task_id: int | None = None, resume: bool = False, open_after: bool = False) -> int:
    """Run Gza on the next pending task or a specific task.

    Uses git worktrees to isolate task execution from the main working directory.
    This allows concurrent work in the main checkout while gza runs.

    Args:
        config: Configuration object
        task_id: Optional specific task ID to run. If None, runs next pending task.
        resume: If True, resume from previous session using stored session_id.
        open_after: If True, open the report file in $EDITOR after completion (for review tasks).
    """
    load_dotenv(config.project_dir)

    # Create hourly backup before running
    backup_database(config.db_path, config.project_dir)

    # Load tasks from SQLite
    store = SqliteTaskStore(config.db_path)

    if task_id:
        task = store.get(task_id)
        if not task:
            error_message(f"Error: Task #{task_id} not found")
            return 1

        # Resume mode validation
        if resume:
            if task.status not in ("failed", "pending"):
                error_message(f"Error: Can only resume failed tasks (task is {task.status})")
                return 1
            if not task.session_id:
                error_message(f"Error: Task #{task_id} has no session ID (cannot resume)")
                console.print("Use 'gza retry' to start fresh instead")
                return 1
            # Mark task as in_progress
            store.mark_in_progress(task)
        else:
            # Check if task is blocked by dependencies
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                error_message(f"Error: Task #{task_id} is blocked by task #{blocking_id} ({blocking_status})")
                return 1
    else:
        if resume:
            error_message("Error: Cannot resume without specifying a task ID")
            return 1
        task = store.get_next_pending()

    if not task:
        console.print("No pending tasks found")
        return 0

    # Get effective model and provider for this task
    effective_model, effective_provider, effective_max_steps = get_effective_config_for_task(task, config)

    # Create a modified config with task-specific settings
    from copy import copy
    task_config = copy(config)
    task_config.model = effective_model or ""
    task_config.provider = effective_provider
    task_config.max_steps = effective_max_steps
    task_config.max_turns = effective_max_steps

    # Get the provider for this task
    provider = get_provider(task_config)

    if not provider.check_credentials():
        error_message(f"Error: No {provider.name} credentials found")
        console.print(f"  {provider.credential_setup_hint}")
        return 1

    # Verify credentials work before proceeding
    console.print(f"Verifying {provider.name} credentials...")
    if not provider.verify_credentials(task_config):
        return 1
    console.print("[green]Credentials verified ✓[/green]")

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
    # Always generate when task_id is not set (new tasks, including new resume tasks).
    # Keep existing task_id only when resuming a task that already has one assigned.
    if task.task_id is None:
        task.task_id = generate_task_id(
            task.prompt,
            existing_id=None,
            log_path=config.log_path,
            git=git,
            project_name=config.project_name,
        )

    task_header(task.prompt, task.task_id or "", task.task_type)

    # For explore, plan, and review tasks, run in project dir without creating a branch
    if task.task_type in ("explore", "plan", "review"):
        return _run_non_code_task(task, task_config, store, provider, git, resume=resume, open_after=open_after)

    # Determine branch name based on resume, same_branch, and branch_mode
    if resume and task.branch:
        # Resume uses the existing branch from the failed task
        branch_name = task.branch
        console.print(f"    Resuming on existing branch: [blue]{branch_name}[/blue]")
    elif resume:
        # Resume but branch wasn't saved - derive from task_id using branch naming strategy
        from gza.branch_naming import generate_branch_name
        assert config.branch_strategy is not None
        assert task.task_id is not None
        branch_name = generate_branch_name(
            pattern=config.branch_strategy.pattern,
            project_name=config.project_name,
            task_id=task.task_id,
            prompt=task.prompt,
            default_type=config.branch_strategy.default_type,
            explicit_type=task.task_type_hint,
        )
        console.print(f"    Resuming on branch: [blue]{branch_name}[/blue]")
    elif task.same_branch:
        # Use the branch from based_on task (for improve tasks) or depends_on task (fallback)
        source_task = None
        if task.based_on:
            source_task = store.get(task.based_on)
        elif task.depends_on:
            source_task = store.get(task.depends_on)

        if source_task and source_task.branch:
            branch_name = source_task.branch
            console.print(f"    Using existing branch from task #{source_task.id}: [blue]{branch_name}[/blue]")
        else:
            error_message(f"Error: Task #{task.id} has same_branch=True but source task has no branch")
            return 1
    elif config.branch_mode == "single":
        branch_name = f"{config.project_name}/gza-work"
    else:  # multi
        # Use branch naming strategy
        from gza.branch_naming import generate_branch_name
        assert config.branch_strategy is not None
        assert task.task_id is not None
        branch_name = generate_branch_name(
            pattern=config.branch_strategy.pattern,
            project_name=config.project_name,
            task_id=task.task_id,
            prompt=task.prompt,
            default_type=config.branch_strategy.default_type,
            explicit_type=task.task_type_hint,
        )

    # Create worktree path
    assert task.task_id is not None
    worktree_path = config.worktree_path / task.task_id

    # Handle branch and worktree creation
    if resume or task.same_branch:
        # Validate branch exists before attempting to check it out
        if not git.branch_exists(branch_name):
            error_message(f"Error: Branch '{branch_name}' no longer exists. Cannot resume.")
            console.print("The branch may have been deleted or merged.")
            return 1

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
        except GitError as e:
            error_message(f"Error: Could not check out branch {branch_name} in worktree: {e}")
            return 1
    else:
        # Delete existing branch if in single mode (worktree_add will recreate it)
        if config.branch_mode == "single" and git.branch_exists(branch_name):
            git._run("branch", "-D", branch_name, check=False)

        try:
            # Create worktree with new branch based on the most up-to-date ref
            # Compare local main vs origin/main and use whichever is ahead
            base_ref = default_branch
            origin_ref = f"origin/{default_branch}"

            # Check if origin ref exists
            origin_exists = git._run("rev-parse", "--verify", origin_ref, check=False).returncode == 0

            if origin_exists:
                # Compare local vs origin - use whichever is ahead
                local_ahead = git.count_commits_ahead(default_branch, origin_ref)
                origin_ahead = git.count_commits_ahead(origin_ref, default_branch)

                if origin_ahead > 0 and local_ahead == 0:
                    # Origin is strictly ahead, use it
                    base_ref = origin_ref
                elif local_ahead > 0 and origin_ahead == 0:
                    # Local is strictly ahead, use it
                    base_ref = default_branch
                elif local_ahead > 0 and origin_ahead > 0:
                    # Diverged - prefer local to include unpushed changes
                    base_ref = default_branch
                else:
                    # Same commit, use either (default to local)
                    base_ref = default_branch

            console.print(f"Creating worktree: {worktree_path}")
            git.worktree_add(worktree_path, branch_name, base_ref)
        except GitError as e:
            error_message(f"Git error: {e}")
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
                original_task_id = original_task.task_id
        _restore_wip_changes(task, worktree_git, config, branch_name, original_task_id=original_task_id)

    # Mark task in progress (unless resuming, in which case already set)
    if not resume:
        store.mark_in_progress(task)

    # Setup logging - use task_id for naming (logs stay in main project)
    config.log_path.mkdir(parents=True, exist_ok=True)
    log_file = config.log_path / f"{task.task_id}.log"

    # Setup summary directory and path for task/implement types
    summary_dir = config.project_dir / SUMMARY_DIR
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_filename = f"{task.task_id}.md"
    summary_path = summary_dir / summary_filename
    summary_file_relative = str(summary_path.relative_to(config.project_dir))

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

    # Run provider in the worktree
    if resume:
        prompt = PromptBuilder().resume_prompt()
    else:
        prompt = build_prompt(task, config, store, report_path=None, summary_path=prompt_summary_path, git=git)

    try:
        result = provider.run(task_config, prompt, log_file, worktree_path, resume_session_id=task.session_id if resume else None)

        exit_code = result.exit_code
        stats = _run_result_to_stats(result)

        # Store session_id if available
        if result.session_id:
            task.session_id = result.session_id
            store.update(task)

        # Handle failures - check error_type first, then exit codes
        if result.error_type in ("max_turns", "max_steps"):
            # Save WIP changes before marking failed
            _save_wip_changes(task, worktree_git, config, branch_name)
            error_message(f"Task failed: max steps of {task_config.max_steps} exceeded")
            stats_line(stats, has_commits=False)
            console.print(f"Task ID: {task.id}")
            next_steps([
                (f"gza retry {task.id}", "retry from scratch"),
                (f"gza resume {task.id}", "resume from where it left off"),
            ])
            # Check log for agent-written marker; prefer MAX_STEPS for provider-detected over-budget failures.
            detected = extract_failure_reason(log_file)
            failure_reason = detected if detected != "UNKNOWN" else "MAX_STEPS"
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, branch=branch_name, failure_reason=failure_reason)
            return 0
        elif exit_code == 124:
            # Save WIP changes before marking failed
            _save_wip_changes(task, worktree_git, config, branch_name)
            error_message(f"Task failed: {provider.name} timed out after {config.timeout_minutes} minutes")
            stats_line(stats, has_commits=False)
            console.print(f"Task ID: {task.id}")
            next_steps([
                (f"gza retry {task.id}", "retry from scratch"),
                (f"gza resume {task.id}", "resume from where it left off"),
            ])
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, branch=branch_name, failure_reason=extract_failure_reason(log_file))
            return 0
        elif exit_code != 0:
            # Save WIP changes before marking failed
            _save_wip_changes(task, worktree_git, config, branch_name)
            error_message(f"Task failed: {provider.name} exited with code {exit_code}")
            stats_line(stats, has_commits=False)
            console.print(f"Task ID: {task.id}")
            next_steps([
                (f"gza retry {task.id}", "retry from scratch"),
                (f"gza resume {task.id}", "resume from where it left off"),
            ])
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, branch=branch_name, failure_reason=extract_failure_reason(log_file))
            return 0

        # For regular tasks: require code changes
        has_uncommitted = worktree_git.has_changes(".")
        if not has_uncommitted:
            # Check if branch already has commits from a previous run
            default_branch = worktree_git.default_branch()
            commits_ahead = worktree_git.count_commits_ahead(branch_name, default_branch)
            if commits_ahead == 0:
                # No uncommitted changes and no commits on branch - real failure
                # Note: No need to save WIP here since there are no changes
                error_message("No changes made")
                stats_line(stats, has_commits=False)
                console.print(f"Task ID: {task.id}")
                next_steps([
                    (f"gza retry {task.id}", "retry from scratch"),
                    (f"gza resume {task.id}", "resume from where it left off"),
                ])
                store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, branch=branch_name, failure_reason=extract_failure_reason(log_file))
                return 0
            # else: branch has commits from a previous run - treat as success without committing

        if has_uncommitted:
            # Squash any WIP commits before creating final commit
            _squash_wip_commits(worktree_git, task)

            # Commit changes in worktree
            worktree_git.add(".")

            # Build commit message with trailer for improve tasks
            commit_message = f"Gza: {task.prompt[:50]}\n\nTask ID: {task.task_id}"

            # Add review trailer for improve tasks
            if task.task_type == "improve" and task.depends_on:
                review_task = store.get(task.depends_on)
                if review_task and review_task.task_type == "review":
                    commit_message += f"\nGza-Review: #{review_task.id}"

            worktree_git.commit(commit_message)

        # Copy summary file from worktree to main project directory
        output_content = None
        if worktree_summary_path.exists():
            # Ensure target directory exists
            summary_dir.mkdir(parents=True, exist_ok=True)
            # Copy summary content from worktree to project dir
            summary_path.write_text(worktree_summary_path.read_text())
            output_content = summary_path.read_text()

        # Compute diff stats vs. default branch before marking completed
        default_branch = worktree_git.default_branch()
        numstat_output = worktree_git.get_diff_numstat(f"{default_branch}...{branch_name}")
        diff_files, diff_added, diff_removed = parse_diff_numstat(numstat_output)

        # Mark completed
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
        )
        auto_learnings = maybe_auto_regenerate_learnings(store, config)

        # Clear review state on the based_on implementation task after improve completes.
        # The improve task has addressed the review feedback, so the old review no longer
        # reflects the current code state.
        if task.task_type == "improve" and task.based_on:
            store.clear_review_state(task.based_on)

        console.print("")
        success_message("Done")
        stats_line(stats, has_commits=True)
        console.print(f"Task ID: {task.id}")
        console.print(f"Branch: [blue]{branch_name}[/blue]")
        next_steps([
            (f"gza merge {task.id}", "merge branch for task"),
            (f"gza pr {task.id}", "create a PR"),
            (f"gza retry {task.id}", "retry from scratch"),
            (f"gza resume {task.id}", "resume from where it left off"),
        ])
        if auto_learnings:
            info_line(
                f"Updated learnings from {auto_learnings.tasks_used} tasks "
                f"({auto_learnings.path.relative_to(config.project_dir)})"
            )
        console.print("")
        console.print("To review changes:")
        console.print(f"  [cyan]git diff {default_branch}...{branch_name} --[/cyan]")
        console.print("")
        console.print("To merge:")
        console.print(f"  [cyan]gza merge {task.id}[/cyan]  [dim]# or: git merge --squash {branch_name}[/dim]")

        # Auto-create and run review task if requested
        if task.create_review:
            return _create_and_run_review_task(task, config, store)

        return 0

    except GitError as e:
        error_message(f"Git error: {e}")
        return 1
    except KeyboardInterrupt:
        # Save WIP changes before returning
        _save_wip_changes(task, worktree_git, config, branch_name)
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
) -> int:
    """Run a non-code task (explore, plan, review) in a worktree (no branch creation).

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
        console.print(f"    Resuming with session: [dim]{task.session_id[:12]}...[/dim]")

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
    assert task.task_id is not None
    worktree_path = config.worktree_path / f"{task.task_id}-{task.task_type}"

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
        worktree_report_dir = worktree_path / report_dir.relative_to(config.project_dir)
        worktree_report_dir.mkdir(parents=True, exist_ok=True)
        worktree_report_path = worktree_path / report_path.relative_to(config.project_dir)

        # For Docker containers, use /workspace-relative path instead of host worktree path
        # The container only has /workspace mounted, so we need to use a path inside that
        # For native mode, use the actual worktree path
        if config.use_docker:
            prompt_report_path = Path("/workspace") / report_path.relative_to(config.project_dir)
        else:
            prompt_report_path = worktree_report_path

        # Run provider in the worktree
        if resume:
            prompt = PromptBuilder().resume_prompt()
        else:
            prompt = build_prompt(task, config, store, report_path=prompt_report_path, git=git)
        hidden_git_backup = _hide_invalid_worktree_git_metadata_for_docker(task, config, worktree_path)
        try:
            result = provider.run(config, prompt, log_file, worktree_path, resume_session_id=task.session_id if resume else None)
        except KeyboardInterrupt:
            console.print("\nInterrupted")
            return 130
        finally:
            _restore_worktree_git_metadata(worktree_path, hidden_git_backup)

        exit_code = result.exit_code
        stats = _run_result_to_stats(result)

        # Store session_id if available
        if result.session_id:
            task.session_id = result.session_id
            store.update(task)

        # Handle failures - check error_type first, then exit codes
        if result.error_type in ("max_turns", "max_steps"):
            error_message(f"Task failed: max steps of {config.max_steps} exceeded")
            stats_line(stats, has_commits=False)
            console.print(f"Task ID: {task.id}")
            next_steps([
                (f"gza retry {task.id}", "retry from scratch"),
                (f"gza resume {task.id}", "resume from where it left off"),
            ])
            detected = extract_failure_reason(log_file)
            failure_reason = detected if detected != "UNKNOWN" else "MAX_STEPS"
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, failure_reason=failure_reason)
            return 0
        elif exit_code == 124:
            error_message(f"Task failed: {provider.name} timed out after {config.timeout_minutes} minutes")
            stats_line(stats, has_commits=False)
            console.print(f"Task ID: {task.id}")
            next_steps([
                (f"gza retry {task.id}", "retry from scratch"),
                (f"gza resume {task.id}", "resume from where it left off"),
            ])
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, failure_reason=extract_failure_reason(log_file))
            return 0
        elif exit_code != 0:
            error_message(f"Task failed: {provider.name} exited with code {exit_code}")
            stats_line(stats, has_commits=False)
            console.print(f"Task ID: {task.id}")
            next_steps([
                (f"gza retry {task.id}", "retry from scratch"),
                (f"gza resume {task.id}", "resume from where it left off"),
            ])
            store.mark_failed(task, log_file=str(log_file.relative_to(config.project_dir)), stats=stats, failure_reason=extract_failure_reason(log_file))
            return 0

        # Copy report file from worktree to main project directory
        if worktree_report_path.exists():
            console.print(f"Report written to: {report_file_relative}")
            # Ensure target directory exists
            report_dir.mkdir(parents=True, exist_ok=True)
            # Copy report content from worktree to project dir
            report_path.write_text(worktree_report_path.read_text())
        else:
            # Report file was not created - task likely failed to write output
            console.print(f"[yellow]Warning: Report file not created by provider[/yellow]")
            console.print(f"See log file for details: {log_file.relative_to(config.project_dir)}")

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
        auto_learnings = maybe_auto_regenerate_learnings(store, config)

        # For review tasks, post to PR if applicable
        if task.task_type == "review" and task.depends_on:
            impl_task = store.get(task.depends_on)
            if impl_task:
                post_review_to_pr(task, impl_task, store, config.project_dir, required=False)

        console.print("")
        success_message(f"{task_type_display} Complete")
        stats_line(stats, has_commits=False)
        console.print(f"Task ID: {task.id}")
        console.print(f"Report: {report_file_relative}")
        if task.task_type == "review":
            verdict = _extract_review_verdict(output_content)
            if verdict:
                verdict_color = {
                    "APPROVED": "green",
                    "CHANGES_REQUESTED": "yellow",
                    "NEEDS_DISCUSSION": "blue",
                }.get(verdict, "white")
                console.print(f"Verdict: [{verdict_color}]{verdict}[/{verdict_color}]")
        console.print("")

        if task.task_type == "explore":
            console.print("To implement based on this exploration, add a task with:")
            console.print(f"  [cyan]gza add --based-on {task.id}[/cyan]")
        elif task.task_type == "plan":
            console.print("To implement this plan, add a task with:")
            console.print(f"  [cyan]gza add --type implement --based-on {task.id}[/cyan]")
        elif task.task_type == "review" and task.depends_on:
            console.print("To address review feedback, run:")
            console.print(f"  [cyan]gza improve {task.depends_on}[/cyan]")

        next_steps([
            (f"gza retry {task.id}", "retry from scratch"),
            (f"gza resume {task.id}", "resume from where it left off"),
        ])
        if auto_learnings:
            info_line(
                f"Updated learnings from {auto_learnings.tasks_used} tasks "
                f"({auto_learnings.path.relative_to(config.project_dir)})"
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
        return 1
