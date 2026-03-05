"""Git-related CLI commands: merge, rebase, checkout, diff, PR, refresh, advance."""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from ..config import Config
from ..console import (
    console,
    get_terminal_width,
    truncate,
    MAX_PR_BODY_LENGTH,
    MAX_PR_TITLE_LENGTH,
    MAX_PROMPT_DISPLAY_SHORT,
)
from ..db import SqliteTaskStore, Task as DbTask
from ..git import Git, GitError, cleanup_worktree_for_branch, parse_diff_numstat
from ..github import GitHub, GitHubError
from ..prompts import PromptBuilder
from ..runner import get_effective_config_for_task

from gza._query import (
    get_base_task_slug as _get_base_task_slug,
    get_reviews_for_root as _get_reviews_for_root_task,
    get_improves_for_root as _get_improves_for_root_task,
)

from ._common import (
    get_store,
    get_review_verdict,
    _create_resume_task,
    _spawn_background_worker,
    _spawn_background_rebase_worker,
    _spawn_background_resume_worker,
)


def cmd_refresh(args: argparse.Namespace) -> int:
    """Refresh cached diff stats for one or all unmerged tasks."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    default_branch = git.default_branch()

    if args.task_id is not None:
        # Single task by ID
        task = store.get(args.task_id)
        if task is None:
            console.print(f"[red]Error: Task #{args.task_id} not found[/red]")
            return 1
        tasks_to_refresh = [task]
    else:
        # All unmerged tasks (optionally including failed tasks with branches)
        all_unmerged = store.get_unmerged()
        tasks_to_refresh = [t for t in all_unmerged if t.status == "completed"]
        if getattr(args, 'include_failed', False):
            all_tasks = store.get_history(limit=None, status='failed')
            for t in all_tasks:
                if t.branch and t not in tasks_to_refresh:
                    tasks_to_refresh.append(t)

    refreshed = 0
    skipped = 0
    for task in tasks_to_refresh:
        if not task.branch:
            console.print(f"[dim]#{task.id}: no branch, skipping[/dim]")
            skipped += 1
            continue
        if not git.branch_exists(task.branch):
            console.print(f"[dim]#{task.id} {task.branch}: branch no longer exists, skipping[/dim]")
            skipped += 1
            continue
        revision_range = f"{default_branch}...{task.branch}"
        numstat_output = git.get_diff_numstat(revision_range)
        files_changed, lines_added, lines_removed = parse_diff_numstat(numstat_output)
        assert task.id is not None
        store.update_diff_stats(task.id, files_changed, lines_added, lines_removed)
        console.print(f"#{task.id} {task.branch}: +{lines_added} -{lines_removed} in {files_changed} files")
        refreshed += 1

    console.print(f"\nRefreshed {refreshed} task(s), skipped {skipped}.")
    return 0


def _merge_single_task(
    task_id: int,
    config: Config,
    store,
    git: Git,
    args: argparse.Namespace,
    current_branch: str,
) -> int:
    """Merge a single task's branch. Returns 0 on success, 1 on failure."""
    # Get the task
    task = store.get(task_id)
    if not task:
        print(f"Error: Task #{task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged"):
        print(f"Error: Task #{task.id} is not completed or unmerged (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task #{task.id} has no branch")
        return 1

    # Handle --mark-only flag
    if args.mark_only:
        # Check for conflicting flags
        if args.rebase or args.squash or args.delete:
            print("Error: --mark-only cannot be used with --rebase, --squash, or --delete")
            return 1

        store.set_merge_status(task.id, "merged")
        print(f"✓ Marked task #{task.id} as merged (branch '{task.branch}' preserved)")
        return 0

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

    # Validate --remote flag
    if hasattr(args, 'remote') and args.remote and not args.rebase:
        print("Error: --remote requires --rebase")
        return 1

    # Validate --resolve flag
    if getattr(args, 'resolve', False) and not args.rebase:
        print("Error: --resolve requires --rebase")
        return 1

    # Perform the merge or rebase
    try:
        if args.rebase:
            # Determine the target branch to rebase onto
            rebase_target = current_branch
            if hasattr(args, 'remote') and args.remote:
                # Fetch from origin first
                print(f"Fetching from origin...")
                git.fetch("origin")
                print("✓ Fetched from origin")
                rebase_target = f"origin/{current_branch}"

            # For rebase: checkout the task branch, rebase onto target, then fast-forward merge
            print(f"Rebasing '{task.branch}' onto '{rebase_target}'...")
            git.checkout(task.branch)
            git.rebase(rebase_target)
            print(f"✓ Successfully rebased {task.branch}")

            # Switch back and fast-forward merge
            git.checkout(current_branch)
            git.merge(task.branch, squash=False)
            print(f"✓ Fast-forwarded {current_branch} to {task.branch}")
        else:
            # Regular merge or squash merge
            merge_type = "squash merging" if args.squash else "merging"
            print(f"Merging '{task.branch}' into '{current_branch}'...")

            # For squash merge, create a commit message from the task
            commit_message = None
            if args.squash:
                # Get a concise summary of the task
                task_summary = truncate(task.prompt, MAX_PR_TITLE_LENGTH)
                commit_message = f"Squash merge: {task_summary}\n\nTask #{task.id}: {task.prompt}"

            git.merge(task.branch, squash=args.squash, commit_message=commit_message)

            if args.squash:
                print(f"✓ Successfully squash merged {task.branch} and created commit")
            else:
                print(f"✓ Successfully merged {task.branch}")

        # Delete branch if requested
        if args.delete:
            try:
                git.delete_branch(task.branch)
                print(f"✓ Deleted branch {task.branch}")
            except GitError as e:
                print(f"Warning: Could not delete branch: {e}")

        store.set_merge_status(task.id, "merged")
        return 0

    except GitError as e:
        operation = "rebase" if args.rebase else "merge"

        if args.rebase and getattr(args, 'resolve', False):
            # --resolve: invoke Claude to fix conflicts
            print("Conflicts detected. Invoking Claude to resolve...")
            resolved = invoke_claude_resolve(task, task.branch, rebase_target, config)

            if not resolved:
                print("Could not resolve conflicts automatically.")
                try:
                    git.rebase_abort()
                    try:
                        git.checkout(current_branch)
                    except GitError:
                        pass
                except GitError as abort_error:
                    print(f"Warning: Could not abort rebase: {abort_error}")
                return 1

            # Switch back and fast-forward merge
            git.checkout(current_branch)
            git.merge(task.branch, squash=False)
            print(f"✓ Fast-forwarded {current_branch} to {task.branch}")

            # Delete branch if requested
            if args.delete:
                try:
                    git.delete_branch(task.branch)
                    print(f"✓ Deleted branch {task.branch}")
                except GitError as del_error:
                    print(f"Warning: Could not delete branch: {del_error}")

            store.set_merge_status(task.id, "merged")
            return 0

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


def cmd_merge(args: argparse.Namespace) -> int:
    """Merge task branches into the current branch."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Get current branch once
    current_branch = git.current_branch()

    # Determine the list of task IDs to merge
    task_ids = list(args.task_ids)

    use_all = getattr(args, 'all', False)
    if use_all:
        # Find all completed/unmerged tasks with branches not yet merged
        history = store.get_history(limit=None)
        seen_ids = set(task_ids)
        # Process oldest first (history is newest-first, so reverse it)
        for task in reversed(history):
            if task.id in seen_ids:
                continue
            if task.status in ("completed", "unmerged") and task.branch and task.has_commits:
                if task.merge_status != "merged" and not git.is_merged(task.branch, current_branch):
                    task_ids.append(task.id)
                    seen_ids.add(task.id)
        if not task_ids:
            print("No unmerged done tasks found")
            return 0
    elif not task_ids:
        print("Error: either provide task_id(s) or use --all to merge all unmerged done tasks")
        return 1

    # Track success/failure
    merged_tasks = []
    failed_task_id = None

    # Merge each task in sequence
    for task_id in task_ids:
        if use_all:
            print(f"Merging task #{task_id}...")
        result = _merge_single_task(task_id, config, store, git, args, current_branch)

        if result != 0:
            # Merge failed, stop processing
            failed_task_id = task_id
            break

        merged_tasks.append(task_id)
        if use_all:
            print()

    # Report results
    if merged_tasks:
        print(f"\n✓ Successfully merged {len(merged_tasks)} task(s): {', '.join(f'#{tid}' for tid in merged_tasks)}")

    if failed_task_id is not None:
        remaining = [tid for tid in task_ids if tid not in merged_tasks and tid != failed_task_id]
        if remaining:
            print(f"⚠ Stopped at task #{failed_task_id}. Remaining tasks not processed: {', '.join(f'#{tid}' for tid in remaining)}")
        return 1

    return 0


def _resolve_runtime_skill_dir(project_dir: Path, provider: str) -> tuple[str, Path] | None:
    """Resolve runtime skill directory for a provider."""
    codex_home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
    gemini_home = Path(os.environ.get("GEMINI_HOME", str(Path.home() / ".gemini"))).expanduser()
    target_map = {
        "claude": ("claude", project_dir / ".claude" / "skills"),
        "codex": ("codex", codex_home / "skills"),
        "gemini": ("gemini", gemini_home / "skills"),
    }
    return target_map.get(provider)


def ensure_skill(skill_name: str, provider: str, project_dir: Path) -> bool:
    """Ensure a skill is available for the provider runtime, installing if missing.

    Resolves the runtime skill directory for the provider, checks whether the
    skill file exists, and if not attempts to auto-install it from the bundled
    package via skills_utils.copy_skill.

    Args:
        skill_name: Name of the skill to ensure (e.g. 'gza-rebase').
        provider: Provider name ('claude', 'codex', or 'gemini').
        project_dir: Project directory used to resolve the runtime skill path.

    Returns:
        True if the skill is available after the check/install, False otherwise.
    """
    from ..skills_utils import copy_skill

    runtime = _resolve_runtime_skill_dir(project_dir, provider)
    if not runtime:
        return False
    _, runtime_dir = runtime
    skill_path = runtime_dir / skill_name / "SKILL.md"
    if skill_path.exists():
        return True
    # Skill missing — attempt auto-install from bundled package.
    runtime_dir.mkdir(parents=True, exist_ok=True)
    ok, _ = copy_skill(skill_name, runtime_dir)
    return ok and skill_path.exists()


def invoke_claude_resolve(task: DbTask, branch: str, target: str, config: Config) -> bool:
    """Invoke active provider runtime to resolve rebase conflicts via /gza-rebase."""
    from dataclasses import replace
    from ..providers import get_provider

    # Always run directly (not in Docker) since we need access to the
    # host's git rebase state on the local filesystem
    effective_model, effective_provider, effective_max_steps = get_effective_config_for_task(task, config)

    runtime = _resolve_runtime_skill_dir(config.project_dir, effective_provider)
    if not runtime:
        print(f"Error: Provider '{effective_provider}' does not support runtime skills for auto-resolve.")
        return False

    target_name, runtime_dir = runtime
    if not ensure_skill("gza-rebase", effective_provider, config.project_dir):
        print(f"Error: Missing required 'gza-rebase' skill for provider '{effective_provider}'.")
        print(
            "Install it with: "
            f"uv run gza skills-install --target {target_name} gza-rebase --project {config.project_dir}"
        )
        return False

    resolve_config = replace(
        config,
        use_docker=False,
        provider=effective_provider,
        model=effective_model or "",
        max_steps=effective_max_steps,
        max_turns=effective_max_steps,
    )

    log_dir = config.project_dir / config.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file = log_dir / f"resolve-{timestamp}.log"

    provider = get_provider(resolve_config)
    provider.run(resolve_config, "/gza-rebase --auto", log_file, config.project_dir)

    # Check if rebase completed (no longer in rebase state)
    rebase_in_progress = Path(".git/rebase-merge").exists() or Path(".git/rebase-apply").exists()

    return not rebase_in_progress


def cmd_rebase(args: argparse.Namespace) -> int:
    """Rebase a task's branch onto a target branch."""
    config = Config.load(args.project_dir)

    # Handle background mode - spawn worker to perform the rebase
    if getattr(args, 'background', False):
        return _spawn_background_rebase_worker(args, config)

    store = get_store(config)
    git = Git(config.project_dir)

    # Get the task
    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged", "running"):
        print(f"Error: Task #{task.id} is not completed, unmerged, or running (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task #{task.id} has no branch")
        return 1

    # Check if branch exists
    if not git.branch_exists(task.branch):
        print(f"Error: Branch '{task.branch}' does not exist")
        return 1

    # Get current branch and determine rebase target
    current_branch = git.current_branch()
    default_branch = git.default_branch()

    # Determine rebase target: use --onto if provided, else current branch
    rebase_target = getattr(args, 'onto', None) or current_branch

    # Handle --remote flag
    if hasattr(args, 'remote') and args.remote:
        print(f"Fetching from origin...")
        git.fetch("origin")
        print("✓ Fetched from origin")
        rebase_target = f"origin/{rebase_target}"

    # Check for uncommitted changes
    if git.has_changes(include_untracked=False):
        print("Error: You have uncommitted changes. Please commit or stash them first.")
        return 1

    # Clean up worktree if branch is checked out in one
    try:
        force = getattr(args, 'force', False)
        worktree_path = cleanup_worktree_for_branch(git, task.branch, force=force)
        if worktree_path:
            print(f"Removing stale worktree at {worktree_path}...")
            print(f"✓ Removed worktree")
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    # Perform the rebase
    print(f"Rebasing task #{task.id}...")
    try:
        print(f"Rebasing '{task.branch}' onto '{rebase_target}'...")
        git.checkout(task.branch)
        git.rebase(rebase_target)
        print(f"✓ Successfully rebased {task.branch} onto {rebase_target}")

        # Switch back to original branch
        git.checkout(current_branch)
        print(f"✓ Switched back to {current_branch}")

        print()
        return 0

    except GitError as e:
        # Check if --resolve flag is set
        if not getattr(args, 'resolve', False):
            # Original behavior: abort and return error
            print(f"Error during rebase: {e}")
            print(f"\nAborting rebase and restoring clean state...")
            try:
                git.rebase_abort()
                try:
                    git.checkout(current_branch)
                except GitError:
                    pass  # Best effort to return to original branch
                print("✓ Rebase aborted, working directory restored")
            except GitError as abort_error:
                print(f"Warning: Could not abort rebase: {abort_error}")
            print()
            return 1

        # --resolve: invoke Claude to fix conflicts
        print("Conflicts detected. Invoking Claude to resolve...")
        resolved = invoke_claude_resolve(task, task.branch, rebase_target, config)

        if not resolved:
            print("Could not resolve conflicts automatically.")
            git.rebase_abort()
            git.checkout(current_branch)
            print()
            return 1

        # Force push the resolved branch
        print(f"Pushing {task.branch}...")
        git.push_force_with_lease(task.branch)

        # Always checkout main at the end
        git.checkout(default_branch)

        print(f"✓ Successfully rebased {task.branch}")
        print()
        return 0


def cmd_checkout(args: argparse.Namespace) -> int:
    """Checkout a task's branch, removing any stale worktree if needed."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Resolve task ID or branch name
    task = None
    branch = None

    if args.task_id_or_branch.isdigit():
        # It's a task ID
        task = store.get(int(args.task_id_or_branch))
        if not task:
            print(f"Error: Task #{args.task_id_or_branch} not found")
            return 1
        if not task.branch:
            print(f"Error: Task #{task.id} has no branch")
            return 1
        branch = task.branch
    else:
        # It's a branch name
        branch = args.task_id_or_branch

    # Check if branch exists
    if not git.branch_exists(branch):
        print(f"Error: Branch '{branch}' does not exist locally")
        return 1

    # Clean up worktree if branch is checked out in one
    try:
        worktree_path = cleanup_worktree_for_branch(git, branch, force=args.force)
        if worktree_path:
            print(f"Removing stale worktree at {worktree_path}...")
            print(f"✓ Removed worktree")
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    # Checkout the branch
    try:
        git.checkout(branch)
        print(f"✓ Checked out '{branch}'")
        return 0
    except GitError as e:
        print(f"Error checking out branch: {e}")
        return 1


def cmd_diff(args: argparse.Namespace) -> int:
    """Run git diff with colored output and pager support."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Build git diff command
    git_cmd = ["git", "diff"]

    # Add --color=always to force colored output
    git_cmd.append("--color=always")

    # Process arguments - check if first arg is a task ID
    diff_args = args.diff_args if hasattr(args, 'diff_args') and args.diff_args else []

    if diff_args and diff_args[0].isdigit():
        # First argument is a numeric task ID
        task_id = int(diff_args[0])
        task = store.get(task_id)

        if not task:
            print(f"Error: Task #{task_id} not found")
            return 1

        if not task.branch:
            print(f"Error: Task #{task_id} has no branch")
            return 1

        # Replace task ID with branch diff range
        default_branch = git.default_branch()
        diff_args = [f"{default_branch}...{task.branch}"] + diff_args[1:]

    # Add any additional arguments passed to gza diff
    if diff_args:
        git_cmd.extend(diff_args)

    # Check if stdout is a TTY (not redirected/piped)
    use_pager = sys.stdout.isatty()

    try:
        if use_pager:
            # Determine which pager to use
            pager = _get_pager(config.project_dir)

            # Run git diff and pipe to pager
            git_proc = subprocess.Popen(
                git_cmd,
                cwd=config.project_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            pager_proc = subprocess.Popen(
                pager,
                stdin=git_proc.stdout,
                cwd=config.project_dir,
                shell=True,
            )

            # Close git's stdout in parent to allow git_proc to receive SIGPIPE
            if git_proc.stdout:
                git_proc.stdout.close()

            # Wait for pager to finish
            pager_proc.wait()
            git_proc.wait()

            # Return git's exit code if it failed, otherwise pager's
            if git_proc.returncode != 0:
                # Print any stderr from git
                if git_proc.stderr:
                    stderr = git_proc.stderr.read().decode()
                    if stderr:
                        print(stderr, file=sys.stderr, end='')
                return git_proc.returncode
            return pager_proc.returncode
        else:
            # No pager - output directly (for redirection/piping)
            result = subprocess.run(
                git_cmd,
                cwd=config.project_dir,
                check=False,
            )
            return result.returncode

    except Exception as e:
        print(f"Error running git diff: {e}", file=sys.stderr)
        return 1


def _get_pager(repo_dir: Path) -> str:
    """Determine which pager to use for git diff output.

    Checks in order:
    1. $GIT_PAGER environment variable
    2. git config core.pager
    3. $PAGER environment variable
    4. Falls back to 'less'

    Args:
        repo_dir: Path to git repository

    Returns:
        The pager command to use
    """
    # Check $GIT_PAGER
    git_pager = os.environ.get('GIT_PAGER')
    if git_pager:
        return git_pager

    # Check git config core.pager
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
        pass

    # Check $PAGER
    pager = os.environ.get('PAGER')
    if pager:
        return pager

    # Default to 'less -R' (-R interprets ANSI color codes)
    return 'less -R'


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
    prompt = PromptBuilder().pr_description_prompt(
        task_prompt=task.prompt,
        commit_log=commit_log,
        diff_stat=diff_stat,
    )

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
        title = task.task_id or truncate(task.prompt.split("\n")[0], MAX_PR_TITLE_LENGTH)

    body = "\n".join(body_lines).strip()
    if not body:
        body = f"Task: {truncate(task.prompt, MAX_PR_BODY_LENGTH)}"

    return title, body


def _fallback_pr_content(task: DbTask, commit_log: str) -> tuple[str, str]:
    """Generate simple PR content without AI."""
    # Title from task_id or prompt
    if task.task_id:
        # Convert slug like "20240106-add-feature" to "Add feature"
        parts = task.task_id.split("-")[1:]  # Remove date prefix
        title = " ".join(parts).capitalize()
    else:
        title = truncate(task.prompt.split("\n")[0], MAX_PR_TITLE_LENGTH)

    body = f"""## Task Prompt

> {truncate(task.prompt, MAX_PR_BODY_LENGTH).replace(chr(10), chr(10) + '> ')}

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

    # Get the task first (validate task exists and state before checking gh)
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

    # Check merge_status before requiring gh (local DB check, no external dependencies)
    if task.merge_status == "merged":
        print(f"Error: Task #{task.id} is already marked as merged")
        return 1

    # Check gh CLI is available (after task validation so tests can run without gh)
    if not gh.is_available():
        print("Error: GitHub CLI (gh) is not installed or not authenticated")
        print("Install: https://cli.github.com/")
        print("Auth: gh auth login")
        return 1

    default_branch = git.default_branch()

    # Check branch is not actually merged into default branch
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
        body = f"## Summary\n{truncate(task.prompt, MAX_PR_BODY_LENGTH)}"
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

        # Cache PR number in task
        if pr.number:
            task.pr_number = pr.number
            store.update(task)

        return 0
    except GitHubError as e:
        print(f"Error creating PR:\n{e}")
        return 1


def _count_completed_review_cycles(store: SqliteTaskStore, impl_task_id: int) -> int:
    """Count completed review/improve cycles for an implementation task.

    Counts completed improve tasks for the root task, since each improve
    corresponds to one completed review→changes_requested→improve cycle.
    """
    improve_tasks = store.get_improve_tasks_by_root(impl_task_id)
    return sum(1 for t in improve_tasks if t.status == 'completed')


def _determine_advance_action(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    default_branch: str,
    impl_based_on_ids: set[int] | None = None,
) -> dict:
    """Determine the next action needed to advance a task.

    Returns a dict with:
        type: action type ('merge', 'create_review', 'create_implement', 'improve',
                           'needs_rebase', 'wait_review', 'wait_improve',
                           'needs_discussion', 'skip')
        description: human-readable description of the action
        review_task: (optional) the review task involved

    Args:
        impl_based_on_ids: Pre-computed set of plan IDs that already have an
            implement task.  Pass this to avoid repeated DB queries when
            processing many plan tasks.  If *None*, it will be fetched on
            demand for plan tasks.
    """
    assert task.id is not None

    # Completed plan tasks: check if an implement child already exists
    if task.task_type == 'plan':
        if impl_based_on_ids is None:
            impl_based_on_ids = store.get_impl_based_on_ids()
        if task.id not in impl_based_on_ids:
            return {
                'type': 'create_implement',
                'description': 'Create and start implement task',
            }
        return {
            'type': 'skip',
            'description': 'SKIP: implement task already exists for this plan',
        }

    # Tasks with no branch (no commits) cannot be merged or reviewed
    if not task.branch:
        return {
            'type': 'skip',
            'description': 'SKIP: task has no branch (no commits)',
        }

    # Check for merge conflicts against the default branch (the merge target)
    if not git.can_merge(task.branch, default_branch):
        return {
            'type': 'needs_rebase',
            'description': 'SKIP: needs manual rebase (conflicts detected)',
        }

    # Check review state
    reviews = _get_reviews_for_root_task(store, task)

    if reviews:
        latest_review = reviews[0]

        # Determine if the review has been cleared by a subsequent improve task
        review_cleared = (
            task.review_cleared_at is not None
            and latest_review.completed_at is not None
            and task.review_cleared_at >= latest_review.completed_at
        )

        # If review was cleared, check if code changed since the review
        # (i.e., a completed improve task exists after the latest review).
        # In that case, the review is stale and we need a new one.
        if review_cleared and latest_review.completed_at is not None:
            # But first, check if a new review is already pending/in_progress
            active_review = next(
                (r for r in reviews if r.status in ('pending', 'in_progress')),
                None,
            )
            if active_review:
                if active_review.status == 'pending':
                    return {
                        'type': 'run_review',
                        'description': f'Spawn worker for pending review #{active_review.id}',
                        'review_task': active_review,
                    }
                return {
                    'type': 'wait_review',
                    'description': f'SKIP: review #{active_review.id} is in_progress',
                    'review_task': active_review,
                }

            improves = _get_improves_for_root_task(store, task)
            completed_improves = [
                t for t in improves
                if t.status == 'completed' and t.completed_at is not None
            ]
            if completed_improves:
                latest_improve = max(completed_improves, key=lambda t: t.completed_at or datetime.min)
                if latest_improve.completed_at is not None and latest_improve.completed_at > latest_review.completed_at:
                    return {
                        'type': 'create_review',
                        'description': 'Create review (code changed since last review)',
                    }

        if not review_cleared:
            # Active (non-cleared) review exists
            if latest_review.status == 'pending':
                return {
                    'type': 'run_review',
                    'description': f'Spawn worker for pending review #{latest_review.id}',
                    'review_task': latest_review,
                }
            if latest_review.status == 'in_progress':
                return {
                    'type': 'wait_review',
                    'description': f'SKIP: review #{latest_review.id} is in_progress',
                    'review_task': latest_review,
                }

            verdict = get_review_verdict(config, latest_review)
            if verdict == 'APPROVED':
                return {
                    'type': 'merge',
                    'description': 'Merge (review APPROVED)',
                    'review_task': latest_review,
                }
            elif verdict == 'CHANGES_REQUESTED':
                # Check cycle limit before creating a new improve
                completed_cycles = _count_completed_review_cycles(store, task.id)
                if completed_cycles >= config.max_review_cycles:
                    return {
                        'type': 'max_cycles_reached',
                        'description': f'SKIP: max review cycles ({config.max_review_cycles}) reached, needs manual intervention',
                    }
                # Check if an improve task is already pending/in_progress
                assert latest_review.id is not None
                existing_improve = store.get_improve_tasks_for(task.id, latest_review.id)
                active_improve_running = [t for t in existing_improve if t.status == 'in_progress']
                if active_improve_running:
                    return {
                        'type': 'wait_improve',
                        'description': f'SKIP: improve task #{active_improve_running[0].id} is in_progress',
                    }
                active_improve_pending = [t for t in existing_improve if t.status == 'pending']
                if active_improve_pending:
                    return {
                        'type': 'run_improve',
                        'description': f'Spawn worker for pending improve #{active_improve_pending[0].id}',
                        'improve_task': active_improve_pending[0],
                    }
                return {
                    'type': 'improve',
                    'description': 'Create improve task (review CHANGES_REQUESTED)',
                    'review_task': latest_review,
                }
            else:
                return {
                    'type': 'needs_discussion',
                    'description': f'SKIP: review verdict is {verdict or "unknown"}, needs manual attention',
                    'review_task': latest_review,
                }

    # Review was cleared by an improve task — always mergeable regardless of config.
    if reviews:
        return {
            'type': 'merge',
            'description': 'Merge (previous review addressed)',
        }

    # Reached only when no reviews exist (all earlier paths with reviews have returned).
    # Non-implement types (plan, explore, improve, etc.) go straight to merge.
    # improve tasks are already produced by a review cycle; they merge directly.
    if task.task_type != 'implement':
        return {
            'type': 'merge',
            'description': 'Merge task (no review yet)',
        }

    # implement task with no review — consult config flags.
    # Note: advance_create_reviews is only consulted when advance_requires_review=True.
    # When advance_requires_review=False, tasks merge directly regardless of advance_create_reviews
    # (there is no review gate, so creating one informally is not relevant).
    if config.advance_requires_review:
        if config.advance_create_reviews:
            return {
                'type': 'create_review',
                'description': 'Create review (required before merge)',
            }
        else:
            return {
                'type': 'skip',
                'description': 'SKIP: no review exists and advance_create_reviews=false (run gza review manually)',
            }
    # advance_requires_review=false — merge directly without a review.
    return {
        'type': 'merge',
        'description': 'Merge task (no review yet)',
    }


def _cmd_advance_plans(
    config: "Config",
    store: SqliteTaskStore,
    dry_run: bool = False,
    create: bool = False,
) -> int:
    """List completed plans that have no implementation task.

    With --create, creates queued implement tasks for each such plan.
    """
    all_completed_plans = store.get_history(limit=None, status="completed", task_type="plan")

    # Find plans that have no implement task pointing at them (via based_on).
    # Use a targeted query instead of a full table scan to avoid loading every
    # task (including output_content blobs) into memory.
    impl_based_on_ids: set[int] = store.get_impl_based_on_ids()

    pending_plans = [p for p in all_completed_plans if p.id not in impl_based_on_ids]

    if not pending_plans:
        print("No completed plans without implementation tasks.")
        return 0

    print(f"Completed plans without implementation ({len(pending_plans)}):")
    print()
    for plan in pending_plans:
        assert plan.id is not None
        prompt_display = truncate(plan.prompt, MAX_PROMPT_DISPLAY_SHORT)
        print(f"  #{plan.id}  {prompt_display}")
        print(f"       → gza implement {plan.id}")
    print()

    if not create:
        print("Run 'gza advance' or 'gza advance --type plan' to create and start implement tasks.")
        return 0

    # Create queued implement tasks
    created_count = 0
    for plan in pending_plans:
        assert plan.id is not None
        if dry_run:
            print(f"[dry-run] Would create implement task for plan #{plan.id}")
            continue
        slug = _get_base_task_slug(plan)
        prompt_text = f"Implement plan from task #{plan.id}: {slug}" if slug else f"Implement plan from task #{plan.id}"
        impl_task = store.add(
            prompt=prompt_text,
            task_type="implement",
            based_on=plan.id,
            group=plan.group,
        )
        print(f"✓ Created implement task #{impl_task.id} for plan #{plan.id}")
        created_count += 1

    if not dry_run:
        print(f"\nCreated {created_count} implement task(s). Run 'gza work' to execute them.")
    return 0


# Maps advance action types to their execution priority (lower = runs first).
# 'merge' actions are fast and synchronous; running them first ensures freshly
# merged code is on the default branch before any review/improve workers are
# spawned, reducing rebase conflicts for those workers.
_ADVANCE_ACTION_ORDER: dict[str, int] = {'merge': 0}


def cmd_advance(args: argparse.Namespace) -> int:
    """Intelligently progress unmerged tasks through their lifecycle."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    dry_run: bool = args.dry_run
    auto: bool = getattr(args, 'auto', False)
    max_tasks: int | None = getattr(args, 'max', None)
    batch_limit: int | None = getattr(args, 'batch', None)
    task_id: int | None = getattr(args, 'task_id', None)
    plans_mode: bool = getattr(args, 'plans', False)
    create_mode: bool = getattr(args, 'create', False)
    no_resume_failed: bool = getattr(args, 'no_resume_failed', False)
    max_resume_attempts_override: int | None = getattr(args, 'max_resume_attempts', None)
    advance_type: str | None = getattr(args, 'advance_type', None)

    # Determine effective max_resume_attempts
    max_resume_attempts = max_resume_attempts_override if max_resume_attempts_override is not None else config.max_resume_attempts

    new_mode: bool = getattr(args, 'new', False)

    max_review_cycles_override: int | None = getattr(args, 'max_review_cycles', None)

    if max_review_cycles_override is not None:
        config.max_review_cycles = max_review_cycles_override

    squash_threshold_override: int | None = getattr(args, 'squash_threshold', None)
    if squash_threshold_override is not None:
        config.merge_squash_threshold = squash_threshold_override

    if new_mode and batch_limit is None:
        print("Error: --new requires --batch", file=sys.stderr)
        return 1

    if batch_limit is not None and batch_limit < 1:
        print("Error: --batch must be a positive integer", file=sys.stderr)
        return 1

    # --plans mode: list completed plans without implementations
    if plans_mode:
        return _cmd_advance_plans(config, store, dry_run=dry_run, create=create_mode)

    # Pre-compute the set of plan IDs that already have implement children
    # to avoid repeated DB queries in _determine_advance_action.
    impl_based_on_ids: set[int] = store.get_impl_based_on_ids()

    # Determine which tasks to advance
    if task_id is not None:
        task = store.get(task_id)
        if not task:
            print(f"Error: Task #{task_id} not found")
            return 1
        if task.status == 'failed':
            # Allow a specific failed task if it's resumable
            is_resumable = (
                task.failure_reason in ('MAX_STEPS', 'MAX_TURNS')
                and task.session_id is not None
                and not no_resume_failed
            )
            if not is_resumable:
                print(f"Error: Task #{task_id} is not completed (status: {task.status})")
                return 1
            tasks = []
            failed_tasks: list[DbTask] = [task]
        else:
            if task.status != 'completed':
                print(f"Error: Task #{task_id} is not completed (status: {task.status})")
                return 1
            if task.merge_status == 'merged':
                print(f"Task #{task_id} is already merged")
                return 0
            tasks = [task]
            failed_tasks = []
    else:
        # Get all unmerged completed tasks
        all_unmerged = store.get_unmerged()
        tasks = [t for t in all_unmerged if t.status == 'completed']
        # Also collect resumable failed tasks (unless disabled)
        failed_tasks = [] if no_resume_failed else store.get_resumable_failed_tasks()

        # Also fetch completed plans that have no implement child yet.
        # These are invisible to get_unmerged() (no branch/merge_status)
        # but can be auto-advanced by creating implement tasks.
        if advance_type != 'implement':
            completed_plans = store.get_history(limit=None, status="completed", task_type="plan")
            pending_plans = [
                p for p in completed_plans
                if p.id not in impl_based_on_ids
            ]
            # Avoid duplicates: only add plans not already in the unmerged list
            existing_ids = {t.id for t in tasks}
            for p in pending_plans:
                if p.id not in existing_ids:
                    tasks.append(p)

        # Apply --type filter
        if advance_type == 'plan':
            tasks = [t for t in tasks if t.task_type == 'plan']
            failed_tasks = []  # plans don't have failed/resume logic
        elif advance_type == 'implement':
            tasks = [t for t in tasks if t.task_type == 'implement']
            failed_tasks = [t for t in failed_tasks if t.task_type == 'implement']

    if not tasks and not failed_tasks and not new_mode:
        print("No eligible tasks to advance")
        return 0

    # Apply --max limit
    if max_tasks is not None:
        tasks = tasks[:max_tasks]

    # Use the default branch as the merge target for all operations.
    # advance is a batch command and operators may not be on main, so we
    # always merge into the canonical default branch (main/master).
    default_branch = git.default_branch()

    # Analyze each completed task to determine the next action
    plan: list[tuple[DbTask, dict]] = []
    for task in tasks:
        action = _determine_advance_action(config, store, git, task, default_branch, impl_based_on_ids=impl_based_on_ids)
        plan.append((task, action))

    # Analyze each resumable failed task
    for failed_task in failed_tasks:
        assert failed_task.id is not None
        failure_reason = failed_task.failure_reason or "UNKNOWN"
        # If this task already has any resume children, skip it — the child
        # task is what should be evaluated for further resume attempts, not this one.
        children = store.get_based_on_children(failed_task.id)
        if children:
            continue
        # Check resume chain depth
        depth = store.count_resume_chain_depth(failed_task.id)
        if depth >= max_resume_attempts:
            plan.append((failed_task, {
                'type': 'skip',
                'description': f"SKIP: max resume attempts ({max_resume_attempts}) reached",
            }))
        else:
            attempt_num = depth + 1
            plan.append((failed_task, {
                'type': 'resume',
                'description': f"Resume (failed: {failure_reason}, attempt {attempt_num}/{max_resume_attempts})",
            }))

    # Sort so merges execute before worker spawns. See _ADVANCE_ACTION_ORDER for
    # the rationale. The sort is stable, preserving DB order within each group.
    # dry-run output inherits this order, so it accurately reflects execution.
    plan.sort(key=lambda item: _ADVANCE_ACTION_ORDER.get(item[1]['type'], 1))

    # If the plan is empty or every item is a skip, there's nothing actionable
    # (unless --new is set, in which case we still want to start pending tasks).
    if not plan or all(action['type'] == 'skip' for _, action in plan):
        if not new_mode:
            print("No eligible tasks to advance")
            if plan:
                print()
                for task, action in plan:
                    prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY_SHORT)
                    print(f"  #{task.id} {prompt_display}")
                    print(f"      → {action['description']}")
                print()
            return 0
        else:
            # --new with no existing actions: skip straight to spawning new tasks
            if plan:
                for task, action in plan:
                    prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY_SHORT)
                    print(f"  #{task.id} {prompt_display}")
                    print(f"      → {action['description']}")
                print()

    if dry_run:
        print(f"Would advance {len(plan)} task(s):\n")
        for task, action in plan:
            prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY_SHORT)
            print(f"  #{task.id} {prompt_display}")
            description = action['description']
            if action['type'] == 'merge' and config.merge_squash_threshold > 0 and task.branch:
                commit_count = git.count_commits_ahead(task.branch, default_branch)
                if commit_count >= config.merge_squash_threshold:
                    description = f"{description} (auto-squash, {commit_count} commits)"
            print(f"      → {description}")
            print()
        if new_mode and batch_limit is not None:
            worker_action_types = frozenset({'run_review', 'run_improve', 'create_review', 'create_implement', 'improve', 'resume'})
            planned_workers = sum(1 for _, a in plan if a['type'] in worker_action_types)
            remaining = max(0, batch_limit - planned_workers)
            if remaining > 0:
                pending_tasks = store.get_pending(limit=remaining)
                if pending_tasks:
                    prompt_width = int(get_terminal_width() * 0.7)
                    print(f"Would start {len(pending_tasks)} new pending task(s):\n")
                    for pt in pending_tasks:
                        flat_prompt = '. '.join(line.strip() for line in pt.prompt.splitlines() if line.strip())
                        prompt_display = truncate(flat_prompt, prompt_width)
                        console.print(f"  [cyan]#{pt.id}[/cyan] [#ff99cc]{prompt_display}[/#ff99cc]")
                        console.print(f"      [cyan]→ Start new worker[/cyan]")
                        print()
                else:
                    print("No pending tasks available to fill batch\n")
        return 0

    # Show the plan and prompt for confirmation
    actionable_plan = [item for item in plan if item[1]['type'] != 'skip']
    if actionable_plan:
        print(f"Will advance {len(actionable_plan)} task(s):\n")
        for task, action in plan:
            prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY_SHORT)
            print(f"  #{task.id} {prompt_display}")
            print(f"      → {action['description']}")
            print()

    new_pending_tasks: list = []
    if new_mode and batch_limit is not None:
        worker_action_types = frozenset({'run_review', 'run_improve', 'create_review', 'improve', 'resume'})
        planned_workers = sum(1 for _, a in plan if a['type'] in worker_action_types)
        remaining = max(0, batch_limit - planned_workers)
        if remaining > 0:
            new_pending_tasks = store.get_pending(limit=remaining)
            if new_pending_tasks:
                prompt_width = int(get_terminal_width() * 0.7)
                print(f"Will start {len(new_pending_tasks)} new pending task(s):\n")
                for pt in new_pending_tasks:
                    flat_prompt = '. '.join(line.strip() for line in pt.prompt.splitlines() if line.strip())
                    prompt_display = truncate(flat_prompt, prompt_width)
                    console.print(f"  [cyan]#{pt.id}[/cyan] [#ff99cc]{prompt_display}[/#ff99cc]")
                    console.print(f"      [cyan]→ Start new worker[/cyan]")
                    print()

    if not auto and (actionable_plan or new_mode):
        try:
            answer = input("Proceed? [Y/n] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if answer not in ('', 'y', 'yes'):
            print("Aborted.")
            return 0

    # Execute actions
    success_count = 0
    skip_count = 0
    error_count = 0
    workers_started = 0
    # Track tasks skipped for actionable reasons (needs human attention)
    _ACTIONABLE_SKIP_TYPES = frozenset({'needs_rebase', 'needs_discussion', 'max_cycles_reached', 'max_resume_attempts'})
    attention_tasks: list[tuple[DbTask, dict]] = []

    for task, action in plan:
        assert task.id is not None
        prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY_SHORT)
        action_type = action['type']

        if action_type in ('needs_rebase', 'wait_review', 'wait_improve', 'needs_discussion', 'skip', 'max_cycles_reached'):
            print(f"  #{task.id} {prompt_display}")
            print(f"      {action['description']}")
            skip_count += 1
            if action_type in _ACTIONABLE_SKIP_TYPES:
                attention_tasks.append((task, action))
            continue

        # Worker-spawning actions: check batch limit before proceeding
        if action_type in ('run_review', 'run_improve', 'create_review', 'create_implement', 'improve', 'resume'):
            if batch_limit is not None and workers_started >= batch_limit:
                print(f"  #{task.id} {prompt_display}")
                print(f"      — batch limit reached ({workers_started}/{batch_limit}), skipping")
                print()
                skip_count += 1
                continue

        print(f"  #{task.id} {prompt_display}")
        print(f"      → {action['description']}")

        if action_type == 'merge':
            # Determine whether to auto-squash based on commit count and threshold
            should_squash = False
            if config.merge_squash_threshold > 0 and task.branch:
                commit_count = git.count_commits_ahead(task.branch, default_branch)
                if commit_count >= config.merge_squash_threshold:
                    should_squash = True
            # Build a minimal args namespace for _merge_single_task
            merge_args = argparse.Namespace(
                rebase=False,
                squash=should_squash,
                delete=False,
                mark_only=False,
                remote=False,
                resolve=False,
            )
            rc = _merge_single_task(task.id, config, store, git, merge_args, default_branch)
            if rc == 0:
                print(f"      ✓ Merged")
                success_count += 1
            else:
                error_count += 1

        elif action_type == 'create_review':
            if task.task_type != 'implement':
                print(f"      SKIP: cannot create review for task type '{task.task_type}'")
                skip_count += 1
                continue

            # Check for an already-pending/in_progress review (idempotency guard)
            existing_reviews = store.get_reviews_for_task(task.id)
            active_reviews = [r for r in existing_reviews if r.status in ('pending', 'in_progress')]
            if active_reviews:
                print(f"      SKIP: review #{active_reviews[0].id} is already {active_reviews[0].status}")
                skip_count += 1
                continue

            from ..prompts import PromptBuilder
            review_prompt = PromptBuilder().review_task_prompt(task.id, task.prompt)
            review_task = store.add(
                prompt=review_prompt,
                task_type='review',
                depends_on=task.id,
                group=task.group,
                based_on=task.based_on,
            )
            print(f"      ✓ Created review task #{review_task.id}")

            # Spawn background worker to run the review
            assert review_task.id is not None
            worker_args = argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
            )
            rc = _spawn_background_worker(worker_args, config, task_id=review_task.id)
            workers_started += 1
            if rc == 0:
                print(f"      ✓ Started review worker")
                success_count += 1
            else:
                print(f"      ✗ Failed to start review worker")
                error_count += 1

        elif action_type == 'run_review':
            # Spawn worker for an existing pending review task
            review_task = action['review_task']
            assert review_task.id is not None
            worker_args = argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
            )
            rc = _spawn_background_worker(worker_args, config, task_id=review_task.id)
            workers_started += 1
            if rc == 0:
                print(f"      ✓ Started review worker for #{review_task.id}")
                success_count += 1
            else:
                print(f"      ✗ Failed to start review worker for #{review_task.id}")
                error_count += 1

        elif action_type == 'improve':
            review_task = action['review_task']
            assert review_task.id is not None

            from ..prompts import PromptBuilder
            improve_prompt = PromptBuilder().improve_task_prompt(task.id, review_task.id)
            improve_task = store.add(
                prompt=improve_prompt,
                task_type='improve',
                depends_on=review_task.id,
                based_on=task.id,
                same_branch=True,
                group=task.group,
            )
            print(f"      ✓ Created improve task #{improve_task.id}")

            # Spawn background worker to run the improve task
            assert improve_task.id is not None
            worker_args = argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
            )
            rc = _spawn_background_worker(worker_args, config, task_id=improve_task.id)
            workers_started += 1
            if rc == 0:
                print(f"      ✓ Started improve worker")
                success_count += 1
            else:
                print(f"      ✗ Failed to start improve worker")
                error_count += 1

        elif action_type == 'run_improve':
            # Spawn worker for an existing pending improve task
            improve_task = action['improve_task']
            assert improve_task.id is not None
            worker_args = argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
            )
            rc = _spawn_background_worker(worker_args, config, task_id=improve_task.id)
            workers_started += 1
            if rc == 0:
                print(f"      ✓ Started improve worker for #{improve_task.id}")
                success_count += 1
            else:
                print(f"      ✗ Failed to start improve worker for #{improve_task.id}")
                error_count += 1

        elif action_type == 'resume':
            # Create a resume task and spawn a background worker for it
            resume_task = _create_resume_task(store, task)
            assert resume_task.id is not None
            print(f"      ✓ Created resume task #{resume_task.id}")
            worker_args = argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
            )
            rc = _spawn_background_resume_worker(worker_args, config, resume_task.id)
            workers_started += 1
            if rc == 0:
                print(f"      ✓ Started resume worker")
                success_count += 1
            else:
                print(f"      ✗ Failed to start resume worker")
                error_count += 1

        elif action_type == 'create_implement':
            # Create an implement task for a completed plan and spawn a worker
            slug = _get_base_task_slug(task)
            prompt_text = f"Implement plan from task #{task.id}: {slug}" if slug else f"Implement plan from task #{task.id}"
            impl_task = store.add(
                prompt=prompt_text,
                task_type="implement",
                based_on=task.id,
                group=task.group,
            )
            print(f"      ✓ Created implement task #{impl_task.id}")

            assert impl_task.id is not None
            worker_args = argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
            )
            rc = _spawn_background_worker(worker_args, config, task_id=impl_task.id)
            workers_started += 1
            if rc == 0:
                print(f"      ✓ Started implement worker")
                success_count += 1
            else:
                print(f"      ✗ Failed to start implement worker")
                error_count += 1

        print()

    # --new: start pending tasks to fill remaining batch slots
    if new_mode and batch_limit is not None and workers_started < batch_limit:
        remaining = batch_limit - workers_started
        new_started = 0
        for _ in range(remaining):
            worker_args = argparse.Namespace(
                no_docker=getattr(args, 'no_docker', False),
                max_turns=None,
            )
            rc = _spawn_background_worker(worker_args, config)
            if rc != 0:
                break  # no more pending tasks or error
            new_started += 1
            workers_started += 1
        if new_started > 0:
            print(f"Started {new_started} new pending task(s) to fill batch")
            success_count += new_started

    print(f"Advanced: {success_count} task(s), skipped: {skip_count}, errors: {error_count}")

    if attention_tasks:
        print(f"\nNeeds attention ({len(attention_tasks)} task{'s' if len(attention_tasks) != 1 else ''}):")
        for atask, aaction in attention_tasks:
            prompt_display = truncate(atask.prompt, MAX_PROMPT_DISPLAY_SHORT)
            # Strip leading "SKIP: " prefix from description for display
            desc = aaction['description']
            if desc.startswith('SKIP: '):
                desc = desc[len('SKIP: '):]
            print(f"  #{atask.id}  {prompt_display}")
            print(f"       → {desc}")

    return 0 if error_count == 0 else 1
