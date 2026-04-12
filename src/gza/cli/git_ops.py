"""Git-related CLI commands: merge, rebase, checkout, diff, PR, refresh, advance."""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import gza.colors as _colors
from gza.query import get_base_task_slug as _get_base_task_slug

from .. import runner as runner_mod
from ..colors import pink
from ..commit_messages import build_task_commit_message
from ..config import Config
from ..console import (
    MAX_PR_BODY_LENGTH,
    MAX_PR_TITLE_LENGTH,
    console,
    prompt_available_width,
    shorten_prompt,
    truncate,
)
from ..db import SqliteTaskStore, Task as DbTask
from ..git import Git, GitError, cleanup_worktree_for_branch, parse_diff_numstat
from ..github import GitHub, GitHubError
from ..prompts import PromptBuilder
from ..runner import get_effective_config_for_task, load_dotenv
from ._common import (
    DuplicateReviewError,
    _create_rebase_task,
    _create_resume_task,
    _create_review_task,
    _get_pager,
    _looks_like_task_id,
    _spawn_background_iterate_worker,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    get_store,
    resolve_id,
)
from .advance_engine import determine_next_action

logger = logging.getLogger(__name__)


def cmd_refresh(args: argparse.Namespace) -> int:
    """Refresh cached diff stats for one or all unmerged tasks."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    default_branch = git.default_branch()

    if args.task_id is not None:
        # Single task by ID
        task_id = resolve_id(config, args.task_id)
        task = store.get(task_id)
        if task is None:
            console.print(f"[red]Error: Task {task_id} not found[/red]")
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
            console.print(f"[dim]{task.id}: no branch, skipping[/dim]")
            skipped += 1
            continue
        if not git.branch_exists(task.branch):
            console.print(f"[dim]{task.id} {task.branch}: branch no longer exists, skipping[/dim]")
            skipped += 1
            continue
        revision_range = f"{default_branch}...{task.branch}"
        numstat_output = git.get_diff_numstat(revision_range)
        files_changed, lines_added, lines_removed = parse_diff_numstat(numstat_output)
        assert task.id is not None
        store.update_diff_stats(task.id, files_changed, lines_added, lines_removed)
        console.print(f"{task.id} {task.branch}: +{lines_added} -{lines_removed} in {files_changed} files")
        refreshed += 1

    print(f"\nRefreshed {refreshed} task(s), skipped {skipped}.")
    return 0


def _require_default_branch(git: Git, current_branch: str, command: str) -> bool:
    """Enforce that a command is being run from the repo's default branch.

    Returns True if on default branch; prints an error and returns False otherwise.
    """
    default = git.default_branch()
    if current_branch != default:
        print(
            f"Error: `gza {command}` must be run from the default branch "
            f"'{default}' (currently on '{current_branch}')."
        )
        return False
    return True


def _merge_single_task(
    task_id: str,
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
        print(f"Error: Task {task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged"):
        print(f"Error: Task {task.id} is not completed or unmerged (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task {task.id} has no branch")
        return 1

    # Handle --mark-only flag
    if args.mark_only:
        # Check for conflicting flags
        if args.rebase or args.squash or args.delete:
            print("Error: --mark-only cannot be used with --rebase, --squash, or --delete")
            return 1

        store.set_merge_status(task.id, "merged")
        print(f"✓ Marked task {task.id} as merged (branch '{task.branch}' preserved)")
        return 0

    # Check if branch already merged
    if git.is_merged(task.branch, current_branch):
        default_branch = git.default_branch()
        if current_branch != default_branch and not git.is_merged(task.branch, default_branch):
            print(
                f"Error: Branch '{task.branch}' is already merged into current branch "
                f"'{current_branch}', but still unmerged from default branch '{default_branch}'"
            )
        else:
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
                print("Fetching from origin...")
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
            print(f"Merging '{task.branch}' into '{current_branch}'...")

            # For squash merge, create a commit message from the task
            commit_message = None
            if args.squash:
                assert task.id is not None, "Task ID must be set before squash merge commit"
                commit_message = build_task_commit_message(
                    task.prompt,
                    task_id=task.id,
                    task_slug=task.slug,
                    subject_prefix="Squash merge: ",
                )

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
            print("Conflicts detected. Invoking provider to resolve...")
            resolved = invoke_provider_resolve(task, task.branch, rebase_target, config)

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
    print(f"On branch {current_branch}")

    # --mark-only is a DB-only escape hatch for users who merge manually;
    # it does not run git operations so the default-branch rule does not apply.
    if getattr(args, 'mark_only', False):
        default = git.default_branch()
        if current_branch != default:
            print(
                f"Note: --mark-only on non-default branch "
                f"'{current_branch}' (default is '{default}')"
            )
    else:
        if not _require_default_branch(git, current_branch, "merge"):
            return 1

    # Determine the list of task IDs to merge
    task_ids = [resolve_id(config, tid) for tid in args.task_ids]

    use_all = getattr(args, 'all', False)
    if use_all:
        # Find all completed/unmerged tasks with branches not yet merged
        history = store.get_history(limit=None)
        seen_ids = set(task_ids)
        # Process oldest first (history is newest-first, so reverse it)
        for task in reversed(history):
            if task.id in seen_ids:
                continue
            if task.id is not None and task.status in ("completed", "unmerged") and task.branch and task.has_commits:
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
            print(f"Merging task {task_id}...")
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
        print(f"\n✓ Successfully merged {len(merged_tasks)} task(s): {', '.join(str(tid) for tid in merged_tasks)}")

    if failed_task_id is not None:
        remaining = [tid for tid in task_ids if tid not in merged_tasks and tid != failed_task_id]
        if remaining:
            print(f"⚠ Stopped at task {failed_task_id}. Remaining tasks not processed: {', '.join(str(tid) for tid in remaining)}")
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


def _is_rebase_in_progress(worktree_path: Path) -> bool:
    """Check if a git rebase is in progress in the given directory.

    Handles both regular repositories and git worktrees (where .git is a file
    pointing to the actual gitdir).
    """
    git_file = worktree_path / ".git"
    if git_file.is_file():
        try:
            git_dir_text = git_file.read_text().strip()
            if git_dir_text.startswith("gitdir: "):
                raw = git_dir_text[len("gitdir: "):]
                git_dir: Path = Path(raw) if Path(raw).is_absolute() else (worktree_path / raw).resolve()
            else:
                git_dir = git_file
        except OSError:
            git_dir = worktree_path / ".git"
    else:
        git_dir = worktree_path / ".git"
    return (git_dir / "rebase-merge").exists() or (git_dir / "rebase-apply").exists()


def invoke_provider_resolve(
    task: DbTask,
    branch: str,
    target: str,
    config: Config,
    *,
    worktree_path: Path | None = None,
) -> bool:
    """Invoke active provider runtime to resolve rebase conflicts via /gza-rebase.

    When *worktree_path* is given the provider is run inside that worktree and
    the skill is installed there.  When omitted the legacy behaviour is used:
    the provider runs in the project directory with ``/gza-rebase --auto
    --continue`` (rebase already in progress in the main working tree).
    """
    from dataclasses import replace

    from ..providers import get_provider

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

    # When running in a worktree, install the skill there so the provider finds it.
    if worktree_path is not None:
        from ..skills_utils import copy_skill
        worktree_skills_dir = worktree_path / ".claude" / "skills"
        worktree_skills_dir.mkdir(parents=True, exist_ok=True)
        ok, msg = copy_skill("gza-rebase", worktree_skills_dir)
        if not ok:
            logger.warning("Failed to copy gza-rebase skill to worktree: %s", msg)

    resolve_config = replace(
        config,
        provider=effective_provider,
        model=effective_model or "",
        max_steps=effective_max_steps,
        max_turns=effective_max_steps,
    )

    log_dir = config.project_dir / config.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file = log_dir / f"resolve-{timestamp}.log"

    load_dotenv(config.project_dir)
    provider = get_provider(resolve_config)
    store = get_store(config)
    task_id_label = getattr(task, "id", None)
    task_ref = f"{task_id_label}" if task_id_label is not None else "<unknown>"

    if worktree_path is not None:
        # New worktree-based flow: fresh rebase, no --continue needed.
        skill_cmd = "/gza-rebase --auto"
        work_dir = worktree_path
    else:
        # Legacy flow: rebase already in progress in the main working tree.
        skill_cmd = "/gza-rebase --auto --continue"
        work_dir = config.project_dir

    internal_prompt = (
        f"Resolve rebase conflicts for task {task_ref} branch '{branch}' onto '{target}' "
        f"using {skill_cmd}."
    )
    internal_task = store.add(
        prompt=internal_prompt,
        task_type="internal",
        skip_learnings=True,
    )
    store.mark_in_progress(internal_task)

    try:
        run_result = provider.run(resolve_config, skill_cmd, log_file, work_dir)
    except Exception:
        store.mark_failed(internal_task, log_file=str(log_file), failure_reason="UNKNOWN")
        raise

    if run_result.exit_code != 0:
        store.mark_failed(internal_task, log_file=str(log_file), failure_reason="UNKNOWN")
        return False

    # Check if rebase completed (no longer in rebase state).
    if worktree_path is not None:
        rebase_in_progress = _is_rebase_in_progress(worktree_path)
    else:
        rebase_in_progress = (
            (config.project_dir / ".git" / "rebase-merge").exists()
            or (config.project_dir / ".git" / "rebase-apply").exists()
        )
    output_content = (
        f"Resolved rebase conflicts with {skill_cmd}."
        if not rebase_in_progress
        else f"Rebase still in progress after {skill_cmd}."
    )
    if rebase_in_progress:
        store.mark_failed(internal_task, log_file=str(log_file), failure_reason="UNKNOWN")
        return False

    store.mark_completed(
        internal_task,
        log_file=str(log_file),
        output_content=output_content,
        has_commits=False,
    )
    return True


def cmd_rebase(args: argparse.Namespace) -> int:
    """Rebase a task's branch onto a target branch."""
    config = Config.load(args.project_dir)
    task_id = resolve_id(config, args.task_id)
    git = Git(config.project_dir)

    current_branch = git.current_branch()
    if not _require_default_branch(git, current_branch, "rebase"):
        return 1

    # Handle background mode - create a rebase task and run through the standard runner
    if getattr(args, 'background', False):
        store = get_store(config)
        task = store.get(task_id)
        if not task:
            print(f"Error: Task {task_id} not found")
            return 1
        if not task.branch:
            print(f"Error: Task {task_id} has no branch")
            return 1
        target = getattr(args, 'onto', None) or git.default_branch()
        if getattr(args, 'remote', False):
            target = f"origin/{target}"
        rebase_task = _create_rebase_task(store, task_id, task.branch, target)
        worker_args = argparse.Namespace(no_docker=False, max_turns=None)
        return _spawn_background_worker(worker_args, config, task_id=rebase_task.id)

    store = get_store(config)

    # Get the task
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged", "running"):
        print(f"Error: Task {task.id} is not completed, unmerged, or running (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task {task.id} has no branch")
        return 1

    # Check if branch exists
    if not git.branch_exists(task.branch):
        print(f"Error: Branch '{task.branch}' does not exist")
        return 1

    print(f"On branch {current_branch}")

    # Determine rebase target: use --onto if provided, else current branch
    rebase_target = getattr(args, 'onto', None) or current_branch

    # Handle --remote flag
    if hasattr(args, 'remote') and args.remote:
        print("Fetching from origin...")
        git.fetch("origin")
        print("✓ Fetched from origin")
        rebase_target = f"origin/{rebase_target}"

    # Set up the task's worktree so the rebase never touches the main working tree.
    # --force and --resolve are accepted for backward compatibility but are now no-ops:
    # worktrees are always force-cleaned and auto-resolve is always the fallback.
    worktree_path = config.worktree_path / str(task.id)
    print(f"Rebasing task {task.id}...")
    try:
        # Remove any existing worktree for this branch (may be at a different path).
        stale_path = cleanup_worktree_for_branch(git, task.branch, force=True)
        if stale_path:
            print(f"Removing stale worktree at {stale_path}...")
            print("✓ Removed worktree")
        # Also clean up the target path if it still exists (may be an orphaned dir
        # not registered with git, in which case worktree_remove is a no-op).
        if worktree_path.exists():
            git.worktree_remove(worktree_path, force=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), task.branch)
    except GitError as e:
        print(f"Error setting up worktree: {e}")
        return 1

    worktree_git = Git(worktree_path)

    try:
        # Attempt a mechanical rebase inside the worktree (no LLM needed if it succeeds).
        print(f"Rebasing '{task.branch}' onto '{rebase_target}'...")
        try:
            worktree_git.rebase(rebase_target)
            print(f"✓ Successfully rebased {task.branch} onto {rebase_target}")
            print()
            return 0

        except GitError as e:
            # Conflicts detected — abort so the worktree is clean before the provider runs.
            print(f"Conflicts detected: {e}")
            try:
                worktree_git.rebase_abort()
            except GitError:
                pass

        # Fall back to provider-driven resolution via /gza-rebase --auto.
        print("Invoking provider to resolve via /gza-rebase --auto...")
        resolved = invoke_provider_resolve(
            task, task.branch, rebase_target, config, worktree_path=worktree_path
        )

        if not resolved:
            print("Could not resolve conflicts automatically.")
            print()
            return 1

        # Force-push the resolved branch (provider never pushes automatically).
        print(f"Pushing {task.branch}...")
        worktree_git.push_force_with_lease(task.branch)

        print(f"✓ Successfully rebased {task.branch}")
        print()
        return 0

    finally:
        # Clean up the temporary worktree on all exit paths (success, failure, exception).
        try:
            git.worktree_remove(worktree_path, force=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
        except Exception:
            logger.warning("Failed to remove rebase worktree at %s", worktree_path)


def cmd_checkout(args: argparse.Namespace) -> int:
    """Checkout a task's branch, removing any stale worktree if needed."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)

    # Resolve task ID or branch name
    task = None
    branch = None

    arg = args.task_id_or_branch
    if _looks_like_task_id(arg):
        resolved_task_id = resolve_id(config, arg)
        task = store.get(resolved_task_id)
        if task is not None:
            if not task.branch:
                print(f"Error: Task {task.id} has no branch")
                return 1
            branch = task.branch
        else:
            # Not found as a task ID — fall back to treating it as a branch name
            branch = arg
    else:
        # It's a branch name
        branch = arg

    # Check if branch exists
    if not git.branch_exists(branch):
        print(f"Error: Branch '{branch}' does not exist locally")
        return 1

    # Clean up worktree if branch is checked out in one
    try:
        worktree_path = cleanup_worktree_for_branch(git, branch, force=args.force)
        if worktree_path:
            print(f"Removing stale worktree at {worktree_path}...")
            print("✓ Removed worktree")
    except (ValueError, GitError) as e:
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

    if diff_args and not diff_args[0].startswith("-") and _looks_like_task_id(diff_args[0]):
        # First argument is a full prefixed decimal task ID ("prefix-decimal").
        task_id: str = resolve_id(config, diff_args[0])
        task = store.get(task_id)

        if not task:
            # Not found as a task ID — fall back to treating arg as a branch/ref, same
            # as cmd_checkout does.
            pass
        elif not task.branch:
            print(f"Error: Task {task_id} has no branch")
            return 1
        else:
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


def _generate_pr_content(
    task: DbTask,
    commit_log: str,
    diff_stat: str,
    config: Config,
    store: SqliteTaskStore,
) -> tuple[str, str]:
    """Generate PR title and body using an internal task.

    Args:
        task: The task to create a PR for
        commit_log: Git log output for the branch
        diff_stat: Git diff --stat output

    Returns:
        Tuple of (title, body)
    """
    # Build prompt using the strict PR description contract template.
    prompt = PromptBuilder().pr_description_prompt(
        task_prompt=task.prompt,
        commit_log=commit_log,
        diff_stat=diff_stat,
    )

    internal_task = store.add(
        prompt=prompt,
        task_type="internal",
        skip_learnings=True,
    )

    if internal_task.id is None:
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)
    internal_task_id = internal_task.id

    def _mark_internal_task_failed_if_nonterminal() -> None:
        refreshed = store.get(internal_task_id)
        if refreshed is None:
            return
        if refreshed.status in {"pending", "in_progress"}:
            store.mark_failed(refreshed, failure_reason="UNKNOWN")

    try:
        exit_code = runner_mod.run(config, task_id=internal_task_id)
    except Exception as exc:
        _mark_internal_task_failed_if_nonterminal()
        print(
            f"Warning: PR description internal task {internal_task_id} failed: {exc}",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    completed_task = store.get(internal_task_id)
    if exit_code != 0 or completed_task is None or completed_task.status != "completed":
        _mark_internal_task_failed_if_nonterminal()
        print(
            f"Warning: PR description internal task {internal_task_id} did not complete successfully",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    response = (completed_task.output_content or "").strip()
    if not response:
        print(
            f"Warning: PR description internal task {internal_task_id} produced no output",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    has_title = any(line.startswith("TITLE:") for line in response.splitlines())
    has_body = any(line.strip() == "BODY:" for line in response.splitlines())
    if not (has_title and has_body):
        print(
            f"Warning: PR description internal task {internal_task_id} produced malformed output",
            file=sys.stderr,
        )
        return _fallback_pr_content(task, commit_log, project_prefix=config.project_prefix or None)

    return _parse_pr_response(response, task)



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
        title = task.slug or truncate(task.prompt.split("\n")[0], MAX_PR_TITLE_LENGTH)

    body = "\n".join(body_lines).strip()
    if not body:
        body = f"Task: {truncate(task.prompt, MAX_PR_BODY_LENGTH)}"

    return title, body


def _fallback_pr_content(
    task: DbTask, commit_log: str, project_prefix: str | None = None
) -> tuple[str, str]:
    """Generate simple PR content without AI."""
    # Title from task_id or prompt
    if task.slug:
        # Convert slug like "20240106-myproj-add-feature" to "Add feature"
        # Strip date prefix (8-digit prefix + hyphen)
        slug_no_date = task.slug.split("-", 1)[1] if "-" in task.slug else task.slug
        # Strip project_prefix if present at the start
        if project_prefix and slug_no_date.startswith(f"{project_prefix}-"):
            slug_no_date = slug_no_date[len(project_prefix) + 1:]
        title = slug_no_date.replace("-", " ").capitalize()
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
    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    # Validate task state
    if task.status not in ("completed", "unmerged"):
        print(f"Error: Task {task.id} is not completed (status: {task.status})")
        return 1

    if not task.branch:
        print(f"Error: Task {task.id} has no branch")
        return 1

    if not task.has_commits:
        print(f"Error: Task {task.id} has no commits")
        return 1

    # Check merge_status before requiring gh (local DB check, no external dependencies)
    if task.merge_status == "merged":
        print(f"Error: Task {task.id} is already marked as merged")
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
        title, body = _generate_pr_content(task, commit_log, diff_stat, config, store)

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


def _determine_advance_action(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    target_branch: str,
    impl_based_on_ids: set[str] | None = None,
    max_resume_attempts: int | None = None,
) -> dict:
    """Backward-compatible wrapper around the shared advance engine."""
    return determine_next_action(
        config,
        store,
        git,
        task,
        target_branch,
        impl_based_on_ids=impl_based_on_ids,
        max_resume_attempts=max_resume_attempts,
    )


def _unimplemented_implement_prompt(task: DbTask) -> str:
    """Build the default implement prompt for a completed upstream task."""
    assert task.id is not None
    slug = _get_base_task_slug(task)
    if task.task_type == "plan":
        return f"Implement plan from task {task.id}: {slug}" if slug else f"Implement plan from task {task.id}"
    return f"Implement findings from task {task.id}: {slug}" if slug else f"Implement findings from task {task.id}"


def _cmd_advance_unimplemented(
    config: "Config",
    store: SqliteTaskStore,
    dry_run: bool = False,
    create: bool = False,
    task_types: tuple[str, ...] = ("plan", "explore"),
) -> int:
    """List completed task types that have no implementation task.

    With --create, creates queued implement tasks for each such task.
    """
    all_completed: list[DbTask] = []
    for task_type in task_types:
        all_completed.extend(store.get_history(limit=None, status="completed", task_type=task_type))

    # Find tasks that have no implement task pointing at them (via based_on).
    # Use a targeted query instead of a full table scan to avoid loading every
    # task (including output_content blobs) into memory.
    impl_based_on_ids: set[str] = store.get_impl_based_on_ids()

    pending_tasks = [task for task in all_completed if task.id not in impl_based_on_ids]

    if not pending_tasks:
        task_label = "/".join(task_types)
        print(f"No completed {task_label} tasks without implementation tasks.")
        return 0

    task_label = "/".join(task_types)
    print(f"Completed {task_label} tasks without implementation ({len(pending_tasks)}):")
    print()
    for task in pending_tasks:
        assert task.id is not None
        prefix_len = len(f"  {task.id}  [{task.task_type}] ")
        prompt_display = shorten_prompt(task.prompt, prompt_available_width(prefix=prefix_len))
        print(f"  {task.id}  [{task.task_type}] {prompt_display}")
        print(f"       → gza implement {task.id}")
    print()

    if not create:
        print("Run 'gza advance' to create and start implement tasks for completed plan tasks.")
        if "explore" in task_types:
            print("Run 'gza advance --unimplemented --create' to create implement tasks for completed explore tasks.")
        return 0

    # Create queued implement tasks
    created_count = 0
    for task in pending_tasks:
        assert task.id is not None
        if dry_run:
            print(f"[dry-run] Would create implement task for {task.task_type} {task.id}")
            continue
        prompt_text = _unimplemented_implement_prompt(task)
        impl_task = store.add(
            prompt=prompt_text,
            task_type="implement",
            based_on=task.id,
            group=task.group,
        )
        print(f"✓ Created implement task {impl_task.id} for {task.task_type} {task.id}")
        created_count += 1

    if not dry_run:
        print(f"\nCreated {created_count} implement task(s). Run 'gza work' to execute them.")
    return 0


# Maps advance action types to their execution priority (lower = runs first).
# 'merge' actions are fast and synchronous; running them first ensures freshly
# merged code is on the current branch before any review/improve workers are
# spawned, reducing rebase conflicts for those workers.
_ADVANCE_ACTION_ORDER: dict[str, int] = {'merge': 0}


def _advance_action_color(action_type: str) -> str:
    """Return a Rich color for an advance action type."""
    ac = _colors.ADVANCE_COLORS
    if action_type == 'merge':
        return ac.merge
    if action_type in ('needs_rebase', 'needs_discussion', 'max_cycles_reached', 'max_resume_attempts'):
        return ac.error
    if action_type in ('skip', 'wait_review', 'wait_improve'):
        return ac.waiting
    return ac.default


def cmd_advance(args: argparse.Namespace) -> int:
    """Intelligently progress unmerged tasks through their lifecycle."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    # Themed advance colors — resolved once after Config.load() applies the theme.
    _ac = _colors.ADVANCE_COLORS
    _c_tid = _colors.TASK_COLORS.task_id
    _c_ok = _ac.merge
    _c_err = _ac.error
    _c_warn = _ac.waiting
    _c_default = _ac.default
    # Prefix for advance lines: "  #NNN " — compute available prompt width per task.
    def _prompt_avail(task_id: str | None) -> int:
        return prompt_available_width(prefix=len(task_id or "") + 4)  # "  #NNN "
    git = Git(config.project_dir)

    dry_run: bool = args.dry_run
    auto: bool = getattr(args, 'auto', False)
    max_tasks: int | None = getattr(args, 'max', None)
    batch_limit: int | None = getattr(args, 'batch', None)
    force: bool = getattr(args, 'force', False)
    task_id: str | None = resolve_id(config, args.task_id) if getattr(args, 'task_id', None) is not None else None
    plans_mode: bool = getattr(args, 'plans', False)
    unimplemented_mode: bool = getattr(args, 'unimplemented', False)
    create_mode: bool = getattr(args, 'create', False)
    no_resume_failed: bool = getattr(args, 'no_resume_failed', False)
    max_resume_attempts_override: int | None = getattr(args, 'max_resume_attempts', None)
    advance_mode: str = config.advance_mode
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

    # --unimplemented mode: list completed plans/explores without implementations
    # Legacy --plans is supported as an alias scoped to plans only.
    if unimplemented_mode or plans_mode:
        unimplemented_types: tuple[str, ...] = ("plan",) if plans_mode and not unimplemented_mode else ("plan", "explore")
        if plans_mode:
            print("Warning: --plans is deprecated. Use --unimplemented instead.", file=sys.stderr)
        return _cmd_advance_unimplemented(
            config,
            store,
            dry_run=dry_run,
            create=create_mode,
            task_types=unimplemented_types,
        )

    # Pre-compute the set of plan IDs that already have implement children
    # to avoid repeated DB queries in _determine_advance_action.
    impl_based_on_ids: set[str] = store.get_impl_based_on_ids()

    # Determine which tasks to advance
    if task_id is not None:
        task = store.get(task_id)
        if not task:
            print(f"Error: Task {task_id} not found")
            return 1
        if task.status == 'failed':
            # Allow a specific failed task if it's resumable
            is_resumable = (
                task.failure_reason in ('MAX_STEPS', 'MAX_TURNS')
                and task.session_id is not None
                and not no_resume_failed
            )
            if not is_resumable:
                print(f"Error: Task {task_id} is not completed (status: {task.status})")
                return 1
            tasks = [task]
        else:
            if task.status != 'completed':
                print(f"Error: Task {task_id} is not completed (status: {task.status})")
                return 1
            if task.merge_status == 'merged':
                print(f"Task {task_id} is already merged")
                return 0
            tasks = [task]
    else:
        # Get all unmerged completed tasks
        all_unmerged = store.get_unmerged()
        tasks = [t for t in all_unmerged if t.status == 'completed']
        # Also collect resumable failed tasks (unless disabled)
        if not no_resume_failed:
            tasks.extend(store.get_resumable_failed_tasks())

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
        elif advance_type == 'implement':
            tasks = [t for t in tasks if t.task_type == 'implement']

    if not tasks and not new_mode:
        print("No eligible tasks to advance")
        return 0

    # Apply --max limit
    if max_tasks is not None:
        tasks = tasks[:max_tasks]

    # Use the currently checked-out branch as the target for conflict checks,
    # merge execution, and rebase task creation.
    target_branch = git.current_branch()

    # Analyze each task to determine the next action
    plan: list[tuple[DbTask, dict]] = []
    for task in tasks:
        action = _determine_advance_action(
            config,
            store,
            git,
            task,
            target_branch,
            impl_based_on_ids=impl_based_on_ids,
            max_resume_attempts=max_resume_attempts,
        )
        if (
            task.status == "failed"
            and action.get("type") == "skip"
            and action.get("description") == "SKIP: resume child already exists"
        ):
            continue
        plan.append((task, action))

    # Sort so merges execute before worker spawns. See _ADVANCE_ACTION_ORDER for
    # the rationale. The sort is stable, preserving DB order within each group.
    # dry-run output inherits this order, so it accurately reflects execution.
    plan.sort(key=lambda item: _ADVANCE_ACTION_ORDER.get(item[1]['type'], 1))

    # If the plan is empty or every item is a skip, there's nothing actionable
    # (unless --new is set, in which case we still want to start pending tasks).
    if not plan or all(action['type'] in {'skip', 'max_resume_attempts'} for _, action in plan):
        if not new_mode:
            print("No eligible tasks to advance")
            if plan:
                print()
                for task, action in plan:
                    prompt_display = shorten_prompt(task.prompt, _prompt_avail(task.id))
                    console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    _color = _advance_action_color(action['type'])
                    console.print(f"      [{_color}]→ {action['description']}[/{_color}]")
                print()
            return 0
        else:
            # --new with no existing actions: skip straight to spawning new tasks
            if plan:
                for task, action in plan:
                    prompt_display = shorten_prompt(task.prompt, _prompt_avail(task.id))
                    console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    _color = _advance_action_color(action['type'])
                    console.print(f"      [{_color}]→ {action['description']}[/{_color}]")
                print()

    if dry_run:
        print(f"Would advance {len(plan)} task(s):\n")
        for task, action in plan:
            prompt_display = shorten_prompt(task.prompt, _prompt_avail(task.id))
            console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            description = action['description']
            if action['type'] == 'merge' and config.merge_squash_threshold > 0 and task.branch:
                commit_count = git.count_commits_ahead(task.branch, target_branch)
                if commit_count >= config.merge_squash_threshold:
                    description = f"{description} (auto-squash, {commit_count} commits)"
            _color = _advance_action_color(action['type'])
            console.print(f"      [{_color}]→ {description}[/{_color}]")
            print()
        if new_mode and batch_limit is not None:
            worker_action_types = frozenset({'run_review', 'run_improve', 'create_review', 'create_implement', 'improve', 'resume'})
            planned_workers = sum(1 for _, a in plan if a['type'] in worker_action_types)
            remaining = max(0, batch_limit - planned_workers)
            if remaining > 0:
                pending_tasks = store.get_pending(limit=remaining)
                if pending_tasks:
                    print(f"Would start {len(pending_tasks)} new pending task(s):\n")
                    for pt in pending_tasks:
                        prompt_display = shorten_prompt(pt.prompt, _prompt_avail(pt.id))
                        console.print(f"  [{_c_tid}]{pt.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                        console.print(f"      [{_c_default}]→ Start new worker[/{_c_default}]")
                        print()
                else:
                    print("No pending tasks available to fill batch\n")
        return 0

    # Show the plan and prompt for confirmation
    actionable_plan = [item for item in plan if item[1]['type'] not in {'skip', 'max_resume_attempts'}]
    if actionable_plan:
        print(f"Will advance {len(actionable_plan)} task(s):\n")
        for task, action in plan:
            prompt_display = shorten_prompt(task.prompt, _prompt_avail(task.id))
            console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            _color = _advance_action_color(action['type'])
            console.print(f"      [{_color}]→ {action['description']}[/{_color}]")
            print()

    new_pending_tasks: list = []
    if new_mode and batch_limit is not None:
        worker_action_types = frozenset({'run_review', 'run_improve', 'create_review', 'improve', 'resume'})
        planned_workers = sum(1 for _, a in plan if a['type'] in worker_action_types)
        remaining = max(0, batch_limit - planned_workers)
        if remaining > 0:
            new_pending_tasks = store.get_pending(limit=remaining)
            if new_pending_tasks:
                print(f"Will start {len(new_pending_tasks)} new pending task(s):\n")
                for pt in new_pending_tasks:
                    prompt_display = shorten_prompt(pt.prompt, _prompt_avail(pt.id))
                    console.print(f"  [{_c_tid}]{pt.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                    console.print(f"      [{_c_default}]→ Start new worker[/{_c_default}]")
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
    _ACTIONABLE_SKIP_TYPES = frozenset({'needs_discussion', 'max_cycles_reached', 'max_resume_attempts'})
    attention_tasks: list[tuple[DbTask, dict]] = []

    def _worker_args() -> argparse.Namespace:
        return argparse.Namespace(
            no_docker=getattr(args, 'no_docker', False),
            max_turns=None,
            force=force,
        )

    for task, action in plan:
        assert task.id is not None
        prompt_display = shorten_prompt(task.prompt, _prompt_avail(task.id))
        action_type = action['type']

        if action_type in ('wait_review', 'wait_improve', 'needs_discussion', 'skip', 'max_cycles_reached', 'max_resume_attempts'):
            console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            _color = _advance_action_color(action_type)
            console.print(f"      [{_color}]{action['description']}[/{_color}]")
            skip_count += 1
            if action_type in _ACTIONABLE_SKIP_TYPES:
                attention_tasks.append((task, action))
            continue

        # Worker-spawning actions: check batch limit before proceeding
        if action_type in ('needs_rebase', 'run_review', 'run_improve', 'create_review', 'create_implement', 'improve', 'resume'):
            if batch_limit is not None and workers_started >= batch_limit:
                console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                console.print(f"      [{_c_warn}]— batch limit reached ({workers_started}/{batch_limit}), skipping[/{_c_warn}]")
                print()
                skip_count += 1
                continue

        console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
        _color = _advance_action_color(action_type)
        console.print(f"      [{_color}]→ {action['description']}[/{_color}]")

        if advance_mode == "iterate" and action_type in {
            "create_review",
            "run_review",
            "improve",
            "run_improve",
            "needs_rebase",
            "resume",
            "create_implement",
        }:
            iterate_target: DbTask | None = None
            iterate_resume = False

            if action_type == "create_implement":
                prompt_text = _unimplemented_implement_prompt(task)
                impl_task = store.add(
                    prompt=prompt_text,
                    task_type="implement",
                    based_on=task.id,
                    group=task.group,
                )
                assert impl_task.id is not None
                console.print(f"      [{_c_ok}]✓ Created implement task {impl_task.id}[/{_c_ok}]")
                iterate_target = impl_task
            elif task.task_type == "implement":
                iterate_target = task
                iterate_resume = action_type == "resume"
            else:
                iterate_target = None

            if iterate_target is None or iterate_target.id is None:
                console.print(
                    f"      [{_c_warn}]SKIP: iterate mode requires an implement task target[/{_c_warn}]"
                )
                skip_count += 1
                print()
                continue

            worker_args = argparse.Namespace(
                no_docker=getattr(args, "no_docker", False),
            )
            rc = _spawn_background_iterate_worker(
                worker_args,
                config,
                iterate_target,
                max_iterations=config.iterate_max_iterations,
                resume=iterate_resume,
                retry=False,
                quiet=True,
            )
            workers_started += 1
            if rc == 0:
                console.print(
                    f"      [{_c_ok}]✓ Started iterate worker for {iterate_target.id}[/{_c_ok}]"
                )
                success_count += 1
            else:
                console.print(
                    f"      [{_c_err}]✗ Failed to start iterate worker for {iterate_target.id}[/{_c_err}]"
                )
                error_count += 1
            print()
            continue

        if action_type == 'merge':
            # Determine whether to auto-squash based on commit count and threshold
            should_squash = False
            if config.merge_squash_threshold > 0 and task.branch:
                commit_count = git.count_commits_ahead(task.branch, target_branch)
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
            rc = _merge_single_task(task.id, config, store, git, merge_args, target_branch)
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Merged[/{_c_ok}]")
                success_count += 1
            else:
                task_branch = task.branch
                conflict_detected = (
                    task_branch is not None and not git.can_merge(task_branch, target_branch)
                )
                if conflict_detected:
                    console.print(f"      [{_c_warn}]! Merge had conflicts against '{target_branch}'[/{_c_warn}]")
                    try:
                        # _merge_single_task already attempts merge --abort.
                        # For failed squash merges, MERGE_HEAD may be absent, so
                        # force cleanup as a final fallback.
                        git.reset_hard_head()
                        console.print(f"      [{_c_ok}]✓ Restored clean git state[/{_c_ok}]")
                    except GitError as cleanup_error:
                        console.print(
                            f"      [{_c_err}]✗ Cleanup failed after merge conflict: {cleanup_error}. "
                            f"Manual intervention required.[/{_c_err}]"
                        )
                        error_count += 1
                        continue
                    assert task_branch is not None  # guaranteed by conflict_detected guard
                    rebase_task = _create_rebase_task(store, task.id, task_branch, target_branch)
                    assert rebase_task.id is not None
                    console.print(
                        f"      [{_c_ok}]✓ Created rebase task {rebase_task.id} "
                        f"(target: {target_branch})[/{_c_ok}]"
                    )
                    worker_args = argparse.Namespace(
                        no_docker=getattr(args, 'no_docker', False),
                        max_turns=None,
                        force=force,
                    )
                    rebase_rc = _spawn_background_worker(worker_args, config, task_id=rebase_task.id, quiet=True)
                    workers_started += 1
                    if rebase_rc == 0:
                        console.print(f"      [{_c_ok}]✓ Started rebase worker[/{_c_ok}]")
                        success_count += 1
                    else:
                        console.print(f"      [{_c_err}]✗ Failed to start rebase worker[/{_c_err}]")
                        error_count += 1
                else:
                    console.print(f"      [{_c_err}]✗ Merge failed[/{_c_err}]")
                    error_count += 1

        elif action_type == 'create_review':
            try:
                review_task = _create_review_task(store, task)
            except DuplicateReviewError as e:
                review_task = e.active_review
                console.print(f"      [{_c_warn}]SKIP: review {review_task.id} is already {review_task.status}[/{_c_warn}]")
                skip_count += 1
                continue
            except ValueError as e:
                console.print(f"      [{_c_warn}]SKIP: {e}[/{_c_warn}]")
                skip_count += 1
                continue
            console.print(f"      [{_c_ok}]✓ Created review task {review_task.id}[/{_c_ok}]")

            # Spawn background worker to run the review
            assert review_task.id is not None
            worker_args = _worker_args()
            rc = _spawn_background_worker(worker_args, config, task_id=review_task.id, quiet=True)
            workers_started += 1
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Started review worker[/{_c_ok}]")
                success_count += 1
            else:
                console.print(f"      [{_c_err}]✗ Failed to start review worker[/{_c_err}]")
                error_count += 1

        elif action_type == 'run_review':
            # Spawn worker for an existing pending review task
            review_task = action['review_task']
            assert review_task.id is not None
            worker_args = _worker_args()
            rc = _spawn_background_worker(worker_args, config, task_id=review_task.id, quiet=True)
            workers_started += 1
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Started review worker for {review_task.id}[/{_c_ok}]")
                success_count += 1
            else:
                console.print(f"      [{_c_err}]✗ Failed to start review worker for {review_task.id}[/{_c_err}]")
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
            console.print(f"      [{_c_ok}]✓ Created improve task {improve_task.id}[/{_c_ok}]")

            # Spawn background worker to run the improve task
            assert improve_task.id is not None
            worker_args = _worker_args()
            rc = _spawn_background_worker(worker_args, config, task_id=improve_task.id, quiet=True)
            workers_started += 1
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Started improve worker[/{_c_ok}]")
                success_count += 1
            else:
                console.print(f"      [{_c_err}]✗ Failed to start improve worker[/{_c_err}]")
                error_count += 1

        elif action_type == 'run_improve':
            # Spawn worker for an existing pending improve task
            improve_task = action['improve_task']
            assert improve_task.id is not None
            worker_args = _worker_args()
            rc = _spawn_background_worker(worker_args, config, task_id=improve_task.id, quiet=True)
            workers_started += 1
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Started improve worker for {improve_task.id}[/{_c_ok}]")
                success_count += 1
            else:
                console.print(f"      [{_c_err}]✗ Failed to start improve worker for {improve_task.id}[/{_c_err}]")
                error_count += 1

        elif action_type == 'resume':
            # Create a resume task and spawn a background worker for it
            resume_task = _create_resume_task(store, task)
            assert resume_task.id is not None
            console.print(f"      [{_c_ok}]✓ Created resume task {resume_task.id}[/{_c_ok}]")
            worker_args = _worker_args()
            rc = _spawn_background_resume_worker(worker_args, config, resume_task.id, quiet=True)
            workers_started += 1
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Started resume worker[/{_c_ok}]")
                success_count += 1
            else:
                console.print(f"      [{_c_err}]✗ Failed to start resume worker[/{_c_err}]")
                error_count += 1

        elif action_type == 'create_implement':
            # Create an implement task for a completed plan and spawn a worker
            prompt_text = _unimplemented_implement_prompt(task)
            impl_task = store.add(
                prompt=prompt_text,
                task_type="implement",
                based_on=task.id,
                group=task.group,
            )
            console.print(f"      [{_c_ok}]✓ Created implement task {impl_task.id}[/{_c_ok}]")

            assert impl_task.id is not None
            worker_args = _worker_args()
            rc = _spawn_background_worker(worker_args, config, task_id=impl_task.id, quiet=True)
            workers_started += 1
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Started implement worker[/{_c_ok}]")
                success_count += 1
            else:
                console.print(f"      [{_c_err}]✗ Failed to start implement worker[/{_c_err}]")
                error_count += 1

        elif action_type == 'needs_rebase':
            assert task.id is not None
            if not task.branch:
                console.print(f"      [{_c_err}]✗ Cannot rebase: task {task.id} has no branch[/{_c_err}]")
                error_count += 1
                continue
            rebase_task = _create_rebase_task(store, task.id, task.branch, target_branch)
            assert rebase_task.id is not None
            console.print(f"      [{_c_ok}]✓ Created rebase task {rebase_task.id}[/{_c_ok}]")

            worker_args = _worker_args()
            rc = _spawn_background_worker(worker_args, config, task_id=rebase_task.id, quiet=True)
            workers_started += 1
            if rc == 0:
                console.print(f"      [{_c_ok}]✓ Started rebase worker[/{_c_ok}]")
                success_count += 1
            else:
                console.print(f"      [{_c_err}]✗ Failed to start rebase worker[/{_c_err}]")
                error_count += 1

        print()

    # --new: start pending tasks to fill remaining batch slots
    new_started = 0
    if new_mode and batch_limit is not None and workers_started < batch_limit:
        # Use the pre-fetched new_pending_tasks list so each worker gets a
        # distinct task.  If we didn't pre-fetch (e.g. no confirmation prompt
        # was shown), fetch now.
        if not new_pending_tasks:
            remaining = batch_limit - workers_started
            new_pending_tasks = store.get_pending(limit=remaining)
        for pt in new_pending_tasks:
            if workers_started >= batch_limit:
                break
            worker_args = _worker_args()
            rc = _spawn_background_worker(worker_args, config, task_id=pt.id, quiet=True)
            if rc != 0:
                break  # error spawning
            new_started += 1
            workers_started += 1

    parts = []
    if success_count:
        parts.append(f"[{_c_ok}]{success_count} advanced[/{_c_ok}]")
    if new_started > 0:
        parts.append(f"[{_c_ok}]{new_started} new[/{_c_ok}]")
    if skip_count:
        parts.append(f"[{_c_warn}]{skip_count} skipped[/{_c_warn}]")
    if error_count:
        parts.append(f"[{_c_err}]{error_count} errors[/{_c_err}]")
    console.print(", ".join(parts) if parts else "Nothing to do")

    if attention_tasks:
        console.print(f"\n[{_c_err}]Needs attention ({len(attention_tasks)} task{'s' if len(attention_tasks) != 1 else ''}):[/{_c_err}]")
        for atask, aaction in attention_tasks:
            prompt_display = shorten_prompt(atask.prompt, _prompt_avail(atask.id))
            # Strip leading "SKIP: " prefix from description for display
            desc = aaction['description']
            if desc.startswith('SKIP: '):
                desc = desc[len('SKIP: '):]
            _color = _advance_action_color(aaction['type'])
            console.print(f"  [{_c_tid}]{atask.id}[/{_c_tid}]  [{pink}]{prompt_display}[/{pink}]")
            console.print(f"       [{_color}]→ {desc}[/{_color}]")

    return 0 if error_count == 0 else 1
