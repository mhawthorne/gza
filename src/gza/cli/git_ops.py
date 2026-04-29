"""Git-related CLI commands: merge, rebase, checkout, diff, PR, refresh, advance."""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
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
from ..pickup import (
    count_worker_consuming_actions,
    get_runnable_pending_tasks,
    is_worker_consuming_advance_action,
)
from ..pr_ops import ensure_task_pr
from ..prompts import PromptBuilder
from ..runner import (
    TaskExecutionLogger,
    ensure_task_log_path,
    get_effective_config_for_task,
    load_dotenv,
    task_log_storage_path,
    write_log_entry,
)
from ._common import (
    DuplicateReviewError,
    _create_or_reuse_followup_tasks,
    _create_rebase_task,
    _create_resume_task,
    _create_review_task,
    _get_pager,
    _looks_like_task_id,
    _spawn_background_iterate_worker,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    get_review_verdict,  # noqa: F401  # re-exported for test patching
    get_store,
    resolve_id,
)
from .advance_engine import determine_next_action, is_resumable_failed_task
from .advance_executor import AdvanceActionExecutionContext, execute_advance_action

logger = logging.getLogger(__name__)


def _advance_uses_iterate(config: Config) -> bool:
    """Whether advance should launch implement work through the iterate loop."""
    return getattr(config, "advance_mode", "default") == "iterate"


def _collect_advance_completed_tasks(
    store: SqliteTaskStore,
    *,
    advance_type: str | None = None,
) -> tuple[list[DbTask], set[str]]:
    """Collect completed tasks eligible for advance-style action planning.

    Returns completed unmerged tasks and also completed plan tasks without
    implement children (except when filtering to implement-only mode).
    """
    impl_based_on_ids: set[str] = store.get_impl_based_on_ids()

    all_unmerged = store.get_unmerged()
    tasks = [t for t in all_unmerged if t.status == 'completed']

    if advance_type != 'implement':
        completed_plans = store.get_history(limit=None, status='completed', task_type='plan')
        existing_ids = {t.id for t in tasks}
        for plan_task in completed_plans:
            if plan_task.id in impl_based_on_ids:
                continue
            if plan_task.id in existing_ids:
                continue
            tasks.append(plan_task)

    if advance_type == 'plan':
        tasks = [t for t in tasks if t.task_type == 'plan']
    elif advance_type == 'implement':
        tasks = [t for t in tasks if t.task_type == 'implement']

    return tasks, impl_based_on_ids


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


def _auto_squash_commit_count(
    config: Config,
    git: Git,
    task: DbTask,
    target_branch: str,
) -> int | None:
    """Return commit count when task should auto-squash, otherwise None."""
    if config.merge_squash_threshold <= 0 or not task.branch:
        return None
    commit_count = git.count_commits_ahead(task.branch, target_branch)
    if commit_count < config.merge_squash_threshold:
        return None
    return commit_count


def _build_auto_merge_args(
    config: Config,
    git: Git,
    task: DbTask,
    target_branch: str,
) -> argparse.Namespace:
    """Build merge args with auto-squash behavior aligned across entrypoints."""
    should_squash = _auto_squash_commit_count(config, git, task, target_branch) is not None
    return argparse.Namespace(
        rebase=False,
        squash=should_squash,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )


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
            resolve_log = ensure_task_log_path(config, store, task)
            resolved = invoke_provider_resolve(
                task,
                task.branch,
                rebase_target,
                config,
                log_file=resolve_log,
                logger=TaskExecutionLogger(resolve_log, echo=True),
            )

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


def _branch_has_commits(config: Config, branch: str | None) -> bool:
    """Return whether a branch is ahead of the default branch."""
    if not branch:
        return False
    try:
        git = Git(config.project_dir)
        default_branch = git.default_branch()
        return git.count_commits_ahead(branch, default_branch) > 0
    except (GitError, OSError, ValueError):
        return False


def invoke_provider_resolve(
    task: DbTask,
    branch: str,
    target: str,
    config: Config,
    *,
    log_file: Path,
    logger: TaskExecutionLogger | None = None,
    worktree_path: Path | None = None,
) -> bool:
    """Invoke active provider runtime to resolve rebase conflicts via /gza-rebase.

    Provider output is appended to ``log_file`` owned by the caller's task row.
    """
    from dataclasses import replace

    from ..providers import get_provider

    log_file.parent.mkdir(parents=True, exist_ok=True)
    if not log_file.exists():
        log_file.touch()
    task_logger = logger or TaskExecutionLogger(log_file, echo=True)
    task_id_label = getattr(task, "id", None)
    task_ref = f"{task_id_label}" if task_id_label is not None else "<unknown>"

    effective_model, effective_provider, effective_max_steps = get_effective_config_for_task(task, config)

    runtime = _resolve_runtime_skill_dir(config.project_dir, effective_provider)
    if not runtime:
        task_logger.error(
            f"Error: Provider '{effective_provider}' does not support runtime skills for auto-resolve."
        )
        return False

    target_name, _runtime_dir = runtime
    if not ensure_skill("gza-rebase", effective_provider, config.project_dir):
        task_logger.error(
            f"Error: Missing required 'gza-rebase' skill for provider '{effective_provider}'."
        )
        task_logger.error(
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
            task_logger.warning(f"Warning: Failed to copy gza-rebase skill to worktree: {msg}")

    resolve_config = replace(
        config,
        provider=effective_provider,
        model=effective_model or "",
        reasoning_effort=config.get_reasoning_effort_for_task(task.task_type, effective_provider) or "",
        max_steps=effective_max_steps,
        max_turns=effective_max_steps,
    )

    load_dotenv(config.project_dir)
    provider = get_provider(resolve_config)

    if worktree_path is not None:
        skill_cmd = "/gza-rebase --auto"
        work_dir = worktree_path
    else:
        skill_cmd = "/gza-rebase --auto --continue"
        work_dir = config.project_dir

    task_logger.phase(
        f"Provider fallback: resolving conflicts for task {task_ref} branch '{branch}' onto '{target}'.",
        extra={"provider": effective_provider, "model": effective_model or "default"},
    )
    task_logger.command(
        f"Running provider command: {skill_cmd}",
        extra={"provider": effective_provider, "command": skill_cmd},
    )
    try:
        run_result = provider.run(resolve_config, skill_cmd, log_file, work_dir)
    except Exception as exc:
        task_logger.error(f"Provider resolve failed with exception: {exc}")
        return False

    if run_result.exit_code != 0:
        task_logger.error(f"Provider resolve failed with exit code {run_result.exit_code}.")
        return False

    rebase_in_progress = _is_rebase_in_progress(worktree_path or config.project_dir)
    if rebase_in_progress:
        task_logger.error(f"Rebase still in progress after {skill_cmd}.")
        return False

    task_logger.info("Provider resolve completed successfully.")
    return True


def _run_task_backed_rebase(
    *,
    config: Config,
    store: SqliteTaskStore,
    rebase_task: DbTask,
    branch: str,
    target_branch: str,
    remote: bool = False,
    parent_task_id: str | None = None,
    failure_hint_lines: list[str] | None = None,
) -> int:
    """Execute a foreground rebase flow with single-task log/state ownership."""
    git = Git(config.project_dir)
    log_file = ensure_task_log_path(config, store, rebase_task)
    logger = TaskExecutionLogger(log_file, echo=True)
    log_file_storage = task_log_storage_path(config, log_file)

    if rebase_task.status != "in_progress":
        store.mark_in_progress(rebase_task)

    rebase_target = target_branch
    logger.info(f"Rebasing task {rebase_task.id}...")
    logger.phase(f"Current branch: {git.current_branch()}")
    logger.phase(f"Target branch: {target_branch}")
    write_log_entry(
        log_file,
        {
            "type": "gza",
            "subtype": "branch",
            "message": f"Branch: {branch}",
            "branch": branch,
            "target_branch": target_branch,
        },
    )

    if remote:
        logger.command("Fetching from origin...")
        try:
            git.fetch("origin")
        except GitError as e:
            logger.error(f"Error fetching from origin: {e}")
            store.mark_failed(rebase_task, log_file=log_file_storage, branch=branch, failure_reason="GIT_ERROR")
            return 1
        logger.info("✓ Fetched from origin")
        rebase_target = f"origin/{target_branch}"
        logger.phase(f"Resolved remote target: {rebase_target}")

    worktree_path = config.worktree_path / str(rebase_task.id)
    try:
        stale_path = cleanup_worktree_for_branch(git, branch, force=True)
        if stale_path:
            logger.phase(f"Removing stale worktree at {stale_path}...")
            logger.info("✓ Removed worktree")
        if worktree_path.exists():
            logger.phase(f"Removing existing worktree path {worktree_path}...")
            git.worktree_remove(worktree_path, force=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        logger.phase(f"Creating worktree at {worktree_path}...")
        git._run("worktree", "add", str(worktree_path), branch)
    except GitError as e:
        logger.error(f"Error setting up worktree: {e}")
        store.mark_failed(rebase_task, log_file=log_file_storage, branch=branch, failure_reason="GIT_ERROR")
        return 1

    worktree_git = Git(worktree_path)

    try:
        logger.command(f"Rebasing '{branch}' onto '{rebase_target}'...")
        resolved_by_provider = False
        try:
            worktree_git.rebase(rebase_target)
            output_content = f"Rebased '{branch}' onto '{rebase_target}'."
        except GitError as e:
            logger.warning(f"Conflicts detected: {e}")
            try:
                worktree_git.rebase_abort()
                logger.phase("Aborted conflicted mechanical rebase before provider fallback.")
            except GitError as abort_error:
                logger.warning(f"Warning: Could not abort rebase cleanly: {abort_error}")

            logger.phase("Invoking provider to resolve via /gza-rebase --auto...")
            resolved = invoke_provider_resolve(
                rebase_task,
                branch,
                rebase_target,
                config,
                log_file=log_file,
                logger=logger,
                worktree_path=worktree_path,
            )
            if not resolved:
                logger.error("Could not resolve conflicts automatically.")
                if failure_hint_lines:
                    for line in failure_hint_lines:
                        logger.error(line)
                store.mark_failed(
                    rebase_task,
                    log_file=log_file_storage,
                    branch=branch,
                    failure_reason="TEST_FAILURE",
                )
                print()
                return 1

            resolved_by_provider = True
            logger.command(f"Pushing {branch}...")
            worktree_git.push_force_with_lease(branch)
            logger.info(f"✓ Pushed {branch}")
            output_content = f"Resolved conflicts and rebased '{branch}' onto '{rebase_target}'."

        has_commits = _branch_has_commits(config, branch)
        store.mark_completed(
            rebase_task,
            branch=branch,
            log_file=log_file_storage,
            output_content=output_content,
            has_commits=has_commits,
        )

        target_parent_id = parent_task_id or rebase_task.based_on
        if target_parent_id:
            store.invalidate_review_state(target_parent_id)
            parent = store.get(target_parent_id)
            if parent and parent.id is not None and parent.merge_status == "merged":
                store.set_merge_status(parent.id, "unmerged")

        if resolved_by_provider:
            logger.info(f"✓ Successfully rebased {branch} with provider assistance")
        else:
            logger.info(f"✓ Successfully rebased {branch} onto {rebase_target}")
        print()
        return 0

    except GitError as e:
        logger.error(f"Error during rebase: {e}")
        store.mark_failed(rebase_task, log_file=log_file_storage, branch=branch, failure_reason="GIT_ERROR")
        print()
        return 1
    finally:
        try:
            logger.phase(f"Cleaning up worktree at {worktree_path}...")
            git.worktree_remove(worktree_path, force=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
            logger.phase("Worktree cleanup complete.")
        except Exception:
            logger.warning(f"Warning: Failed to remove rebase worktree at {worktree_path}")


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

    rebase_task = _create_rebase_task(store, task_id, task.branch, rebase_target)
    assert rebase_task.id is not None
    rebase_task.branch = task.branch
    store.update(rebase_task)

    return _run_task_backed_rebase(
        config=config,
        store=store,
        rebase_task=rebase_task,
        branch=task.branch,
        target_branch=rebase_target,
        remote=bool(getattr(args, "remote", False)),
        parent_task_id=task.id,
    )


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

    default_branch = git.default_branch()

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

    result = ensure_task_pr(
        task,
        store,
        git,
        title=title,
        body=body,
        draft=args.draft,
        merged_behavior="error",
    )
    if result.ok and result.status == "created":
        print(f"✓ Created PR: {result.pr_url}")
        return 0
    if result.ok and result.status == "existing":
        print(f"PR already exists: {result.pr_url}")
        return 0
    if result.ok and result.status == "cached" and task.pr_number:
        print(f"PR already exists: #{task.pr_number}")
        return 0
    if result.status == "gh_unavailable":
        print("Error: GitHub CLI (gh) is not installed or not authenticated")
        print("Install: https://cli.github.com/")
        print("Auth: gh auth login")
        return 1
    if result.status == "push_failed":
        print(f"Error pushing branch: {result.error}")
        return 1
    if result.status == "merged":
        print(f"Error: Branch '{task.branch}' is already merged into {default_branch}")
        return 1
    if result.status == "create_failed":
        print(f"Error creating PR:\n{result.error}")
        return 1
    print("Error creating PR")
    return 1


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
            depends_on=task.id,
            tags=task.tags,
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
_ADVANCE_ACTION_ORDER: dict[str, int] = {'merge': 0, 'merge_with_followups': 0}


@dataclass
class _MergeActionResult:
    rc: int
    created_followups: list[DbTask]
    reused_followups: list[DbTask]


@dataclass
class _CreateReviewActionResult:
    status: str
    review_task: DbTask | None
    message: str


def _prepare_create_review_action(store: SqliteTaskStore, task: DbTask) -> _CreateReviewActionResult:
    """Create or resolve the review task for an advance-style create_review action."""
    try:
        review_task = _create_review_task(store, task)
    except DuplicateReviewError as exc:
        review_task = exc.active_review
        return _CreateReviewActionResult(
            status="skip",
            review_task=review_task,
            message=f"SKIP: review {review_task.id} is already {review_task.status}",
        )
    except ValueError as exc:
        return _CreateReviewActionResult(
            status="skip",
            review_task=None,
            message=f"SKIP: {exc}",
        )

    return _CreateReviewActionResult(
        status="created",
        review_task=review_task,
        message=f"Created review task {review_task.id}",
    )


def _execute_merge_action(
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    action: dict,
    *,
    target_branch: str,
    current_branch: str,
) -> _MergeActionResult:
    """Execute a merge-style advance action and materialize follow-up tasks if needed."""
    created_followups: list[DbTask] = []
    reused_followups: list[DbTask] = []

    if action.get("type") == "merge_with_followups":
        review_task = action.get("review_task")
        followup_findings = action.get("followup_findings")
        if isinstance(review_task, DbTask) and isinstance(followup_findings, tuple):
            created_followups, reused_followups = _create_or_reuse_followup_tasks(
                store,
                review_task=review_task,
                impl_task=task,
                findings=followup_findings,
            )

    assert task.id is not None
    merge_args = _build_auto_merge_args(config, git, task, target_branch)
    rc = _merge_single_task(task.id, config, store, git, merge_args, current_branch)
    return _MergeActionResult(
        rc=rc,
        created_followups=created_followups,
        reused_followups=reused_followups,
    )


def _advance_action_color(action_type: str) -> str:
    """Return a Rich color for an advance action type."""
    ac = _colors.WORK_COLORS
    if action_type in {'merge', 'merge_with_followups'}:
        return ac.merge
    if action_type in ('needs_rebase', 'needs_discussion', 'max_cycles_reached', 'max_improve_attempts'):
        return ac.error
    if action_type in ('skip', 'wait_review', 'wait_improve'):
        return ac.waiting
    return ac.default


def cmd_advance(args: argparse.Namespace) -> int:
    """Intelligently progress unmerged tasks through their lifecycle."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    # Themed work/advance colors — resolved once after Config.load() applies the theme.
    _ac = _colors.WORK_COLORS
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
    # to avoid repeated DB queries in evaluate_advance_rules.
    impl_based_on_ids: set[str] = store.get_impl_based_on_ids()

    failed_tasks: list[DbTask] = []
    # Determine which tasks to advance
    if task_id is not None:
        task = store.get(task_id)
        if not task:
            print(f"Error: Task {task_id} not found")
            return 1
        if task.status == 'failed':
            # Allow a specific failed task if it's resumable
            is_resumable = is_resumable_failed_task(task) and not no_resume_failed
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
        tasks, impl_based_on_ids = _collect_advance_completed_tasks(store, advance_type=advance_type)

        # Apply failed-task filters after completed-task type filtering above.
        if advance_type == 'plan':
            tasks = [t for t in tasks if t.task_type == 'plan']
        elif advance_type == 'implement':
            tasks = [t for t in tasks if t.task_type == 'implement']

        # Collect resumable failed tasks separately so --max applies only
        # to completed/unmerged candidates, preserving legacy behavior.
        if not no_resume_failed:
            failed_tasks = store.get_resumable_failed_tasks()
            if advance_type == 'plan':
                failed_tasks = []
            elif advance_type == 'implement':
                failed_tasks = [t for t in failed_tasks if t.task_type == 'implement']

    if not tasks and not failed_tasks and not new_mode:
        print("No eligible tasks to advance")
        return 0

    # Apply --max limit
    if max_tasks is not None:
        tasks = tasks[:max_tasks]

    # Use the currently checked-out branch as the target for conflict checks,
    # merge execution, and rebase task creation.
    target_branch = git.current_branch()
    use_iterate_mode = _advance_uses_iterate(config)

    def _worker_args() -> argparse.Namespace:
        return argparse.Namespace(
            no_docker=getattr(args, 'no_docker', False),
            max_turns=None,
            force=force,
        )

    def _build_action_context(*, dry_run_mode: bool) -> AdvanceActionExecutionContext:
        def _create_rebase_from_task(parent_task: DbTask) -> DbTask:
            assert parent_task.id is not None
            assert parent_task.branch is not None
            return _create_rebase_task(store, parent_task.id, parent_task.branch, target_branch)

        def _create_implement_from_task(parent_task: DbTask) -> DbTask:
            assert parent_task.id is not None
            return store.add(
                prompt=_unimplemented_implement_prompt(parent_task),
                task_type="implement",
                depends_on=parent_task.id,
                tags=parent_task.tags,
            )

        return AdvanceActionExecutionContext(
            store=store,
            dry_run=dry_run_mode,
            max_resume_attempts=max_resume_attempts,
            use_iterate_for_create_implement=use_iterate_mode,
            use_iterate_for_needs_rebase=use_iterate_mode,
            prepare_create_review=lambda t: _prepare_create_review_action(store, t),
            create_resume_task=lambda t: _create_resume_task(store, t),
            create_rebase_task=_create_rebase_from_task,
            create_implement_task=_create_implement_from_task,
            spawn_worker=lambda task_id, _kind: _spawn_background_worker(
                _worker_args(), config, task_id=task_id, quiet=True
            ),
            spawn_resume_worker=lambda task_id, _kind: _spawn_background_resume_worker(
                _worker_args(), config, task_id, quiet=True
            ),
            spawn_iterate_worker=lambda task_obj, _kind: _spawn_background_iterate_worker(
                argparse.Namespace(
                    no_docker=getattr(args, 'no_docker', False),
                    force=force,
                ),
                config,
                task_obj,
                max_iterations=config.iterate_max_iterations,
                quiet=True,
            ),
        )

    # Analyze each task to determine the next action
    plan: list[tuple[DbTask, dict]] = []
    for task in tasks:
        action = determine_next_action(
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
    for failed_task in failed_tasks:
        action = determine_next_action(
            config,
            store,
            git,
            failed_task,
            target_branch,
            impl_based_on_ids=impl_based_on_ids,
            max_resume_attempts=max_resume_attempts,
        )
        if action.get("type") == "skip" and action.get("description") == "SKIP: resume child already exists":
            continue
        plan.append((failed_task, action))

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
        dry_run_context = _build_action_context(dry_run_mode=True)
        print(f"Would advance {len(plan)} task(s):\n")
        for task, action in plan:
            prompt_display = shorten_prompt(task.prompt, _prompt_avail(task.id))
            console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            description = action['description']
            if action['type'] in {'merge', 'merge_with_followups'}:
                commit_count = _auto_squash_commit_count(config, git, task, target_branch)
                if commit_count is not None:
                    description = f"{description} (auto-squash, {commit_count} commits)"
            elif is_worker_consuming_advance_action(action['type']):
                dry_result = execute_advance_action(task=task, action=action, context=dry_run_context)
                if dry_result.status == "dry_run" and dry_result.message:
                    description = dry_result.message
            _color = _advance_action_color(action['type'])
            console.print(f"      [{_color}]→ {description}[/{_color}]")
            print()
        if new_mode and batch_limit is not None:
            planned_workers = count_worker_consuming_actions([action for _, action in plan])
            remaining = max(0, batch_limit - planned_workers)
            if remaining > 0:
                pending_tasks = get_runnable_pending_tasks(store, limit=remaining)
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
    actionable_plan = [item for item in plan if item[1]['type'] != 'skip']
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
        planned_workers = count_worker_consuming_actions([action for _, action in plan])
        remaining = max(0, batch_limit - planned_workers)
        if remaining > 0:
            new_pending_tasks = get_runnable_pending_tasks(store, limit=remaining)
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
    _ACTIONABLE_SKIP_TYPES = frozenset({'needs_discussion', 'max_cycles_reached', 'max_improve_attempts'})
    attention_tasks: list[tuple[DbTask, dict]] = []
    action_context = _build_action_context(dry_run_mode=False)

    for task, action in plan:
        assert task.id is not None
        prompt_display = shorten_prompt(task.prompt, _prompt_avail(task.id))
        action_type = action['type']

        if action_type in ('wait_review', 'wait_improve', 'needs_discussion', 'skip', 'max_cycles_reached', 'max_improve_attempts'):
            console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
            _color = _advance_action_color(action_type)
            console.print(f"      [{_color}]{action['description']}[/{_color}]")
            skip_count += 1
            if action_type in _ACTIONABLE_SKIP_TYPES:
                attention_tasks.append((task, action))
            continue

        # Worker-spawning actions: check batch limit before proceeding
        if is_worker_consuming_advance_action(action_type):
            if batch_limit is not None and workers_started >= batch_limit:
                console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
                console.print(f"      [{_c_warn}]— batch limit reached ({workers_started}/{batch_limit}), skipping[/{_c_warn}]")
                print()
                skip_count += 1
                continue

        console.print(f"  [{_c_tid}]{task.id}[/{_c_tid}] [{pink}]{prompt_display}[/{pink}]")
        _color = _advance_action_color(action_type)
        console.print(f"      [{_color}]→ {action['description']}[/{_color}]")

        if action_type in {'merge', 'merge_with_followups'}:
            merge_result = _execute_merge_action(
                config,
                store,
                git,
                task,
                action,
                target_branch=target_branch,
                current_branch=target_branch,
            )
            if merge_result.created_followups:
                created_ids = ", ".join(str(t.id) for t in merge_result.created_followups if t.id is not None)
                console.print(f"      [{_c_ok}]✓ Created follow-up task(s): {created_ids}[/{_c_ok}]")
            if merge_result.reused_followups:
                reused_ids = ", ".join(str(t.id) for t in merge_result.reused_followups if t.id is not None)
                console.print(f"      [{_c_warn}]↺ Reused follow-up task(s): {reused_ids}[/{_c_warn}]")
            rc = merge_result.rc
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
                        success_count += 1
                    else:
                        error_count += 1
                else:
                    console.print(f"      [{_c_err}]✗ Merge failed[/{_c_err}]")
                    error_count += 1

        else:
            exec_result = execute_advance_action(task=task, action=action, context=action_context)
            if exec_result.attempted_spawn:
                workers_started += 1

            if exec_result.status == "skip":
                console.print(f"      [{_c_warn}]{exec_result.message}[/{_c_warn}]")
                skip_count += 1
                if exec_result.attention_type == "max_improve_attempts":
                    attention_tasks.append(
                        (
                            task,
                            {
                                "type": "max_improve_attempts",
                                "description": exec_result.message,
                            },
                        )
                    )
                continue

            if exec_result.status == "error":
                if exec_result.success_message:
                    console.print(f"      [{_c_ok}]✓ {exec_result.success_message}[/{_c_ok}]")
                err_message = exec_result.error_message or exec_result.message or f"Failed to execute {action_type}"
                console.print(f"      [{_c_err}]✗ {err_message}[/{_c_err}]")
                error_count += 1
                continue

            success_message = exec_result.success_message or exec_result.message
            if success_message:
                console.print(f"      [{_c_ok}]✓ {success_message}[/{_c_ok}]")

            if exec_result.worker_started:
                success_count += 1
            elif exec_result.worker_consuming:
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
            new_pending_tasks = get_runnable_pending_tasks(store, limit=remaining)
        for pt in new_pending_tasks:
            if workers_started >= batch_limit:
                break
            if _advance_uses_iterate(config) and pt.task_type == "implement":
                iterate_args = argparse.Namespace(
                    no_docker=getattr(args, 'no_docker', False),
                    force=force,
                )
                rc = _spawn_background_iterate_worker(
                    iterate_args,
                    config,
                    pt,
                    max_iterations=config.iterate_max_iterations,
                    quiet=True,
                )
            else:
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
            if aaction['type'] in {'max_cycles_reached', 'max_improve_attempts'}:
                console.print(f"       [{_color}]→ Recommended next step: uv run gza fix {atask.id}[/{_color}]")

    return 0 if error_count == 0 else 1
