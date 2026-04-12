"""Task execution commands: run, add, edit, retry, resume, review, improve, iterate."""

import argparse
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from ..config import Config
from ..console import format_duration
from ..db import (
    SqliteTaskStore,
    Task as DbTask,
    add_task_interactive,
    edit_task_interactive,
    task_id_numeric_key,
    validate_prompt,
)
from ..git import Git
from ..query import get_base_task_slug as _get_base_task_slug
from ..runner import run
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import (
    DuplicateReviewError,
    _create_improve_task,
    _create_resume_task,
    _create_review_task,
    _run_as_worker,
    _run_foreground,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    _spawn_background_workers,
    get_review_verdict,
    get_store,
    get_task_step_count,
    resolve_id,
    set_task_urgency,
)
from .log import _latest_worker_for_task, _running_worker_id_for_task
from .query import _get_orphaned_tasks, _print_orphaned_warning


def cmd_run(args: argparse.Namespace) -> int:
    """Run the next pending task(s) or specific tasks."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns

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

    # Warn about orphaned tasks before starting new work (skip in resume mode)
    is_resume = getattr(args, 'resume', False)
    if not is_resume:
        orphaned = _get_orphaned_tasks(registry, store)
        if orphaned:
            _print_orphaned_warning(orphaned)
            print()
    task_id_for_registration = None

    # Check if specific task IDs were provided
    if hasattr(args, 'task_ids') and args.task_ids:
        # Resolve and validate all task IDs first
        args.task_ids = [resolve_id(config, tid) for tid in args.task_ids]
        for task_id in args.task_ids:
            task = store.get(task_id)
            if not task:
                print(f"Error: Task {task_id} not found")
                return 1

            if task.status != "pending":
                print(f"Error: Task {task_id} is not pending (status: {task.status})")
                return 1

            # Check if task is blocked by a dependency
            is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
            if is_blocked:
                print(f"Error: Task {task_id} is blocked by task {blocking_id} ({blocking_status})")
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
        task_id=task_id_for_registration,
        pid=os.getpid(),
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

    # Track elapsed time for the work session
    start_time = time.time()

    try:
        # Run the task(s)
        if hasattr(args, 'task_ids') and args.task_ids:
            # Run the specific tasks
            tasks_completed = 0
            task_separator = "\n" + "-" * 32 + "\n"
            for task_id in args.task_ids:
                if tasks_completed > 0:
                    print(task_separator)
                    # Update worker registry to track the current task
                    worker.task_id = task_id
                    registry.update(worker)
                result = run(
                    config,
                    task_id=task_id,
                    skip_precondition_check=getattr(args, "force", False),
                )
                if result != 0:
                    if tasks_completed == 0:
                        # First task failed
                        registry.mark_completed(worker_id, exit_code=result, status="failed")
                        return result
                    else:
                        # We completed some tasks before failure
                        print(f"\nCompleted {tasks_completed} task(s) before task {task_id} failed")
                        registry.mark_completed(worker_id, exit_code=result, status="failed")
                        return result
                tasks_completed += 1

            # All tasks completed successfully
            if tasks_completed > 1:
                elapsed = format_duration(time.time() - start_time)
                print(f"\n=== Completed {tasks_completed} tasks in {elapsed} ===")
            registry.mark_completed(worker_id, exit_code=0, status="completed")
            return 0

        # Determine how many tasks to run
        count = args.count if args.count is not None else config.work_count

        # Run tasks in a loop
        tasks_completed = 0
        task_separator = "\n" + "-" * 32 + "\n"
        for i in range(count):
            if tasks_completed > 0:
                print(task_separator)
            result = run(config, skip_precondition_check=getattr(args, "force", False))

            # Any non-zero exit means the run failed.
            if result != 0:
                if tasks_completed == 0:
                    # First task failed, return the error code.
                    registry.mark_completed(worker_id, exit_code=result, status="failed")
                    return result
                print(f"\nCompleted {tasks_completed} task(s) before a task failed")
                registry.mark_completed(worker_id, exit_code=result, status="failed")
                return result

            tasks_completed += 1

            # Check if there are more pending tasks
            if i < count - 1:  # Not the last iteration
                from ..db import SqliteTaskStore
                store = SqliteTaskStore(config.db_path)
                next_task = store.get_next_pending()
                if not next_task:
                    elapsed = format_duration(time.time() - start_time)
                    print(f"\nCompleted {tasks_completed} task(s) in {elapsed}. No more pending tasks.")
                    break
                # Update worker registry to track the next task
                worker.task_id = next_task.id
                registry.update(worker)

        if tasks_completed > 1:
            elapsed = format_duration(time.time() - start_time)
            print(f"\n=== Completed {tasks_completed} tasks in {elapsed} ===")

        # Clean up worker registration on normal exit
        registry.mark_completed(worker_id, exit_code=0, status="completed")
        return 0

    except Exception:
        # Clean up worker registration on exception
        registry.mark_completed(worker_id, exit_code=1, status="failed")
        raise


def cmd_implement(args: argparse.Namespace) -> int:
    """Create an implementation task from a completed plan task and run it."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns

    store = get_store(config)

    plan_task_id = resolve_id(config, args.plan_task_id)
    plan_task = store.get(plan_task_id)
    if not plan_task:
        print(f"Error: Task {plan_task_id} not found")
        return 1
    if plan_task.task_type != "plan":
        print(f"Error: Task {plan_task.id} is a {plan_task.task_type} task. Expected a completed plan task.")
        return 1
    if plan_task.status != "completed":
        print(f"Error: Task {plan_task.id} is {plan_task.status}. Plan task must be completed.")
        return 1

    prompt = args.prompt
    if not prompt:
        slug = _get_base_task_slug(plan_task)
        if slug:
            prompt = f"Implement plan from task {plan_task.id}: {slug}"
        else:
            prompt = f"Implement plan from task {plan_task.id}"

    group = args.group if hasattr(args, 'group') and args.group else None
    depends_on = resolve_id(config, args.depends_on) if hasattr(args, 'depends_on') and args.depends_on else None
    create_review = args.review if hasattr(args, 'review') and args.review else False
    same_branch = args.same_branch if hasattr(args, 'same_branch') and args.same_branch else False
    branch_type = args.branch_type if hasattr(args, 'branch_type') and args.branch_type else None
    model = args.model if hasattr(args, 'model') and args.model else None
    provider = args.provider if hasattr(args, 'provider') and args.provider else None
    skip_learnings = args.skip_learnings if hasattr(args, 'skip_learnings') and args.skip_learnings else False

    impl_task = store.add(
        prompt,
        task_type="implement",
        based_on=plan_task.id,
        group=group,
        depends_on=depends_on,
        create_review=create_review,
        same_branch=same_branch,
        task_type_hint=branch_type,
        model=model,
        provider=provider,
        skip_learnings=skip_learnings,
    )

    print(f"✓ Created implement task {impl_task.id}")
    print(f"  Based on: plan {plan_task.id}")

    # Handle background mode - spawn worker to run the implement task
    if hasattr(args, 'background') and args.background:
        assert impl_task.id is not None
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [impl_task.id]
        return _spawn_background_worker(worker_args, config, task_id=impl_task.id)

    # Handle queue mode - add to queue without executing
    if hasattr(args, 'queue') and args.queue:
        return 0

    # Default: run the implement task immediately
    print(f"\nRunning implement task {impl_task.id}...")
    return _run_foreground(config, task_id=impl_task.id, force=getattr(args, "force", False))


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
        task_type = "implement"

    # Validate task type
    valid_types = ["explore", "plan", "implement", "review"]
    if task_type == "improve":
        print("Error: Cannot create improve tasks directly. Use 'gza improve <task_id>' instead.")
        return 1
    if task_type not in valid_types:
        print(f"Error: Invalid task type '{task_type}'. Must be one of: {', '.join(valid_types)}")
        return 1

    # Get optional parameters
    group = args.group if hasattr(args, 'group') and args.group else None
    depends_on = resolve_id(config, args.depends_on) if hasattr(args, 'depends_on') and args.depends_on else None
    based_on = resolve_id(config, args.based_on) if hasattr(args, 'based_on') and args.based_on else None
    create_review = args.review if hasattr(args, 'review') and args.review else False
    same_branch = args.same_branch if hasattr(args, 'same_branch') and args.same_branch else False
    spec = args.spec if hasattr(args, 'spec') and args.spec else None
    branch_type = args.branch_type if hasattr(args, 'branch_type') and args.branch_type else None
    model = args.model if hasattr(args, 'model') and args.model else None
    provider = args.provider if hasattr(args, 'provider') and args.provider else None
    skip_learnings = args.skip_learnings if hasattr(args, 'skip_learnings') and args.skip_learnings else False
    mark_next = bool(getattr(args, "next", False))

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
            print(f"Error: Task {based_on} not found")
            return 1

    # Validation: --depends-on must reference an existing task
    if depends_on:
        dep_task = store.get(depends_on)
        if not dep_task:
            print(f"Error: Task {depends_on} not found")
            return 1

    # Handle --prompt-file argument
    if hasattr(args, 'prompt_file') and args.prompt_file is not None:
        if args.prompt:
            print("Error: Cannot use both --prompt-file and prompt argument")
            return 1
        if args.edit:
            print("Error: Cannot use both --prompt-file and --edit")
            return 1
        try:
            with open(args.prompt_file) as f:
                prompt_text = f.read().strip()
        except FileNotFoundError:
            print(f"Error: File not found: {args.prompt_file}")
            return 1
        except Exception as e:
            print(f"Error reading file: {e}")
            return 1

        # Create task with prompt from file
        task = store.add(
            prompt_text,
            task_type=task_type,
            based_on=based_on,
            group=group,
            depends_on=depends_on,
            create_review=create_review,
            same_branch=same_branch,
            spec=spec,
            task_type_hint=branch_type,
            model=model,
            provider=provider,
            skip_learnings=skip_learnings,
        )
        if mark_next:
            assert task.id is not None
            set_task_urgency(store, task.id, urgent=True)
        print(f"✓ Added task {task.id}")
        return 0

    if args.edit or not args.prompt:
        # Interactive mode with $EDITOR
        new_task = add_task_interactive(
            store,
            task_type=task_type,
            based_on=based_on,
            spec=spec,
            group=group,
            depends_on=depends_on,
            create_review=create_review,
            same_branch=same_branch,
            task_type_hint=branch_type,
            model=model,
            provider=provider,
            skip_learnings=skip_learnings,
        )
        if not new_task:
            return 1
        if mark_next:
            assert new_task.id is not None
            set_task_urgency(store, new_task.id, urgent=True)
        print(f"✓ Added task {new_task.id}")
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
            model=model,
            provider=provider,
            skip_learnings=skip_learnings,
        )
        if mark_next:
            assert task.id is not None
            set_task_urgency(store, task.id, urgent=True)
        print(f"✓ Added task {task.id}")
        return 0


def cmd_edit(args: argparse.Namespace) -> int:
    """Edit a task's prompt or metadata."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
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
            print(f"✓ Removed task {task.id} from group")
            return 0
        else:
            task.group = args.group_flag
            store.update(task)
            print(f"✓ Moved task {task.id} to group '{args.group_flag}'")
            return 0

    # Handle --based-on flag (lineage/parent relationship)
    if hasattr(args, 'based_on_flag') and args.based_on_flag is not None:
        based_on_id = resolve_id(config, args.based_on_flag)
        parent_task = store.get(based_on_id)
        if not parent_task:
            print(f"Error: Task {based_on_id} not found")
            return 1
        task.based_on = based_on_id
        store.update(task)
        print(f"✓ Set task {task.id} based_on task {based_on_id}")
        return 0

    # Handle --depends-on flag (execution blocking dependency)
    if hasattr(args, 'depends_on_flag') and args.depends_on_flag is not None:
        depends_on_id = resolve_id(config, args.depends_on_flag)
        dep_task = store.get(depends_on_id)
        if not dep_task:
            print(f"Error: Task {depends_on_id} not found")
            return 1
        task.depends_on = depends_on_id
        store.update(task)
        print(f"✓ Set task {task.id} to depend on task {depends_on_id}")
        return 0

    # Handle --review flag
    if hasattr(args, 'review') and args.review:
        task.create_review = True
        store.update(task)
        print(f"✓ Enabled automatic review task creation for task {task.id}")
        return 0

    # Handle --model flag
    if hasattr(args, 'model') and args.model is not None:
        task.model = args.model
        store.update(task)
        print(f"✓ Set model override to '{args.model}' for task {task.id}")
        return 0

    # Handle --provider flag
    if hasattr(args, 'provider') and args.provider is not None:
        task.provider = args.provider
        task.provider_is_explicit = True
        store.update(task)
        print(f"✓ Set provider override to '{args.provider}' for task {task.id}")
        return 0

    # Handle --no-learnings flag
    if hasattr(args, 'skip_learnings') and args.skip_learnings:
        task.skip_learnings = True
        store.update(task)
        print(f"✓ Set skip_learnings for task {task.id}")
        return 0

    if args.explore and args.task:
        print("Error: Cannot use both --explore and --task")
        return 1

    # Handle type conversion without opening editor
    if args.explore or args.task:
        new_type = "explore" if args.explore else "implement"
        if task.task_type == new_type:
            print(f"Task {task.id} is already a {new_type}")
            return 0
        task.task_type = new_type
        store.update(task)
        print(f"✓ Converted task {task.id} to {new_type}")
        return 0

    # Handle non-interactive prompt editing
    if hasattr(args, 'prompt_file') and args.prompt_file is not None:
        if hasattr(args, 'prompt') and args.prompt is not None:
            print("Error: Cannot use both --prompt-file and --prompt")
            return 1
        try:
            with open(args.prompt_file) as f:
                new_prompt = f.read().strip()
        except FileNotFoundError:
            print(f"Error: File not found: {args.prompt_file}")
            return 1
        except Exception as e:
            print(f"Error reading file: {e}")
            return 1

        errors = validate_prompt(new_prompt)
        if errors:
            print("Validation errors:")
            for error in errors:
                print(f"  - {error}")
            return 1

        task.prompt = new_prompt
        store.update(task)
        print(f"✓ Updated task {task.id}")
        return 0

    if hasattr(args, 'prompt') and args.prompt is not None:
        # Handle stdin (-) or direct prompt text
        if args.prompt == '-':
            import sys
            new_prompt = sys.stdin.read().strip()
        else:
            new_prompt = args.prompt

        errors = validate_prompt(new_prompt)
        if errors:
            print("Validation errors:")
            for error in errors:
                print(f"  - {error}")
            return 1

        task.prompt = new_prompt
        store.update(task)
        print(f"✓ Updated task {task.id}")
        return 0

    if edit_task_interactive(store, task):
        print(f"✓ Updated task {task.id}")
        return 0
    return 1


def cmd_retry(args: argparse.Namespace) -> int:
    """Retry a failed or completed task by creating a new pending task."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns

    store = get_store(config)

    # Get the original task
    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    # Validate status
    if task.status not in ("completed", "failed"):
        print(f"Error: Can only retry completed or failed tasks (task is {task.status})")
        return 1

    # Check if task already has a successful retry
    children = store.get_based_on_children(task_id)
    successful_retry = next((c for c in children if c.status == "completed"), None)
    if successful_retry:
        print(f"Error: Task {task_id} already has a successful retry ({successful_retry.id}).")
        return 1

    # Create new task copying relevant fields
    new_task = store.add(
        prompt=task.prompt,
        task_type=task.task_type,
        group=task.group,
        spec=task.spec,
        depends_on=task.depends_on,
        create_review=task.create_review,
        same_branch=task.same_branch,
        task_type_hint=task.task_type_hint,
        based_on=task_id,  # Track retry lineage
        model=task.model,
        provider=task.provider if task.provider_is_explicit else None,
        provider_is_explicit=task.provider_is_explicit,
    )

    print(f"✓ Created task {new_task.id} (retry of {task_id})")

    # Handle background mode - spawn worker to run the new task
    if args.background:
        # Create a temporary args object for the worker with the new task_id
        assert new_task.id is not None
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [new_task.id]
        return _spawn_background_worker(worker_args, config, task_id=new_task.id)

    # Handle queue mode - add to queue without executing
    if hasattr(args, 'queue') and args.queue:
        return 0

    # Default: run the new task immediately
    print(f"\nRunning task {new_task.id}...")
    return _run_foreground(config, task_id=new_task.id, force=getattr(args, "force", False))


def _default_mark_completed_mode(task_type: str) -> str:
    """Choose default completion mode based on task type."""
    if task_type in {"task", "implement", "improve"}:
        return "verify-git"
    return "force"


def cmd_mark_completed(args: argparse.Namespace) -> int:
    """Mark a task as completed with either git verification or status-only mode."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    if task.status == "completed":
        print(f"Error: Task {task_id} is already completed")
        return 1

    if args.verify_git and args.force:
        print("Error: Cannot use --verify-git and --force together")
        return 1

    mode = "verify-git" if args.verify_git else ("force" if args.force else _default_mark_completed_mode(task.task_type))

    # Warn if task wasn't failed (but still proceed)
    if task.status != "failed":
        print(f"Warning: Task {task_id} is not in failed status (current status: {task.status}), proceeding anyway")

    if mode == "force":
        old_status = task.status
        store.mark_completed(task, branch=task.branch if task.branch else None)
        _cleanup_worker_registry(config, task_id)
        print(f"✓ Task {task_id} status changed: {old_status} → completed (status-only)")
        return 0

    # verify-git mode: validate branch and commit state
    if not task.branch:
        print(f"Error: Task {task_id} has no branch set. Use --force for status-only completion.")
        return 1

    git = Git(config.project_dir)
    if not git.branch_exists(task.branch):
        print(f"Error: Branch '{task.branch}' does not exist. Use --force for status-only completion.")
        return 1

    default_branch = git.default_branch()
    commit_count = git.count_commits_ahead(task.branch, default_branch)
    if commit_count <= 0:
        print(f"Note: No commits found on branch '{task.branch}' compared to '{default_branch}'")
        store.mark_completed(task, branch=task.branch, has_commits=False)
        _cleanup_worker_registry(config, task_id)
        print(f"✓ Task {task_id} marked as completed")
        return 0

    store.mark_completed(task, branch=task.branch, has_commits=True)
    _cleanup_worker_registry(config, task_id)
    print(f"✓ Task {task_id} marked as completed (unmerged, {commit_count} commit(s) on branch '{task.branch}')")

    return 0


def cmd_set_status(args: argparse.Namespace) -> int:
    """Manually force a task's status to any valid value."""
    if args.reason and args.status != "failed":
        print(f"Warning: --reason is only meaningful for 'failed' status (current target: '{args.status}')")

    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    old_status = task.status
    task.status = args.status

    if args.status in ("completed", "failed", "dropped"):
        task.completed_at = datetime.now(UTC)
    else:
        task.completed_at = None

    if args.status == "failed" and args.reason:
        task.failure_reason = args.reason
    elif args.status != "failed":
        task.failure_reason = None

    store.update(task)
    _cleanup_worker_registry(config, task_id)

    print(f"Task {task_id} status: {old_status} → {args.status}")
    return 0


def _cleanup_worker_registry(config: "Config", task_id: str) -> None:
    """Mark any running worker for a task as completed in the worker registry.

    Looks up the most recent worker associated with the task and calls
    registry.mark_completed() to update worker metadata and remove the PID file.
    If no worker exists for the task, this is a no-op.
    """
    registry = WorkerRegistry(config.workers_path)
    worker = _latest_worker_for_task(registry, task_id)
    if worker is None:
        return
    if worker.status in ("running", "stale"):
        registry.mark_completed(worker.worker_id, exit_code=0, status="completed")


def _resolve_impl_task(
    store: SqliteTaskStore, task_id: str
) -> tuple[DbTask, None] | tuple[None, str]:
    """Walk up the lineage chain to find the root implement task.

    Accepts implement, review, or improve task IDs and resolves to the
    root implement task.  Returns ``(impl_task, None)`` on success or
    ``(None, error_message)`` on failure.
    """
    task = store.get(task_id)
    if not task:
        return None, f"Task {task_id} not found"

    if task.task_type == "implement":
        return task, None

    if task.task_type == "improve":
        if not task.based_on:
            return None, f"Improve task {task.id} has no based_on implementation task"
        parent = store.get(task.based_on)
        if parent is None:
            return None, f"Improve task {task.id} points to task {task.based_on}, which was not found"
        if parent.task_type != "implement":
            return None, (
                f"Improve task {task.id} points to task {task.based_on}, "
                "which is not an implementation task"
            )
        return parent, None

    if task.task_type == "review":
        if not task.depends_on:
            return None, f"Review task {task.id} has no depends_on implementation task"
        parent = store.get(task.depends_on)
        if parent is None:
            return None, f"Review task {task.id} points to task {task.depends_on}, which was not found"
        if parent.task_type != "implement":
            return None, (
                f"Review task {task.id} points to task {task.depends_on}, "
                "which is not an implementation task"
            )
        return parent, None

    return None, (
        f"Task {task_id} is a {task.task_type} task, not an implementation, improve, or review task"
    )


def cmd_improve(args: argparse.Namespace) -> int:
    """Create an improve task based on an implementation task and its most recent review."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns

    store = get_store(config)

    impl_task, err = _resolve_impl_task(store, resolve_id(config, args.task_id))
    if err:
        print(f"Error: {err}")
        return 1
    assert impl_task is not None
    assert impl_task.id is not None

    review_id_override = getattr(args, "review_id", None)
    if review_id_override is not None:
        resolved_review_id = resolve_id(config, review_id_override)
        review_task = store.get(resolved_review_id)
        if review_task is None:
            print(f"Error: Review task {resolved_review_id} not found.")
            return 1
        if review_task.task_type != "review":
            print(
                f"Error: Task {resolved_review_id} is a {review_task.task_type} task, not a review."
            )
            return 1
        if review_task.depends_on != impl_task.id:
            print(
                f"Error: Review {resolved_review_id} reviews task {review_task.depends_on}, "
                f"not implementation {impl_task.id}."
            )
            return 1
        if review_task.status != "completed":
            print(
                f"Warning: Review {review_task.id} is {review_task.status}. "
                "The improve task will be blocked until it completes."
            )
    else:
        # Auto-pick the most recent usable review. Dropped/failed reviews are
        # terminal bad states — they cannot produce a usable report, and binding
        # an improve task to one creates an unrunnable dependency. Pending and
        # in_progress reviews are still eligible since they may yet complete.
        review_tasks = store.get_reviews_for_task(impl_task.id)
        usable_reviews = [
            r for r in review_tasks if r.status not in ("dropped", "failed")
        ]

        if not usable_reviews:
            if review_tasks:
                statuses = ", ".join(
                    f"{r.id} ({r.status})" for r in review_tasks
                )
                print(
                    f"Error: Task {impl_task.id} has no usable review "
                    f"(all existing reviews are dropped or failed: {statuses})."
                )
                print("Run a new review, or pass --review-id <id> to pick a specific one.")
            else:
                print(f"Error: Task {impl_task.id} has no review. Run a review first:")
                print(f"  gza add --type review --depends-on {impl_task.id}")
            return 1

        review_task = usable_reviews[0]

        # Warn if the selected review is not yet completed (pending/in_progress).
        if review_task.status != "completed":
            print(
                f"Warning: Review {review_task.id} is {review_task.status}. "
                "The improve task will be blocked until it completes."
            )

    # Create improve task (using shared helper)
    try:
        improve_task = _create_improve_task(
            store,
            impl_task,
            review_task,
            create_review=args.review if hasattr(args, 'review') and args.review else False,
            model=args.model if hasattr(args, 'model') and args.model else None,
            provider=args.provider if hasattr(args, 'provider') and args.provider else None,
        )
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    print(f"✓ Created improve task {improve_task.id}")
    print(f"  Based on: implementation {impl_task.id}")
    print(f"  Review: {review_task.id}")
    print(f"  Branch: {impl_task.branch or '(will use implementation branch)'}")

    # Handle background mode - spawn worker to run the improve task
    if hasattr(args, 'background') and args.background:
        assert improve_task.id is not None
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [improve_task.id]
        return _spawn_background_worker(worker_args, config, task_id=improve_task.id)

    # Handle queue mode - add to queue without executing
    if hasattr(args, 'queue') and args.queue:
        return 0

    # Default: run the improve task immediately
    print(f"\nRunning improve task {improve_task.id}...")
    return _run_foreground(config, task_id=improve_task.id, force=getattr(args, "force", False))


def cmd_review(args: argparse.Namespace) -> int:
    """Create a review task for an implementation/improve task and optionally run it."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    store = get_store(config)

    # Resolve target implementation from provided task (accepts implement, improve, or review)
    impl_task, err = _resolve_impl_task(store, resolve_id(config, args.task_id))
    if err:
        print(f"Error: {err}")
        return 1
    assert impl_task is not None

    # Check if task is completed
    if impl_task.status != "completed":
        print(f"Error: Task {impl_task.id} is {impl_task.status}. Can only review completed tasks.")
        return 1

    # Create review task (using shared helper)
    model = args.model if hasattr(args, 'model') and args.model else None
    provider = args.provider if hasattr(args, 'provider') and args.provider else None
    try:
        review_task = _create_review_task(store, impl_task, model=model, provider=provider)
    except DuplicateReviewError as e:
        review = e.active_review
        print(f"Warning: A review task already exists for implementation {impl_task.id}")
        print(f"  Existing review: {review.id} (status: {review.status})")
        print(f"  Use 'gza work' to run it, or 'gza review {impl_task.id}' after it completes.")
        return 1
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    print(f"✓ Created review task {review_task.id}")
    print(f"  Implementation: {impl_task.id}")
    if impl_task.group:
        print(f"  Group: {impl_task.group}")

    # Handle background mode - spawn worker to run the review task
    if hasattr(args, 'background') and args.background:
        assert review_task.id is not None
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [review_task.id]
        return _spawn_background_worker(worker_args, config, task_id=review_task.id)

    # Handle queue mode - add to queue without executing
    if hasattr(args, 'queue') and args.queue:
        return 0

    # Default: run the review task immediately
    # Note: PR posting happens in _run_non_code_task, no need to do it here
    print(f"\nRunning review task {review_task.id}...")
    open_after = hasattr(args, 'open') and args.open
    return _run_foreground(
        config,
        task_id=review_task.id,
        open_after=open_after,
        force=getattr(args, "force", False),
    )


def _spawn_background_iterate(
    args: argparse.Namespace,
    config: Config,
    impl_task: DbTask,
    *,
    quiet: bool = False,
) -> int:
    """Spawn the iterate loop as a detached background process."""
    from ._common import _spawn_detached_worker_process

    registry = WorkerRegistry(config.workers_path)
    worker_id = registry.generate_worker_id()

    inner_cmd = [
        sys.executable, "-m", "gza",
        "iterate",
        str(impl_task.id),
        "--max-iterations", str(getattr(args, 'max_iterations', 3) or 3),
    ]

    if getattr(args, 'no_docker', False):
        inner_cmd.append("--no-docker")
    if getattr(args, 'resume', False):
        inner_cmd.append("--resume")
    if getattr(args, 'retry', False):
        inner_cmd.append("--retry")
    if getattr(args, "force", False):
        inner_cmd.append("--force")

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
        if not quiet:
            print(f"Started iterate worker {worker_id} (PID {proc.pid})")
            print(f"  Task: {impl_task.id}")
            print()
            print("Use 'gza ps' to view running workers")
            print(f"Use 'gza log -w {worker_id} -f' to follow output")
        return 0
    except Exception as e:
        print(f"Error spawning background iterate worker: {e}")
        return 1


def cmd_iterate(args: argparse.Namespace) -> int:
    """Run an automated review/improve loop for an implementation task.

    Loops: create+run review -> parse verdict -> if CHANGES_REQUESTED create+run improve -> repeat.
    Stops on APPROVED, max iterations reached, NEEDS_DISCUSSION, or failure.
    """
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    store = get_store(config)
    max_iterations: int = getattr(args, 'max_iterations', 3) or 3
    dry_run: bool = getattr(args, 'dry_run', False)
    use_resume: bool = getattr(args, 'resume', False)
    use_retry: bool = getattr(args, 'retry', False)
    background: bool = getattr(args, 'background', False)

    # cmd_iterate intentionally only accepts implement task IDs (not improve/review);
    # it manages the full review/improve cycle lifecycle and requires the root impl task.
    impl_task_id = resolve_id(config, args.impl_task_id)
    impl_task = store.get(impl_task_id)
    if not impl_task:
        print(f"Error: Task {impl_task_id} not found")
        return 1
    if impl_task.task_type != "implement":
        print(f"Error: Task {impl_task.id} is a {impl_task.task_type} task. Expected an implement task.")
        return 1

    allowed_statuses = {"completed", "pending", "failed"}
    if impl_task.status not in allowed_statuses:
        print(f"Error: Task {impl_task.id} is {impl_task.status}. Can only iterate completed, pending, or failed tasks.")
        return 1

    if impl_task.status == "failed" and not use_resume and not use_retry:
        print(f"Error: Task {impl_task.id} is failed. Use --resume or --retry to specify how to restart it.")
        return 1

    if (use_resume or use_retry) and impl_task.status != "failed":
        flag = "--resume" if use_resume else "--retry"
        print(f"Error: {flag} is only valid for failed tasks (task {impl_task.id} is {impl_task.status}).")
        return 1

    assert impl_task.id is not None

    # Handle background mode: re-exec this command as a detached process.
    if background:
        if dry_run:
            print(f"[dry-run] Would run iterate in background for {impl_task.id}")
            return 0
        return _spawn_background_iterate(args, config, impl_task)

    # If the task is pending, run it first before entering the review/improve loop.
    if impl_task.status == "pending":
        if dry_run:
            print(f"[dry-run] Would run pending implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
            return 0

        print(f"Running pending implementation {impl_task.id}...")
        rc = _run_foreground(config, task_id=impl_task.id, force=getattr(args, "force", False))
        if rc != 0:
            print(f"Implementation {impl_task.id} failed (exit code {rc})")
            return 1
        # Re-fetch to get updated status
        impl_task = store.get(impl_task.id) or impl_task
        assert impl_task.id is not None
        if impl_task.status == "failed":
            print(f"Implementation {impl_task.id} failed, cannot continue iteration.")
            return 1

    # If the task is failed, resume or retry it first.
    if impl_task.status == "failed":
        if use_resume:
            if not impl_task.session_id:
                print(f"Error: Task {impl_task.id} has no session ID (cannot resume). Use --retry instead.")
                return 1
            if dry_run:
                print(f"[dry-run] Would resume failed implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
                return 0
            new_task = _create_resume_task(store, impl_task)
            assert new_task.id is not None
            print(f"Resuming failed implementation {impl_task.id} as {new_task.id}...")
            rc = _run_foreground(
                config,
                task_id=new_task.id,
                resume=True,
                force=getattr(args, "force", False),
            )
        else:
            # --retry
            if dry_run:
                print(f"[dry-run] Would retry failed implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
                return 0
            new_task = store.add(
                prompt=impl_task.prompt,
                task_type=impl_task.task_type,
                group=impl_task.group,
                spec=impl_task.spec,
                depends_on=impl_task.depends_on,
                create_review=impl_task.create_review,
                same_branch=impl_task.same_branch,
                task_type_hint=impl_task.task_type_hint,
                based_on=impl_task.id,
                model=impl_task.model,
                provider=impl_task.provider if impl_task.provider_is_explicit else None,
                provider_is_explicit=impl_task.provider_is_explicit,
            )
            assert new_task.id is not None
            print(f"Retrying failed implementation {impl_task.id} as {new_task.id}...")
            rc = _run_foreground(config, task_id=new_task.id, force=getattr(args, "force", False))

        if rc != 0:
            action = "Resume" if use_resume else "Retry"
            print(f"{action} of {impl_task.id} failed (exit code {rc})")
            return 1

        # The new task is now the impl task for the review/improve loop
        impl_task = store.get(new_task.id) or new_task
        assert impl_task.id is not None
        if impl_task.status == "failed":
            action = "Resume" if use_resume else "Retry"
            print(f"{action} of {impl_task_id} failed, cannot continue iteration.")
            return 1

    assert impl_task.id is not None

    def _task_order_key(task: DbTask) -> tuple[datetime, int]:
        created = task.created_at or datetime.min
        return (created, task_id_numeric_key(task.id))

    def _latest_with_status(tasks: list[DbTask], status: str) -> DbTask | None:
        matching = [task for task in tasks if task.status == status]
        if not matching:
            return None
        return max(matching, key=_task_order_key)

    def _latest_active_review(reviews: list[DbTask]) -> DbTask | None:
        # In-progress work must dominate pending siblings to avoid duplicate parallel work.
        in_progress = _latest_with_status(reviews, "in_progress")
        if in_progress is not None:
            return in_progress
        return _latest_with_status(reviews, "pending")

    def _latest_relevant_completed_review(reviews: list[DbTask]) -> DbTask | None:
        completed = [review for review in reviews if review.status == "completed"]
        review_cleared_at = impl_task.review_cleared_at
        if review_cleared_at is not None:
            completed = [
                review
                for review in completed
                if review.completed_at is None or review.completed_at > review_cleared_at
            ]
        if not completed:
            return None
        return max(
            completed,
            key=lambda review: (
                review.completed_at or datetime.min,
                review.created_at or datetime.min,
                task_id_numeric_key(review.id),
            ),
        )

    latest_review: DbTask | None = None
    latest_verdict: str | None = None
    start_with_new_improve = False
    start_with_existing_improve: DbTask | None = None
    start_with_in_progress_improve: DbTask | None = None
    reviews = store.get_reviews_for_task(impl_task.id)
    active_review = _latest_active_review(reviews)
    if active_review is not None:
        latest_review = active_review
    else:
        latest_review = _latest_relevant_completed_review(reviews)
    if latest_review and latest_review.status == "completed":
        latest_verdict = get_review_verdict(config, latest_review)
        if latest_verdict == "APPROVED":
            print(f"Latest review {latest_review.id} is APPROVED; nothing to do.")
            return 0
        if latest_verdict in ("NEEDS_DISCUSSION", None):
            label = "no verdict" if latest_verdict is None else latest_verdict
            print(f"Latest review {latest_review.id} is blocked ({label}); manual review required.")
            return 3
        if latest_verdict == "CHANGES_REQUESTED":
            assert latest_review.id is not None
            improves = store.get_improve_tasks_for(impl_task.id, latest_review.id)
            pending_improve = _latest_with_status(improves, "pending")
            in_progress_improve = _latest_with_status(improves, "in_progress")
            if in_progress_improve is not None:
                start_with_in_progress_improve = in_progress_improve
            elif pending_improve is not None:
                start_with_existing_improve = pending_improve
            elif not improves:
                start_with_new_improve = True

    if dry_run:
        if start_with_in_progress_improve is not None and latest_review is not None:
            print(
                f"[dry-run] Would wait for in-progress improve {start_with_in_progress_improve.id} "
                f"for review {latest_review.id} on implementation {impl_task.id}"
            )
        elif start_with_existing_improve is not None and latest_review is not None:
            print(
                f"[dry-run] Would run existing improve {start_with_existing_improve.id} "
                f"for review {latest_review.id} on implementation {impl_task.id} "
                f"(max {max_iterations} iterations)"
            )
        elif start_with_new_improve and latest_review is not None:
            print(
                f"[dry-run] Would create improve for existing review {latest_review.id} "
                f"on implementation {impl_task.id} (max {max_iterations} iterations)"
            )
        elif active_review is not None:
            if active_review.status == "pending":
                print(
                    f"[dry-run] Would run existing pending review {active_review.id} "
                    f"for implementation {impl_task.id} (max {max_iterations} iterations)"
                )
            else:
                print(
                    f"[dry-run] Would wait for in-progress review {active_review.id} "
                    f"on implementation {impl_task.id}"
                )
        else:
            print(f"[dry-run] Would iterate implementation {impl_task.id} (max {max_iterations} iterations)")
        return 0

    print(f"Iterating implementation {impl_task.id} (max {max_iterations} iterations)...")

    @dataclass(frozen=True)
    class IterateSummaryRow:
        iteration_index: int
        task_type: str
        task_id: str | None
        verdict: str | None
        duration_seconds: float | None
        steps: int | None
        cost_usd: float | None
        status: str
        failure_reason: str | None

    def _format_compact_duration(seconds: float | None) -> str:
        if seconds is None:
            return "-"
        return format_duration(seconds).replace(" ", "")

    def _format_summary_status(row: IterateSummaryRow) -> str:
        if row.failure_reason:
            return f"{row.status} ({row.failure_reason})"
        return row.status

    def _append_summary_row(
        rows: list[IterateSummaryRow],
        *,
        iteration_index: int,
        task_type: str,
        task: DbTask | None,
        verdict: str | None = None,
        status: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        refreshed_task = task
        if task is not None and task.id is not None:
            refreshed_task = store.get(task.id) or task

        row_status = status or (refreshed_task.status if refreshed_task else "failed")
        row_failure_reason = (refreshed_task.failure_reason if refreshed_task else None) or failure_reason

        rows.append(
            IterateSummaryRow(
                iteration_index=iteration_index,
                task_type=task_type,
                task_id=refreshed_task.id if refreshed_task else None,
                verdict=verdict,
                duration_seconds=refreshed_task.duration_seconds if refreshed_task else None,
                steps=get_task_step_count(refreshed_task) if refreshed_task else None,
                cost_usd=refreshed_task.cost_usd if refreshed_task else None,
                status=row_status,
                failure_reason=row_failure_reason,
            )
        )

    iterate_started_at = time.monotonic()
    summary_rows: list[IterateSummaryRow] = []

    final_status = "maxed_out"
    final_stop_reason = "max_iterations"
    iteration = 0

    if start_with_in_progress_improve is not None and latest_review is not None:
        print(
            f"  Waiting: improve {start_with_in_progress_improve.id} for review {latest_review.id} is in_progress."
        )
        _append_summary_row(
            summary_rows,
            iteration_index=iteration,
            task_type="review",
            task=latest_review,
            verdict=latest_verdict,
        )
        _append_summary_row(
            summary_rows,
            iteration_index=iteration,
            task_type="improve",
            task=start_with_in_progress_improve,
            status="in_progress",
        )
        final_status = "blocked"
        final_stop_reason = "improve_in_progress"
        iteration = max_iterations

    if (
        (start_with_new_improve or start_with_existing_improve is not None)
        and latest_review is not None
        and iteration < max_iterations
    ):
        print(f"\nIteration {iteration + 1}/{max_iterations} (starting from existing review {latest_review.id})")
        _append_summary_row(
            summary_rows,
            iteration_index=iteration,
            task_type="review",
            task=latest_review,
            verdict=latest_verdict,
        )
        improve_task: DbTask | None = None
        if start_with_existing_improve is not None:
            improve_task = start_with_existing_improve
            assert improve_task.id is not None
            print(f"  Reusing existing improve {improve_task.id}...")
        else:
            try:
                improve_task = _create_improve_task(store, impl_task, latest_review)
            except ValueError as e:
                print(f"  Error creating improve: {e}")
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="improve",
                    task=None,
                    status="failed",
                    failure_reason=str(e),
                )
                final_status = "blocked"
                final_stop_reason = "improve_failed"
                iteration = max_iterations
            else:
                assert improve_task.id is not None

        if iteration < max_iterations:
            assert improve_task is not None
            print(f"  Running improve {improve_task.id}...")
            rc = _run_foreground(config, task_id=improve_task.id, force=getattr(args, "force", False))
            if rc != 0:
                print(f"  Improve {improve_task.id} failed (exit code {rc})")
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="improve",
                    task=improve_task,
                    status="failed",
                    failure_reason=f"exit code {rc}",
                )
                final_status = "blocked"
                final_stop_reason = "improve_failed"
                iteration = max_iterations
            else:
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="improve",
                    task=improve_task,
                )
                iteration += 1

    while iteration < max_iterations:
        print(f"\nIteration {iteration + 1}/{max_iterations}")

        # --- REVIEW PHASE ---
        latest_active_review = _latest_active_review(store.get_reviews_for_task(impl_task.id))
        if latest_active_review is not None:
            review_task = latest_active_review
            if review_task.status == "in_progress":
                print(f"  Waiting: review {review_task.id} is in_progress.")
                final_status = "blocked"
                final_stop_reason = "review_in_progress"
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=review_task,
                    status="in_progress",
                )
                break
            print(f"  Reusing existing pending review {review_task.id}...")
        else:
            try:
                review_task = _create_review_task(store, impl_task)
            except DuplicateReviewError as e:
                review_task = e.active_review
                if review_task.status == "in_progress":
                    print(f"  Waiting: review {review_task.id} is in_progress.")
                    final_status = "blocked"
                    final_stop_reason = "review_in_progress"
                    _append_summary_row(
                        summary_rows,
                        iteration_index=iteration,
                        task_type="review",
                        task=review_task,
                        status="in_progress",
                    )
                    break
                print(f"  Reusing existing pending review {review_task.id}...")
            except ValueError as e:
                print(f"  Error creating review: {e}")
                final_status = "blocked"
                final_stop_reason = "review_failed"
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=None,
                    status="failed",
                    failure_reason=str(e),
                )
                break

        assert review_task.id is not None

        print(f"  Running review {review_task.id}...")
        rc = _run_foreground(config, task_id=review_task.id, force=getattr(args, "force", False))
        if rc != 0:
            print(f"  Review {review_task.id} failed (exit code {rc})")
            final_status = "blocked"
            final_stop_reason = "review_failed"
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type="review",
                task=review_task,
                status="failed",
                failure_reason=f"exit code {rc}",
            )
            break

        # Re-fetch review task to get updated output_content
        review_task = store.get(review_task.id) or review_task

        # Parse verdict
        verdict = get_review_verdict(config, review_task)
        _append_summary_row(
            summary_rows,
            iteration_index=iteration,
            task_type="review",
            task=review_task,
            verdict=verdict,
        )

        print(f"  Review {review_task.id}: verdict={verdict or '(none)'}")

        if verdict == "APPROVED":
            final_status = "approved"
            final_stop_reason = "approved"
            break

        if verdict == "NEEDS_DISCUSSION" or verdict is None:
            final_status = "blocked"
            final_stop_reason = "needs_discussion" if verdict == "NEEDS_DISCUSSION" else "no_verdict"
            break

        # verdict == "CHANGES_REQUESTED"
        # --- IMPROVE PHASE ---
        try:
            # create_review is intentionally omitted (defaults to False) here:
            # iterate manages the review/improve cadence itself, so the improve
            # task must NOT auto-create a follow-up review on completion.
            improve_task = _create_improve_task(store, impl_task, review_task)
        except ValueError as e:
            print(f"  Error creating improve: {e}")
            final_status = "blocked"
            final_stop_reason = "improve_failed"
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type="improve",
                task=None,
                status="failed",
                failure_reason=str(e),
            )
            break

        assert improve_task.id is not None

        print(f"  Running improve {improve_task.id}...")
        rc = _run_foreground(config, task_id=improve_task.id, force=getattr(args, "force", False))
        if rc != 0:
            print(f"  Improve {improve_task.id} failed (exit code {rc})")
            final_status = "blocked"
            final_stop_reason = "improve_failed"
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type="improve",
                task=improve_task,
                status="failed",
                failure_reason=f"exit code {rc}",
            )
            break

        _append_summary_row(
            summary_rows,
            iteration_index=iteration,
            task_type="improve",
            task=improve_task,
        )

        # Reviews in subsequent iterations still target the original impl_task because
        # improve runs on same_branch=True, so the branch already has the latest code.
        iteration += 1

    iterate_wall_seconds = time.monotonic() - iterate_started_at
    total_steps = sum(row.steps or 0 for row in summary_rows)
    total_cost = sum(row.cost_usd or 0.0 for row in summary_rows)

    # Print summary table
    print(f"\n{'=' * 60}")
    print(f"Iterate complete: {final_status.upper()} ({final_stop_reason})")
    print(f"{'=' * 60}")
    print(f"{'Iter':<5} {'Type':<8} {'Task':<10} {'Verdict':<18} {'Duration':>8} {'Steps':>5} {'Cost':>8} Status")
    print(f"{'-' * 5} {'-' * 8} {'-' * 10} {'-' * 18} {'-' * 8} {'-' * 5} {'-' * 8} {'-' * 12}")
    for row in summary_rows:
        iter_str = str(row.iteration_index + 1)
        verdict_str = row.verdict or "-"
        duration_str = _format_compact_duration(row.duration_seconds)
        steps_str = str(row.steps) if row.steps is not None else "-"
        cost_str = f"${row.cost_usd:.2f}" if row.cost_usd is not None else "-"
        status_str = _format_summary_status(row)
        task_str = row.task_id or "-"
        print(
            f"{iter_str:<5} {row.task_type:<8} {task_str:<10} {verdict_str:<18} "
            f"{duration_str:>8} {steps_str:>5} {cost_str:>8} {status_str}"
        )
    print(f"Totals: {_format_compact_duration(iterate_wall_seconds)} wall | {total_steps} steps | ${total_cost:.2f}")
    print()

    if final_status == "approved":
        return 0
    if final_status == "maxed_out":
        print(f"Max iterations ({max_iterations}) reached.")
        return 2
    if final_stop_reason in {"review_in_progress", "improve_in_progress"}:
        print(f"Iterate waiting: {final_stop_reason}. Existing task is already in progress.")
        return 3
    print(f"Iterate blocked: {final_stop_reason}. Manual review required.")
    return 3


def cmd_resume(args: argparse.Namespace) -> int:
    """Resume a failed or orphaned task from where it left off."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

    # Override max_turns if specified
    if hasattr(args, 'max_turns') and args.max_turns is not None:
        config.max_steps = args.max_turns
        config.max_turns = args.max_turns

    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    if task.status not in ("failed", "in_progress"):
        print(f"Error: Can only resume failed or orphaned tasks (task is {task.status})")
        return 1

    if task.status == "in_progress":
        # Allow resume only if the task is orphaned (no live worker)
        assert task.id is not None
        registry = WorkerRegistry(config.workers_path)
        running_worker = _running_worker_id_for_task(registry, task.id)
        if running_worker is not None:
            print(f"Error: Task {task_id} is still running (worker {running_worker})")
            print("Use 'gza cancel' to stop it first, or wait for it to finish")
            return 1
        print(f"Note: Task {task_id} appears orphaned (in_progress but no live worker), resuming...")
    elif task.status == "failed" and task.failure_reason == "WORKER_DIED":
        print(f"Note: Task {task_id} appears orphaned (worker died), resuming...")

    if not task.session_id:
        print(f"Error: Task {task_id} has no session ID (cannot resume)")
        print("Use 'gza retry' to start fresh instead")
        return 1

    # Create a new task (like retry) to track this resumed run.
    # The original task stays failed with its stats preserved.
    new_task = _create_resume_task(store, task)
    assert new_task.id is not None

    print(f"✓ Created task {new_task.id} (resume of {task_id})")

    # Handle background mode
    if args.background:
        return _spawn_background_resume_worker(args, config, new_task.id)

    # Handle queue mode - add to queue without executing
    if hasattr(args, 'queue') and args.queue:
        return 0

    # Default: run the new resume task immediately
    return _run_foreground(
        config,
        task_id=new_task.id,
        resume=True,
        force=getattr(args, "force", False),
    )
