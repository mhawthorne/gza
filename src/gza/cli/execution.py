"""Task execution commands: run, add, edit, retry, resume, review, improve, fix, iterate."""

import argparse
import json
import os
import re
import signal
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..config import DEFAULT_MAX_RESUME_ATTEMPTS, Config
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
from ..prompts import PromptBuilder
from ..query import get_base_task_slug as _get_base_task_slug
from ..runner import run
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import (
    DuplicateReviewError,
    _allow_pr_required_retry,
    _create_improve_task,
    _create_rebase_task,
    _create_resume_task,
    _create_review_task,
    _run_as_worker,
    _run_foreground,
    _spawn_background_iterate_worker,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    _spawn_background_workers,
    get_review_verdict,
    get_store,
    get_task_step_count,
    resolve_id,
    resolve_improve_action,
    run_with_resume,
    set_task_urgency,
)
from .advance_engine import determine_next_action
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

            allow_pr_retry = _allow_pr_required_retry(args, task)
            if task.status != "pending" and not allow_pr_retry:
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
        run_kwargs: dict[str, Any] = {
            "skip_precondition_check": getattr(args, "force", False),
        }
        if getattr(args, "create_pr", False):
            run_kwargs["create_pr"] = True

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
                result = run(config, task_id=task_id, **run_kwargs)
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
            result = run(config, **run_kwargs)

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

    # For same_branch tasks (improve/review) that have run and have a branch,
    # fork a new branch from the parent branch instead of reusing it.
    # This gives the retry agent a clean start without inheriting WIP commits
    # from the failed attempt.
    retry_same_branch = task.same_branch
    retry_base_branch: str | None = None
    if task.same_branch and task.branch:
        retry_same_branch = False
        retry_base_branch = task.branch

    # Create new task copying relevant fields
    new_task = store.add(
        prompt=task.prompt,
        task_type=task.task_type,
        group=task.group,
        spec=task.spec,
        depends_on=task.depends_on,
        create_review=task.create_review,
        same_branch=retry_same_branch,
        task_type_hint=task.task_type_hint,
        based_on=task_id,  # Track retry lineage
        model=task.model,
        provider=task.provider if task.provider_is_explicit else None,
        provider_is_explicit=task.provider_is_explicit,
        base_branch=retry_base_branch,
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


def _log_indicates_inline_skill(task: DbTask, config: Config) -> tuple[bool, str | None]:
    """Check synthetic inline-skill provenance in task logs.

    Returns ``(found, warning)`` where ``warning`` is set when log provenance
    could not be fully evaluated due to read or parse issues.
    """
    if not task.log_file:
        return False, None
    log_path = config.project_dir / Path(task.log_file)
    if not log_path.exists():
        return False, None
    malformed_lines = 0
    try:
        with log_path.open(encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    malformed_lines += 1
                    continue
                if entry.get("type") != "gza" or entry.get("subtype") != "provenance":
                    continue
                if entry.get("inline") is True and entry.get("skill"):
                    return True, None
    except OSError as exc:
        return (
            False,
            (
                f"Warning: Could not read task log '{task.log_file}' while checking inline provenance: "
                f"{exc.__class__.__name__}: {exc}"
            ),
        )
    if malformed_lines:
        return (
            False,
            (
                f"Warning: Found {malformed_lines} malformed JSON line(s) in task log "
                f"'{task.log_file}' while checking inline provenance; execution mode was not promoted."
            ),
        )
    return False, None


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

    if task.execution_mode is None:
        inline_skill_detected, log_warning = _log_indicates_inline_skill(task, config)
        if log_warning:
            print(log_warning)
        if inline_skill_detected:
            task.execution_mode = "skill_inline"
            store.update(task)

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
    if args.execution_mode and args.status != "in_progress":
        print("Error: --execution-mode is only valid when setting status to 'in_progress'")
        return 1

    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id = resolve_id(config, args.task_id)
    task = store.get(task_id)
    if not task:
        print(f"Error: Task {task_id} not found")
        return 1

    old_status = task.status
    task.status = args.status
    if args.status == "in_progress":
        if args.execution_mode:
            task.execution_mode = args.execution_mode
        else:
            task.execution_mode = "manual"

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


def _latest_completed_review_for_impl(store: SqliteTaskStore, impl_task_id: str) -> DbTask | None:
    reviews = [r for r in store.get_reviews_for_task(impl_task_id) if r.status == "completed"]
    return reviews[0] if reviews else None


def _resolve_impl_task(
    store: SqliteTaskStore, task_id: str
) -> tuple[DbTask, None] | tuple[None, str]:
    """Resolve implement/review/improve/fix IDs to the owning implementation task."""
    task = store.get(task_id)
    if not task:
        return None, f"Task {task_id} not found"

    if task.task_type == "implement":
        return task, None

    if task.task_type in {"improve", "fix"}:
        label = "Improve" if task.task_type == "improve" else "Fix"
        if not task.based_on:
            return None, f"{label} task {task.id} has no based_on implementation task"
        parent = store.get(task.based_on)
        if parent is None:
            return None, f"{label} task {task.id} points to task {task.based_on}, which was not found"
        seen: set[str] = set()
        while parent.task_type in {"improve", "fix"}:
            if parent.id is None:
                return None, f"{label} task {task.id} points to an invalid retry ancestor"
            if parent.id in seen:
                return None, f"{label} task {task.id} has a cycle in its based_on chain"
            seen.add(parent.id)
            if not parent.based_on:
                return None, (
                    f"{label} task {task.id} points to task {parent.id}, "
                    "which has no based_on implementation task"
                )
            next_parent = store.get(parent.based_on)
            if next_parent is None:
                return None, (
                    f"{label} task {task.id} points to task {parent.based_on}, "
                    "which was not found"
                )
            parent = next_parent
        if parent.task_type != "implement":
            return None, (
                f"{label} task {task.id} points to task {parent.id}, "
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
        f"Task {task_id} is a {task.task_type} task, not an implementation, improve, review, or fix task"
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


def _extract_fix_result(output_content: str | None) -> str | None:
    """Extract fix_result value from the machine-readable closure ledger."""
    if not output_content:
        return None
    match = re.search(r"^\s*fix_result:\s*([a-z_]+)\s*$", output_content, flags=re.MULTILINE)
    return match.group(1) if match else None


def cmd_fix(args: argparse.Namespace) -> int:
    """Create and run a fix task for a stuck implementation workflow."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

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

    if impl_task.status in {"pending", "in_progress"}:
        print(
            f"Error: Task {impl_task.id} is {impl_task.status}. "
            "Run/finish the implementation first, then run fix for stuck review/improve churn."
        )
        return 1

    latest_review = _latest_completed_review_for_impl(store, impl_task.id)
    review_id = latest_review.id if latest_review is not None else None
    fix_prompt = PromptBuilder().fix_task_prompt(impl_task.id, review_id)
    fix_task = store.add(
        fix_prompt,
        task_type="fix",
        based_on=impl_task.id,
        depends_on=review_id,
        same_branch=True,
        group=impl_task.group,
        model=args.model if hasattr(args, "model") and args.model else None,
        provider=args.provider if hasattr(args, "provider") and args.provider else None,
    )
    assert fix_task.id is not None

    print(f"✓ Created fix task {fix_task.id}")
    print(f"  Implementation: {impl_task.id}")
    if review_id:
        print(f"  Latest completed review: {review_id}")
    else:
        print("  Latest completed review: (none found)")
    print("  Handoff policy: changed code requires a fresh independent review")

    if hasattr(args, 'background') and args.background:
        worker_args = argparse.Namespace(**vars(args))
        worker_args.task_ids = [fix_task.id]
        return _spawn_background_worker(worker_args, config, task_id=fix_task.id)

    if hasattr(args, 'queue') and args.queue:
        return 0

    commits_before: int | None = None
    default_branch: str | None = None
    git_runtime: Git | None = None
    if impl_task.branch:
        try:
            git_runtime = Git(config.project_dir)
            default_branch = git_runtime.default_branch()
            commits_before = git_runtime.count_commits_ahead(impl_task.branch, default_branch)
        except Exception:
            commits_before = None
            default_branch = None
            git_runtime = None

    print(f"\nRunning fix task {fix_task.id}...")
    rc = _run_foreground(config, task_id=fix_task.id, force=getattr(args, "force", False))
    if rc != 0:
        return rc

    completed_fix = store.get(fix_task.id)
    if completed_fix is None:
        return rc

    fix_result = _extract_fix_result(completed_fix.output_content)
    if fix_result:
        print(f"Fix result: {fix_result}")

    code_changed: bool | None = None
    if impl_task.branch and commits_before is not None and default_branch and git_runtime is not None:
        try:
            commits_after = git_runtime.count_commits_ahead(impl_task.branch, default_branch)
            code_changed = commits_after > commits_before
        except Exception:
            code_changed = None

    if code_changed is False:
        print("Fix completed without new commits; no follow-up review was auto-created.")
        if not fix_result:
            print("Suggested handoff: diagnosed_no_change or needs_user.")
        return rc

    try:
        review_task = _create_review_task(store, impl_task)
        print(f"✓ Created follow-up review task {review_task.id} for implementation {impl_task.id}")
        print(f"Next step: uv run gza work {review_task.id}")
    except DuplicateReviewError as exc:
        review_task = exc.active_review
        print(
            f"Follow-up review already exists: {review_task.id} ({review_task.status}). "
            "Use `uv run gza work` to execute it."
        )
    except ValueError as exc:
        print(f"Warning: could not create follow-up review automatically: {exc}")
        print(f"Next step: uv run gza review {impl_task.id}")

    return rc


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
    max_iterations: int | None = None,
    dry_run: bool = False,
) -> int:
    """Spawn the iterate loop as a detached background process."""
    effective_max_iterations = max_iterations
    if effective_max_iterations is None:
        arg_value = getattr(args, "max_iterations", None)
        effective_max_iterations = arg_value if isinstance(arg_value, int) else config.iterate_max_iterations

    return _spawn_background_iterate_worker(
        args,
        config,
        impl_task,
        max_iterations=effective_max_iterations,
        resume=getattr(args, "resume", False),
        retry=getattr(args, "retry", False),
        dry_run=dry_run,
    )


@dataclass(frozen=True)
class _AdvanceEngineConfigAdapter:
    """Minimal config surface required by determine_next_action()."""

    project_dir: Any
    advance_requires_review: bool
    advance_create_reviews: bool
    max_review_cycles: int
    max_resume_attempts: int


def cmd_iterate(args: argparse.Namespace) -> int:
    """Run an automated lifecycle loop for an implementation task."""
    config = Config.load(args.project_dir)
    if hasattr(args, 'no_docker') and args.no_docker:
        config.use_docker = False

    store = get_store(config)
    def _int_config(value: object, default: int) -> int:
        return value if isinstance(value, int) else default

    max_iterations_arg = getattr(args, "max_iterations", None)
    max_iterations = max_iterations_arg if max_iterations_arg is not None else config.iterate_max_iterations
    if max_iterations <= 0:
        print("Error: --max-iterations must be a positive integer.")
        return 1
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
        return _spawn_background_iterate(
            args,
            config,
            impl_task,
            max_iterations=max_iterations,
            dry_run=dry_run,
        )

    def _run_task_with_resume(task_to_run: DbTask, *, initial_resume: bool = False) -> tuple[DbTask, int]:
        def _run_one(t: DbTask, resume_flag: bool) -> int:
            assert t.id is not None
            force = getattr(args, "force", False)
            if resume_flag or initial_resume:
                return _run_foreground(config, task_id=t.id, resume=True, force=force)
            return _run_foreground(config, task_id=t.id, force=force)

        def _on_resume(
            failed_task: DbTask,
            resume_task: DbTask,
            attempt: int,
            max_attempts: int,
        ) -> None:
            assert failed_task.id is not None
            assert resume_task.id is not None
            reason = failed_task.failure_reason or "UNKNOWN"
            print(
                f"  Auto-resume: {failed_task.id} failed with {reason}; "
                f"created {resume_task.id} (attempt {attempt}/{max_attempts})."
            )

        return run_with_resume(
            config,
            store,
            task_to_run,
            run_task=_run_one,
            max_resume_attempts=_int_config(
                getattr(config, "max_resume_attempts", None),
                DEFAULT_MAX_RESUME_ATTEMPTS,
            ),
            on_resume=_on_resume,
        )

    # If the task is pending, run it first before entering the loop.
    if impl_task.status == "pending":
        if dry_run:
            print(f"[dry-run] Would run pending implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
            return 0

        print(f"Running pending implementation {impl_task.id}...")
        impl_task, rc = _run_task_with_resume(impl_task)
        if rc != 0:
            print(f"Implementation {impl_task.id} failed (exit code {rc})")
            return 1
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
            run_start_task = _create_resume_task(store, impl_task)
            assert run_start_task.id is not None
            print(f"Resuming failed implementation {impl_task.id} as {run_start_task.id}...")
            impl_task, rc = _run_task_with_resume(run_start_task, initial_resume=True)
        else:
            # --retry
            if dry_run:
                print(f"[dry-run] Would retry failed implementation {impl_task.id} then iterate (max {max_iterations} iterations)")
                return 0
            run_start_task = store.add(
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
            assert run_start_task.id is not None
            print(f"Retrying failed implementation {impl_task.id} as {run_start_task.id}...")
            impl_task, rc = _run_task_with_resume(run_start_task)

        if rc != 0:
            action_label = "Resume" if use_resume else "Retry"
            print(f"{action_label} of {impl_task_id} failed (exit code {rc})")
            return 1

        # The new task is now the impl task for the loop
        assert impl_task.id is not None
        if impl_task.status == "failed":
            action_label = "Resume" if use_resume else "Retry"
            print(f"{action_label} of {impl_task_id} failed, cannot continue iteration.")
            return 1

    assert impl_task.id is not None

    try:
        git_runtime: Any = Git(config.project_dir)
        target_branch = git_runtime.current_branch()
    except Exception as exc:
        print(f"Error: failed to initialize git runtime for iterate: {exc}")
        return 1

    max_resume_attempts = _int_config(
        getattr(config, "max_resume_attempts", None),
        DEFAULT_MAX_RESUME_ATTEMPTS,
    )
    engine_config = _AdvanceEngineConfigAdapter(
        project_dir=config.project_dir,
        advance_requires_review=bool(getattr(config, "advance_requires_review", True)),
        advance_create_reviews=bool(getattr(config, "advance_create_reviews", True)),
        max_review_cycles=_int_config(getattr(config, "max_review_cycles", None), 3),
        max_resume_attempts=max_resume_attempts,
    )
    initial_action = determine_next_action(
        engine_config,
        store,
        git_runtime,
        impl_task,
        target_branch,
        max_resume_attempts=max_resume_attempts,
    )
    initial_action_type = initial_action["type"]
    initial_action_description = initial_action.get("description")
    if not isinstance(initial_action_description, str) or not initial_action_description:
        initial_action_description = initial_action_type

    iteration_actions = {"create_review", "run_review", "improve", "run_improve"}

    if dry_run:
        print(f"[dry-run] Would iterate implementation {impl_task.id} (max {max_iterations} iterations)")
        if initial_action_type in iteration_actions:
            print(f"[dry-run] First iteration 1/{max_iterations} action: {initial_action_type} - {initial_action_description}")
        else:
            print(f"[dry-run] First next action: {initial_action_type} - {initial_action_description}")
        return 0
    print(f"Iterating implementation {impl_task.id} (max {max_iterations} iterations)...")
    impl_task_key = impl_task.id
    assert impl_task_key is not None

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

    def _task_sort_key(task: DbTask) -> tuple[datetime, int]:
        return (task.created_at or datetime.min, task_id_numeric_key(task.id))

    def _latest_with_status(tasks: list[DbTask], status: str) -> DbTask | None:
        matching = [task for task in tasks if task.status == status]
        if not matching:
            return None
        return max(matching, key=_task_sort_key)

    def _latest_completed_review() -> DbTask | None:
        reviews = [r for r in store.get_reviews_for_task(impl_task_key) if r.status == "completed"]
        if not reviews:
            return None
        return max(
            reviews,
            key=lambda review: (
                review.completed_at or datetime.min,
                review.created_at or datetime.min,
                review.id or "",
            ),
        )

    iterate_started_at = time.monotonic()
    summary_rows: list[IterateSummaryRow] = []
    final_status = "maxed_out"
    final_stop_reason = "max_iterations"
    iteration = 0
    max_resume_attempts = _int_config(
        getattr(config, "max_resume_attempts", None),
        DEFAULT_MAX_RESUME_ATTEMPTS,
    )
    engine_config = _AdvanceEngineConfigAdapter(
        project_dir=config.project_dir,
        advance_requires_review=bool(getattr(config, "advance_requires_review", True)),
        advance_create_reviews=bool(getattr(config, "advance_create_reviews", True)),
        max_review_cycles=_int_config(getattr(config, "max_review_cycles", None), 3),
        max_resume_attempts=max_resume_attempts,
    )

    while iteration < max_iterations:
        action = determine_next_action(
            engine_config,
            store,
            git_runtime,
            impl_task,
            target_branch,
            max_resume_attempts=max_resume_attempts,
        )
        action_type = action["type"]
        if action_type in iteration_actions:
            print(f"\nIteration {iteration + 1}/{max_iterations}: {action_type}")
        else:
            print(f"\nNext action: {action_type}")

        if action_type == "merge":
            final_status = "merge_ready"
            final_stop_reason = "merge_ready"
            maybe_review_verdict: str | None = None
            maybe_review = action.get("review_task")
            if isinstance(maybe_review, DbTask):
                maybe_review_verdict = get_review_verdict(config, maybe_review) if maybe_review.status == "completed" else None
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=maybe_review,
                    verdict=maybe_review_verdict,
                )
            if maybe_review_verdict == "APPROVED":
                final_status = "approved"
                final_stop_reason = "approved"
            else:
                merge_desc = action.get("description")
                if isinstance(merge_desc, str) and merge_desc:
                    final_stop_reason = merge_desc
            break

        if action_type in {"needs_discussion", "max_cycles_reached", "skip"}:
            final_status = "blocked"
            final_stop_reason = action_type
            maybe_review = action.get("review_task")
            if isinstance(maybe_review, DbTask):
                maybe_verdict = get_review_verdict(config, maybe_review) if maybe_review.status == "completed" else None
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="review", task=maybe_review, verdict=maybe_verdict)
            break

        if action_type == "wait_review":
            final_status = "blocked"
            final_stop_reason = "review_in_progress"
            review_task = action.get("review_task")
            if isinstance(review_task, DbTask):
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=review_task,
                    status="in_progress",
                )
            else:
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="review", task=None, status="in_progress")
            break

        if action_type == "wait_improve":
            final_status = "blocked"
            final_stop_reason = "improve_in_progress"
            latest_review = _latest_completed_review()
            if latest_review is not None:
                review_verdict = get_review_verdict(config, latest_review)
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="review",
                    task=latest_review,
                    verdict=review_verdict,
                )
                assert latest_review.id is not None
                improves = store.get_improve_tasks_for(impl_task_key, latest_review.id)
                running_improve = _latest_with_status(improves, "in_progress")
                if running_improve is None:
                    running_improve = _latest_with_status(improves, "pending")
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="improve",
                    task=running_improve,
                    status="in_progress",
                )
            else:
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="improve", task=None, status="in_progress")
            break

        action_task: DbTask | None = None
        verdict: str | None = None
        initial_resume = False
        review_row_task: DbTask | None = None
        review_row_verdict: str | None = None

        if action_type == "resume":
            action_task = _create_resume_task(store, impl_task)
            assert action_task.id is not None
            initial_resume = True
            print(f"  Resuming implementation as {action_task.id}...")
        elif action_type == "needs_rebase":
            if not impl_task.branch:
                print(f"  Cannot rebase {impl_task.id}: no branch")
                final_status = "blocked"
                final_stop_reason = "needs_rebase"
                _append_summary_row(summary_rows, iteration_index=iteration, task_type="rebase", task=None, status="failed")
                break
            action_task = _create_rebase_task(store, impl_task.id, impl_task.branch, target_branch)
            assert action_task.id is not None
            print(f"  Created rebase task {action_task.id}...")
        elif action_type == "create_review":
            try:
                action_task = _create_review_task(store, impl_task)
            except DuplicateReviewError as e:
                action_task = e.active_review
                assert action_task.id is not None
                if action_task.status == "in_progress":
                    print(f"  Waiting for review {action_task.id}: already in progress.")
                    final_status = "blocked"
                    final_stop_reason = "review_in_progress"
                    _append_summary_row(
                        summary_rows,
                        iteration_index=iteration,
                        task_type="review",
                        task=action_task,
                        status="in_progress",
                    )
                    break
                if action_task.status != "pending":
                    print(f"  Error creating review: duplicate review {action_task.id} has unexpected status {action_task.status}.")
                    final_status = "blocked"
                    final_stop_reason = "review_failed"
                    _append_summary_row(summary_rows, iteration_index=iteration, task_type="review", task=action_task, status="failed")
                    break
                print(f"  Reusing pending review {action_task.id}...")
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
            assert action_task.id is not None
            print(f"  Running review {action_task.id}...")
        elif action_type == "run_review":
            action_task = action["review_task"]
            assert action_task.id is not None
            print(f"  Running pending review {action_task.id}...")
        elif action_type == "improve":
            review_task = action["review_task"]
            review_row_task = review_task
            review_row_verdict = get_review_verdict(config, review_task)
            assert impl_task.id is not None
            assert review_task.id is not None

            # Use shared logic to decide resume/retry/new for this impl+review pair
            improve_action, failed_improve = resolve_improve_action(
                store, impl_task.id, review_task.id, max_resume_attempts=max_resume_attempts
            )
            if improve_action == "give_up" and failed_improve is not None:
                assert failed_improve.id is not None
                print(
                    f"  Improve for {review_task.id} has exceeded max_resume_attempts"
                    f" ({max_resume_attempts}); latest failure: {failed_improve.id}"
                )
                final_status = "blocked"
                final_stop_reason = "max_improve_attempts"
                _append_summary_row(
                    summary_rows,
                    iteration_index=iteration,
                    task_type="improve",
                    task=failed_improve,
                    status="failed",
                )
                break
            if improve_action == "resume" and failed_improve is not None:
                assert failed_improve.id is not None
                action_task = _create_resume_task(store, failed_improve)
                initial_resume = True
                print(f"  Created improve task {action_task.id} (resume of {failed_improve.id})")
            elif improve_action == "retry" and failed_improve is not None:
                assert failed_improve.id is not None
                retry_same_branch = failed_improve.same_branch
                retry_base_branch: str | None = None
                if failed_improve.same_branch and failed_improve.branch:
                    retry_same_branch = False
                    retry_base_branch = failed_improve.branch
                action_task = store.add(
                    prompt=failed_improve.prompt,
                    task_type='improve',
                    depends_on=failed_improve.depends_on,
                    based_on=failed_improve.id,
                    same_branch=retry_same_branch,
                    group=failed_improve.group,
                    base_branch=retry_base_branch,
                )
                print(f"  Created improve task {action_task.id} (retry of {failed_improve.id})")
            else:
                try:
                    action_task = _create_improve_task(store, impl_task, review_task)
                except ValueError as e:
                    print(f"  Error creating improve task: {e}")
                    final_status = "blocked"
                    final_stop_reason = "improve_failed"
                    if review_row_task is not None:
                        _append_summary_row(
                            summary_rows,
                            iteration_index=iteration,
                            task_type="review",
                            task=review_row_task,
                            verdict=review_row_verdict,
                        )
                    _append_summary_row(
                        summary_rows,
                        iteration_index=iteration,
                        task_type="improve",
                        task=None,
                        status="failed",
                        failure_reason=str(e),
                    )
                    break
            assert action_task.id is not None
            print(f"  Running improve {action_task.id}...")
        elif action_type == "run_improve":
            action_task = action["improve_task"]
            assert action_task.id is not None
            if action_task.depends_on:
                maybe_review = store.get(action_task.depends_on)
                if maybe_review is not None and maybe_review.task_type == "review":
                    review_row_task = maybe_review
                    review_row_verdict = get_review_verdict(config, maybe_review)
            print(f"  Running pending improve {action_task.id}...")
        else:
            final_status = "blocked"
            final_stop_reason = f"unsupported_action:{action_type}"
            _append_summary_row(summary_rows, iteration_index=iteration, task_type=action_type, task=None, status="failed")
            break

        if review_row_task is not None:
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type="review",
                task=review_row_task,
                verdict=review_row_verdict,
            )

        assert action_task is not None
        action_task, rc = _run_task_with_resume(action_task, initial_resume=initial_resume)
        if rc != 0:
            final_status = "blocked"
            final_stop_reason = f"{action_type}_failed"
            task_type = "review" if action_type in {"create_review", "run_review"} else "improve" if action_type in {"improve", "run_improve"} else action_type
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type=task_type,
                task=action_task,
                status="failed",
                failure_reason=f"exit code {rc}",
            )
            break

        if action_task.id is not None:
            action_task = store.get(action_task.id) or action_task

        if action_type in {"create_review", "run_review"}:
            verdict = get_review_verdict(config, action_task)
            print(f"  Review {action_task.id}: verdict={verdict or '(none)'}")
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type="review",
                task=action_task,
                verdict=verdict,
            )
            if verdict == "APPROVED":
                final_status = "approved"
                final_stop_reason = "approved"
                break
            if verdict in {"NEEDS_DISCUSSION", None}:
                final_status = "blocked"
                final_stop_reason = "needs_discussion" if verdict == "NEEDS_DISCUSSION" else "no_verdict"
                break
        else:
            task_type = "improve" if action_type in {"improve", "run_improve"} else action_type
            _append_summary_row(
                summary_rows,
                iteration_index=iteration,
                task_type=task_type,
                task=action_task,
                verdict=verdict,
            )
        if action_type in {"create_review", "run_review"}:
            # Count full change+review cycles by completed review actions.
            iteration += 1
        impl_task = store.get(impl_task.id) or impl_task

    iterate_wall_seconds = time.monotonic() - iterate_started_at
    total_steps = sum(row.steps or 0 for row in summary_rows)
    total_cost = sum(row.cost_usd or 0.0 for row in summary_rows)

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

    if final_status in {"approved", "merge_ready"}:
        return 0
    if final_status == "maxed_out":
        print(f"Max iterations ({max_iterations}) reached.")
        return 2
    if final_stop_reason in {"review_in_progress", "improve_in_progress"}:
        print(f"Iterate waiting: {final_stop_reason}. Existing task is already in progress.")
        return 3
    if final_stop_reason in {"max_cycles_reached", "max_improve_attempts"}:
        print(f"Iterate blocked: {final_stop_reason}.")
        print(f"Recommended next step: uv run gza fix {impl_task_key}")
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
