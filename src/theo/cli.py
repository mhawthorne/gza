"""Command-line interface for Theo."""

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

from .config import Config, ConfigError
from .db import SqliteTaskStore, add_task_interactive, edit_task_interactive, Task as DbTask
from .git import Git
from .importer import parse_import_file, validate_import, import_tasks
from .runner import run
from .tasks import YamlTaskStore, Task as YamlTask


def get_store(config: Config) -> SqliteTaskStore:
    """Get the SQLite task store."""
    return SqliteTaskStore(config.db_path)


def cmd_run(args: argparse.Namespace) -> int:
    """Run the next pending task(s)."""
    config = Config.load(args.project_dir)
    if args.no_docker:
        config.use_docker = False

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

    return 0


def cmd_next(args: argparse.Namespace) -> int:
    """List upcoming pending tasks in order."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    pending = store.get_pending()

    if not pending:
        print("No pending tasks")
        return 0

    for i, task in enumerate(pending, 1):
        type_label = f" [{task.task_type}]" if task.task_type != "task" else ""
        # Get first line only, then truncate
        first_line = task.prompt.split('\n')[0].strip()
        prompt_display = first_line[:60] + "..." if len(first_line) > 60 else first_line
        print(f"{i}. [#{task.id}]{type_label} {prompt_display}")
        if task.based_on:
            print(f"    based_on: task #{task.based_on}")
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
    """Validate the theo.yaml configuration file."""
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
    """Generate a new theo.yaml configuration file with defaults."""
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

    # Generate config file with project_name required and other defaults commented out
    config_content = f"""# Theo Configuration

# Project name (required) - used for branch prefixes and Docker image naming
project_name: {default_project_name}

# All settings below show default values and are commented out.
# Uncomment and modify any setting you want to change.

# Path to tasks file (relative to project directory) - deprecated, using SQLite now
# tasks_file: {DEFAULT_TASKS_FILE}

# Directory for log files (relative to project directory)
# log_dir: {DEFAULT_LOG_DIR}

# Whether to run Claude in Docker container
# use_docker: {str(DEFAULT_USE_DOCKER).lower()}

# Custom Docker image name (defaults to <project_name>-theo)
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


def cmd_log(args: argparse.Namespace) -> int:
    """Display the log for a given task slug."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task_query = args.task_slug

    # Try to find by ID first
    task = None
    try:
        task_id = int(task_query)
        task = store.get(task_id)
    except ValueError:
        pass

    # Try to find by task_id (slug)
    if not task:
        task = store.get_by_task_id(task_query)

    # Try partial match
    if not task:
        all_tasks = store.get_all()
        for t in all_tasks:
            if t.task_id and task_query in t.task_id:
                task = t
                break

    if not task:
        print(f"Error: No task found matching '{task_query}'")
        return 1

    if not task.log_file:
        print(f"Error: Task has no log file")
        return 1

    log_path = config.project_dir / task.log_file
    if not log_path.exists():
        print(f"Error: Log file not found at {log_path}")
        return 1

    # Read and parse the log file (supports both single JSON and JSONL formats)
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

        if log_data is None:
            print("Error: No result entry found in log file")
            return 1
    except Exception as e:
        print(f"Error: Failed to read log file: {e}")
        return 1

    # Display the log content as markdown text instead of JSON
    print("=" * 70)
    print(f"Task: {task.prompt[:100]}")
    print(f"ID: {task.id} | Slug: {task.task_id}")
    print(f"Status: {task.status}")
    if task.branch:
        print(f"Branch: {task.branch}")
    print("=" * 70)
    print()

    if args.turns and entries:
        # Show the full conversation turns
        _display_conversation_turns(entries)
    else:
        # Extract and display the result field (which contains markdown)
        if "result" in log_data:
            print(log_data["result"])
        else:
            # No result - show the subtype (e.g., error_max_turns)
            subtype = log_data.get("subtype", "unknown")
            print(f"Run ended with: {subtype}")
            if log_data.get("errors"):
                print(f"Errors: {log_data['errors']}")

    print()
    print("=" * 70)

    # Display summary stats if available
    if "duration_ms" in log_data:
        duration_sec = log_data["duration_ms"] / 1000
        print(f"Duration: {format_duration(duration_sec)}")
    if "num_turns" in log_data:
        print(f"Turns: {log_data['num_turns']}")
    if "total_cost_usd" in log_data:
        print(f"Cost: ${log_data['total_cost_usd']:.4f}")

    return 0


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

    task_type = "explore" if args.explore else "task"
    based_on = args.based_on

    if args.edit:
        # Interactive mode with $EDITOR
        task = add_task_interactive(store, task_type=task_type, based_on=based_on)
        if task:
            print(f"✓ Added task #{task.id}")
            return 0
        return 1
    elif args.prompt:
        # Inline prompt
        task = store.add(args.prompt, task_type=task_type, based_on=based_on)
        print(f"✓ Added task #{task.id}")
        return 0
    else:
        # No prompt provided, open editor
        task = add_task_interactive(store, task_type=task_type, based_on=based_on)
        if task:
            print(f"✓ Added task #{task.id}")
            return 0
        return 1


def cmd_edit(args: argparse.Namespace) -> int:
    """Edit a task's prompt."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    if task.status != "pending":
        print(f"Error: Can only edit pending tasks (task is {task.status})")
        return 1

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
    if task.branch:
        print(f"Branch: {task.branch}")
    if task.log_file:
        print(f"Log: {task.log_file}")
    if task.report_file:
        print(f"Report: {task.report_file}")
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
    # Handle legacy usage: theo import <project_dir>
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
            print("Usage: theo import <file> [--dry-run] [--force]")
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

        # Create task in SQLite
        task = store.add(yaml_task.description, task_type=yaml_task.type)

        # Copy over fields
        task.status = yaml_task.status
        task.task_id = yaml_task.task_id
        task.branch = yaml_task.branch
        task.log_file = yaml_task.log_file
        task.report_file = yaml_task.report_file
        task.has_commits = yaml_task.has_commits
        task.duration_seconds = yaml_task.duration_seconds
        task.num_turns = yaml_task.num_turns
        task.cost_usd = yaml_task.cost_usd
        if yaml_task.completed_at:
            task.completed_at = datetime.combine(yaml_task.completed_at, datetime.min.time())

        store.update(task)
        imported += 1

    print(f"✓ Imported {imported} tasks")
    if skipped:
        print(f"  Skipped {skipped} already imported tasks")

    return 0


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to a subparser."""
    parser.add_argument(
        "project_dir",
        nargs="?",
        default=".",
        help="Target project directory (default: current directory)",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Theo - AI agent task runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # work command
    work_parser = subparsers.add_parser("work", help="Run the next pending task")
    add_common_args(work_parser)
    work_parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run Claude directly instead of in Docker",
    )
    work_parser.add_argument(
        "--count",
        type=int,
        metavar="N",
        help="Number of tasks to run before stopping (overrides config default)",
    )

    # next command
    next_parser = subparsers.add_parser("next", help="List upcoming pending tasks")
    add_common_args(next_parser)

    # history command
    history_parser = subparsers.add_parser("history", help="List recent completed/failed tasks")
    add_common_args(history_parser)

    # unmerged command
    unmerged_parser = subparsers.add_parser("unmerged", help="List tasks with unmerged work")
    add_common_args(unmerged_parser)

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
    validate_parser = subparsers.add_parser("validate", help="Validate theo.yaml configuration")
    add_common_args(validate_parser)

    # init command
    init_parser = subparsers.add_parser("init", help="Generate new theo.yaml with defaults")
    add_common_args(init_parser)
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing theo.yaml file",
    )

    # log command
    log_parser = subparsers.add_parser("log", help="Display log for a given task")
    log_parser.add_argument(
        "task_slug",
        help="Task ID, slug, or partial slug to match",
    )
    log_parser.add_argument(
        "--turns",
        action="store_true",
        help="Show the full conversation turns instead of just the summary",
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
        "--explore",
        action="store_true",
        help="Create an explore task (no code changes required)",
    )
    add_parser.add_argument(
        "--based-on",
        type=int,
        metavar="ID",
        help="Base this task on a previous task's output",
    )
    add_common_args(add_parser)

    # edit command
    edit_parser = subparsers.add_parser("edit", help="Edit a pending task's prompt")
    edit_parser.add_argument(
        "task_id",
        type=int,
        help="Task ID to edit",
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

    args = parser.parse_args()

    # Handle project_dir for commands that have positional args before it
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
        elif args.command == "show":
            return cmd_show(args)
        elif args.command == "import":
            return cmd_import(args)
    except ConfigError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
