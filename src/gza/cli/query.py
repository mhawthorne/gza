"""CLI commands for querying and displaying task state.

Covers: next, history, unmerged, groups, status, ps, stop, delete, show, attach.
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from rich.markup import escape as rich_escape

from ..config import Config
from ..console import (
    console,
    get_terminal_width,
    truncate,
    MAX_PROMPT_DISPLAY,
    MAX_PROMPT_DISPLAY_SHORT,
)
from ..db import SqliteTaskStore, Task as DbTask
from ..git import Git
from ..runner import get_effective_config_for_task, _get_task_output
from ..workers import WorkerMetadata, WorkerRegistry

from ._common import (
    get_store,
    TASK_COLORS,
    format_stats,
    _format_lineage,
    _failure_summary,
    _failure_next_steps,
    _parse_iso,
    get_review_verdict,
)
from ..colors import NEXT_COLORS_DICT

from ..query import (
    get_reviews_for_root as _get_reviews_for_root_task,
    get_improves_for_root as _get_improves_for_root_task,
    build_lineage_tree as _build_lineage_tree_for_root,
    filter_lineage_tree as _filter_lineage_tree,
    resolve_lineage_root as _resolve_lineage_root_task,
    TaskLineageNode,
)
from ..colors import (
    LINEAGE_COLORS,
    LINEAGE_STATUS_COLORS as _LINEAGE_STATUS_COLORS,
    PS_STATUS_COLORS,
    SHOW_COLORS_DICT,
    UNMERGED_COLORS_DICT,
    pink,
    CYCLE_STATUS_COLORS,
)

_LINEAGE_REL_LABELS: dict[str, str] = {
    "review": "review",
    "improve-from-review": "improve",
    "improve": "improve",
    "implement-depends": "implement",
    "implement-based": "implement",
    "depends-and-based": "retry",
    "depends": "depends",
    "based": "retry",
    # Relationships not in this map (e.g. "plan", "explore", "task", "internal")
    # silently produce no label — this is intentional for unusual/unknown relationships.
}


def cmd_next(args: argparse.Namespace) -> int:
    """List upcoming pending tasks in order."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    pending = store.get_pending()

    # Check for orphaned/stale tasks once, regardless of whether pending tasks exist
    registry = WorkerRegistry(config.workers_path)
    orphaned = _get_orphaned_tasks(registry, store)

    if not pending:
        console.print("No pending tasks")
        if orphaned:
            _print_orphaned_warning(orphaned)
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

    # Colors consistent with cmd_history
    c = NEXT_COLORS_DICT

    # Terminal-width-aware column widths
    terminal_width = get_terminal_width()
    idx_width = 3
    id_width = 6    # e.g. "#1234" fits in 6 chars
    type_width = 12  # e.g. "[implement]" = 11 chars + 1 space padding
    # 2 spaces between each column (3 gaps)
    fixed_cols = idx_width + 2 + id_width + 2 + type_width + 2
    prompt_width = max(20, terminal_width - fixed_cols)

    def _print_task_row(i: int, task: DbTask, blocking_id: int | None = None) -> None:
        idx_str = str(i)
        id_str = f"#{task.id}"
        type_str = task.task_type or "implement"
        # Build visible type label with brackets, padded to fixed width
        type_visible = f"[{type_str}]"
        type_padded = f"{type_visible:<{type_width}}"
        first_line = task.prompt.split('\n')[0].strip()
        blocked_label = (
            f" [{c['blocked']}](blocked by #{blocking_id})[/{c['blocked']}]"
            if blocking_id else ""
        )
        blocked_raw_len = len(f" (blocked by #{blocking_id})") if blocking_id else 0
        avail = max(10, prompt_width - blocked_raw_len)
        prompt_display = truncate(first_line, avail)
        console.print(
            f"[{c['index']}]{idx_str:>{idx_width}}[/{c['index']}]"
            f"  [{c['task_id']}]{id_str:<{id_width}}[/{c['task_id']}]"
            f"  [{c['type']}]{rich_escape(type_padded)}[/{c['type']}]"
            f"  [{c['prompt']}]{prompt_display}[/{c['prompt']}]"
            f"{blocked_label}"
        )

    # Show runnable tasks
    if runnable:
        for i, task in enumerate(runnable, 1):
            _print_task_row(i, task)
    else:
        if not show_all:
            console.print("No runnable tasks")

    # Show blocked tasks if --all is specified
    if show_all and blocked:
        if runnable:
            console.print()
        for i, (task, blocking_id) in enumerate(blocked, len(runnable) + 1):
            _print_task_row(i, task, blocking_id)

    # Show blocked count at the bottom (only if not showing all)
    if not show_all and blocked:
        console.print()
        count = len(blocked)
        plural = "tasks" if count != 1 else "task"
        console.print(f"[{c['blocked']}]({count} {plural} blocked by dependencies)[/{c['blocked']}]")

    if orphaned:
        _print_orphaned_warning(orphaned)

    return 0


def cmd_history(args: argparse.Namespace) -> int:
    """List recent completed/failed tasks."""
    from gza.query import HistoryFilter, query_history, query_history_with_lineage, TaskLineageNode

    config = Config.load(args.project_dir)
    store = get_store(config)

    status = getattr(args, 'status', None)
    task_type = getattr(args, 'type', None)
    incomplete = getattr(args, 'incomplete', False)
    days = getattr(args, 'days', None)
    start_date = getattr(args, 'start_date', None)
    end_date = getattr(args, 'end_date', None)
    lineage_depth = getattr(args, 'lineage_depth', 0)

    # If a date-based filter is active and --last/-n wasn't explicitly provided,
    # don't cap results with the default limit.
    has_date_filter = days is not None or start_date is not None or end_date is not None
    explicit_last = '--last' in sys.argv or '-n' in sys.argv
    limit = args.last if (explicit_last or not has_date_filter) else None

    f = HistoryFilter(
        limit=limit,
        status=status,
        task_type=task_type,
        incomplete=incomplete,
        days=days,
        start_date=start_date,
        end_date=end_date,
        lineage_depth=lineage_depth,
    )

    c = TASK_COLORS

    # Fixed width for status labels to ensure alignment
    STATUS_WIDTH = 9  # "completed" is the longest at 9 chars

    def _render_task_line(task: DbTask, indent: str = "") -> None:
        """Render a single task entry."""
        if task.merge_status == "unmerged":
            status_label = "unmerged"
            status_color = c['unmerged']
        elif task.status == "completed":
            status_label = "completed"
            status_color = c['success']
        elif task.status == "dropped":
            status_label = "dropped"
            status_color = c['failure']
        else:
            status_label = "failed"
            status_color = c['failure']
        status_padded = f"{status_label:<{STATUS_WIDTH}}"
        status_icon = f"[{status_color}]{status_padded}[/{status_color}]"
        date_str = (
            f"[{c['task_id']}]({task.completed_at.strftime('%Y-%m-%d %H:%M')})[/{c['task_id']}]"
            if task.completed_at
            else ""
        )
        prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY_SHORT)
        console.print(
            f"{indent}{status_icon} [{c['task_id']}]#{task.id}[/{c['task_id']}] {date_str}"
            f" [{c['prompt']}]{prompt_display}[/{c['prompt']}]"
        )
        # Failed reason on its own line
        if task.status == "failed":
            reason = task.failure_reason or "UNKNOWN"
            console.print(f"{indent}    [{c['failure']}]reason: {reason}[/{c['failure']}]")
        # Type + deps on a separate line
        type_label = f"\\[{task.task_type}]"
        merge_label = " \\[merged]" if task.merge_status == "merged" else ""
        if task.based_on and task.depends_on:
            parent_label = f" ← #{task.based_on} (dep #{task.depends_on})"
        elif task.based_on:
            parent_label = f" ← #{task.based_on}"
        elif task.depends_on:
            parent_label = f" ← #{task.depends_on}"
        else:
            parent_label = ""
        console.print(f"{indent}    {type_label}{merge_label}{parent_label}")
        if task.branch:
            console.print(f"{indent}    branch: [{c['branch']}]{task.branch}[/{c['branch']}]")
        if task.report_file:
            console.print(f"{indent}    report: [{c['task_id']}]{task.report_file}[/{c['task_id']}]")
        stats_str = format_stats(task)
        if stats_str:
            console.print(f"{indent}    stats: [{c['stats']}]{stats_str}[/{c['stats']}]")

    def _render_lineage_node(node: TaskLineageNode) -> None:
        """Render a lineage tree using branch connectors."""

        def _render_subtree(current: TaskLineageNode, prefix: str = "", is_last: bool = True) -> None:
            if prefix:
                connector = "└── " if is_last else "├── "
                indent = f"[{c['lineage']}]{prefix}{connector}[/{c['lineage']}]"
            else:
                indent = ""
            _render_task_line(current.task, indent=indent)

            next_prefix = f"{prefix}{'    ' if is_last else '│   '}"
            for index, child in enumerate(current.children):
                _render_subtree(
                    child,
                    prefix=next_prefix,
                    is_last=index == (len(current.children) - 1),
                )

        _render_subtree(node)
        print()

    # Check for orphaned tasks (only when no status filter is active)
    orphaned: list[DbTask] = []
    if not status:
        registry = WorkerRegistry(config.workers_path)
        orphaned = _get_orphaned_tasks(registry, store)

    if lineage_depth > 0:
        nodes = query_history_with_lineage(store, f)
        if not nodes and not orphaned:
            _print_history_empty_message(status, task_type, incomplete, days)
            return 0
        # Show orphaned tasks at the top
        for task in orphaned:
            _render_orphaned_task(task, c)
        for node in nodes:
            _render_lineage_node(node)
    else:
        recent = query_history(store, f)
        if not recent and not orphaned:
            _print_history_empty_message(status, task_type, incomplete, days)
            return 0

        # Show orphaned tasks at the top so they're immediately visible
        for task in orphaned:
            _render_orphaned_task(task, c)

        for task in recent:
            _render_task_line(task)
            print()

    return 0


def _print_history_empty_message(
    status: str | None,
    task_type: str | None,
    incomplete: bool,
    days: int | None,
) -> None:
    """Print an appropriate 'no tasks found' message for gza history."""
    status_msg = f" with status '{status}'" if status else ""
    type_msg = f" with type '{task_type}'" if task_type else ""
    incomplete_msg = " (incomplete only)" if incomplete else ""
    lookback_msg = f" in the last {days} days" if days is not None else ""
    console.print(
        f"No completed or failed tasks{status_msg}{type_msg}{incomplete_msg}{lookback_msg}"
    )


def _render_orphaned_task(task: "DbTask", c: dict) -> None:
    """Render a single orphaned task entry for gza history."""
    status_padded = f"{'orphaned':<9}"
    status_icon = f"[{c['orphaned']}]⚠ {status_padded}[/{c['orphaned']}]"
    date_str = ""
    if task.started_at:
        date_str = (
            f"[{c['task_id']}](started {task.started_at.strftime('%Y-%m-%d %H:%M')})"
            f"[/{c['task_id']}]"
        )
    prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY_SHORT)
    console.print(
        f"{status_icon} [{c['task_id']}]#{task.id}[/{c['task_id']}] {date_str}"
        f" [{c['prompt']}]{prompt_display}[/{c['prompt']}]"
    )
    type_label = f"\\[{task.task_type}]"
    console.print(f"    {type_label}")
    if task.branch:
        console.print(f"    branch: [{c['branch']}]{task.branch}[/{c['branch']}]")
    console.print(f"    [{c['task_id']}]Run 'gza work {task.id}' to resume[/{c['task_id']}]")
    print()


def cmd_unmerged(args: argparse.Namespace) -> int:
    """List tasks with unmerged work on branches."""
    from gza.db import needs_merge_status_migration, migrate_merge_status

    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    default_branch = git.default_branch()
    print(f"On branch {git.current_branch()}")

    # Backfill merge_status for existing tasks if needed (one-time migration)
    if needs_merge_status_migration(store):
        console.print("[dim]Migrating merge status for existing tasks...[/dim]")
        migrate_merge_status(store, git)

    # Query tasks with merge_status='unmerged' from the database, completed only
    # --commits-only and --all flags are kept for backwards compatibility but are no-ops
    all_unmerged = store.get_unmerged()
    unmerged = [t for t in all_unmerged if t.status == "completed"]

    if not unmerged:
        console.print("No unmerged tasks")
        return 0

    # Colors for unmerged output — defined in gza.colors.
    UNMERGED_COLORS = UNMERGED_COLORS_DICT

    # Group tasks by branch
    branch_groups: dict[str, list] = {}
    for task in unmerged:
        if task.branch:
            if task.branch not in branch_groups:
                branch_groups[task.branch] = []
            branch_groups[task.branch].append(task)

    # Define task separator (same style as gza work logs)
    task_separator = "\n" + "-"*32 + "\n"

    # Display grouped by branch
    first_task = True
    for branch, tasks in branch_groups.items():
        # Add separator between tasks (not before first task)
        if not first_task:
            console.print(task_separator)
        first_task = False

        # Sort tasks by created_at to find the root task (earliest)
        tasks_sorted = sorted(tasks, key=lambda t: t.created_at if t.created_at else datetime.min)

        # Find the root implementation task and any improve tasks that reference it
        root_task = None

        # First pass: identify the root implementation task
        for task in tasks_sorted:
            if task.task_type == "implement":
                root_task = task
                break

        # If no implement task, use the earliest task as root
        if not root_task:
            root_task = tasks_sorted[0]

        reviews = _get_reviews_for_root_task(store, root_task)
        improve_tasks = _get_improves_for_root_task(store, root_task)
        lineage_tree = _build_lineage_tree_for_root(store, root_task)
        filtered_lineage_tree = _filter_lineage_tree(lineage_tree, {"review", "improve"})
        c = UNMERGED_COLORS  # shorthand
        lineage_str = _format_lineage(
            filtered_lineage_tree,
            annotate=True,
            review_verdict_resolver=lambda review_task: get_review_verdict(config, review_task),
        )

        # Classify review freshness/status for this implementation.
        latest_review = next((r for r in reviews if r.completed_at is not None), None)
        latest_improve = max(
            (imp for imp in improve_tasks if imp.completed_at is not None),
            key=lambda imp: imp.completed_at or datetime.min,
            default=None,
        )

        review_classification = "no review"
        review_status_color = UNMERGED_COLORS["review_none"]
        review_detail = None
        review_verdict = None

        if latest_review:
            latest_review_completed = latest_review.completed_at
            assert latest_review_completed is not None

            review_is_stale = False
            if root_task.review_cleared_at and root_task.review_cleared_at >= latest_review_completed:
                review_is_stale = True
            if latest_improve and latest_improve.completed_at and latest_improve.completed_at > latest_review_completed:
                review_is_stale = True

            if review_is_stale:
                review_classification = "review stale"
                review_status_color = UNMERGED_COLORS["review_changes"]
                latest_review_id = latest_review.id if latest_review.id is not None else "?"
                if latest_improve and latest_improve.id is not None:
                    review_detail = f"last review #{latest_review_id} before latest improve #{latest_improve.id}"
                else:
                    review_detail = f"last review #{latest_review_id} before latest improve"
            else:
                review_classification = "reviewed"
                review_status_color = UNMERGED_COLORS["review_approved"]

            # Preserve verdict extraction behavior by scanning newest-to-oldest
            # and taking the first parseable verdict after stale filtering.
            if review_classification != "review stale":
                for review in reviews:
                    if review.completed_at is None:
                        continue
                    if root_task.review_cleared_at and root_task.review_cleared_at >= review.completed_at:
                        continue
                    verdict = get_review_verdict(config, review)
                    if verdict:
                        review_verdict = verdict
                        break

        verdict_label = None
        if review_verdict == "APPROVED":
            verdict_label = "✓ approved"
        elif review_verdict == "CHANGES_REQUESTED":
            verdict_label = "⚠ changes requested"
        elif review_verdict == "NEEDS_DISCUSSION":
            verdict_label = "💬 needs discussion"

        review_line = review_classification
        if review_detail:
            review_line = f"{review_line} ({review_detail})"
        if verdict_label:
            review_line = f"{review_line} [{verdict_label}]"

        suffix = ""
        # Append failure reason if present and not UNKNOWN
        if root_task.status == "failed" and root_task.failure_reason and root_task.failure_reason != "UNKNOWN":
            suffix += f" [red]failed ({root_task.failure_reason})[/red]"

        # Header line: task ID, completion time, prompt
        first_line = root_task.prompt.split('\n')[0]
        prompt_display = truncate(first_line, MAX_PROMPT_DISPLAY_SHORT)
        date_str = f"[{c['task_id']}]({root_task.completed_at.strftime('%Y-%m-%d %H:%M')})[/{c['task_id']}]" if root_task.completed_at else ""

        console.print(f"⚡ [{c['task_id']}]#{root_task.id}[/{c['task_id']}] {date_str} [{c['prompt']}]{prompt_display}[/{c['prompt']}]{suffix}")

        if lineage_str:
            console.print("lineage:")
            console.print(lineage_str)

        # Show branch with diff stats (branch may no longer exist if deleted)
        if git.branch_exists(branch):
            # Use cached diff stats if available; fall back to live git call
            if root_task.diff_files_changed is not None:
                files_changed = root_task.diff_files_changed
                insertions = root_task.diff_lines_added or 0
                deletions = root_task.diff_lines_removed or 0
                commit_count = git.count_commits_ahead(branch, default_branch)
                commits_label = "commit" if commit_count == 1 else "commits"
                diff_str = f"+{insertions}/-{deletions} LOC, {files_changed} files" if files_changed else ""
                branch_detail = f"[{c['branch']}]{commit_count} {commits_label}[/{c['branch']}]"
                if diff_str:
                    branch_detail += f", [{c['branch']}]{diff_str}[/{c['branch']}]"
            else:
                revision_range = f"{default_branch}...{branch}"
                files_changed, insertions, deletions = git.get_diff_stat_parsed(revision_range)
                commit_count = git.count_commits_ahead(branch, default_branch)
                commits_label = "commit" if commit_count == 1 else "commits"
                diff_str = f"+{insertions}/-{deletions} LOC, {files_changed} files" if files_changed else ""
                branch_detail = f"[{c['branch']}]{commit_count} {commits_label}[/{c['branch']}]"
                if diff_str:
                    branch_detail += f", [{c['branch']}]{diff_str}[/{c['branch']}]"
            console.print(f"branch: [{c['branch']}]{branch}[/{c['branch']}] ({branch_detail})")
            if not git.can_merge(branch, default_branch):
                console.print("[yellow]⚠️  has conflicts[/yellow]")
        else:
            console.print(f"branch: [{c['branch']}]{branch}[/{c['branch']}] ([{c['task_id']}]branch deleted[/{c['task_id']}])")

        # Review freshness status for this implementation.
        console.print(f"review: [{review_status_color}]{review_line}[/{review_status_color}]")

        if root_task.report_file:
            console.print(f"report: [{c['task_id']}]{root_task.report_file}[/{c['task_id']}]")

        stats_str = format_stats(root_task)
        if stats_str:
            console.print(f"stats: [{c['stats']}]{stats_str}[/{c['stats']}]")

    return 0


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
        for status in ["pending", "in_progress", "completed", "failed", "unmerged", "dropped"]:
            if status in status_counts and status_counts[status] > 0:
                parts.append(f"{status_counts[status]} {status}")

        status_str = ", ".join(parts) if parts else "0 tasks"
        print(f"{group_name:<20} {total} tasks ({status_str})")

    # Show ungrouped tasks
    if ungrouped_counts:
        total = sum(ungrouped_counts.values())
        parts = []
        for status in ["pending", "in_progress", "completed", "failed", "unmerged", "dropped"]:
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
        type_label = f"[{task.task_type}] " if task.task_type != "implement" else ""

        # Get first line of prompt
        first_line = task.prompt.split('\n')[0].strip()
        prompt_display = truncate(first_line, MAX_PROMPT_DISPLAY_SHORT)

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

    # Check for orphaned tasks in this group and warn the user
    registry = WorkerRegistry(config.workers_path)
    orphaned = _get_orphaned_tasks(registry, store)
    # Filter orphaned tasks to those belonging to this group
    group_orphaned = [t for t in orphaned if t.group == group_name]
    if group_orphaned:
        _print_orphaned_warning(group_orphaned)

    return 0


def _print_ps_output(
    args: argparse.Namespace,
    registry: "WorkerRegistry",
    store: "SqliteTaskStore",
    poll_interval: int | None = None,
    seen_tasks: "dict | None" = None,
    show_all: bool = False,
) -> None:
    """Print ps output once. Used by cmd_ps directly and in poll loop.

    When seen_tasks is provided (poll mode), rows from this dict are merged with
    live results so that completed/failed tasks remain visible.
    """
    import datetime
    # Include completed workers so startup failures and poll transitions remain visible.
    live_rows, _ = _build_ps_rows(registry, store, include_completed=True)

    # In poll mode: update seen_tasks with new live data, preserving vanished tasks.
    if seen_tasks is not None:
        live_keys = set()
        for row in live_rows:
            key = row["task_id"] if row["task_id"] is not None else row["worker_id"]
            # Only adopt a row into seen_tasks if it's currently active, if we
            # already track it (status transition), or if it is a startup
            # failure. This preserves first-seen startup failures in poll mode
            # while still avoiding unrelated completed history.
            if (
                key in seen_tasks
                or row["status"] in ("running", "in_progress")
                or row.get("startup_failure", False)
            ):
                seen_tasks[key] = row
            live_keys.add(key)

        # Re-fetch DB status for ALL tracked tasks that still appear active.
        # This catches status transitions regardless of whether the task is
        # still in live_rows (e.g. worker exists but task completed in DB).
        for key, row in list(seen_tasks.items()):
            if isinstance(key, int) and row["status"] in ("running", "in_progress"):
                task = store.get(key)
                if task and task.status in ("completed", "failed"):
                    row["status"] = task.status

        rows = list(seen_tasks.values())
        rows.sort(key=_ps_sort_key)
    else:
        rows = live_rows

    # Outside poll mode, filter out completed/failed tasks except startup failures.
    # In poll mode, completed tasks remain visible via seen_tasks.
    # With --all, show everything including ordinary completed/failed rows.
    if seen_tasks is None and not show_all:
        rows = [
            r
            for r in rows
            if r["status"] not in ("completed", "failed") or r.get("startup_failure", False)
        ]

    if poll_interval is not None:
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"Refreshing every {poll_interval}s — last updated: {now}  (Ctrl+C to exit)")
        print()

    if not rows:
        print("No running workers or in-progress tasks (use --poll to monitor)")
        return

    if hasattr(args, "quiet") and args.quiet:
        for row in rows:
            if row["task_id"] is not None:
                print(row["task_id"])
        return

    if hasattr(args, "json") and args.json:
        import json as json_lib
        print(json_lib.dumps(rows, indent=2))
        return

    # Color scheme for ps output — defined in gza.colors.
    STATUS_COLORS = PS_STATUS_COLORS

    header = (
        f"{'TASK ID':<10} {'TYPE':<10} "
        f"{'STATUS':<16} {'PID':<8} {'STARTED':<24} {'STEPS':<7} {'DURATION':<10} {'TASK'}"
    )
    console.print(f"[bold]{header}[/bold]", soft_wrap=True)
    console.print("[bold]" + "─" * 106 + "[/bold]", soft_wrap=True)

    for row in rows:
        task_id_display = f"#{row['task_id']}" if row["task_id"] is not None else ""
        status = row['status']
        if status == "failed" and row.get("startup_failure"):
            status = "failed(startup)"
        sc = STATUS_COLORS.get(status, "white")

        # Escape Rich markup in task display (may contain brackets from truncation)
        task_display = row['task'].replace('[', '\\[') if row['task'] else ''

        console.print(
            f"[cyan]{task_id_display:<10}[/cyan] {row['type']:<10} "
            f"[{sc}]{status:<16}[/{sc}] {row['pid']:<8} {row['started']:<24} {row['steps']:<7} {row['duration']:<10} "
            f"[{pink}]{task_display}[/{pink}]",
            soft_wrap=True,
        )


def cmd_ps(args: argparse.Namespace) -> int:
    """List running and completed workers."""
    import time
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)
    # Worker registry is now a thin process index; no ps-specific cleanup.
    poll_interval: int | None = getattr(args, "poll", None)
    show_all: bool = getattr(args, "all", False)

    if poll_interval is not None:
        if poll_interval < 1:
            print(f"error: --poll value must be at least 1 second (got {poll_interval})", file=sys.stderr)
            return 1
        # Poll runs indefinitely until Ctrl+C — no auto-stop when tasks complete,
        # since new tasks may start at any time.
        seen_tasks: dict = {}
        try:
            while True:
                if sys.stdout.isatty():
                    print("\033[2J\033[H", end="")  # clear screen, move cursor to top
                _print_ps_output(args, registry, store, poll_interval=poll_interval, seen_tasks=seen_tasks, show_all=show_all)
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            return 0
    else:
        _print_ps_output(args, registry, store, show_all=show_all)

    return 0


def _build_ps_rows(
    registry: WorkerRegistry,
    store: SqliteTaskStore,
    include_completed: bool,
) -> tuple[list[dict], list[DbTask]]:
    """Build reconciled ps rows from worker registry and DB in-progress tasks.

    Returns a tuple of (rows, in_progress_tasks) so callers can reuse the
    already-fetched in-progress task objects without an extra DB round-trip.
    """
    workers = registry.list_all(include_completed=include_completed)
    in_progress_tasks = store.get_in_progress()
    merged: dict[tuple[str, str], dict] = {}

    for worker in workers:
        if worker.status == "running" and not registry.is_running(worker.worker_id):
            worker.status = "stale"

        key = ("task", str(worker.task_id)) if worker.task_id is not None else ("worker", worker.worker_id)
        existing = merged.get(key)
        if existing and existing["worker"] is not None:
            if _prefer_worker(existing["worker"], worker):
                existing["worker"] = worker
            continue

        task = store.get(worker.task_id) if worker.task_id is not None else None
        merged[key] = {"worker": worker, "task": task}

    for task in in_progress_tasks:
        assert task.id is not None
        key = ("task", str(task.id))
        if key in merged:
            merged[key]["task"] = task
        else:
            merged[key] = {"worker": None, "task": task}

    rows = [_to_ps_row(item["worker"], item["task"], store) for item in merged.values()]
    rows.sort(key=_ps_sort_key)
    return rows, in_progress_tasks


def _get_orphaned_tasks(registry: WorkerRegistry, store: SqliteTaskStore) -> list[DbTask]:
    """Return in-progress tasks that have no active worker (orphaned/stale)."""
    rows, in_progress = _build_ps_rows(registry, store, include_completed=False)
    orphaned_task_ids = {
        row["task_id"] for row in rows
        if row["is_orphaned"] and row["task_id"] is not None
    }
    if not orphaned_task_ids:
        return []
    return [t for t in in_progress if t.id in orphaned_task_ids]


def _print_orphaned_warning(orphaned: list[DbTask]) -> None:
    """Print a warning about orphaned tasks with a suggestion to resume."""
    count = len(orphaned)
    plural = "tasks" if count != 1 else "task"
    console.print(f"\n[yellow]⚠  {count} orphaned {plural} found (in-progress with no active worker):[/yellow]")
    for task in orphaned:
        type_label = f"\\[{task.task_type}] " if task.task_type != "implement" else ""
        first_line = task.prompt.split('\n')[0].strip()
        prompt_display = truncate(first_line, MAX_PROMPT_DISPLAY)
        console.print(f"   [cyan](#{task.id})[/cyan] {type_label}[{pink}]{prompt_display}[/{pink}]")
    console.print("   Run [cyan]gza work <id>[/cyan] to resume, or [cyan]gza mark-completed --force <id>[/cyan] to clear.")


def _ps_sort_key(row: dict) -> tuple[int, bool, str, int, str]:
    """Sort ps rows by status group, then by start time, then stable identifiers.

    Failed tasks surface at top for immediate attention, in_progress/running
    in the middle, completed at the bottom."""
    status = row.get("status", "")
    # Failed=0 (top), in_progress/running=1 (middle), completed/other=2 (bottom)
    if status == "failed":
        status_group = 0
    elif status in ("running", "in_progress"):
        status_group = 1
    else:
        status_group = 2

    sort_timestamp = row["sort_timestamp"] or ""
    has_no_timestamp = sort_timestamp == ""

    raw_task_id = row.get("task_id")
    task_id_sort = raw_task_id if isinstance(raw_task_id, int) else sys.maxsize
    worker_id = row.get("worker_id", "")
    return (status_group, has_no_timestamp, sort_timestamp, task_id_sort, worker_id)


def _worker_failed_during_startup(worker: WorkerMetadata | None, task: DbTask | None) -> bool:
    """Return True when worker failed before main task logging initialized."""
    if worker is None:
        return False
    has_startup_hint = bool(worker.startup_log_file) or bool(task and task.task_id)
    if worker.status != "failed" or not has_startup_hint:
        return False
    has_main_log = bool(task and task.log_file)
    return not has_main_log


def _prefer_worker(existing: WorkerMetadata, candidate: WorkerMetadata) -> bool:
    """Return True when candidate worker should replace existing worker."""
    priority = {"running": 3, "stale": 2, "failed": 1, "completed": 0}
    existing_rank = priority.get(existing.status, -1)
    candidate_rank = priority.get(candidate.status, -1)
    if candidate_rank != existing_rank:
        return candidate_rank > existing_rank

    existing_started = _parse_iso(existing.started_at)
    candidate_started = _parse_iso(candidate.started_at)
    if existing_started and candidate_started:
        return candidate_started > existing_started
    if candidate_started:
        return True
    return False


def _get_ps_steps(task: "DbTask | None", store: "SqliteTaskStore | None") -> str:
    """Return step count for display: use num_steps_computed when available, else count DB rows."""
    if task is None or task.id is None:
        return "-"
    if task.num_steps_computed is not None:
        return str(task.num_steps_computed)
    if store is not None:
        count = store.count_steps(task.id)
        return str(count) if count > 0 else "-"
    return "-"


def _to_ps_row(worker: WorkerMetadata | None, task: DbTask | None, store: "SqliteTaskStore | None" = None) -> dict:
    """Convert a reconciled worker/task pair into display data."""
    source = "both" if worker and task else "worker" if worker else "db"

    status = "unknown"
    if source == "db":
        # Use actual DB status instead of assuming in_progress — the task
        # may have already completed/failed by the time the worker is gone.
        status = task.status if task and task.status else "in_progress"
    elif source == "worker" and worker is not None:
        status = worker.status if worker.status in ("failed", "completed", "stale") else "running"
    elif worker is not None and task is not None:
        # Both worker and task exist.
        if task.status in ("completed", "failed"):
            status = task.status
        elif not (task and task.running_pid):
            status = "stale"
        else:
            status = "running"
    elif worker is not None:
        status = worker.status if worker.status in ("failed", "completed", "stale") else "running"

    is_stale = worker is not None and worker.status == "stale"
    is_orphaned = (
        task is not None
        and task.status == "in_progress"
        and (worker is None or worker.status != "running")
    )

    started = _started_at(worker, task)
    ended = _ended_at(worker)
    duration = _format_duration(started, ended)

    worker_id = worker.worker_id if worker else "-"
    pid = str(worker.pid) if worker else "-"
    if task:
        task_type_display = task.task_type
    else:
        task_type_display = "-"

    task_id = task.id if task and task.id is not None else worker.task_id if worker else None
    task_display = ""
    if task and task.task_id:
        task_display = task.task_id
    elif task:
        task_display = truncate(task.prompt, 25)
    elif worker:
        if worker.task_slug:
            task_display = worker.task_slug
        else:
            task_display = f"task #{worker.task_id}" if worker.task_id is not None else ""

    flags = []
    if is_stale:
        flags.append("stale")
    if is_orphaned:
        flags.append("orphaned")
    startup_failure = _worker_failed_during_startup(worker, task)
    if startup_failure:
        flags.append("startup-failure")

    return {
        "worker_id": worker_id,
        "pid": pid,
        "type": task_type_display,
        "source": source,
        "task_id": task_id,
        "status": status,
        "flags": ",".join(flags),
        "task": task_display,
        "started": _format_started(started),
        "started_at": started.isoformat() if started else None,
        "steps": _get_ps_steps(task, store),
        "duration": duration,
        "is_stale": is_stale,
        "is_orphaned": is_orphaned,
        "startup_failure": startup_failure,
        "startup_log_file": (f".gza/workers/{task.task_id}.startup.log" if task and task.task_id else (worker.startup_log_file if worker else None)),
        "sort_timestamp": started.isoformat() if started else "",
    }



def _started_at(worker: WorkerMetadata | None, task: DbTask | None) -> datetime | None:
    """Get the best available started timestamp."""
    if worker:
        started = _parse_iso(worker.started_at)
        if started:
            return started
    if task:
        return task.started_at or task.created_at
    return None


def _ended_at(worker: WorkerMetadata | None) -> datetime | None:
    """Get completed timestamp when available."""
    if not worker:
        return None
    return _parse_iso(worker.completed_at)


def _format_duration(started: datetime | None, ended: datetime | None = None) -> str:
    """Format duration from timestamps."""
    if not started:
        return "-"
    end_time = ended or datetime.now(timezone.utc)
    duration_sec = max(0.0, (end_time - started).total_seconds())
    if duration_sec < 60:
        return f"{duration_sec:.0f}s"
    minutes = int(duration_sec // 60)
    seconds = int(duration_sec % 60)
    return f"{minutes}m {seconds}s"


def _format_started(started: datetime | None) -> str:
    """Format start timestamp for ps output."""
    if not started:
        return "-"
    if started.tzinfo is None:
        return started.strftime("%Y-%m-%d %H:%M:%S")
    started_utc = started.astimezone(timezone.utc)
    return started_utc.strftime("%Y-%m-%d %H:%M:%S UTC")


def _has_pid_ownership_mismatch(worker: WorkerMetadata, store: SqliteTaskStore) -> bool:
    """Return True when task runtime ownership disagrees with worker metadata PID."""
    if worker.task_id is None:
        return False
    task = store.get(worker.task_id)
    return (
        task is not None
        and task.status == "in_progress"
        and task.running_pid is not None
        and task.running_pid != worker.pid
    )


def cmd_stop(args: argparse.Namespace) -> int:
    """Stop a running worker."""
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)

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
            if _has_pid_ownership_mismatch(worker, store):
                task = store.get(worker.task_id) if worker.task_id is not None else None
                running_pid = task.running_pid if task is not None else None
                print(
                    f"Skipping worker {worker.worker_id}: PID ownership mismatch "
                    f"(worker PID {worker.pid}, task running_pid {running_pid})."
                )
                continue
            print(f"Stopping worker {worker.worker_id} (PID {worker.pid})...")
            if registry.stop(worker.worker_id, force=args.force if hasattr(args, 'force') else False):
                print(f"  ✓ Sent stop signal")
            else:
                print(f"  ✗ Failed to stop worker")

        return 0

    # Stop specific worker
    maybe_worker = registry.get(args.worker_id)
    if not maybe_worker:
        print(f"Error: Worker {args.worker_id} not found")
        return 1
    worker = maybe_worker

    if worker.status != "running":
        print(
            f"Refusing to stop worker {args.worker_id}: "
            f"status is '{worker.status}', not 'running'."
        )
        print("Run 'gza cleanup' to remove stale worker records.")
        return 1

    if _has_pid_ownership_mismatch(worker, store):
        task = store.get(worker.task_id) if worker.task_id is not None else None
        running_pid = task.running_pid if task is not None else None
        print(
            f"Refusing to stop worker {args.worker_id}: PID ownership mismatch "
            f"(worker PID {worker.pid}, task running_pid {running_pid})."
        )
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

    # Support both --force (deprecated) and --yes/-y
    skip_confirmation = args.force or args.yes

    if not skip_confirmation:
        prompt_display = truncate(task.prompt, MAX_PROMPT_DISPLAY)
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


def cmd_lineage(args: argparse.Namespace) -> int:
    """Show the full lineage tree for a given task."""
    from rich.tree import Tree as RichTree

    config = Config.load(args.project_dir)
    store = get_store(config)

    task_id: int = args.task_id
    task = store.get(task_id)
    if task is None:
        console.print(f"[red]Error: Task #{task_id} not found[/red]")
        return 1

    root = _resolve_lineage_root_task(store, task)
    lineage_tree = _build_lineage_tree_for_root(store, root, max_depth=None)

    def _status_text(t: DbTask) -> str:
        if t.status == "failed":
            if t.failure_reason and t.failure_reason != "UNKNOWN":
                return f"failed ({t.failure_reason})"
            return "failed"
        return t.status or "unknown"

    def _node_label(node: TaskLineageNode) -> str:
        t = node.task
        is_target = t.id == task_id

        status = _status_text(t)
        type_str = t.task_type or "implement"
        first_line = t.prompt.split("\n")[0].strip()
        prompt_short = first_line[:60] + "…" if len(first_line) > 60 else first_line

        lc = LINEAGE_COLORS
        rel = _LINEAGE_REL_LABELS.get(node.relationship, "")
        rel_part = f" [{lc.relationship}]{rich_escape(f'[{rel}]')}[/{lc.relationship}]" if rel else ""

        stats = format_stats(t)
        stats_part = f" [{lc.stats}]({stats})[/{lc.stats}]" if stats else ""

        status_color = _LINEAGE_STATUS_COLORS.get(t.status or "", "white")

        label = (
            f"[{lc.task_id}]#{t.id}[/{lc.task_id}]"
            f" [{lc.type_label}]{rich_escape(type_str)}[/{lc.type_label}]"
            f" [{status_color}]{rich_escape(status)}[/{status_color}]"
            f"{rel_part}"
            f"  [{lc.prompt}]'{rich_escape(prompt_short)}'[/{lc.prompt}]"
            f"{stats_part}"
        )

        if is_target:
            label = f"[{lc.target_highlight}]→ {label}[/{lc.target_highlight}]"

        return label

    def _populate(node: TaskLineageNode, rich_parent: RichTree) -> None:
        for child in node.children:
            child_branch = rich_parent.add(_node_label(child))
            _populate(child, child_branch)

    rich_tree = RichTree(_node_label(lineage_tree))
    _populate(lineage_tree, rich_tree)
    console.print(rich_tree)
    return 0


def _show_built_prompt(task: DbTask, config: "Config", store: "SqliteTaskStore") -> int:
    """Build and print the full prompt for a task as JSON.

    Uses the same build_prompt() path as background execution, so the output
    is identical to what a background worker would receive.
    """
    import json
    from ..git import Git
    from ..runner import build_prompt, get_task_output_paths

    report_path, summary_path = get_task_output_paths(task, config.project_dir)

    git = Git(config.project_dir)
    prompt = build_prompt(task, config, store, report_path=report_path, summary_path=summary_path, git=git)

    output = {
        "task_id": task.id,
        "task_type": task.task_type,
        "task_slug": task.task_id,
        "branch": task.branch,
        "prompt": prompt,
        "report_path": str(report_path) if report_path else None,
        "summary_path": str(summary_path) if summary_path else None,
        "verify_command": config.verify_command,
    }
    print(json.dumps(output, indent=2))
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    """Show details of a specific task."""
    from .log import _latest_worker_for_task
    from ._common import _resolve_task_log_path, _extract_failure_log_context

    config = Config.load(args.project_dir)
    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        console.print(f"[red]Error: Task #{args.task_id} not found[/red]")
        return 1

    # --prompt: emit the fully built prompt as JSON and exit
    if getattr(args, "prompt", False):
        return _show_built_prompt(task, config, store)

    # --path: print only the report file path and exit
    if getattr(args, "path", False):
        if task.report_file:
            report_path = config.project_dir / task.report_file
            print(report_path)
            return 0
        console.print(f"[red]Error: Task #{args.task_id} has no report file[/red]")
        return 1

    # --output: print only the raw output content and exit
    if getattr(args, "output", False):
        output = _get_task_output(task, config.project_dir)
        if output:
            print(output)
            return 0
        console.print(f"[red]Error: Task #{args.task_id} has no output content[/red]")
        return 1

    # Colors for show output — defined in gza.colors.
    SHOW_COLORS = SHOW_COLORS_DICT
    c = SHOW_COLORS

    status_color_map = {
        "pending": c["status_pending"],
        "in_progress": c["status_running"],
        "completed": c["status_completed"],
        "failed": c["status_failed"],
        "unmerged": c["status_pending"],
        "dropped": c["status_failed"],
    }
    status_color = status_color_map.get(task.status, c["status_default"])

    console.print(f"[{c['heading']}]Task #{task.id}[/{c['heading']}]")
    console.print(f"[{c['section']}]{'=' * 50}[/{c['section']}]")
    console.print(f"[{c['label']}]Status:[/{c['label']}] [{status_color}]{task.status}[/{status_color}]")
    if task.merge_status:
        console.print(f"[{c['label']}]Merge Status:[/{c['label']}] [{c['value']}]{task.merge_status}[/{c['value']}]")
    console.print(f"[{c['label']}]Type:[/{c['label']}] [{c['value']}]{task.task_type}[/{c['value']}]")
    if task.task_id:
        console.print(f"[{c['label']}]Slug:[/{c['label']}] [{c['value']}]{task.task_id}[/{c['value']}]")
    if task.based_on:
        console.print(f"[{c['label']}]Based on:[/{c['label']}] [{c['value']}]task #{task.based_on}[/{c['value']}]")
    if task.depends_on:
        console.print(f"[{c['label']}]Depends on:[/{c['label']}] [{c['value']}]task #{task.depends_on}[/{c['value']}]")
    if task.id is not None:
        depended_on_by = [
            t for t in store.get_all()
            if t.depends_on == task.id or t.based_on == task.id
        ]
        if depended_on_by:
            dep_parts = [f"#{t.id}[{t.task_type}]" for t in depended_on_by if t.id is not None]
            console.print(f"[{c['label']}]Depended on by:[/{c['label']}] [{c['value']}]{', '.join(dep_parts)}[/{c['value']}]")
    if task.group:
        console.print(f"[{c['label']}]Group:[/{c['label']}] [{c['value']}]{task.group}[/{c['value']}]")
    if task.spec:
        console.print(f"[{c['label']}]Spec:[/{c['label']}] [{c['value']}]{task.spec}[/{c['value']}]")
    if task.skip_learnings:
        console.print(f"[{c['label']}]Skip Learnings:[/{c['label']}] [green]yes[/green]")
    if task.branch:
        console.print(f"[{c['label']}]Branch:[/{c['label']}] [{c['branch']}]{task.branch}[/{c['branch']}]")
    if task.log_file:
        console.print(f"[{c['label']}]Log:[/{c['label']}] [{c['value']}]{task.log_file}[/{c['value']}]")
    if task.report_file:
        console.print(f"[{c['label']}]Report:[/{c['label']}] [{c['value']}]{task.report_file}[/{c['value']}]")
        # Detect if disk file is newer than task completion (drift warning)
        if task.completed_at and task.output_content:
            report_path = config.project_dir / task.report_file
            if report_path.exists():
                file_mtime = datetime.fromtimestamp(report_path.stat().st_mtime, tz=timezone.utc)
                if file_mtime > task.completed_at:
                    console.print(f"[yellow]Warning: Report on disk has been modified since task completion[/yellow]")
    if task.session_id:
        console.print(f"[{c['label']}]Session ID:[/{c['label']}] [{c['value']}]{task.session_id}[/{c['value']}]")

    root_task = _resolve_lineage_root_task(store, task)
    lineage_tree = _build_lineage_tree_for_root(store, root_task)
    lineage_str = _format_lineage(lineage_tree, c["task_id"])
    if lineage_str:
        console.print(f"[{c['label']}]Lineage:[/{c['label']}]")
        console.print(lineage_str)

    console.print()
    console.print(f"[{c['label']}]Prompt:[/{c['label']}]")
    console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
    console.print(f"[{c['prompt']}]{task.prompt}[/{c['prompt']}]")
    console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
    console.print()
    if task.created_at:
        console.print(f"[{c['label']}]Created:[/{c['label']}] [{c['value']}]{task.created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC[/{c['value']}]")
    if task.started_at:
        console.print(f"[{c['label']}]Started:[/{c['label']}] [{c['value']}]{task.started_at.strftime('%Y-%m-%d %H:%M:%S')} UTC[/{c['value']}]")
    if task.completed_at:
        console.print(f"[{c['label']}]Completed:[/{c['label']}] [{c['value']}]{task.completed_at.strftime('%Y-%m-%d %H:%M:%S')} UTC[/{c['value']}]")
    stats_str = format_stats(task)
    if stats_str:
        console.print(f"[{c['label']}]Stats:[/{c['label']}] [{c['stats']}]{stats_str}[/{c['stats']}]")

    if task.id is not None:
        latest_worker = _latest_worker_for_task(WorkerRegistry(config.workers_path), task.id)
        if latest_worker:
            run_mode = "background" if latest_worker.is_background else "foreground"
            pid_part = f", PID {latest_worker.pid}" if latest_worker.pid else ""
            worker_label = f"{run_mode} ({latest_worker.worker_id}){pid_part}"
            console.print(f"[{c['label']}]Run Context:[/{c['label']}] [{c['value']}]{worker_label}[/{c['value']}]")
            if _worker_failed_during_startup(latest_worker, task):
                console.print(
                    f"[{c['label']}]Worker Failure:[/{c['label']}] "
                    f"[{c['status_failed']}]failed during startup (before main log setup)[/{c['status_failed']}]"
                )
                if latest_worker.startup_log_file:
                    console.print(
                        f"[{c['label']}]Startup Log:[/{c['label']}] "
                        f"[{c['value']}]{latest_worker.startup_log_file}[/{c['value']}]"
                    )

    if task.status == "failed":
        reason = task.failure_reason or "UNKNOWN"
        console.print(f"[{c['label']}]Failure Reason:[/{c['label']}] [{c['status_failed']}]{reason}[/{c['status_failed']}]")
        console.print(f"[{c['label']}]Failure Summary:[/{c['label']}] [{c['value']}]{_failure_summary(reason)}[/{c['value']}]")

        if reason in {"MAX_STEPS", "MAX_TURNS"}:
            _, _, effective_max_steps = get_effective_config_for_task(task, config)
            steps_used = task.num_steps_reported if task.num_steps_reported is not None else task.num_steps_computed
            if steps_used is not None:
                console.print(
                    f"[{c['label']}]Step Limit:[/{c['label']}] "
                    f"[{c['value']}]{steps_used} / {effective_max_steps}[/{c['value']}]"
                )
            turns_used = task.num_turns_reported if task.num_turns_reported is not None else task.num_turns_computed
            if turns_used is not None:
                console.print(
                    f"[{c['label']}]Legacy Turns:[/{c['label']}] "
                    f"[{c['value']}]{turns_used}[/{c['value']}]"
                )

        log_path = _resolve_task_log_path(config, task)
        if log_path and log_path.exists():
            verify_context, result_context = _extract_failure_log_context(log_path, config.verify_command)
            if verify_context:
                console.print(
                    f"[{c['label']}]Last Verify Failure:[/{c['label']}] "
                    f"[{c['value']}]{verify_context}[/{c['value']}]"
                )
            if result_context:
                console.print(
                    f"[{c['label']}]Last Result Context:[/{c['label']}] "
                    f"[{c['value']}]{result_context}[/{c['value']}]"
                )

        next_step_commands = _failure_next_steps(task, reason)
        if next_step_commands:
            console.print(f"[{c['label']}]Next Steps:[/{c['label']}]")
            for command in next_step_commands:
                console.print(f"[{c['value']}]  - {command}[/{c['value']}]")

    # Show cycle state for implement tasks (Phase 4)
    if task.task_type == "implement" and task.id is not None:
        cycles = store.get_cycles_for_impl(task.id)
        if cycles:
            latest_cycle = cycles[0]
            cycle_color = CYCLE_STATUS_COLORS.get(latest_cycle.status, c["value"])
            console.print(
                f"[{c['label']}]Latest Cycle:[/{c['label']}] "
                f"[{cycle_color}]#{latest_cycle.id} {latest_cycle.status}[/{cycle_color}]"
                + (f" ({latest_cycle.stop_reason})" if latest_cycle.stop_reason else "")
            )
            iters = store.get_cycle_iterations(latest_cycle.id)
            if iters:
                for it in iters:
                    verdict_str = it.review_verdict or "-"
                    imp_str = f"  improve #{it.improve_task_id}" if it.improve_task_id else ""
                    console.print(
                        f"[{c['value']}]  iter {it.iteration_index + 1}: "
                        f"review #{it.review_task_id} [{verdict_str}]{imp_str}[/{c['value']}]"
                    )

    # Display output content using precedence logic (disk version when newer)
    output = _get_task_output(task, config.project_dir)
    if output:
        console.print()
        console.print(f"[{c['label']}]Output:[/{c['label']}]")
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")
        full_mode = getattr(args, "full", False)
        lines = output.splitlines()
        if not full_mode and len(lines) > 30:
            truncated = "\n".join(lines[:20])
            remainder = len(lines) - 20
            console.print(truncated)
            console.print(f"[{c['section']}](... truncated, {remainder} more lines — use `gza show {task.id} --full` to see all)[/{c['section']}]")
        else:
            console.print(output)
        console.print(f"[{c['section']}]{'-' * 50}[/{c['section']}]")

    return 0


# Providers where the human can interact (type messages, approve/deny tools)
_INTERACTIVE_PROVIDERS = {"claude"}
# Providers that run headless — attach is observe-only
_OBSERVE_ONLY_PROVIDERS = {"codex", "gemini"}


def cmd_attach(args: argparse.Namespace) -> int:
    """Attach to a running task's tmux session."""
    config = Config.load(args.project_dir)
    registry = WorkerRegistry(config.workers_path)
    store = get_store(config)

    target = args.worker_id

    # Try as worker ID first, then as numeric task ID
    worker = registry.get(target)
    if worker is None:
        try:
            task_id_int = int(target)
        except ValueError:
            task_id_int = None

        if task_id_int is not None:
            for w in registry.list_all(include_completed=False):
                if w.task_id == task_id_int:
                    worker = w
                    break

    if worker is None or worker.status != "running":
        print(f"No running worker found for: {target}")
        return 1

    if worker.task_id is None:
        print(f"Worker {worker.worker_id} has no associated task ID")
        return 1

    session_name = worker.tmux_session or f"gza-{worker.task_id}"

    # Verify the tmux session exists
    result = subprocess.run(
        ["tmux", "has-session", "-t", session_name],
        capture_output=True,
    )
    if result.returncode != 0:
        print(f"No tmux session found: {session_name}")
        print("This task may have been started without tmux support.")
        return 1

    # Determine provider to decide attach mode
    task = store.get(worker.task_id)
    provider_name = "claude"
    if task is not None:
        provider_name = (task.provider or config.provider or "claude").lower()

    # When already inside tmux, use switch-client instead of attach-session
    # to avoid the "sessions should be nested with care" error.
    inside_tmux = bool(os.environ.get("TMUX"))

    if inside_tmux:
        # When task ends and its session is destroyed, switch back to the
        # previous session instead of detaching from tmux entirely.
        # Scoped to the task session (not -g) to avoid mutating global config.
        # Requires tmux 3.2+ for session-level detach-on-destroy.
        result = subprocess.run(
            ["tmux", "set-option", "-t", session_name,
             "detach-on-destroy", "previous"],
            capture_output=True,
        )
        if result.returncode != 0:
            print(
                "Warning: could not set detach-on-destroy on task session. "
                "When the task ends you may be detached from tmux.",
                file=sys.stderr,
            )

    if provider_name in _OBSERVE_ONLY_PROVIDERS:
        print(f"Attaching to task #{worker.task_id} (provider: {provider_name})...")
        print(
            f"Note: {provider_name.title()} runs in headless mode. You can observe"
        )
        print("output but cannot interact. Use Ctrl-B D to detach.")
        print(
            f"To intervene, stop this task (gza stop {worker.task_id}) and re-run with Claude."
        )
        print()
        if inside_tmux:
            os.execvp("tmux", ["tmux", "switch-client", "-r", "-t", session_name])
        else:
            os.execvp("tmux", ["tmux", "attach-session", "-r", "-t", session_name])
    else:
        print(f"Attaching to task #{worker.task_id} (provider: {provider_name})...")
        print("You have full interactive control. Ctrl-B D to detach.")
        print()
        if inside_tmux:
            os.execvp("tmux", ["tmux", "switch-client", "-t", session_name])
        else:
            os.execvp("tmux", ["tmux", "attach-session", "-t", session_name])

    return 0  # unreachable after execvp but satisfies the return type
