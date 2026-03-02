"""Query service for gza task history and lineage.

This module provides a typed query interface between the CLI and the SQLite
storage layer. The interfaces here are designed for eventual promotion to a
gza.api.v0 scripting namespace.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from gza.db import SqliteTaskStore, Task


@dataclass
class HistoryFilter:
    """Query parameters for task history. Designed for promotion to gza.api.v0."""

    limit: int | None = 10
    status: str | None = None  # 'completed' | 'failed' | 'unmerged'
    task_type: str | None = None  # 'task' | 'implement' | 'review' | ...
    incomplete: bool = False  # Only tasks not yet merged/resolved
    days: int | None = None  # Only tasks within the last N days
    start_date: str | None = None  # Only tasks on or after this date (YYYY-MM-DD)
    end_date: str | None = None  # Only tasks on or before this date (YYYY-MM-DD)
    lineage_depth: int = 0  # Expand lineage N levels (0 = flat)


@dataclass
class TaskLineageNode:
    """A task with optional lineage context. Designed for promotion to gza.api.v0."""

    task: Task
    depth: int = 0
    ancestors: list[TaskLineageNode] = field(default_factory=list)
    descendants: list[TaskLineageNode] = field(default_factory=list)


def is_lineage_complete(task: Task) -> bool:
    """Return True if task represents a fully-resolved outcome (no action needed).

    A task is considered complete when:
    - status is 'completed' AND merge_status is 'merged', OR
    - status is 'completed' AND has_commits is False (non-code tasks like
      explore/plan/review produce no commits and are treated as complete)

    A task is considered incomplete when:
    - status is 'failed', OR
    - status is 'completed' AND merge_status is 'unmerged', OR
    - status is 'completed' AND has_commits is True AND merge_status is None
      (committed but merge not tracked yet)
    """
    if task.status == "failed":
        return False
    if task.status == "completed":
        if task.merge_status == "merged":
            return True
        # Non-code tasks (explore/plan/review) produce no commits; treat as complete
        if not task.has_commits:
            return True
        # Code-producing tasks need explicit merge confirmation
        if task.merge_status == "unmerged":
            return False
        # has_commits=True but merge_status is None: treat as incomplete
        return False
    # 'unmerged' legacy status or any unexpected status
    return False


def query_history(store: SqliteTaskStore, f: HistoryFilter) -> list[Task]:
    """Return a flat filtered task history list.

    When f.incomplete is True, fetches all tasks (ignoring limit) then
    post-filters in Python, then applies the limit. This is correct at
    gza scale (typically <1000 tasks).
    """
    since: datetime | None = None
    if f.days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=f.days)
    elif f.start_date is not None:
        since = datetime.fromisoformat(f.start_date).replace(tzinfo=timezone.utc)

    until: datetime | None = None
    if f.end_date is not None:
        until = datetime.fromisoformat(f.end_date).replace(tzinfo=timezone.utc)
        # Include the full end date day
        until = until.replace(hour=23, minute=59, second=59)

    # When post-filtering for incomplete, defer the limit to after filtering
    effective_limit = None if f.incomplete else f.limit

    tasks = store.get_history(
        limit=effective_limit,
        status=f.status,
        task_type=f.task_type,
        since=since,
        until=until,
    )

    if f.incomplete:
        tasks = [t for t in tasks if not is_lineage_complete(t)]
        if f.limit is not None:
            tasks = tasks[: f.limit]

    return tasks


def get_task_lineage(store: SqliteTaskStore, task_id: int, depth: int) -> TaskLineageNode:
    """Return a TaskLineageNode with ancestors/descendants populated up to depth."""
    task = store.get(task_id)
    if task is None:
        raise ValueError(f"Task {task_id} not found")
    return _build_node(store, task, current_depth=0, max_depth=depth)


def query_history_with_lineage(
    store: SqliteTaskStore, f: HistoryFilter
) -> list[TaskLineageNode]:
    """Return filtered history with lineage expanded to f.lineage_depth levels."""
    tasks = query_history(store, f)
    return [_build_node(store, t, 0, f.lineage_depth) for t in tasks]


# --- Internal helpers ---


def _build_node(
    store: SqliteTaskStore, task: Task, current_depth: int, max_depth: int
) -> TaskLineageNode:
    """Recursively build a TaskLineageNode up to max_depth levels."""
    node = TaskLineageNode(task=task, depth=current_depth)
    if current_depth >= max_depth:
        return node

    # Ancestors: follow based_on chain upward
    if task.based_on is not None:
        parent = store.get(task.based_on)
        if parent is not None:
            node.ancestors.append(
                _build_node(store, parent, current_depth + 1, max_depth)
            )

    # Descendants: tasks that have based_on = task.id
    if task.id is not None:
        children = store.get_based_on_children(task.id)
        for child in children:
            node.descendants.append(
                _build_node(store, child, current_depth + 1, max_depth)
            )

    return node
