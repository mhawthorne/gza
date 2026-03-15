"""Query service for gza task history and lineage.

This module provides a typed query interface between the CLI and the SQLite
storage layer. The interfaces here are designed for eventual promotion to a
gza.api.v0 scripting namespace.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from gza.db import SqliteTaskStore, Task
from gza.task_slug import get_base_task_slug as _get_base_task_slug
from gza.task_slug import get_task_slug as _get_task_slug_from_task_id


@dataclass
class HistoryFilter:
    """Query parameters for task history. Designed for promotion to gza.api.v0."""

    limit: int | None = 10
    status: str | None = None  # 'completed' | 'failed' | 'unmerged' | 'dropped'
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
        raise KeyError(f"Task {task_id} not found")
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


# --- Lineage helpers (formerly _query.py) ---


def task_time_for_lineage(task: Task) -> datetime:
    """Return best-effort timestamp for lineage ordering."""
    return task.completed_at or task.created_at or datetime.min


def get_task_slug(task: Task) -> str | None:
    """Return the full slug including any trailing revision suffix.

    Strips only the leading date prefix (YYYYMMDD-). Revision suffixes such as
    '-2', '-3' are preserved so callers that need an exact match against the
    original task_id slug string get the right value.
    """
    return _get_task_slug_from_task_id(task.task_id)


def get_base_task_slug(task: Task) -> str | None:
    """Return canonical slug with trailing revision suffix stripped.

    Strips the leading date prefix (YYYYMMDD-) and removes a trailing numeric
    revision suffix such as '-2' or '-3'. Use this when matching across task
    retries/revisions.
    """
    return _get_base_task_slug(task.task_id)


def get_reviews_for_root(store: SqliteTaskStore, root_task: Task) -> list[Task]:
    """Get reviews for a root task, with fallback for unlinked manual reviews."""
    if root_task.id is None:
        return []
    reviews = store.get_reviews_for_task(root_task.id)
    if reviews:
        return reviews
    slug = get_task_slug(root_task)
    if not slug:
        return []
    return store.get_unlinked_reviews_for_slug(slug)


def get_improves_for_root(store: SqliteTaskStore, root_task: Task) -> list[Task]:
    """Get improve tasks related to the root implementation task.

    Uses a targeted query instead of get_all() to avoid a full table scan.
    """
    if root_task.id is None:
        return []
    return store.get_improve_tasks_by_root(root_task.id)


def _get_downstream_impls(store: SqliteTaskStore, task_id: int) -> list[Task]:
    """Get implement tasks that depend on or are based on a given task."""
    return store.get_impl_tasks_by_depends_on_or_based_on(task_id)


def build_lineage(store: SqliteTaskStore, root_task: Task) -> list[Task]:
    """Build deduplicated lineage tasks for a root task, including dependency chains."""
    seen_ids: set[int] = set()
    all_tasks: list[Task] = []

    def _collect(task: Task) -> None:
        if task.id is None or task.id in seen_ids:
            return
        seen_ids.add(task.id)
        all_tasks.append(task)

        for review in get_reviews_for_root(store, task):
            if review.id is not None and review.id not in seen_ids:
                seen_ids.add(review.id)
                all_tasks.append(review)

        for improve in get_improves_for_root(store, task):
            if improve.id is not None and improve.id not in seen_ids:
                seen_ids.add(improve.id)
                all_tasks.append(improve)

        if task.id is not None:
            for downstream in _get_downstream_impls(store, task.id):
                _collect(downstream)

    _collect(root_task)
    return sorted(all_tasks, key=task_time_for_lineage)


def resolve_lineage_root(store: SqliteTaskStore, task: Task) -> Task:
    """Resolve the root task for lineage display, walking up through dependency links."""
    # For review tasks, navigate to the implementation they review
    if task.task_type == "review" and task.depends_on:
        depends = store.get(task.depends_on)
        if depends is not None:
            task = depends

    # For improve tasks, navigate to the implementation they improve
    if task.task_type == "improve" and task.based_on:
        based = store.get(task.based_on)
        if based is not None:
            task = based

    # Walk the based_on chain upward to find the topmost ancestor (e.g. a plan)
    current = task
    seen: set[int] = set()
    if current.id is not None:
        seen.add(current.id)
    while current.based_on:
        next_task = store.get(current.based_on)
        if next_task is None or next_task.id is None or next_task.id in seen:
            break
        seen.add(next_task.id)
        current = next_task

    return current
