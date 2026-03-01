"""Internal query service for task lineage.

Not part of the public API. Used by cli.py and gza.api.v0.
"""

from __future__ import annotations

import re
from datetime import datetime

from gza.db import SqliteTaskStore, Task


def task_time_for_lineage(task: Task) -> datetime:
    """Return best-effort timestamp for lineage ordering."""
    return task.completed_at or task.created_at or datetime.min


def get_task_slug(task: Task) -> str | None:
    """Return the full slug including any trailing revision suffix.

    Strips only the leading date prefix (YYYYMMDD-). Revision suffixes such as
    '-2', '-3' are preserved so callers that need an exact match against the
    original task_id slug string get the right value.
    """
    if not task.task_id:
        return None
    match = re.match(r"^\d{8}-(.+)$", task.task_id)
    return match.group(1) if match else task.task_id


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


def get_downstream_impls(store: SqliteTaskStore, task_id: int) -> list[Task]:
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
            for downstream in get_downstream_impls(store, task.id):
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
