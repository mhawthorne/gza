"""Internal query service for task lineage.

Not part of the public API. Used by cli.py and gza.api.v0.
"""

from __future__ import annotations

from datetime import datetime

from gza.db import SqliteTaskStore, Task
from gza.task_slug import get_base_task_slug as _get_base_task_slug
from gza.task_slug import get_task_slug as _get_task_slug_from_task_id


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
    """Get implement tasks that depend on or are based on a given task.

    Internal helper used only within this module; not part of the public API.
    """
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
