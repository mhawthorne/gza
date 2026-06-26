"""Helpers for walking task lineage relationships."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

from .db import SqliteTaskStore, Task, task_id_numeric_key


def walk_based_on_descendants(
    store: SqliteTaskStore,
    task: Task,
    *,
    task_type: str | None = None,
) -> Iterator[Task]:
    """Yield descendants reachable through based_on links.

    When ``task_type`` is provided, only descendants of that type are followed.
    """
    if task.id is None:
        return

    visited: set[str] = {task.id}
    queue: list[Task] = (
        list(store.get_based_on_children_by_type(task.id, task_type))
        if task_type is not None
        else list(store.get_based_on_children(task.id))
    )

    while queue:
        child = queue.pop(0)
        if child.id is None or child.id in visited:
            continue
        visited.add(child.id)
        yield child
        queue.extend(
            store.get_based_on_children_by_type(child.id, task_type)
            if task_type is not None
            else store.get_based_on_children(child.id)
        )


def walk_lineage_descendants(store: SqliteTaskStore, task: Task) -> Iterator[Task]:
    """Yield descendants reachable through based_on or depends_on links."""
    if task.id is None:
        return

    visited: set[str] = {task.id}
    queue: list[Task] = list(store.get_lineage_children(task.id))

    while queue:
        child = queue.pop(0)
        if child.id is None or child.id in visited:
            continue
        visited.add(child.id)
        yield child
        queue.extend(store.get_lineage_children(child.id))


def walk_ancestors(
    store: SqliteTaskStore,
    task: Task,
    *,
    follow_based_on: bool = True,
    follow_depends_on: bool = True,
) -> Iterator[Task]:
    """Yield ancestor tasks reachable via lineage links."""
    if task.id is None:
        return

    stack: list[str] = []
    if follow_based_on and task.based_on:
        stack.append(task.based_on)
    if follow_depends_on and task.depends_on:
        stack.append(task.depends_on)

    visited: set[str] = {task.id}
    while stack:
        task_id = stack.pop()
        if task_id in visited:
            continue
        visited.add(task_id)
        ancestor = store.get(task_id)
        if ancestor is None or ancestor.id is None:
            continue
        yield ancestor
        if follow_based_on and ancestor.based_on:
            stack.append(ancestor.based_on)
        if follow_depends_on and ancestor.depends_on:
            stack.append(ancestor.depends_on)


def get_plan_for_task(store: SqliteTaskStore, task: Task) -> Task | None:
    """Find an ancestor plan task via based_on/depends_on links."""
    if task.task_type == "plan":
        return task
    for ancestor in walk_ancestors(store, task, follow_based_on=True, follow_depends_on=True):
        if ancestor.task_type == "plan":
            return ancestor
    return None


def get_root_impl(store: SqliteTaskStore, task: Task) -> Task:
    """Return the oldest implementation in an implementation retry chain."""
    current = task
    visited: set[str] = set()
    while current.id is not None:
        if current.id in visited:
            break
        visited.add(current.id)
        if not current.based_on:
            break
        parent = store.get(current.based_on)
        if parent is None or parent.task_type != "implement":
            break
        current = parent
    return current


def _normalize_lineage_time(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=UTC)
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value.replace(tzinfo=UTC)


def _task_time_for_lineage(task: Task) -> datetime | None:
    return task.created_at


def _parent_ids(task: Task) -> list[str]:
    parent_ids: list[str] = []
    if task.based_on is not None:
        parent_ids.append(task.based_on)
    if task.depends_on is not None:
        parent_ids.append(task.depends_on)
    return parent_ids


def resolve_lineage_root(store: SqliteTaskStore, task: Task) -> Task:
    """Resolve the root task across based_on + depends_on lineage links."""
    if task.id is None:
        return task

    graph_nodes: dict[str, Task] = {task.id: task}
    for ancestor in walk_ancestors(store, task, follow_based_on=True, follow_depends_on=True):
        if ancestor.id is not None:
            graph_nodes[ancestor.id] = ancestor

    if len(graph_nodes) == 1:
        return task

    node_ids = set(graph_nodes)
    root_candidates = [
        candidate
        for candidate in graph_nodes.values()
        if not any(parent_id in node_ids for parent_id in _parent_ids(candidate))
    ]
    candidates = root_candidates or list(graph_nodes.values())
    return min(
        candidates,
        key=lambda candidate: (
            _normalize_lineage_time(_task_time_for_lineage(candidate)),
            task_id_numeric_key(candidate.id),
        ),
    )


def resolve_impl_task(
    store: SqliteTaskStore,
    task_id: str,
) -> tuple[Task, None] | tuple[None, str]:
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
