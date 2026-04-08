"""Query service for gza task history and lineage.

This module provides a typed query interface between the CLI and the SQLite
storage layer. The interfaces here are designed for eventual promotion to a
gza.api.v0 scripting namespace.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

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
    """A lineage tree node rooted at a task."""

    task: Task
    depth: int = 0
    relationship: str = "root"
    children: list[TaskLineageNode] = field(default_factory=list)


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
        since = datetime.now(UTC) - timedelta(days=f.days)
    elif f.start_date is not None:
        since = datetime.fromisoformat(f.start_date).replace(tzinfo=UTC)

    until: datetime | None = None
    if f.end_date is not None:
        until = datetime.fromisoformat(f.end_date).replace(tzinfo=UTC)
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
    """Return lineage tree rooted at the resolved lineage root for task_id."""
    task = store.get(task_id)
    if task is None:
        raise KeyError(f"Task {task_id} not found")
    root = resolve_lineage_root(store, task)
    return build_lineage_tree(store, root, max_depth=depth)


def query_history_with_lineage(
    store: SqliteTaskStore, f: HistoryFilter
) -> list[TaskLineageNode]:
    """Return filtered history with lineage trees expanded to f.lineage_depth."""
    tasks = query_history(store, f)
    root_nodes: list[TaskLineageNode] = []
    seen_root_ids: set[int] = set()

    for task in tasks:
        root = resolve_lineage_root(store, task)
        if root.id is not None and root.id in seen_root_ids:
            continue
        if root.id is not None:
            seen_root_ids.add(root.id)
        root_nodes.append(build_lineage_tree(store, root, max_depth=f.lineage_depth))

    return root_nodes


# --- Lineage helpers ---


def task_time_for_lineage(task: Task) -> datetime:
    """Return best-effort timestamp for lineage ordering."""
    return task.completed_at or task.created_at or datetime.min


def _normalize_lineage_time(value: datetime) -> datetime:
    """Normalize aware/naive datetimes for stable lineage comparisons."""
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


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
    """Get improve tasks directly based on the given root task."""
    if root_task.id is None:
        return []
    return store.get_improve_tasks_by_root(root_task.id)


def _classify_child_relationship(parent: Task, child: Task) -> str:
    """Return a child relationship label for lineage tree rendering/debugging."""
    parent_id = parent.id
    if parent_id is None:
        return "child"

    if child.task_type == "review" and child.depends_on == parent_id:
        return "review"
    if child.task_type == "improve" and child.depends_on == parent_id:
        return "improve-from-review"
    if child.task_type == "improve" and child.based_on == parent_id:
        return "improve"
    if child.task_type == "implement" and child.depends_on == parent_id:
        return "implement-depends"
    if child.task_type == "implement" and child.based_on == parent_id:
        return "implement-based"
    if child.depends_on == parent_id and child.based_on == parent_id:
        return "depends-and-based"
    if child.depends_on == parent_id:
        return "depends"
    if child.based_on == parent_id:
        return "based"
    return child.task_type


def _lineage_child_sort_key(parent: Task, child: Task) -> tuple[int, datetime, int]:
    """Sort children to keep lineage rendering deterministic and readable."""
    relation = _classify_child_relationship(parent, child)
    relation_rank = {
        "review": 0,
        "improve-from-review": 1,
        "implement-depends": 2,
        "implement-based": 3,
        "improve": 4,
        "depends-and-based": 5,
        "depends": 6,
        "based": 7,
    }.get(relation, 8)

    child_time = _normalize_lineage_time(task_time_for_lineage(child))
    child_id = child.id if child.id is not None else 10**9
    return (relation_rank, child_time, child_id)


def build_lineage_tree(
    store: SqliteTaskStore,
    root_task: Task,
    *,
    max_depth: int | None = None,
) -> TaskLineageNode:
    """Build a canonical lineage tree by walking both depends_on and based_on edges."""

    root = TaskLineageNode(task=root_task, depth=0, relationship="root")
    if root_task.id is None:
        return root

    attached_ids: set[int] = {root_task.id}

    def _populate(node: TaskLineageNode) -> None:
        if max_depth is not None and node.depth >= max_depth:
            return
        parent_id = node.task.id
        if parent_id is None:
            return

        children = store.get_lineage_children(parent_id)
        children.sort(key=lambda child: _lineage_child_sort_key(node.task, child))

        for child in children:
            if child.id is None:
                continue
            # If a task references another already-attached node via depends_on,
            # defer attachment so it is picked up under that dependency parent.
            if (
                child.depends_on is not None
                and child.depends_on != parent_id
                and child.depends_on in attached_ids
            ):
                continue
            if child.id in attached_ids:
                # A task may reference a parent by both depends_on and based_on.
                # Attach once to avoid duplicated branches.
                continue
            attached_ids.add(child.id)
            child_node = TaskLineageNode(
                task=child,
                depth=node.depth + 1,
                relationship=_classify_child_relationship(node.task, child),
            )
            node.children.append(child_node)
            _populate(child_node)

    _populate(root)
    return root


def filter_lineage_tree(
    tree: TaskLineageNode,
    allowed_types: set[str] | list[str] | tuple[str, ...],
) -> TaskLineageNode:
    """Return a lineage tree with disallowed descendants pruned.

    The root node is always preserved so callers can keep a stable lineage anchor.
    Any disallowed descendants are removed, and their allowed descendants are
    re-parented to the nearest retained ancestor.
    """
    allowed = set(allowed_types)

    def _filter_children(node: TaskLineageNode) -> list[TaskLineageNode]:
        filtered: list[TaskLineageNode] = []
        for child in node.children:
            kept_grandchildren = _filter_children(child)
            if child.task.task_type in allowed:
                filtered.append(
                    TaskLineageNode(
                        task=child.task,
                        depth=0,
                        relationship=child.relationship,
                        children=kept_grandchildren,
                    )
                )
            else:
                filtered.extend(kept_grandchildren)
        return filtered

    filtered_root = TaskLineageNode(
        task=tree.task,
        depth=0,
        relationship=tree.relationship,
        children=_filter_children(tree),
    )

    def _assign_depths(node: TaskLineageNode, depth: int) -> None:
        node.depth = depth
        for child in node.children:
            _assign_depths(child, depth + 1)

    _assign_depths(filtered_root, 0)
    return filtered_root


def flatten_lineage_tree(node: TaskLineageNode) -> list[Task]:
    """Flatten lineage tree to a deterministic pre-order traversal."""
    items: list[Task] = [node.task]
    for child in node.children:
        items.extend(flatten_lineage_tree(child))
    return items


def build_lineage(store: SqliteTaskStore, root_task: Task) -> list[Task]:
    """Return lineage as a flattened list from the canonical tree builder."""
    if root_task.id is None:
        return []
    return flatten_lineage_tree(build_lineage_tree(store, root_task, max_depth=None))


def _get_parent_ids(task: Task) -> list[int]:
    parent_ids: list[int] = []
    if task.based_on is not None:
        parent_ids.append(task.based_on)
    if task.depends_on is not None:
        parent_ids.append(task.depends_on)
    return parent_ids


def resolve_lineage_root(store: SqliteTaskStore, task: Task) -> Task:
    """Resolve the root task for lineage display across based_on + depends_on chains."""
    if task.id is None:
        return task

    graph_nodes: dict[int, Task] = {task.id: task}
    to_visit = _get_parent_ids(task)

    while to_visit:
        parent_id = to_visit.pop(0)
        if parent_id in graph_nodes:
            continue
        parent = store.get(parent_id)
        if parent is None or parent.id is None:
            continue
        graph_nodes[parent.id] = parent
        to_visit.extend(_get_parent_ids(parent))

    if len(graph_nodes) == 1:
        return task

    node_ids = set(graph_nodes.keys())
    root_candidates = [
        candidate
        for candidate in graph_nodes.values()
        if not any(parent_id in node_ids for parent_id in _get_parent_ids(candidate))
    ]
    candidates = root_candidates or list(graph_nodes.values())

    def _root_order_key(candidate: Task) -> tuple[datetime, int]:
        ts = _normalize_lineage_time(task_time_for_lineage(candidate))
        candidate_id = candidate.id if candidate.id is not None else 10**9
        return (ts, candidate_id)

    return sorted(candidates, key=_root_order_key)[0]
