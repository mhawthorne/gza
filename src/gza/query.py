"""Query service for gza task history and lineage.

This module provides a typed query interface between the CLI and the SQLite
storage layer. The interfaces here are designed for eventual promotion to a
gza.api.v0 scripting namespace.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from gza.db import SqliteTaskStore, Task, task_id_numeric_key
from gza.task_slug import get_base_task_slug as _get_base_task_slug, get_task_slug as _get_task_slug_from_task_id


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


@dataclass
class IncompleteLineage:
    """Unresolved lineage group anchored on the canonical root task."""

    root: Task
    tree: TaskLineageNode
    unresolved_tasks: list[Task]
    latest_unresolved_at: datetime


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


def get_task_lineage(store: SqliteTaskStore, task_id: str, depth: int) -> TaskLineageNode:
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
    seen_root_ids: set[str] = set()

    for task in tasks:
        root = resolve_lineage_root(store, task)
        if root.id is not None and root.id in seen_root_ids:
            continue
        if root.id is not None:
            seen_root_ids.add(root.id)
        root_nodes.append(build_lineage_tree(store, root, max_depth=f.lineage_depth))

    return root_nodes


def _is_shared_branch_descendant(task: Task, root_task: Task) -> bool:
    """Return whether task is treated as a shared-branch lineage descendant."""
    if task.id is None or root_task.id is None:
        return False
    if task.id == root_task.id:
        return False
    if task.task_type == "improve":
        return task.based_on is not None
    if task.task_type == "review":
        return True
    if task.task_type == "rebase":
        return task.based_on is not None
    if task.task_type == "fix":
        return task.based_on is not None or bool(task.same_branch)
    return bool(task.same_branch)


def _iter_retry_descendants(store: SqliteTaskStore, task: Task) -> list[Task]:
    """Return retry descendants by following based_on edges via the store."""
    if task.id is None:
        return []

    descendants: list[Task] = []
    visited: set[str] = {task.id}
    queue: list[Task] = list(store.get_based_on_children(task.id))

    while queue:
        child = queue.pop(0)
        if child.id is None or child.id in visited:
            continue
        visited.add(child.id)
        descendants.append(child)
        queue.extend(store.get_based_on_children(child.id))

    return descendants


def _has_successful_retry_descendant(store: SqliteTaskStore, task: Task) -> bool:
    """Return True when any retry descendant has status='completed'."""
    return any(child.status == "completed" for child in _iter_retry_descendants(store, task))


def _has_merged_retry_descendant(store: SqliteTaskStore, task: Task) -> bool:
    """Return True when any same-type retry descendant has merge_status='merged'."""
    return any(
        child.merge_status == "merged"
        for child in _iter_retry_descendants(store, task)
        if child.task_type == task.task_type
    )


def _get_unresolved_terminal_kind(task: Task) -> str | None:
    """Return unresolved terminal kind for attention queries, else None."""
    if task.status not in {"failed", "completed", "unmerged", "dropped"}:
        return None
    if is_lineage_complete(task):
        return None
    if task.status == "failed":
        return "failed"
    if task.status == "dropped":
        return "dropped"
    if task.status in {"completed", "unmerged"}:
        return "completed_like"
    return None


def _prune_lineage_tree_to_ids(tree: TaskLineageNode, keep_ids: set[str]) -> TaskLineageNode:
    """Return a lineage tree containing only keep_ids (plus root anchor)."""

    def _filter(node: TaskLineageNode, is_root: bool) -> TaskLineageNode | None:
        kept_children: list[TaskLineageNode] = []
        for child in node.children:
            filtered = _filter(child, False)
            if filtered is not None:
                kept_children.append(filtered)
        task_id = node.task.id
        should_keep = is_root or (task_id is not None and task_id in keep_ids) or bool(kept_children)
        if not should_keep:
            return None
        return TaskLineageNode(
            task=node.task,
            depth=node.depth,
            relationship=node.relationship,
            children=kept_children,
        )

    filtered = _filter(tree, True)
    assert filtered is not None
    return filtered


def query_incomplete(store: SqliteTaskStore, f: HistoryFilter) -> list[IncompleteLineage]:
    """Return unresolved lineages grouped by canonical root for attention workflows."""
    filtered = HistoryFilter(
        limit=None,
        status=None,
        task_type=f.task_type,
        incomplete=False,
        days=f.days,
        start_date=f.start_date,
        end_date=f.end_date,
    )
    tasks = query_history(store, filtered)
    if not tasks:
        return []

    unresolved_by_root: dict[str, list[Task]] = {}
    root_by_id: dict[str, Task] = {}

    for task in tasks:
        if task.id is None:
            continue

        unresolved_kind = _get_unresolved_terminal_kind(task)
        if unresolved_kind is None:
            continue

        if unresolved_kind == "failed":
            if _has_successful_retry_descendant(store, task):
                continue
            root = resolve_lineage_root(store, task)
            if root.id is None:
                continue
            unresolved_by_root.setdefault(root.id, []).append(task)
            root_by_id[root.id] = root
            continue

        root = resolve_lineage_root(store, task)
        if root.id is None:
            continue
        root_merged = root.merge_status == "merged"
        shared_descendant = _is_shared_branch_descendant(task, root)

        if shared_descendant:
            if root_merged:
                continue
        else:
            if task.merge_status == "merged":
                continue
            if _has_merged_retry_descendant(store, task):
                continue

        unresolved_by_root.setdefault(root.id, []).append(task)
        root_by_id[root.id] = root

    lineages: list[IncompleteLineage] = []
    for root_id, unresolved in unresolved_by_root.items():
        if not unresolved:
            continue
        root = root_by_id[root_id]
        shown_ids: set[str] = set()
        for task in unresolved:
            if task.id is None:
                continue
            shown_ids.add(task.id)

        tree = _prune_lineage_tree_to_ids(build_lineage_tree(store, root), shown_ids)
        latest_unresolved_at = max(
            _normalize_lineage_time(task_time_for_lineage(task)) for task in unresolved
        )
        shown_tasks = [
            task for task in flatten_lineage_tree(tree)
            if task.id is not None and task.id in shown_ids
        ]
        lineages.append(
            IncompleteLineage(
                root=root,
                tree=tree,
                unresolved_tasks=shown_tasks,
                latest_unresolved_at=latest_unresolved_at,
            )
        )

    lineages.sort(
        key=lambda item: (
            _normalize_lineage_time(item.latest_unresolved_at),
            task_id_numeric_key(item.root.id),
        ),
        reverse=True,
    )
    if f.limit is not None:
        lineages = lineages[: f.limit]
    return lineages


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
    return _get_task_slug_from_task_id(task.slug)


def get_base_task_slug(task: Task) -> str | None:
    """Return canonical slug with trailing revision suffix stripped.

    Strips the leading date prefix (YYYYMMDD-) and removes a trailing numeric
    revision suffix such as '-2' or '-3'. Use this when matching across task
    retries/revisions.
    """
    return _get_base_task_slug(task.slug)


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


def get_fixes_for_root(store: SqliteTaskStore, root_task: Task) -> list[Task]:
    """Get fix tasks transitively based on the given root task."""
    if root_task.id is None:
        return []
    return store.get_fix_tasks_by_root(root_task.id)


def get_code_changing_descendants_for_root(store: SqliteTaskStore, root_task: Task) -> list[Task]:
    """Return same-branch code-changing descendants (improves + fixes) of a root task.

    Used by review-freshness logic: any completed task here invalidates a prior
    review the same way an improve does, because the task ran on the impl's
    shared branch after the review was written.
    """
    return [*get_improves_for_root(store, root_task), *get_fixes_for_root(store, root_task)]


_LINEAGE_REL_LABELS: dict[str, str] = {
    "review": "review",
    "improve-from-review": "improve",
    "improve": "improve",
    "fix-from-review": "fix",
    "fix": "fix",
    "implement-depends": "implement",
    "implement-based": "implement",
    "depends-and-based": "depends",
    "depends": "depends",
    "retry": "retry",
    "resume": "resume",
    # Relationships not in this map (e.g. "rebase", "plan", "explore", "task",
    # "internal") silently produce no label — intentional for relationships
    # whose task_type already conveys everything the UI needs.
}


def _classify_child_relationship(parent: Task, child: Task) -> str:
    """Return a child relationship label for lineage tree rendering/debugging."""
    parent_id = parent.id
    if parent_id is None:
        return "child"

    # Detect resume/retry first: same task_type + based_on pointing to parent
    # indicates a re-execution of the same work, not a lifecycle transition.
    if child.based_on == parent_id and child.task_type == parent.task_type:
        if child.session_id and child.session_id == parent.session_id:
            return "resume"
        return "retry"

    if child.task_type == "rebase" and child.based_on == parent_id:
        return "rebase"
    if child.task_type == "review" and child.depends_on == parent_id:
        return "review"
    if child.task_type == "improve" and child.depends_on == parent_id:
        return "improve-from-review"
    if child.task_type == "improve" and child.based_on == parent_id:
        return "improve"
    if child.task_type == "fix" and child.depends_on == parent_id:
        return "fix-from-review"
    if child.task_type == "fix" and child.based_on == parent_id:
        return "fix"
    if child.task_type == "implement" and child.depends_on == parent_id:
        return "implement-depends"
    if child.task_type == "implement" and child.based_on == parent_id:
        return "implement-based"
    if child.depends_on == parent_id and child.based_on == parent_id:
        return "depends-and-based"
    if child.depends_on == parent_id:
        return "depends"
    return child.task_type


def _lineage_child_sort_key(parent: Task, child: Task) -> tuple[datetime, int]:
    """Sort children chronologically so lineage reads as a timeline.

    Tree structure (not sibling order) conveys relationships: review→improve
    pairs already nest via parent/child edges, so siblings only need to express
    when things happened. Chronological order also exposes pre-run hooks such
    as auto-rebase-before-resume honestly, even when they predate their parent.
    """
    del parent  # relationship no longer affects sibling ordering
    child_time = _normalize_lineage_time(task_time_for_lineage(child))
    # task_id_numeric_key expects str | None; guard against legacy integer IDs
    id_str = child.id if isinstance(child.id, str) else None
    child_id = task_id_numeric_key(id_str)  # returns int; 0 for None/non-string
    return (child_time, child_id)


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

    attached_ids: set[str] = {root_task.id}

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


def _get_parent_ids(task: Task) -> list[str]:
    parent_ids: list[str] = []
    if task.based_on is not None:
        parent_ids.append(task.based_on)
    if task.depends_on is not None:
        parent_ids.append(task.depends_on)
    return parent_ids


def resolve_lineage_root(store: SqliteTaskStore, task: Task) -> Task:
    """Resolve the root task for lineage display across based_on + depends_on chains."""
    if task.id is None:
        return task

    graph_nodes: dict[str, Task] = {task.id: task}
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
        return (ts, task_id_numeric_key(candidate.id))

    return sorted(candidates, key=_root_order_key)[0]
