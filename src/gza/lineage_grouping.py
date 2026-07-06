"""Collapse a per-task lineage tree into a tree of merge-unit groups.

A merge unit is the unit of work that lands on a target branch: an implement and
all the tasks that exist only to get its branch merged (reviews, improves,
rebases, re-attempts). Grouping by merge unit turns a deep per-task lineage tree
into a readable structure where supporting tasks render inline with the implement
they support, and *dependent* implements (which carry their own reviews) form the
real hierarchy.

This module owns the grouping primitives so every renderer — the CLI lineage and
show views and the unified query presenters — builds the same structure.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from gza.db import MergeUnit, SqliteTaskStore, Task as DbTask
from gza.lineage_query import task_id_numeric_key
from gza.query import TaskLineageNode

# Task types that exist only to land an implement's branch (they belong with the
# implement's merge unit rather than forming their own lineage block).
SUPPORTING_TASK_TYPES = {"review", "improve", "rebase", "plan_review", "plan_improve"}


@dataclass
class MergeUnitGroup:
    """A merge unit (or an unmerged solo task) plus its supporting tasks.

    ``header`` is the implement that owns the merge unit (or the solo task itself).
    ``members`` are the supporting tasks attached to the same unit — reviews,
    improves, rebases and re-attempts — ordered by creation. ``children`` are the
    *dependent* merge units (downstream implements that carry their own reviews),
    which is what distinguishes them from the review members rendered inline.
    """

    key: str
    header: DbTask
    unit: MergeUnit | None
    members: list[DbTask]
    children: list[MergeUnitGroup]


def build_merge_unit_group_tree(
    store: SqliteTaskStore, root_node: TaskLineageNode
) -> list[MergeUnitGroup]:
    """Collapse a per-task lineage tree into a tree of merge-unit groups."""
    nodes: dict[str, TaskLineageNode] = {}
    order: list[str] = []

    def _walk(node: TaskLineageNode) -> None:
        tid = node.task.id
        if tid is None:
            return
        if tid not in nodes:
            nodes[tid] = node
            order.append(tid)
        for child in node.children:
            _walk(child)

    _walk(root_node)
    task_by_id = {tid: nodes[tid].task for tid in order}
    unit_cache = {tid: store.resolve_merge_unit_for_task(tid) for tid in order}

    def _ckey(task: DbTask) -> int:
        return task_id_numeric_key(task.id if isinstance(task.id, str) else None)

    # Assign every task to a group in three passes:
    #   1. tasks with a merge unit anchor that unit;
    #   2. remaining block-starting tasks (implements, plans, fixes…) stand alone;
    #   3. supporting tasks (reviews, improves, rebases…) attach to their parent's
    #      group so they render inline with the work they support.
    group_of: dict[str, str] = {}
    for tid in order:
        unit = unit_cache.get(tid)
        if unit is not None:
            group_of[tid] = unit.id
    for tid in order:
        if tid in group_of:
            continue
        if task_by_id[tid].task_type not in SUPPORTING_TASK_TYPES:
            group_of[tid] = f"solo:{tid}"
    for tid in sorted(order, key=lambda t: task_id_numeric_key(t)):
        if tid in group_of:
            continue
        task = task_by_id[tid]
        inherited: str | None = None
        for parent_id in (task.based_on, task.depends_on):
            if parent_id is not None and parent_id in group_of:
                inherited = group_of[parent_id]
                break
        group_of[tid] = inherited if inherited is not None else f"solo:{tid}"

    members: dict[str, list[DbTask]] = {}
    for tid in order:
        members.setdefault(group_of[tid], []).append(task_by_id[tid])
    for key in members:
        members[key].sort(key=_ckey)

    def _resolve_header(key: str, group_members: list[DbTask]) -> tuple[DbTask, MergeUnit | None]:
        if not key.startswith("solo:"):
            unit = store.get_merge_unit(key)
            owner = store.resolve_merge_unit_owner_task(unit) if unit is not None else None
            if owner is not None and owner.id is not None and owner.id in nodes:
                return nodes[owner.id].task, unit
            implements = [m for m in group_members if m.task_type == "implement"]
            return (implements or group_members)[0], unit
        return group_members[0], None

    headers: dict[str, tuple[DbTask, MergeUnit | None]] = {
        key: _resolve_header(key, group_members) for key, group_members in members.items()
    }

    node_ids = set(nodes)

    def _parent_key(key: str) -> str | None:
        header = headers[key][0]
        for parent_id in (header.based_on, header.depends_on):
            if parent_id is not None and parent_id in node_ids:
                parent_group = group_of.get(parent_id)
                if parent_group is not None and parent_group != key:
                    return parent_group
        return None

    child_keys: dict[str, list[str]] = {key: [] for key in members}
    roots: list[str] = []
    for key in members:
        parent = _parent_key(key)
        if parent is None or parent not in members:
            roots.append(key)
        else:
            child_keys[parent].append(key)

    def _hkey(key: str) -> int:
        return _ckey(headers[key][0])

    for key in child_keys:
        child_keys[key].sort(key=_hkey)
    roots.sort(key=_hkey)

    built: set[str] = set()

    def _build(key: str) -> MergeUnitGroup:
        built.add(key)
        header, unit = headers[key]
        group_members = [m for m in members[key] if m.id != header.id]
        # Skip any child already built: guards against a parent-key cycle sending
        # _build into infinite recursion (and double-rendering a group).
        children = [_build(child) for child in child_keys[key] if child not in built]
        return MergeUnitGroup(
            key=key,
            header=header,
            unit=unit,
            members=group_members,
            children=children,
        )

    forest = [_build(root) for root in roots]
    # A parent-key cycle (e.g. a unit whose resolved header is based_on a solo
    # task that is itself based_on one of the unit's own members) leaves some
    # groups neither a root nor reachable from one. Surface each remaining group
    # as its own root so its tasks still render instead of silently vanishing.
    for key in sorted(members, key=_hkey):
        if key not in built:
            forest.append(_build(key))
    return forest


def group_subtree_counts(group: MergeUnitGroup) -> tuple[int, int]:
    """Return (total tasks, total merge units) in a group's subtree, inclusive."""
    tasks = 1 + len(group.members)
    units = 1 if group.unit is not None else 0
    for child in group.children:
        child_tasks, child_units = group_subtree_counts(child)
        tasks += child_tasks
        units += child_units
    return tasks, units


def format_lineage_summary(stats: Mapping[str, object]) -> str:
    """Render the one-line lineage-orientation summary from structured stats.

    Pure: reads the ``lineage_origin_*`` / ``lineage_root_impl_id`` /
    ``lineage_parent_impl_id`` values a query projects onto a row, so both the CLI
    feed renderers and the query presenters share it. Orients the task by the
    meaningful nodes above it (plan/explore origin, root implement, parent
    implement) rather than a raw task count.
    """
    def _id(key: str) -> str | None:
        value = stats.get(key)
        return value if isinstance(value, str) and value else None

    parts: list[str] = []
    origin_id = _id("lineage_origin_id")
    if origin_id is not None:
        origin_type = _id("lineage_origin_type") or "plan"
        parts.append(f"{origin_type} {origin_id}")
    root_impl_id = _id("lineage_root_impl_id")
    if root_impl_id is not None:
        parts.append(f"root impl {root_impl_id}")
    parent_impl_id = _id("lineage_parent_impl_id")
    if parent_impl_id is not None:
        parts.append(f"parent impl {parent_impl_id}")
    if not parts:
        return "root"
    return " · ".join(parts)


def find_group_path(
    groups: list[MergeUnitGroup], task_id: str
) -> list[MergeUnitGroup] | None:
    """Return the path of groups from a root to the group holding task_id."""

    def _walk(group: MergeUnitGroup, trail: list[MergeUnitGroup]) -> list[MergeUnitGroup] | None:
        here = [*trail, group]
        if group.header.id == task_id or any(m.id == task_id for m in group.members):
            return here
        for child in group.children:
            found = _walk(child, here)
            if found is not None:
                return found
        return None

    for root in groups:
        path = _walk(root, [])
        if path is not None:
            return path
    return None
