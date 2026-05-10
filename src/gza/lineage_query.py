"""Owner-keyed lineage query helpers shared by incomplete/advance/watch."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from .db import MergeUnit, SqliteTaskStore, Task as DbTask, task_id_numeric_key

if TYPE_CHECKING:
    from .config import Config
    from .git import Git
    from .task_query import DateFilter


LineageStatus = Literal["resolved", "actionable", "needs_attention", "waiting", "skipped"]
ResolutionReason = Literal["lineage_complete", "branch_merged", "recovery_chain_completed"]


@dataclass(frozen=True)
class UnresolvedLeafSummary:
    task_id: str
    status: str
    task_type: str
    reason: str | None = None


@dataclass(frozen=True)
class LineageOwnerSnapshot:
    owner_task: DbTask
    root_task: DbTask
    members: tuple[DbTask, ...]
    merge_units_by_task_id: Mapping[str, MergeUnit]
    failed_leaves: tuple[DbTask, ...]
    recovery_completed_by_failed_id: Mapping[str, DbTask]


@dataclass(frozen=True)
class LineageResolution:
    resolved: bool
    reasons: tuple[ResolutionReason, ...]
    resolved_by_task_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class LineageOwnerRow:
    owner_task: DbTask
    members: tuple[DbTask, ...]
    tree: Any
    lineage_status: LineageStatus
    next_action: dict[str, Any] | None
    next_action_reason: str
    unresolved_tasks: tuple[DbTask, ...]
    unresolved_leaf_summary: tuple[UnresolvedLeafSummary, ...]
    action_task: DbTask | None = None
    recovery_leaf_task: DbTask | None = None


@dataclass(frozen=True)
class LineageOwnerQuery:
    limit: int | None = None
    task_types: tuple[str, ...] | None = None
    exclude_task_types: tuple[str, ...] | None = None
    tags: tuple[str, ...] | None = None
    any_tag: bool = False
    date_filter: DateFilter | None = None
    include_skipped: bool = False
    max_recovery_attempts: int | None = None
    groups: tuple[str, ...] | None = None
    owner_task_ids: tuple[str, ...] | None = None
    task_ids: tuple[str, ...] | None = None


@dataclass(frozen=True)
class _LineageIndexes:
    tasks: tuple[DbTask, ...]
    task_by_id: dict[str, DbTask]
    based_on_children: dict[str, list[DbTask]]
    depends_on_children: dict[str, list[DbTask]]
    root_by_task_id: dict[str, DbTask]
    owner_by_task_id: dict[str, DbTask]
    members_by_owner_id: dict[str, list[DbTask]]
    merge_units_by_task_id: dict[str, MergeUnit]
    impl_based_on_ids: set[str]


def _normalize_dt(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _task_event_time(task: DbTask) -> datetime:
    return _normalize_dt(task.completed_at or task.created_at)


def _matches_task_filters(
    task: DbTask,
    query: LineageOwnerQuery,
    *,
    tag_matcher: Any,
) -> bool:
    if query.task_types is not None and task.task_type not in set(query.task_types):
        return False
    if query.exclude_task_types is not None and task.task_type in set(query.exclude_task_types):
        return False
    if query.groups is not None:
        task_groups = set(task.tags)
        if task.group:
            task_groups.add(task.group)
        if not task_groups.intersection(query.groups):
            return False
    if query.tags is not None and not tag_matcher(task_tags=task.tags, tag_filters=query.tags, any_tag=query.any_tag):
        return False
    if query.date_filter is not None:
        if not _matches_date_filter(task, query.date_filter):
            return False
    if query.task_ids is not None and task.id not in set(query.task_ids):
        return False
    return True


def _task_time_for_field(task: DbTask, field_name: str) -> datetime | None:
    if field_name == "created":
        return task.created_at
    if field_name == "completed":
        return task.completed_at
    return task.completed_at or task.created_at


def _matches_date_filter(task: DbTask, date_filter: DateFilter) -> bool:
    candidate = _task_time_for_field(task, date_filter.field)
    if candidate is None:
        return False
    normalized = _normalize_dt(candidate)
    if date_filter.days is not None:
        start = _normalize_dt(datetime.now(UTC)) - date_filter.days * (datetime.now(UTC) - datetime.now(UTC))
        # Avoid timedelta import churn while keeping semantics explicit.
        from datetime import timedelta

        start = _normalize_dt(datetime.now(UTC) - timedelta(days=date_filter.days))
        if normalized < start:
            return False
    if date_filter.start is not None:
        from datetime import time

        start_dt = datetime.combine(date_filter.start, time.min, tzinfo=UTC)
        if normalized < _normalize_dt(start_dt):
            return False
    if date_filter.end is not None:
        from datetime import time

        end_dt = datetime.combine(date_filter.end, time.max, tzinfo=UTC)
        if normalized > _normalize_dt(end_dt):
            return False
    return True


def _load_indexes(store: SqliteTaskStore) -> _LineageIndexes:
    from .recovery_engine import get_recovery_chain_root_task_id

    tasks = tuple(store.get_all())
    task_by_id = {task.id: task for task in tasks if task.id is not None}
    based_on_children: dict[str, list[DbTask]] = defaultdict(list)
    depends_on_children: dict[str, list[DbTask]] = defaultdict(list)
    for task in tasks:
        if task.id is None:
            continue
        if task.based_on is not None:
            based_on_children[task.based_on].append(task)
        if task.depends_on is not None:
            depends_on_children[task.depends_on].append(task)

    merge_units_by_task_id: dict[str, MergeUnit] = {}
    if store.supports_merge_units():
        for unit in store.list_active_merge_units():
            for member in store.list_tasks_for_merge_unit(unit.id):
                if member.id is not None:
                    merge_units_by_task_id[member.id] = unit

    def resolve_root(task: DbTask, seen: set[str] | None = None) -> DbTask:
        if task.id is None:
            return task
        cached = root_by_task_id.get(task.id)
        if cached is not None:
            return cached
        if seen is None:
            seen = set()
        if task.id in seen:
            return task
        seen.add(task.id)
        parents: list[DbTask] = []
        if task.based_on and task.based_on in task_by_id:
            parents.append(resolve_root(task_by_id[task.based_on], seen))
        if task.depends_on and task.depends_on in task_by_id:
            parents.append(resolve_root(task_by_id[task.depends_on], seen))
        if not parents:
            root_by_task_id[task.id] = task
            return task
        root = min(parents, key=lambda item: (_task_event_time(item), task_id_numeric_key(item.id)))
        root_by_task_id[task.id] = root
        return root

    def is_shared_branch_descendant(task: DbTask, root_task: DbTask) -> bool:
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

    def resolve_same_type_branch_owner(task: DbTask) -> DbTask:
        current = task
        seen: set[str] = set()
        while current.id is not None and current.id not in seen and current.based_on in task_by_id:
            seen.add(current.id)
            parent = task_by_id[current.based_on]
            if parent.task_type != task.task_type:
                break
            if current.branch and parent.branch and current.branch != parent.branch:
                break
            current = parent
        return current

    root_by_task_id: dict[str, DbTask] = {}
    owner_by_task_id: dict[str, DbTask] = {}
    members_by_owner_id: dict[str, list[DbTask]] = defaultdict(list)

    for task in tasks:
        if task.id is None:
            continue
        root = resolve_root(task)
        attached_unit: MergeUnit | None = merge_units_by_task_id.get(task.id)
        same_type_owner = resolve_same_type_branch_owner(task)
        if attached_unit is not None and attached_unit.owner_task_id is not None and attached_unit.owner_task_id in task_by_id:
            owner = task_by_id[attached_unit.owner_task_id]
        elif is_shared_branch_descendant(task, root):
            owner = root
        elif (
            (recovery_root_id := get_recovery_chain_root_task_id(store, task)) is not None
            and recovery_root_id != task.id
            and recovery_root_id in task_by_id
        ):
            owner = task_by_id[recovery_root_id]
        elif same_type_owner.id is not None and same_type_owner.id != task.id:
            owner = same_type_owner
        else:
            owner = task
        owner_by_task_id[task.id] = owner
        if owner.id is not None:
            members_by_owner_id[owner.id].append(task)

    return _LineageIndexes(
        tasks=tasks,
        task_by_id=task_by_id,
        based_on_children=based_on_children,
        depends_on_children=depends_on_children,
        root_by_task_id=root_by_task_id,
        owner_by_task_id=owner_by_task_id,
        members_by_owner_id=members_by_owner_id,
        merge_units_by_task_id=merge_units_by_task_id,
        impl_based_on_ids=store.get_impl_based_on_ids(),
    )


def is_lineage_resolved(snapshot: LineageOwnerSnapshot) -> LineageResolution:
    reasons: list[ResolutionReason] = []
    resolved_by_ids: list[str] = []

    merged_member_ids = [
        task.id
        for task in snapshot.members
        if task.id is not None
        and (
            snapshot.merge_units_by_task_id.get(task.id, None) is not None
            and snapshot.merge_units_by_task_id[task.id].state == "merged"
            or task.merge_status == "merged"
        )
    ]
    if merged_member_ids:
        reasons.append("branch_merged")
        resolved_by_ids.extend(merged_member_ids)

    if snapshot.failed_leaves and len(snapshot.recovery_completed_by_failed_id) == len(snapshot.failed_leaves):
        reasons.append("recovery_chain_completed")
        resolved_by_ids.extend(
            resolved.id
            for resolved in snapshot.recovery_completed_by_failed_id.values()
            if resolved.id is not None
        )

    if not snapshot.failed_leaves and not merged_member_ids:
        unresolved_nonfailed = [
            task
            for task in snapshot.members
            if task.status in {"failed", "dropped", "pending", "in_progress", "unmerged"}
        ]
        if not unresolved_nonfailed:
            reasons.append("lineage_complete")

    return LineageResolution(
        resolved=bool(reasons),
        reasons=tuple(reasons),
        resolved_by_task_ids=tuple(dict.fromkeys(task_id for task_id in resolved_by_ids if task_id is not None)),
    )


def _build_owner_tree(
    store: SqliteTaskStore,
    *,
    root_task: DbTask,
    owner_task: DbTask,
    unresolved_tasks: Sequence[DbTask],
) -> tuple[Any, tuple[DbTask, ...]]:
    from .query import build_lineage_tree, flatten_lineage_tree

    root_tree = build_lineage_tree(store, root_task, max_depth=None)
    keep_ids = {task.id for task in unresolved_tasks if task.id is not None}
    if owner_task.id is not None:
        keep_ids.add(owner_task.id)
    if root_task.id is not None:
        keep_ids.add(root_task.id)

    def _filter(node: Any, is_root: bool) -> Any | None:
        kept_children: list[Any] = []
        for child in node.children:
            filtered = _filter(child, False)
            if filtered is not None:
                kept_children.append(filtered)
        task_id = node.task.id
        should_keep = is_root or (task_id is not None and task_id in keep_ids) or bool(kept_children)
        if not should_keep:
            return None
        node_type = type(node)
        return node_type(
            task=node.task,
            depth=node.depth,
            relationship=node.relationship,
            children=kept_children,
        )

    filtered = _filter(root_tree, True)
    assert filtered is not None
    return filtered, tuple(flatten_lineage_tree(filtered))


def _select_representative_completed_task(
    store: SqliteTaskStore,
    snapshot: LineageOwnerSnapshot,
    unresolved_tasks: Sequence[DbTask],
    *,
    impl_based_on_ids: set[str],
) -> DbTask | None:
    owner = snapshot.owner_task
    if owner.id is not None and owner.task_type in {"plan", "explore"} and owner.id not in impl_based_on_ids:
        return owner
    actionable = [task for task in unresolved_tasks if task.status in {"completed", "unmerged", "dropped"}]
    if not actionable:
        return None
    owner_unit = snapshot.merge_units_by_task_id.get(owner.id or "")
    if owner_unit is not None:
        rep = store.resolve_merge_unit_representative_task(owner_unit, preferred_task_id=owner.id, require_actionable=False)
        if rep is not None and rep in actionable:
            return rep
    return max(actionable, key=lambda task: (_task_event_time(task), task_id_numeric_key(task.id)))


def _has_completed_same_type_descendant(indexes: _LineageIndexes, task: DbTask) -> bool:
    def _is_manual_follow_up(parent: DbTask, child: DbTask) -> bool:
        if child.recovery_origin == "manual":
            return True
        if parent.prompt != child.prompt or parent.depends_on != child.depends_on:
            return False
        if parent.session_id is None or child.session_id is None:
            return False
        if parent.branch is None or child.branch is None:
            return False
        if parent.session_id == child.session_id or parent.branch == child.branch:
            return False
        if parent.same_branch and child.base_branch == parent.branch and not child.same_branch:
            return False
        return True

    if task.id is None:
        return False
    queue = list(indexes.based_on_children.get(task.id, ()))
    seen: set[str] = set()
    while queue:
        child = queue.pop(0)
        if child.id is None or child.id in seen:
            continue
        seen.add(child.id)
        if (
            child.task_type == task.task_type
            and child.status == "completed"
            and not _is_manual_follow_up(task, child)
        ):
            return True
        queue.extend(indexes.based_on_children.get(child.id, ()))
    return False


def _has_merged_descendant(
    indexes: _LineageIndexes,
    task: DbTask,
    *,
    merge_units_by_member: Mapping[str, MergeUnit],
) -> bool:
    if task.id is None:
        return False
    queue = list(indexes.based_on_children.get(task.id, ()))
    seen: set[str] = set()
    while queue:
        child = queue.pop(0)
        if child.id is None or child.id in seen:
            continue
        seen.add(child.id)
        merge_state = (
            merge_units_by_member[child.id].state
            if child.id in merge_units_by_member
            else child.merge_status
        )
        if merge_state == "merged":
            return True
        queue.extend(indexes.based_on_children.get(child.id, ()))
    return False


def _classify_lineage_status(action: Mapping[str, Any]) -> LineageStatus:
    from .cli.advance_engine import classify_advance_action

    bucket = classify_advance_action(action)
    if bucket == "actionable":
        return "actionable"
    if bucket == "needs_attention":
        return "needs_attention"
    if str(action.get("type", "")) in {"wait_review", "wait_improve"}:
        return "waiting"
    return "skipped"


def _resolve_owner_merge_unit(
    owner: DbTask,
    *,
    merge_units_by_member: Mapping[str, MergeUnit],
) -> MergeUnit | None:
    if owner.id is not None and owner.id in merge_units_by_member:
        return merge_units_by_member[owner.id]
    units = {
        unit.id: unit
        for unit in merge_units_by_member.values()
    }
    if not units:
        return None
    return max(units.values(), key=lambda unit: (unit.updated_at, unit.id))


def query_lineage_owner_rows(
    store: SqliteTaskStore,
    query: LineageOwnerQuery,
    *,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
) -> tuple[LineageOwnerRow, ...]:
    from .cli.advance_engine import determine_next_action, failed_recovery_decision_to_attention_action
    from .query import is_lineage_complete
    from .recovery_engine import (
        decide_failed_task_recovery,
        get_completed_recovery_descendant,
        list_failed_tasks_for_recovery,
    )
    from .task_query import task_matches_tag_filters

    indexes = _load_indexes(store)
    owner_ids_filter = set(query.owner_task_ids) if query.owner_task_ids is not None else None
    task_ids_filter = set(query.task_ids) if query.task_ids is not None else None
    visible_failed_tasks = [task for task in list_failed_tasks_for_recovery(store) if task.id is not None]
    visible_failed_ids = {task.id for task in visible_failed_tasks if task.id is not None}
    visible_failed_order = {
        task.id: index
        for index, task in enumerate(visible_failed_tasks)
        if task.id is not None
    }

    rows: list[LineageOwnerRow] = []
    for owner_id, owner_members in indexes.members_by_owner_id.items():
        owner = indexes.task_by_id.get(owner_id)
        if owner is None:
            continue
        if owner_ids_filter is not None and owner_id not in owner_ids_filter:
            if task_ids_filter is None or not any(task.id in task_ids_filter for task in owner_members if task.id is not None):
                continue
        root = indexes.root_by_task_id.get(owner.id or "", owner)
        merge_units_by_member = {
            task.id: indexes.merge_units_by_task_id[task.id]
            for task in owner_members
            if task.id is not None and task.id in indexes.merge_units_by_task_id
        }
        failed_leaves: list[DbTask] = []
        recovery_completed_by_failed_id: dict[str, DbTask] = {}
        unresolved_tasks: list[DbTask] = []

        merged_owner_branch = any(
            (merge_units_by_member.get(task.id or "") is not None and merge_units_by_member[task.id or ""].state == "merged")
            or task.merge_status == "merged"
            for task in owner_members
        )

        for task in sorted(owner_members, key=lambda item: (_task_event_time(item), task_id_numeric_key(item.id))):
            if task.id is None:
                continue
            if task.status not in {"failed", "completed", "unmerged", "dropped"}:
                continue
            matches = _matches_task_filters(
                task,
                query,
                tag_matcher=task_matches_tag_filters,
            )
            if task.task_type in {"plan", "explore"} and task.status == "completed" and task.id not in indexes.impl_based_on_ids:
                if matches:
                    unresolved_tasks.append(task)
                continue
            if task.status == "failed":
                if _has_completed_same_type_descendant(indexes, task):
                    continue
                if _has_merged_descendant(indexes, task, merge_units_by_member=merge_units_by_member):
                    continue
                completed_recovery = get_completed_recovery_descendant(store, task)
                if completed_recovery is not None:
                    recovery_completed_by_failed_id[task.id] = completed_recovery
                    continue
                if merged_owner_branch and task.task_type in {"review", "improve", "rebase"}:
                    continue
                if task.id not in visible_failed_ids:
                    continue
                failed_leaves.append(task)
                if matches:
                    unresolved_tasks.append(task)
                continue
            if merged_owner_branch:
                continue
            merge_unit = merge_units_by_member.get(task.id)
            explicit_merge_state = merge_unit.state if merge_unit is not None else task.merge_status
            if task.status in {"completed", "unmerged"} and explicit_merge_state == "unmerged":
                if matches:
                    unresolved_tasks.append(task)
                continue
            if is_lineage_complete(task, store=store):
                continue
            if matches:
                unresolved_tasks.append(task)

        snapshot = LineageOwnerSnapshot(
            owner_task=owner,
            root_task=root,
            members=tuple(owner_members),
            merge_units_by_task_id=merge_units_by_member,
            failed_leaves=tuple(failed_leaves),
            recovery_completed_by_failed_id=recovery_completed_by_failed_id,
        )
        owner_merge_unit = _resolve_owner_merge_unit(owner, merge_units_by_member=merge_units_by_member)
        if target_branch and owner_merge_unit is not None and owner_merge_unit.target_branch != target_branch:
            continue
        if not unresolved_tasks:
            continue
        resolution = is_lineage_resolved(snapshot)
        has_unimplemented_source = (
            owner.id is not None
            and owner.task_type in {"plan", "explore"}
            and owner.status == "completed"
            and owner.id not in indexes.impl_based_on_ids
        )
        resolved_in_query = any(
            reason == "recovery_chain_completed"
            for reason in resolution.reasons
        ) or (
            "branch_merged" in resolution.reasons
            and not failed_leaves
        ) or (
            "lineage_complete" in resolution.reasons
            and not has_unimplemented_source
            and not unresolved_tasks
        )
        if resolved_in_query:
            continue

        action_task = _select_representative_completed_task(
            store,
            snapshot,
            unresolved_tasks,
            impl_based_on_ids=indexes.impl_based_on_ids,
        )
        recovery_leaf_task: DbTask | None = None
        max_recovery_attempts = query.max_recovery_attempts if query.max_recovery_attempts is not None else 1
        failed_action_candidate: DbTask | None = None
        for failed_task in sorted(
            failed_leaves,
            key=lambda task: (
                visible_failed_order.get(task.id or "", len(visible_failed_order)),
                _task_event_time(task),
                task_id_numeric_key(task.id),
            ),
        ):
            decision = decide_failed_task_recovery(
                store,
                failed_task,
                max_recovery_attempts=max_recovery_attempts,
            )
            attention_action = failed_recovery_decision_to_attention_action(
                store,
                failed_task,
                decision,
                max_recovery_attempts=max_recovery_attempts,
            )
            if decision.reason_code == "recovery_has_newer_unresolved_descendant":
                continue
            if failed_task.task_type == "improve" and action_task is not None:
                continue
            if decision.action != "skip" or attention_action is not None:
                failed_action_candidate = failed_task
                break
        if failed_action_candidate is not None:
            action_task = failed_action_candidate
            recovery_leaf_task = failed_action_candidate
        elif action_task is None and failed_leaves:
            recovery_leaf_task = max(
                failed_leaves,
                key=lambda task: (_task_event_time(task), task_id_numeric_key(task.id)),
            )
            action_task = recovery_leaf_task
        elif action_task is not None and action_task.status == "failed":
            recovery_leaf_task = action_task
        if action_task is None:
            continue

        action: dict[str, Any] | None = None
        if config is not None and git is not None and target_branch:
            action = determine_next_action(
                config,
                store,
                git,
                action_task,
                target_branch,
                impl_based_on_ids=indexes.impl_based_on_ids,
                max_resume_attempts=query.max_recovery_attempts,
            )
        lineage_status = _classify_lineage_status(action) if action is not None else "actionable"
        if not query.include_skipped and lineage_status == "skipped":
            continue

        tree, rendered_members = _build_owner_tree(
            store,
            root_task=root,
            owner_task=owner,
            unresolved_tasks=tuple(unresolved_tasks),
        )
        summaries = tuple(
            UnresolvedLeafSummary(
                task_id=task.id or "unknown",
                status=task.status,
                task_type=task.task_type,
                reason=task.failure_reason or task.completion_reason,
            )
            for task in unresolved_tasks
            if task.id is not None
        )
        rows.append(
            LineageOwnerRow(
                owner_task=owner,
                members=rendered_members,
                tree=tree,
                lineage_status=lineage_status,
                next_action=action,
                next_action_reason=str(action.get("description", "")) if action is not None else "",
                unresolved_tasks=tuple(unresolved_tasks),
                unresolved_leaf_summary=summaries,
                action_task=action_task,
                recovery_leaf_task=recovery_leaf_task,
            )
        )

    rows.sort(
        key=lambda row: (
            max((_task_event_time(task) for task in row.unresolved_tasks), default=_task_event_time(row.owner_task)),
            task_id_numeric_key(row.owner_task.id),
        ),
        reverse=True,
    )
    if query.limit is not None:
        rows = rows[: query.limit]
    return tuple(rows)


__all__ = [
    "LineageOwnerQuery",
    "LineageOwnerRow",
    "LineageOwnerSnapshot",
    "LineageResolution",
    "UnresolvedLeafSummary",
    "is_lineage_resolved",
    "query_lineage_owner_rows",
]
