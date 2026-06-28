"""Owner-keyed lineage query helpers shared by incomplete/advance/watch."""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from .db import MergeUnit, SqliteTaskStore, Task as DbTask, merge_unit_is_active, task_id_numeric_key
from .git import Git, prime_advance_planning_refs
from .lifecycle_completion import (
    merge_state_is_terminal_for_lifecycle,
    task_is_complete_for_lifecycle,
)
from .main_integration_verify import MAIN_INTEGRATION_VERIFY_REASON, current_main_integration_verify_alert
from .merge_state import classify_branch_merge_state_for_target
from .metrics import instrument_module_functions
from .operator_state import blocked_by_empty_prereq_label, effective_no_work_merge_state
from .recovery_read_context import RecoveryReadContext
from .source_followup import (
    IMPLEMENTATION_SOURCE_TASK_TYPES,
    SourceFollowupState,
    collect_non_dropped_implement_source_ids,
    held_plan_has_blocked_awaiting_review_dependents,
    resolve_source_followup_state,
    source_task_needs_implementation_followup,
)
from .watch_progress import (
    build_watch_progress_candidate,
    get_active_watch_no_progress_attention,
)

if TYPE_CHECKING:
    from .config import Config
    from .task_query import DateFilter


LineageStatus = Literal["resolved", "actionable", "needs_attention", "waiting", "skipped"]
ResolutionReason = Literal["lineage_complete", "branch_merged", "recovery_chain_completed"]
_LOG = logging.getLogger(__name__)


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
    lifecycle_action_task: DbTask | None = None
    recovery_action_task: DbTask | None = None
    recovery_leaf_task: DbTask | None = None


@dataclass(frozen=True)
class StaleUnmergedSweepCandidate:
    owner_task: DbTask
    merge_unit: MergeUnit
    drop_task_ids: tuple[str, ...]
    member_task_ids: tuple[str, ...]
    last_activity_at: datetime
    stale_days: int


@dataclass(frozen=True)
class LineageOwnerQuery:
    limit: int | None = None
    statuses: tuple[str, ...] | None = None
    exclude_statuses: tuple[str, ...] | None = None
    merge_chain_state: tuple[str, ...] | None = None
    exclude_merge_chain_state: tuple[str, ...] | None = None
    task_types: tuple[str, ...] | None = None
    exclude_task_types: tuple[str, ...] | None = None
    tags: tuple[str, ...] | None = None
    exclude_tags: tuple[str, ...] | None = None
    any_tag: bool = True
    date_filter: DateFilter | None = None
    include_skipped: bool = False
    exclude_dropped_from_planning: bool = False
    max_recovery_attempts: int | None = None
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
    skipped_same_branch_members_by_root_id: dict[str, list[DbTask]]
    merge_units_by_task_id: dict[str, MergeUnit]
    historical_merge_units_by_task_id: dict[str, tuple[MergeUnit, ...]]
    impl_based_on_ids: set[str]
    non_dropped_impl_source_ids: set[str]


def _normalize_dt(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _main_integration_alert_matches_query(query: LineageOwnerQuery) -> bool:
    if query.task_types is not None and "internal" not in query.task_types:
        return False
    if query.exclude_task_types is not None and "internal" in query.exclude_task_types:
        return False
    return True


def _iter_task_activity_timestamps(task: DbTask) -> tuple[datetime | None, ...]:
    return (
        task.created_at,
        task.started_at,
        task.completed_at,
        task.pr_last_synced_at,
        task.sync_last_synced_at,
        task.review_cleared_at,
    )


def _iter_merge_unit_activity_timestamps(unit: MergeUnit) -> tuple[datetime | None, ...]:
    return (
        unit.created_at,
        unit.updated_at,
        unit.pr_last_synced_at,
        unit.sync_last_synced_at,
    )


def _task_event_time(task: DbTask) -> datetime:
    return _normalize_dt(task.completed_at or task.created_at)


def _indexed_child_created_order_key(task: DbTask) -> tuple[datetime, int]:
    return (_normalize_dt(task.created_at), task_id_numeric_key(task.id))


def _actionable_lifecycle_tasks(
    unresolved_tasks: Sequence[DbTask],
    *,
    include_dropped: bool,
) -> list[DbTask]:
    allowed_statuses = {"completed", "unmerged"}
    if include_dropped:
        allowed_statuses.add("dropped")
    return [task for task in unresolved_tasks if task.status in allowed_statuses]


def _task_has_only_inactive_historical_merge_units(
    task: DbTask,
    *,
    merge_units_by_task_id: Mapping[str, MergeUnit],
    historical_merge_units_by_task_id: Mapping[str, Sequence[MergeUnit]],
) -> bool:
    if task.id is None or task.id in merge_units_by_task_id:
        return False
    units = historical_merge_units_by_task_id.get(task.id, ())
    if not units:
        return False
    has_inactive_historical_attachment = False
    for unit in units:
        if merge_unit_is_active(unit):
            return False
        has_inactive_historical_attachment = True
    return has_inactive_historical_attachment


def _task_is_excluded_from_dropped_planning(
    task: DbTask,
    *,
    merge_units_by_task_id: Mapping[str, MergeUnit],
    historical_merge_units_by_task_id: Mapping[str, Sequence[MergeUnit]],
) -> bool:
    if task.status == "dropped":
        return True
    return _task_has_only_inactive_historical_merge_units(
        task,
        merge_units_by_task_id=merge_units_by_task_id,
        historical_merge_units_by_task_id=historical_merge_units_by_task_id,
    )


def _task_is_terminal_for_incomplete_display(
    task: DbTask,
    *,
    merge_units_by_task_id: Mapping[str, MergeUnit],
    exclude_dropped: bool,
) -> bool:
    if exclude_dropped and task.status == "dropped":
        return True
    if task.id is not None and _task_is_effectively_merged(task, merge_units_by_task_id=merge_units_by_task_id):
        return True
    return False


def filter_display_unresolved_tasks_for_incomplete(
    unresolved_tasks: Sequence[DbTask],
    *,
    merge_units_by_task_id: Mapping[str, MergeUnit],
    exclude_dropped: bool,
) -> tuple[DbTask, ...]:
    return tuple(
        task
        for task in unresolved_tasks
        if not _task_is_terminal_for_incomplete_display(
            task,
            merge_units_by_task_id=merge_units_by_task_id,
            exclude_dropped=exclude_dropped,
        )
    )


def _canonical_impl_branch_candidates(owner: DbTask, actionable_tasks: Sequence[DbTask]) -> list[DbTask]:
    if owner.task_type != "implement" or not owner.branch:
        return []
    return [task for task in actionable_tasks if task.branch == owner.branch]


def _matches_task_filters(
    task: DbTask,
    query: LineageOwnerQuery,
    *,
    tag_matcher: Any,
    include_tag_filters: bool = True,
    merge_unit: MergeUnit | None = None,
) -> bool:
    if query.statuses is not None:
        allowed_statuses = set(query.statuses)
        if not _matches_status_filters(task, allowed_statuses, merge_unit=merge_unit):
            return False
    if query.exclude_statuses is not None:
        disallowed_statuses = set(query.exclude_statuses)
        if _matches_status_filters(task, disallowed_statuses, merge_unit=merge_unit):
            return False
    if query.merge_chain_state is not None:
        merge_states = set(query.merge_chain_state)
        if not _matches_merge_chain_state(task, merge_states, merge_unit=merge_unit):
            return False
    if query.exclude_merge_chain_state is not None:
        excluded_merge_states = set(query.exclude_merge_chain_state)
        if _matches_merge_chain_state(task, excluded_merge_states, merge_unit=merge_unit):
            return False
    if query.task_types is not None and task.task_type not in set(query.task_types):
        return False
    if query.exclude_task_types is not None and task.task_type in set(query.exclude_task_types):
        return False
    if (
        include_tag_filters
        and query.tags is not None
        and not tag_matcher(task_tags=task.tags, tag_filters=query.tags, any_tag=query.any_tag)
    ):
        return False
    if (
        include_tag_filters
        and query.exclude_tags is not None
        and tag_matcher(task_tags=task.tags, tag_filters=query.exclude_tags, any_tag=query.any_tag)
    ):
        return False
    if query.date_filter is not None:
        if not _matches_date_filter(task, query.date_filter):
            return False
    if query.task_ids is not None and task.id not in set(query.task_ids):
        return False
    return True


def _owner_matches_tag_filters(owner: DbTask, query: LineageOwnerQuery, *, tag_matcher: Any) -> bool:
    if query.tags is not None and not tag_matcher(task_tags=owner.tags, tag_filters=query.tags, any_tag=query.any_tag):
        return False
    if query.exclude_tags is not None and tag_matcher(
        task_tags=owner.tags,
        tag_filters=query.exclude_tags,
        any_tag=query.any_tag,
    ):
        return False
    return True


def _effective_merge_state(task: DbTask, *, merge_unit: MergeUnit | None) -> str | None:
    if merge_unit is not None:
        return effective_no_work_merge_state(task, merge_unit.state)
    return task.merge_status


def _task_effective_merge_state(
    task: DbTask,
    *,
    merge_units_by_task_id: Mapping[str, MergeUnit],
) -> str | None:
    return _effective_merge_state(task, merge_unit=merge_units_by_task_id.get(task.id or ""))


def _task_is_effectively_merged(
    task: DbTask,
    *,
    merge_units_by_task_id: Mapping[str, MergeUnit],
) -> bool:
    return merge_state_is_terminal_for_lifecycle(
        _task_effective_merge_state(task, merge_units_by_task_id=merge_units_by_task_id)
    )


def _matches_status_filters(
    task: DbTask,
    statuses: set[str],
    *,
    merge_unit: MergeUnit | None,
) -> bool:
    if "unmerged" in statuses and _matches_merge_chain_state(task, {"unmerged"}, merge_unit=merge_unit):
        return True
    return task.status in statuses


def _matches_merge_chain_state(
    task: DbTask,
    merge_states: set[str],
    *,
    merge_unit: MergeUnit | None,
) -> bool:
    merge_state = _effective_merge_state(task, merge_unit=merge_unit)
    if "merged" in merge_states and merge_state == "merged":
        return True
    if "unmerged" in merge_states and (
        merge_state == "unmerged" or (task.status == "unmerged" and merge_unit is None and merge_state != "merged")
    ):
        return True
    if "empty" in merge_states and merge_state == "empty":
        return True
    if "redundant" in merge_states and merge_state == "redundant":
        return True
    if (
        "needs_merge" in merge_states
        and task.status == "completed"
        and task.has_commits
        and merge_state not in {"merged", "empty", "redundant"}
    ):
        return True
    return False


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
    for children in based_on_children.values():
        children.sort(key=_indexed_child_created_order_key)
    for children in depends_on_children.values():
        children.sort(key=_indexed_child_created_order_key)

    merge_units_by_task_id: dict[str, MergeUnit] = {}
    historical_merge_units_by_task_id: dict[str, tuple[MergeUnit, ...]] = {}
    if store.supports_merge_units():
        for task in tasks:
            if task.id is None:
                continue
            units = tuple(store.list_merge_units_for_task(task.id))
            if not units:
                continue
            historical_merge_units_by_task_id[task.id] = units
            active_unit = next((unit for unit in units if merge_unit_is_active(unit)), None)
            if active_unit is not None:
                merge_units_by_task_id[task.id] = active_unit

    recovery_read_context = RecoveryReadContext(
        tasks=tasks,
        task_by_id=task_by_id,
        based_on_children=based_on_children,
        depends_on_children=depends_on_children,
        merge_units_by_task_id=merge_units_by_task_id,
        historical_merge_units_by_task_id=historical_merge_units_by_task_id,
        allow_reconcile_mutation=False,
    )

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
    skipped_same_branch_members_by_root_id: dict[str, list[DbTask]] = defaultdict(list)

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
            task.status == "failed"
            and (
                recovery_root_id := get_recovery_chain_root_task_id(
                    store,
                    task,
                    read_context=recovery_read_context,
                )
            )
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
        if _is_broken_same_branch_owner(owner=owner, root=root) and root.id is not None:
            skipped_same_branch_members_by_root_id[root.id].append(task)

    return _LineageIndexes(
        tasks=tasks,
        task_by_id=task_by_id,
        based_on_children=based_on_children,
        depends_on_children=depends_on_children,
        root_by_task_id=root_by_task_id,
        owner_by_task_id=owner_by_task_id,
        members_by_owner_id=members_by_owner_id,
        skipped_same_branch_members_by_root_id=skipped_same_branch_members_by_root_id,
        merge_units_by_task_id=merge_units_by_task_id,
        historical_merge_units_by_task_id=historical_merge_units_by_task_id,
        impl_based_on_ids=store.get_impl_based_on_ids(),
        non_dropped_impl_source_ids=collect_non_dropped_implement_source_ids(tasks),
    )


def is_lineage_resolved(snapshot: LineageOwnerSnapshot) -> LineageResolution:
    reasons: list[ResolutionReason] = []
    resolved_by_ids: list[str] = []

    merged_member_ids = [
        task.id
        for task in snapshot.members
        if task.id is not None
        and _task_is_effectively_merged(task, merge_units_by_task_id=snapshot.merge_units_by_task_id)
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
        unresolved_nonfailed = [task for task in snapshot.members if not _snapshot_task_is_complete(snapshot, task)]
        if not unresolved_nonfailed:
            reasons.append("lineage_complete")

    return LineageResolution(
        resolved=bool(reasons),
        reasons=tuple(reasons),
        resolved_by_task_ids=tuple(dict.fromkeys(task_id for task_id in resolved_by_ids if task_id is not None)),
    )


def _snapshot_task_is_complete(snapshot: LineageOwnerSnapshot, task: DbTask) -> bool:
    merge_state = _task_effective_merge_state(task, merge_units_by_task_id=snapshot.merge_units_by_task_id)
    return task_is_complete_for_lifecycle(task, merge_state=merge_state)


def _build_owner_tree(
    *,
    root_task: DbTask,
    owner_task: DbTask,
    unresolved_tasks: Sequence[DbTask],
    based_on_children: Mapping[str, list[DbTask]],
    depends_on_children: Mapping[str, list[DbTask]],
    drop_excluded_task_ids: frozenset[str] = frozenset(),
) -> tuple[Any, tuple[DbTask, ...]]:
    from .query import build_lineage_tree_from_index, flatten_lineage_tree

    root_tree = build_lineage_tree_from_index(
        root_task,
        based_on_children=based_on_children,
        depends_on_children=depends_on_children,
        max_depth=None,
    )
    keep_ids = {task.id for task in unresolved_tasks if task.id is not None}
    if owner_task.id is not None:
        keep_ids.add(owner_task.id)
    if root_task.id is not None and root_task.id not in drop_excluded_task_ids:
        keep_ids.add(root_task.id)

    def _filter(node: Any, is_root: bool) -> Any | None:
        kept_children: list[Any] = []
        for child in node.children:
            filtered = _filter(child, False)
            if filtered is not None:
                kept_children.append(filtered)
        task_id = node.task.id
        should_keep = (task_id is not None and task_id in keep_ids) or bool(kept_children)
        if task_id is not None and task_id in drop_excluded_task_ids and task_id not in keep_ids:
            should_keep = bool(kept_children)
        elif is_root:
            should_keep = True
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
    while (
        filtered.task.id is not None
        and filtered.task.id in drop_excluded_task_ids
        and filtered.task.id not in keep_ids
        and len(filtered.children) == 1
    ):
        filtered = filtered.children[0]
    return filtered, tuple(flatten_lineage_tree(filtered))


def _select_representative_completed_task(
    store: SqliteTaskStore,
    snapshot: LineageOwnerSnapshot,
    unresolved_tasks: Sequence[DbTask],
    *,
    include_dropped: bool,
) -> DbTask | None:
    owner = snapshot.owner_task
    if owner.task_type in IMPLEMENTATION_SOURCE_TASK_TYPES and owner.status == "completed" and owner in unresolved_tasks:
        return owner
    actionable = _actionable_lifecycle_tasks(unresolved_tasks, include_dropped=include_dropped)
    if not actionable:
        return None
    owner_unit = snapshot.merge_units_by_task_id.get(owner.id or "")
    if owner.task_type == "implement" and owner.branch and owner_unit is not None:
        rep = store.resolve_merge_unit_representative_task(
            owner_unit,
            preferred_task_id=owner.id,
            require_actionable=True,
        )
        if rep is not None and rep.branch == owner.branch:
            return rep
    canonical_branch_tasks = _canonical_impl_branch_candidates(owner, actionable)
    if canonical_branch_tasks:
        return max(canonical_branch_tasks, key=lambda task: (_task_event_time(task), task_id_numeric_key(task.id)))
    if owner.task_type == "implement" and owner.branch and any(task.branch for task in actionable):
        return None
    if owner_unit is not None:
        rep = store.resolve_merge_unit_representative_task(
            owner_unit,
            preferred_task_id=owner.id,
            require_actionable=True,
        )
        if rep is not None:
            return rep
    return max(actionable, key=lambda task: (_task_event_time(task), task_id_numeric_key(task.id)))


def _requires_impl_branch_manual_resolution(
    owner: DbTask,
    actionable_tasks: Sequence[DbTask],
    orphaned_same_branch_tasks: Sequence[DbTask],
    *,
    include_dropped: bool,
) -> bool:
    actionable = _actionable_lifecycle_tasks(actionable_tasks, include_dropped=include_dropped)
    if not actionable:
        actionable = _actionable_lifecycle_tasks(orphaned_same_branch_tasks, include_dropped=include_dropped)
        if not actionable:
            return False
    if _canonical_impl_branch_candidates(owner, actionable):
        return False
    if owner.task_type != "implement" or not owner.branch:
        return False
    return any(task.branch for task in actionable)


def _is_orphan_same_branch_task(*, task: DbTask, root: DbTask) -> bool:
    if task.id is None or root.id is None or task.id == root.id:
        return False
    if root.task_type != "implement":
        return False
    if not task.same_branch or not task.branch or not root.branch:
        return False
    return task.branch != root.branch


def _is_broken_same_branch_owner(*, owner: DbTask, root: DbTask) -> bool:
    return _is_orphan_same_branch_task(task=owner, root=root)


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


def _source_followup_state(
    indexes: _LineageIndexes,
    task: DbTask,
    cache: dict[str, SourceFollowupState],
) -> SourceFollowupState:
    if task.id is None:
        return resolve_source_followup_state(task, get_children=lambda _task_id: ())
    cached = cache.get(task.id)
    if cached is not None:
        return cached
    resolved = resolve_source_followup_state(
        task,
        get_children=lambda task_id: indexes.based_on_children.get(task_id, ()),
    )
    cache[task.id] = resolved
    return resolved


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
        if merge_state_is_terminal_for_lifecycle(merge_state):
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


def _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
    *,
    failed_task: DbTask,
    owner_merge_unit: MergeUnit | None,
    leaf_merge_unit: MergeUnit | None,
    git: Git | None,
) -> bool:
    if owner_merge_unit is None:
        return failed_task.task_type not in {"review", "improve", "rebase"}
    if not merge_state_is_terminal_for_lifecycle(owner_merge_unit.state):
        return True

    owner_target = owner_merge_unit.target_branch
    if not owner_target:
        return True

    leaf_targets_owner = (
        leaf_merge_unit is not None and leaf_merge_unit.target_branch == owner_target
    )
    leaf_has_own_merge_unit = (
        leaf_merge_unit is not None and leaf_merge_unit.owner_task_id == failed_task.id
    )
    if failed_task.branch is None and not leaf_has_own_merge_unit:
        return False
    if leaf_targets_owner and leaf_has_own_merge_unit and leaf_merge_unit is not None:
        if leaf_merge_unit.state == "unmerged" and git is None:
            # Persisted unmerged state on the failed leaf's own merge unit is enough to keep
            # the work visible unless live merge truth later disproves it.
            return True
        if merge_state_is_terminal_for_lifecycle(leaf_merge_unit.state) and git is None:
            return False

    source_branch = failed_task.branch
    if source_branch is None and leaf_targets_owner and leaf_merge_unit is not None:
        source_branch = leaf_merge_unit.source_branch
    if not source_branch:
        return (
            True
            if leaf_targets_owner
            and leaf_has_own_merge_unit
            and leaf_merge_unit is not None
            and leaf_merge_unit.state == "unmerged"
            else False
        )

    if git is None:
        return True

    try:
        classification = classify_branch_merge_state_for_target(
            git=git,
            source_branch=source_branch,
            target_branch=owner_target,
            persisted_state=None,
            merged_proof=None,
            source_has_commits=failed_task.has_commits,
            on_warning=_LOG.warning,
        )
    except Exception as exc:
        _LOG.warning(
            "Could not prove failed leaf %s merge state against terminal owner target %s: %s",
            failed_task.id,
            owner_target,
            exc,
        )
        return True

    if merge_state_is_terminal_for_lifecycle(classification.state):
        return False
    if classification.state == "unmerged":
        return True
    if leaf_targets_owner and leaf_has_own_merge_unit and leaf_merge_unit is not None:
        if leaf_merge_unit.state == "unmerged":
            return True
        if merge_state_is_terminal_for_lifecycle(leaf_merge_unit.state):
            return False
    return True


def resolve_lineage_owner_task_id(store: SqliteTaskStore, task_id: str) -> str | None:
    """Return the lineage owner id for a task without planning every owner row."""
    owner = _load_indexes(store).owner_by_task_id.get(task_id)
    return owner.id if owner is not None else None


def _latest_lineage_activity_at(
    *,
    tasks: Sequence[DbTask],
    merge_unit: MergeUnit,
    observations: Sequence[Any] = (),
) -> datetime:
    latest = datetime.min.replace(tzinfo=UTC)
    for timestamp in _iter_merge_unit_activity_timestamps(merge_unit):
        if timestamp is not None and timestamp > latest:
            latest = timestamp
    for task in tasks:
        for timestamp in _iter_task_activity_timestamps(task):
            if timestamp is not None and timestamp > latest:
                latest = timestamp
    for observation in observations:
        observed_at = getattr(observation, "observed_at", None)
        if observed_at is not None and observed_at > latest:
            latest = observed_at
    return latest


def _owner_has_external_dependency_links(
    owner_id: str,
    *,
    member_task_ids: set[str],
    indexes: _LineageIndexes,
    live_owner_ids: frozenset[str],
) -> bool:
    for task_id in member_task_ids:
        task = indexes.task_by_id.get(task_id)
        if task is None:
            continue
        if task.depends_on is not None and task.depends_on not in member_task_ids:
            dep_owner = indexes.owner_by_task_id.get(task.depends_on)
            if dep_owner is not None and dep_owner.id != owner_id and dep_owner.id in live_owner_ids:
                return True
        for dependent in indexes.depends_on_children.get(task_id, ()):
            dependent_id = dependent.id
            if dependent_id is None or dependent_id in member_task_ids:
                continue
            dependent_owner = indexes.owner_by_task_id.get(dependent_id)
            if (
                dependent_owner is not None
                and dependent_owner.id != owner_id
                and dependent_owner.id in live_owner_ids
            ):
                return True
    return False


def _collect_live_owner_ids_for_stale_dependency_links(
    store: SqliteTaskStore,
    *,
    indexes: _LineageIndexes,
) -> frozenset[str]:
    """Return unresolved owner ids whose lineages still represent live external work."""
    with store.read_session():
        rows, _read_context = _query_lineage_owner_rows_with_context(
            store,
            LineageOwnerQuery(limit=None, exclude_dropped_from_planning=True),
            persist_post_merge_rebase_state=False,
            persist_review_clearance=False,
        )
    live_owner_ids = {
        row.owner_task.id
        for row in rows
        if row.owner_task.id is not None
    }
    for owner_id, members in indexes.members_by_owner_id.items():
        if any(task.status in {"pending", "in_progress"} for task in members):
            live_owner_ids.add(owner_id)
    return frozenset(live_owner_ids)


def collect_stale_unmerged_sweep_candidates(
    store: SqliteTaskStore,
    *,
    threshold_days: int,
    now: datetime | None = None,
) -> tuple[StaleUnmergedSweepCandidate, ...]:
    """Return conservative stale unmerged merge units that are safe to drop."""
    current_time = now or datetime.now(UTC)
    cutoff = current_time - timedelta(days=threshold_days)
    indexes = _load_indexes(store)
    live_owner_ids = _collect_live_owner_ids_for_stale_dependency_links(store, indexes=indexes)
    candidates: list[StaleUnmergedSweepCandidate] = []

    for owner_id, owner, owner_members, _root in _candidate_owner_rows(
        indexes,
        LineageOwnerQuery(limit=None, exclude_dropped_from_planning=True),
        owner_ids_filter=None,
        task_ids_filter=None,
    ):
        merge_units_by_member = {
            task.id: indexes.merge_units_by_task_id[task.id]
            for task in owner_members
            if task.id is not None and task.id in indexes.merge_units_by_task_id
        }
        merge_unit = _resolve_owner_merge_unit(owner, merge_units_by_member=merge_units_by_member)
        if merge_unit is None or merge_unit.state not in {"unmerged", "blocked", "stale"}:
            continue
        member_task_ids = {
            task.id
            for task in store.list_tasks_for_merge_unit(merge_unit.id)
            if task.id is not None
        }
        if not member_task_ids:
            continue
        merge_unit_members = tuple(
            indexes.task_by_id[task_id]
            for task_id in member_task_ids
            if task_id in indexes.task_by_id
        )
        if not merge_unit_members:
            continue
        if any(task.status in {"pending", "in_progress"} for task in merge_unit_members):
            continue
        if _owner_has_external_dependency_links(
            owner_id,
            member_task_ids=member_task_ids,
            indexes=indexes,
            live_owner_ids=live_owner_ids,
        ):
            continue
        observations = (
            store.list_watch_progress_observations(subject_kind="merge_unit", subject_id=merge_unit.id)
            if store.supports_watch_progress_observations()
            else ()
        )
        last_activity_at = _latest_lineage_activity_at(
            tasks=merge_unit_members,
            merge_unit=merge_unit,
            observations=observations,
        )
        if last_activity_at > cutoff:
            continue
        drop_task_ids = tuple(
            task.id
            for task in sorted(
                merge_unit_members,
                key=lambda item: (_task_event_time(item), task_id_numeric_key(item.id)),
            )
            if task.id is not None and task.status != "dropped"
        )
        if not drop_task_ids:
            continue
        candidates.append(
            StaleUnmergedSweepCandidate(
                owner_task=owner,
                merge_unit=merge_unit,
                drop_task_ids=drop_task_ids,
                member_task_ids=tuple(sorted(member_task_ids, key=lambda task_id: task_id_numeric_key(task_id))),
                last_activity_at=last_activity_at,
                stale_days=max(0, (current_time - last_activity_at).days),
            )
        )

    candidates.sort(
        key=lambda candidate: (
            candidate.last_activity_at,
            task_id_numeric_key(candidate.owner_task.id),
        )
    )
    return tuple(candidates)


def _candidate_owner_rows(
    indexes: _LineageIndexes,
    query: LineageOwnerQuery,
    *,
    owner_ids_filter: set[str] | None,
    task_ids_filter: set[str] | None,
) -> tuple[tuple[str, DbTask, tuple[DbTask, ...], DbTask], ...]:
    candidates: list[tuple[str, DbTask, tuple[DbTask, ...], DbTask]] = []
    for owner_id, owner_members in indexes.members_by_owner_id.items():
        owner = indexes.task_by_id.get(owner_id)
        if owner is None:
            continue
        if query.exclude_dropped_from_planning and _task_is_excluded_from_dropped_planning(
            owner,
            merge_units_by_task_id=indexes.merge_units_by_task_id,
            historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
        ):
            continue
        member_matches_task_filter = False
        if task_ids_filter is not None:
            member_matches_task_filter = any(
                task.id in task_ids_filter
                and not (
                    query.exclude_dropped_from_planning
                    and _task_is_excluded_from_dropped_planning(
                        task,
                        merge_units_by_task_id=indexes.merge_units_by_task_id,
                        historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
                    )
                )
                for task in owner_members
                if task.id is not None
            )
            if not member_matches_task_filter:
                member_matches_task_filter = any(
                    task.id in task_ids_filter
                    and not (
                        query.exclude_dropped_from_planning
                        and _task_is_excluded_from_dropped_planning(
                            task,
                            merge_units_by_task_id=indexes.merge_units_by_task_id,
                            historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
                        )
                    )
                    for task in indexes.skipped_same_branch_members_by_root_id.get(owner_id, ())
                    if task.id is not None
                )
        if owner_ids_filter is not None:
            if owner_id not in owner_ids_filter:
                continue
        if task_ids_filter is not None and not member_matches_task_filter:
            continue
        root = indexes.root_by_task_id.get(owner.id or "", owner)
        if _is_broken_same_branch_owner(owner=owner, root=root):
            continue
        candidates.append((owner_id, owner, tuple(owner_members), root))
    return tuple(candidates)


def query_lineage_owner_rows(
    store: SqliteTaskStore,
    query: LineageOwnerQuery,
    *,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
    persist_post_merge_rebase_state: bool = True,
    persist_review_clearance: bool = True,
    reuse_recovery_merge_context: bool = False,
) -> tuple[LineageOwnerRow, ...]:
    rows, read_context = _query_lineage_owner_rows_with_context(
        store,
        query,
        config=config,
        git=git,
        target_branch=target_branch,
        persist_post_merge_rebase_state=persist_post_merge_rebase_state,
        persist_review_clearance=persist_review_clearance,
        reuse_recovery_merge_context=reuse_recovery_merge_context,
    )
    if not read_context.allow_reconcile_mutation:
        _record_pending_recovery_reconciliation_context(store, read_context)
    return rows


def _record_pending_recovery_reconciliation_context(
    store: SqliteTaskStore,
    read_context: RecoveryReadContext,
) -> None:
    pending = getattr(store, "_lineage_query_pending_reconciliation_contexts", None)
    if pending is None:
        pending = []
        setattr(store, "_lineage_query_pending_reconciliation_contexts", pending)
    pending.append(read_context)


def apply_deferred_lineage_query_reconciliations(store: SqliteTaskStore) -> None:
    from .recovery_engine import apply_pending_recovery_reconciliations

    pending = getattr(store, "_lineage_query_pending_reconciliation_contexts", None)
    if not pending:
        return
    setattr(store, "_lineage_query_pending_reconciliation_contexts", [])
    for read_context in pending:
        apply_pending_recovery_reconciliations(store, read_context=read_context)


def query_lineage_owner_rows_in_read_session(
    store: SqliteTaskStore,
    query: LineageOwnerQuery,
    *,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
    persist_post_merge_rebase_state: bool = True,
    persist_review_clearance: bool = True,
    reuse_recovery_merge_context: bool = False,
) -> tuple[tuple[LineageOwnerRow, ...], RecoveryReadContext]:
    from .recovery_engine import apply_pending_recovery_reconciliations

    with store.read_session():
        rows, read_context = _query_lineage_owner_rows_with_context(
            store,
            query,
            config=config,
            git=git,
            target_branch=target_branch,
            persist_post_merge_rebase_state=persist_post_merge_rebase_state,
            persist_review_clearance=persist_review_clearance,
            reuse_recovery_merge_context=reuse_recovery_merge_context,
        )
    apply_pending_recovery_reconciliations(store, read_context=read_context)
    return rows, read_context

def _query_lineage_owner_rows_with_context(
    store: SqliteTaskStore,
    query: LineageOwnerQuery,
    *,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
    persist_post_merge_rebase_state: bool = True,
    persist_review_clearance: bool = True,
    reuse_recovery_merge_context: bool = False,
) -> tuple[tuple[LineageOwnerRow, ...], RecoveryReadContext]:
    from .cli.advance_engine import determine_next_action, failed_recovery_decision_to_attention_action
    from .query import is_lineage_complete
    from .recovery_engine import (
        apply_pending_recovery_reconciliations,
        build_merge_context_from_git,
        decide_failed_task_recovery,
        get_completed_recovery_descendant,
        get_completed_sibling_recovery,
        list_failed_tasks_for_recovery,
    )
    from .task_query import task_matches_tag_filters

    indexes = _load_indexes(store)
    read_context = RecoveryReadContext(
        tasks=indexes.tasks,
        task_by_id=indexes.task_by_id,
        based_on_children=indexes.based_on_children,
        depends_on_children=indexes.depends_on_children,
        root_by_task_id=indexes.root_by_task_id,
        merge_units_by_task_id=indexes.merge_units_by_task_id,
        historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
        allow_reconcile_mutation=store._read_session_depth == 0,
    )
    if isinstance(git, Git) and target_branch is not None:
        read_context.merge_context = build_merge_context_from_git(git, target_branch)
    owner_ids_filter = set(query.owner_task_ids) if query.owner_task_ids is not None else None
    task_ids_filter = set(query.task_ids) if query.task_ids is not None else None
    candidate_owner_rows = _candidate_owner_rows(
        indexes,
        query,
        owner_ids_filter=owner_ids_filter,
        task_ids_filter=task_ids_filter,
    )
    drop_excluded_task_ids = frozenset(
        task.id
        for task in indexes.tasks
        if task.id is not None
        and query.exclude_dropped_from_planning
        and _task_is_excluded_from_dropped_planning(
            task,
            merge_units_by_task_id=indexes.merge_units_by_task_id,
            historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
        )
    )
    visible_failed_tasks = [
        task for task in list_failed_tasks_for_recovery(store, read_context=read_context) if task.id is not None
    ]
    visible_failed_ids = {task.id for task in visible_failed_tasks if task.id is not None}
    visible_failed_order = {
        task.id: index
        for index, task in enumerate(visible_failed_tasks)
        if task.id is not None
    }
    source_followup_cache: dict[str, SourceFollowupState] = {}
    prime_advance_planning_refs(
        git,
        branch_names=(
            task.branch
            for _owner_id, _owner, owner_members, _root in candidate_owner_rows
            for task in owner_members
            if task.branch
        ),
        target_branch=target_branch,
        warning_logger=_LOG,
    )

    rows: list[LineageOwnerRow] = []
    for owner_id, owner, owner_members, root in candidate_owner_rows:
        merge_units_by_member = {
            task.id: indexes.merge_units_by_task_id[task.id]
            for task in owner_members
            if task.id is not None and task.id in indexes.merge_units_by_task_id
        }
        failed_leaves: list[DbTask] = []
        matching_failed_leaves: list[DbTask] = []
        recovery_completed_by_failed_id: dict[str, DbTask] = {}
        unresolved_tasks: list[DbTask] = []
        orphaned_same_branch_tasks: list[DbTask] = []
        skipped_same_branch_members = indexes.skipped_same_branch_members_by_root_id.get(owner_id, ())
        owner_merge_unit = _resolve_owner_merge_unit(owner, merge_units_by_member=merge_units_by_member)

        merged_owner_branch = any(
            _task_is_effectively_merged(task, merge_units_by_task_id=merge_units_by_member)
            for task in owner_members
        )

        for task in sorted(owner_members, key=lambda item: (_task_event_time(item), task_id_numeric_key(item.id))):
            if task.id is None:
                continue
            empty_prereq_block = blocked_by_empty_prereq_label(store, task, read_context=read_context)
            if task.status not in {"failed", "completed", "unmerged", "dropped"} and empty_prereq_block is None:
                continue
            if query.exclude_dropped_from_planning and _task_is_excluded_from_dropped_planning(
                task,
                merge_units_by_task_id=indexes.merge_units_by_task_id,
                historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
            ):
                continue
            merge_unit = merge_units_by_member.get(task.id)
            matches = _matches_task_filters(
                task,
                query,
                tag_matcher=task_matches_tag_filters,
                include_tag_filters=False,
                merge_unit=merge_unit,
            )
            if task.task_type in IMPLEMENTATION_SOURCE_TASK_TYPES and task.status == "completed":
                followup_state = _source_followup_state(indexes, task, source_followup_cache)
                needs_followup = source_task_needs_implementation_followup(
                    task,
                    followup_state,
                    non_dropped_implement_source_ids=indexes.non_dropped_impl_source_ids,
                )
                has_blocked_review_dependents = held_plan_has_blocked_awaiting_review_dependents(
                    task,
                    get_dependents=lambda task_id: indexes.depends_on_children.get(task_id, ()),
                    get_dependency_readiness=lambda dependent: store.get_dependency_readiness(dependent),
                )
                if not needs_followup and not has_blocked_review_dependents:
                    continue
                if matches or has_blocked_review_dependents:
                    unresolved_tasks.append(task)
                continue
            if empty_prereq_block is not None:
                if matches:
                    unresolved_tasks.append(task)
                continue
            if task.status == "failed":
                if _has_completed_same_type_descendant(indexes, task):
                    continue
                if _has_merged_descendant(indexes, task, merge_units_by_member=merge_units_by_member):
                    continue
                completed_recovery = get_completed_recovery_descendant(store, task, read_context=read_context)
                if completed_recovery is not None:
                    recovery_completed_by_failed_id[task.id] = completed_recovery
                    continue
                completed_sibling_recovery = get_completed_sibling_recovery(store, task, read_context=read_context)
                if completed_sibling_recovery is not None:
                    recovery_completed_by_failed_id[task.id] = completed_sibling_recovery
                    continue
                keep_failed_leaf_visible = _failed_leaf_has_unique_unmerged_work_under_terminal_owner(
                    failed_task=task,
                    owner_merge_unit=owner_merge_unit,
                    leaf_merge_unit=merge_unit,
                    git=git,
                )
                if task.id == owner.id and task.id in visible_failed_ids:
                    keep_failed_leaf_visible = True
                if merged_owner_branch and not keep_failed_leaf_visible:
                    continue
                if task.id not in visible_failed_ids and not (merged_owner_branch and keep_failed_leaf_visible):
                    continue
                failed_leaves.append(task)
                if matches:
                    matching_failed_leaves.append(task)
                    unresolved_tasks.append(task)
                continue
            if merged_owner_branch:
                continue
            explicit_merge_state = _effective_merge_state(task, merge_unit=merge_unit)
            if task.status in {"completed", "unmerged"} and explicit_merge_state == "unmerged":
                if matches:
                    unresolved_tasks.append(task)
                continue
            if is_lineage_complete(task, store=store):
                continue
            if matches:
                unresolved_tasks.append(task)

        for task in sorted(
            skipped_same_branch_members,
            key=lambda item: (_task_event_time(item), task_id_numeric_key(item.id)),
        ):
            if task.id is None:
                continue
            merge_unit = indexes.merge_units_by_task_id.get(task.id)
            if not _matches_task_filters(
                task,
                query,
                tag_matcher=task_matches_tag_filters,
                include_tag_filters=False,
                merge_unit=merge_unit,
            ):
                continue
            explicit_merge_state = _effective_merge_state(task, merge_unit=merge_unit)
            if task.status in {"completed", "unmerged"} and explicit_merge_state == "unmerged":
                orphaned_same_branch_tasks.append(task)

        snapshot = LineageOwnerSnapshot(
            owner_task=owner,
            root_task=root,
            members=tuple(owner_members),
            merge_units_by_task_id=merge_units_by_member,
            failed_leaves=tuple(failed_leaves),
            recovery_completed_by_failed_id=recovery_completed_by_failed_id,
        )
        has_empty_prereq_blocked_pending = any(
            blocked_by_empty_prereq_label(store, task, read_context=read_context) is not None for task in unresolved_tasks
        )
        if (
            owner_merge_unit is not None
            and merge_state_is_terminal_for_lifecycle(owner_merge_unit.state)
            and not matching_failed_leaves
            and not has_empty_prereq_blocked_pending
        ):
            continue
        if target_branch and owner_merge_unit is not None and owner_merge_unit.target_branch != target_branch:
            continue
        unresolved_tasks = list(
            filter_display_unresolved_tasks_for_incomplete(
                unresolved_tasks,
                merge_units_by_task_id=merge_units_by_member,
                exclude_dropped=query.exclude_dropped_from_planning,
            )
        )

        if not unresolved_tasks and not matching_failed_leaves and not orphaned_same_branch_tasks:
            continue
        resolution = is_lineage_resolved(snapshot)
        has_unimplemented_source = (
            owner.id is not None
            and owner.task_type in IMPLEMENTATION_SOURCE_TASK_TYPES
            and owner.status == "completed"
            and (
                source_task_needs_implementation_followup(
                    owner,
                    _source_followup_state(indexes, owner, source_followup_cache),
                    non_dropped_implement_source_ids=indexes.non_dropped_impl_source_ids,
                )
                or held_plan_has_blocked_awaiting_review_dependents(
                    owner,
                    get_dependents=lambda task_id: indexes.depends_on_children.get(task_id, ()),
                    get_dependency_readiness=lambda dependent: store.get_dependency_readiness(dependent),
                )
            )
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

        lifecycle_action_task = _select_representative_completed_task(
            store,
            snapshot,
            unresolved_tasks,
            include_dropped=not query.exclude_dropped_from_planning,
        )
        planning_task = lifecycle_action_task
        recovery_action_task: DbTask | None = None
        recovery_leaf_task: DbTask | None = None
        max_recovery_attempts = query.max_recovery_attempts if query.max_recovery_attempts is not None else 1
        failed_action_candidate: DbTask | None = None
        failed_action_candidate_decision = None
        recovery_merge_context = None
        if reuse_recovery_merge_context:
            from .recovery_engine import _MergeContext

            if isinstance(read_context.merge_context, _MergeContext):
                recovery_merge_context = read_context.merge_context
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
                merge_context=recovery_merge_context,
                read_context=read_context,
            )
            attention_action = failed_recovery_decision_to_attention_action(
                store,
                failed_task,
                decision,
                max_recovery_attempts=max_recovery_attempts,
                read_context=read_context,
            )
            if decision.reason_code == "recovery_has_newer_unresolved_descendant":
                continue
            if failed_task.task_type == "improve" and lifecycle_action_task is not None:
                continue
            if decision.action != "skip" or attention_action is not None:
                failed_action_candidate = failed_task
                failed_action_candidate_decision = decision
                break
        if failed_action_candidate is not None:
            recovery_action_task = failed_action_candidate
            recovery_leaf_task = failed_action_candidate
        elif lifecycle_action_task is None and failed_leaves:
            recovery_leaf_task = max(
                failed_leaves,
                key=lambda task: (_task_event_time(task), task_id_numeric_key(task.id)),
            )
            recovery_action_task = recovery_leaf_task
        if planning_task is None:
            planning_task = recovery_action_task
        action: dict[str, Any] | None = None
        if (
            failed_action_candidate is not None
            and failed_action_candidate_decision is not None
            and failed_action_candidate.id is not None
            and failed_action_candidate_decision.action in {"resume", "retry"}
        ):
            recovery_action = {
                "type": failed_action_candidate_decision.action,
                "description": failed_action_candidate_decision.reason_text,
                "recovery_task_id": failed_action_candidate_decision.recovery_task_id,
            }
            candidate = build_watch_progress_candidate(
                store,
                subject_task=failed_action_candidate,
                action=recovery_action,
                action_task=failed_action_candidate,
                failed_task=failed_action_candidate,
            )
            action = get_active_watch_no_progress_attention(store, candidate=candidate)
        if planning_task is None:
            blocked_pending = next(
                (task for task in unresolved_tasks if blocked_by_empty_prereq_label(store, task, read_context=read_context) is not None),
                None,
            )
            if blocked_pending is not None:
                action = {
                    "type": "awaiting_human",
                    "description": blocked_by_empty_prereq_label(store, blocked_pending, read_context=read_context),
                    "needs_attention_reason": "awaiting-human-review",
                    "subject_task_id": blocked_pending.id,
                }
        if planning_task is None and _requires_impl_branch_manual_resolution(
            owner,
            unresolved_tasks,
            orphaned_same_branch_tasks,
            include_dropped=not query.exclude_dropped_from_planning,
        ):
            action = {
                "type": "needs_discussion",
                "description": "SKIP: no descendant on the impl branch; manual resolution required",
                "needs_attention_reason": "no-descendant-on-the-impl-branch",
                "subject_task_id": owner.id,
            }
        if planning_task is None and action is None:
            continue

        displayed_unresolved_tasks = tuple(unresolved_tasks)
        if action is not None and action.get("needs_attention_reason") == "no-descendant-on-the-impl-branch":
            displayed_unresolved_tasks = tuple([*unresolved_tasks, *orphaned_same_branch_tasks])

        if action is None and config is not None and git is not None and target_branch:
            assert planning_task is not None
            action = determine_next_action(
                config,
                store,
                git,
                planning_task,
                target_branch,
                impl_based_on_ids=indexes.non_dropped_impl_source_ids,
                max_resume_attempts=query.max_recovery_attempts,
                persist_post_merge_rebase_state=persist_post_merge_rebase_state,
                persist_review_clearance=persist_review_clearance,
                read_context=read_context,
            )
            if lifecycle_action_task is not None and lifecycle_action_task.id is not None:
                candidate = build_watch_progress_candidate(
                    store,
                    subject_task=owner,
                    action=action,
                    action_task=lifecycle_action_task,
                    failed_task=None,
                )
                parked_attention = get_active_watch_no_progress_attention(store, candidate=candidate)
                if parked_attention is not None:
                    action = parked_attention
        lineage_status = _classify_lineage_status(action) if action is not None else "actionable"
        if not query.include_skipped and lineage_status == "skipped":
            continue
        if not _owner_matches_tag_filters(owner, query, tag_matcher=task_matches_tag_filters):
            continue

        tree, rendered_members = _build_owner_tree(
            root_task=root,
            owner_task=owner,
            unresolved_tasks=tuple(unresolved_tasks),
            based_on_children=indexes.based_on_children,
            depends_on_children=indexes.depends_on_children,
            drop_excluded_task_ids=drop_excluded_task_ids,
        )
        summaries = tuple(
            UnresolvedLeafSummary(
                task_id=task.id or "unknown",
                status=task.status,
                task_type=task.task_type,
                reason=task.failure_reason or task.completion_reason,
            )
            for task in displayed_unresolved_tasks
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
                unresolved_tasks=displayed_unresolved_tasks,
                unresolved_leaf_summary=summaries,
                lifecycle_action_task=lifecycle_action_task,
                recovery_action_task=recovery_action_task,
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
    if git is not None and config is not None and _main_integration_alert_matches_query(query):
        main_alert = current_main_integration_verify_alert(store, git, config)
        if main_alert is not None and main_alert.task.id is not None:
            action = {
                "type": "needs_discussion",
                "description": f"SKIP: {main_alert.alert_message or 'main verify is red; merges halted'}",
                "needs_attention_reason": MAIN_INTEGRATION_VERIFY_REASON,
                "subject_task_id": main_alert.task.id,
            }
            rows.insert(
                0,
                LineageOwnerRow(
                    owner_task=main_alert.task,
                    members=(main_alert.task,),
                    tree=None,
                    lineage_status="needs_attention",
                    next_action=action,
                    next_action_reason=str(action["description"]),
                    unresolved_tasks=(main_alert.task,),
                    unresolved_leaf_summary=(
                        UnresolvedLeafSummary(
                            task_id=main_alert.task.id,
                            status=main_alert.task.status,
                            task_type=main_alert.task.task_type,
                            reason=main_alert.failure or main_alert.verify_exit_status,
                        ),
                    ),
                    lifecycle_action_task=None,
                    recovery_action_task=None,
                    recovery_leaf_task=None,
                ),
            )
    if query.limit is not None:
        rows = rows[: query.limit]
    if read_context.allow_reconcile_mutation:
        apply_pending_recovery_reconciliations(store, read_context=read_context)
    return (tuple(rows), read_context)


__all__ = [
    "LineageOwnerQuery",
    "LineageOwnerRow",
    "LineageOwnerSnapshot",
    "LineageResolution",
    "StaleUnmergedSweepCandidate",
    "UnresolvedLeafSummary",
    "_query_lineage_owner_rows_with_context",
    "collect_stale_unmerged_sweep_candidates",
    "filter_display_unresolved_tasks_for_incomplete",
    "is_lineage_resolved",
    "apply_deferred_lineage_query_reconciliations",
    "query_lineage_owner_rows_in_read_session",
    "query_lineage_owner_rows",
    "resolve_lineage_owner_task_id",
]


instrument_module_functions(
    globals(),
    metric_name="gza_query_function_latency_seconds",
    module_name=__name__,
)
