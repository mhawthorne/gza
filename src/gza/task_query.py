"""Unified task query service for filter/projection/presentation workflows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal, TypeVar

from . import lineage
from .db import SqliteTaskStore, Task as DbTask, _normalize_tags, task_id_numeric_key
from .lifecycle_completion import task_is_complete_for_lifecycle
from .lineage_query import LineageOwnerQuery, query_lineage_owner_rows_in_read_session
from .operator_state import blocked_by_empty_prereq_label, effective_no_work_merge_state

QueryScope = Literal["tasks", "lineages"]
DateField = Literal["created", "completed", "effective"]
PresentationMode = Literal["flat", "blocks", "grouped", "lineage", "tree", "one_line", "json", "rich"]
BranchOwnerMode = Literal["generic", "unmerged_same_branch"]
_T = TypeVar("_T")


@dataclass(frozen=True)
class TextFilter:
    """Simple substring text matching filter."""

    contains: str
    fields: tuple[str, ...] = ("prompt",)


@dataclass(frozen=True)
class DateFilter:
    """Date filtering across created/completed/effective task timestamps."""

    field: DateField = "effective"
    start: date | None = None
    end: date | None = None
    days: int | None = None


@dataclass(frozen=True)
class SortSpec:
    """Deterministic result sorting."""

    field: str = "effective_at"
    descending: bool = True


DEFAULT_SORT = SortSpec()


@dataclass(frozen=True)
class ProjectionSpec:
    """Projection configuration for query results."""

    preset: str = "history_default"
    fields: tuple[str, ...] | None = None


@dataclass(frozen=True)
class PresentationSpec:
    """Presentation configuration for query results."""

    mode: PresentationMode = "flat"


@dataclass(frozen=True)
class TaskQuery:
    """Declarative task query request."""

    scope: QueryScope = "tasks"
    limit: int | None = 10
    text: TextFilter | None = None
    statuses: tuple[str, ...] | None = None
    exclude_statuses: tuple[str, ...] | None = None
    task_types: tuple[str, ...] | None = None
    exclude_task_types: tuple[str, ...] | None = None
    lifecycle_state: tuple[str, ...] | None = None
    merge_chain_state: tuple[str, ...] | None = None
    exclude_merge_chain_state: tuple[str, ...] | None = None
    dependency_state: tuple[str, ...] | None = None
    lineage_of: str | None = None
    exclude_lineage_of: str | None = None
    root_ids: tuple[str, ...] | None = None
    exclude_root_ids: tuple[str, ...] | None = None
    task_ids: tuple[str, ...] | None = None
    branch_owner_ids: tuple[str, ...] | None = None
    branch_owner_mode: BranchOwnerMode = "generic"
    merge_unit_ids: tuple[str, ...] | None = None
    tag_filters: tuple[str, ...] | None = None
    exclude_tag_filters: tuple[str, ...] | None = None
    any_tag: bool = True
    pickup_only: bool = False
    date_filter: DateFilter | None = None
    sort: SortSpec = DEFAULT_SORT
    projection: ProjectionSpec = ProjectionSpec()
    presentation: PresentationSpec = PresentationSpec()


@dataclass(frozen=True)
class TaskRow:
    """Projected task-scoped row."""

    task: DbTask
    values: Mapping[str, object]


@dataclass(frozen=True)
class LineageRow:
    """Projected lineage-scoped row."""

    owner_task: DbTask
    members: tuple[DbTask, ...]
    tree: Any | None
    unresolved_tasks: tuple[DbTask, ...] = ()
    lifecycle_action_task: DbTask | None = None
    recovery_action_task: DbTask | None = None
    recovery_leaf_task: DbTask | None = None
    lineage_status: str | None = None
    next_action_data: Mapping[str, object] | None = None
    values: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskQueryResult:
    """Query result container with renderer helpers."""

    query: TaskQuery
    rows: tuple[TaskRow | LineageRow, ...]
    total_count: int | None = None

    def render(self, mode: PresentationMode | None = None) -> str:
        """Render using the configured (or overridden) presentation mode."""
        from gza.task_presenters import render_query_result

        final_mode = mode or self.query.presentation.mode
        return render_query_result(self, final_mode)

    def to_json(self) -> list[dict[str, object]]:
        """Return JSON-serializable row dictionaries."""
        return [dict(row.values) for row in self.rows]


@dataclass(frozen=True)
class ScopedTagScopeGap:
    """Detectable in-scope owner blocked by an out-of-scope derived child."""

    owner_id: str
    blocking_child_id: str
    child_task_type: str
    child_status: str
    child_tags: tuple[str, ...]
    missing_filter_tags: tuple[str, ...]
    suggested_next_command: str
    blocking_state: str


class TaskProjectionPreset:
    """Named projection presets."""

    HISTORY_DEFAULT = "history_default"
    SEARCH_DEFAULT = "search_default"
    INCOMPLETE_SUMMARY = "incomplete_summary"
    LINEAGE_FULL = "lineage_full"
    SHOW_DETAIL = "show_detail"
    JSON_MINIMAL = "json_minimal"
    QUEUE_DEFAULT = "queue_default"
    UNMERGED_DEFAULT = "unmerged_default"


_TASK_DEFAULT_FIELDS: tuple[str, ...] = (
    "id",
    "prompt",
    "status",
    "task_type",
    "provider",
    "model",
    "created_at",
    "completed_at",
    "effective_at",
    "lineage_root_id",
    "branch_owner_id",
    "tags",
    "branch_merge_state",
    "shares_owner_branch",
    "review_verdict",
    "comments_count",
    "next_action",
    "next_action_reason",
    "next_action_owner_id",
    "blocked",
    "blocking_id",
    "blocking_status",
    "blocking_merge_unit_id",
    "blocking_merge_state",
    "blocking_merge_owner_id",
    "blocking_source_branch",
    "blocking_target_branch",
)

_LINEAGE_DEFAULT_FIELDS: tuple[str, ...] = (
    *_TASK_DEFAULT_FIELDS,
    "member_ids",
    "unresolved_ids",
)

_PROJECTION_PRESET_FIELDS: dict[str, tuple[str, ...]] = {
    TaskProjectionPreset.HISTORY_DEFAULT: _TASK_DEFAULT_FIELDS,
    TaskProjectionPreset.SEARCH_DEFAULT: _TASK_DEFAULT_FIELDS,
    TaskProjectionPreset.INCOMPLETE_SUMMARY: _LINEAGE_DEFAULT_FIELDS,
    TaskProjectionPreset.LINEAGE_FULL: _LINEAGE_DEFAULT_FIELDS,
    TaskProjectionPreset.SHOW_DETAIL: _TASK_DEFAULT_FIELDS,
    TaskProjectionPreset.JSON_MINIMAL: ("id", "status", "task_type", "prompt"),
    TaskProjectionPreset.QUEUE_DEFAULT: (
        "id",
        "prompt",
        "status",
        "task_type",
        "created_at",
        "tags",
        "urgent",
        "queue_position",
        "blocked",
        "blocking_id",
        "blocking_status",
        "blocking_merge_unit_id",
        "blocking_merge_state",
        "blocking_merge_owner_id",
        "blocking_source_branch",
        "blocking_target_branch",
    ),
    TaskProjectionPreset.UNMERGED_DEFAULT: (
        "id",
        "prompt",
        "status",
        "task_type",
        "completed_at",
        "lineage_root_id",
        "branch_owner_id",
        "member_ids",
        "unresolved_ids",
        "lineage_text",
        "branch",
        "source_branch",
        "target_branch",
        "merge_unit_id",
        "merge_unit_state",
        "branch_deleted",
        "commit_count",
        "files_changed",
        "insertions",
        "deletions",
        "has_conflicts",
        "pr_url",
        "review_status",
        "review_detail",
        "review_verdict",
        "review_score",
        "report_file",
        "stats",
        "completion_reason",
        "failure_reason",
    ),
}


class TaskQueryPresets:
    """Convenience constructors for common CLI/API presets."""

    @staticmethod
    def history(
        *,
        limit: int | None = 10,
        statuses: tuple[str, ...] | None = None,
        task_types: tuple[str, ...] | None = None,
        lifecycle_state: tuple[str, ...] | None = None,
        date_filter: DateFilter | None = None,
    ) -> TaskQuery:
        return TaskQuery(
            scope="tasks",
            limit=limit,
            statuses=statuses,
            task_types=task_types,
            lifecycle_state=lifecycle_state,
            date_filter=date_filter,
            projection=ProjectionSpec(preset=TaskProjectionPreset.HISTORY_DEFAULT),
            presentation=PresentationSpec(mode="flat"),
        )

    @staticmethod
    def search(
        term: str,
        *,
        limit: int | None = 10,
        statuses: tuple[str, ...] | None = None,
        exclude_statuses: tuple[str, ...] | None = None,
        task_types: tuple[str, ...] | None = None,
        exclude_task_types: tuple[str, ...] | None = None,
        date_filter: DateFilter | None = None,
        lineage_of: str | None = None,
        exclude_lineage_of: str | None = None,
        root_ids: tuple[str, ...] | None = None,
        exclude_root_ids: tuple[str, ...] | None = None,
    ) -> TaskQuery:
        return TaskQuery(
            scope="tasks",
            limit=limit,
            text=TextFilter(contains=term, fields=("prompt",)),
            statuses=statuses,
            exclude_statuses=exclude_statuses,
            task_types=task_types,
            exclude_task_types=exclude_task_types,
            lineage_of=lineage_of,
            exclude_lineage_of=exclude_lineage_of,
            root_ids=root_ids,
            exclude_root_ids=exclude_root_ids,
            date_filter=date_filter,
            projection=ProjectionSpec(preset=TaskProjectionPreset.SEARCH_DEFAULT),
            presentation=PresentationSpec(mode="flat"),
        )

    @staticmethod
    def incomplete(
        *,
        limit: int | None = 5,
        task_types: tuple[str, ...] | None = None,
        date_filter: DateFilter | None = None,
        mode: PresentationMode = "one_line",
    ) -> TaskQuery:
        return TaskQuery(
            scope="lineages",
            limit=limit,
            task_types=task_types,
            lifecycle_state=("incomplete",),
            date_filter=date_filter,
            projection=ProjectionSpec(preset=TaskProjectionPreset.INCOMPLETE_SUMMARY),
            presentation=PresentationSpec(mode=mode),
        )

    @staticmethod
    def lineage(task_id: str) -> TaskQuery:
        return TaskQuery(
            scope="lineages",
            limit=1,
            lineage_of=task_id,
            projection=ProjectionSpec(preset=TaskProjectionPreset.LINEAGE_FULL),
            presentation=PresentationSpec(mode="tree"),
        )

    @staticmethod
    def queue(
        *,
        limit: int | None = 10,
        tags: tuple[str, ...] | None = None,
        any_tag: bool = True,
    ) -> TaskQuery:
        return TaskQuery(
            scope="tasks",
            limit=limit,
            statuses=("pending",),
            exclude_task_types=("internal",),
            dependency_state=("unblocked",),
            tag_filters=normalize_tag_filters(tags),
            any_tag=any_tag,
            pickup_only=True,
            sort=SortSpec(field="pickup_order", descending=False),
            projection=ProjectionSpec(preset=TaskProjectionPreset.QUEUE_DEFAULT),
            presentation=PresentationSpec(mode="flat"),
        )

    @staticmethod
    def queue_listing(
        *,
        limit: int | None = None,
        tags: tuple[str, ...] | None = None,
        any_tag: bool = True,
    ) -> TaskQuery:
        return TaskQuery(
            scope="tasks",
            limit=limit,
            statuses=("pending",),
            exclude_task_types=("internal",),
            tag_filters=normalize_tag_filters(tags),
            any_tag=any_tag,
            sort=SortSpec(field="pickup_order", descending=False),
            projection=ProjectionSpec(preset=TaskProjectionPreset.QUEUE_DEFAULT),
            presentation=PresentationSpec(mode="flat"),
        )

    @staticmethod
    def unmerged(
        *,
        branch_owner_ids: tuple[str, ...],
        merge_unit_ids: tuple[str, ...] | None = None,
        task_ids: tuple[str, ...] | None = None,
        limit: int | None = 5,
        mode: PresentationMode = "rich",
        projection: ProjectionSpec | None = None,
    ) -> TaskQuery:
        return TaskQuery(
            scope="lineages",
            limit=limit,
            task_ids=task_ids,
            branch_owner_ids=branch_owner_ids,
            branch_owner_mode="unmerged_same_branch",
            merge_unit_ids=merge_unit_ids,
            projection=projection or ProjectionSpec(preset=TaskProjectionPreset.UNMERGED_DEFAULT),
            presentation=PresentationSpec(mode=mode),
        )


class TaskQueryService:
    """Filter/projection/presentation query service over SqliteTaskStore."""

    def __init__(self, store: SqliteTaskStore) -> None:
        self._store = store

    def run(
        self,
        query: TaskQuery,
        *,
        config: Any | None = None,
        git: Any | None = None,
        target_branch: str | None = None,
    ) -> TaskQueryResult:
        """Execute a query and return projected rows."""
        if query.scope == "lineages":
            all_lineages = self._collect_lineages_unlimited(
                query,
                config=config,
                git=git,
                target_branch=target_branch,
            )
            lineages = self._apply_limit(all_lineages, query.limit)
            lineage_rows = tuple(
                self._project_lineage_row(
                    row,
                    query,
                    config=config,
                    git=git,
                    target_branch=target_branch,
                )
                for row in lineages
            )
            return TaskQueryResult(query=query, rows=lineage_rows, total_count=len(all_lineages))

        all_tasks = self._collect_tasks_unlimited(query)
        tasks = self._apply_limit(all_tasks, query.limit)
        task_rows = tuple(self._project_task_row(task, query, target_branch=target_branch) for task in tasks)
        return TaskQueryResult(query=query, rows=task_rows, total_count=len(all_tasks))

    def _collect_tasks(self, query: TaskQuery) -> list[DbTask]:
        return self._apply_limit(self._collect_tasks_unlimited(query), query.limit)

    def _collect_tasks_unlimited(self, query: TaskQuery) -> list[DbTask]:
        tasks = self._base_task_candidates(query)
        tasks = self._apply_task_filters(tasks, query)
        if query.sort.field != "pickup_order":
            tasks.sort(key=lambda task: self._sort_key(task, query.sort), reverse=query.sort.descending)
        return tasks

    def _base_task_candidates(self, query: TaskQuery) -> list[DbTask]:
        if query.scope != "tasks":
            return list(self._store.get_all())

        tags = query.tag_filters
        if query.pickup_only:
            return list(self._store.get_pending_pickup(limit=None, tags=tags, any_tag=query.any_tag))
        if query.sort.field == "pickup_order":
            return list(self._store.get_pending(limit=None, tags=tags, any_tag=query.any_tag))
        return list(self._store.get_all())

    def _collect_lineages(
        self,
        query: TaskQuery,
        *,
        config: Any | None = None,
        git: Any | None = None,
        target_branch: str | None = None,
    ) -> list[LineageRow]:
        return self._apply_limit(
            self._collect_lineages_unlimited(
                query,
                config=config,
                git=git,
                target_branch=target_branch,
            ),
            query.limit,
        )

    def _collect_lineages_unlimited(
        self,
        query: TaskQuery,
        *,
        config: Any | None = None,
        git: Any | None = None,
        target_branch: str | None = None,
    ) -> list[LineageRow]:
        use_incomplete_rollup = bool(
            query.lifecycle_state and "incomplete" in query.lifecycle_state
        )

        rows: list[LineageRow]
        if use_incomplete_rollup:
            if query.task_types and len(query.task_types) > 1:
                raise ValueError(
                    "lineages scope with lifecycle_state=incomplete supports at most one task type"
                )
            owner_rows, _read_context = query_lineage_owner_rows_in_read_session(
                self._store,
                LineageOwnerQuery(
                    limit=None,
                    task_types=query.task_types,
                    exclude_task_types=query.exclude_task_types,
                    tags=query.tag_filters,
                    any_tag=query.any_tag,
                    date_filter=query.date_filter,
                    include_skipped=True,
                    exclude_dropped_from_planning=True,
                    task_ids=query.task_ids,
                    owner_task_ids=query.branch_owner_ids,
                ),
                config=config,
                git=git,
                target_branch=target_branch,
            )
            rows = [
                LineageRow(
                    owner_task=row.owner_task,
                    members=row.members,
                    tree=row.tree,
                    unresolved_tasks=row.unresolved_tasks,
                    lifecycle_action_task=row.lifecycle_action_task,
                    recovery_action_task=row.recovery_action_task,
                    recovery_leaf_task=row.recovery_leaf_task,
                    lineage_status=row.lineage_status,
                    next_action_data=row.next_action,
                )
                for row in owner_rows
            ]
        else:
            grouped: dict[str, list[DbTask]] = {}
            owner_by_id: dict[str, DbTask] = {}
            for task in self._collect_tasks(
                TaskQuery(
                    scope="tasks",
                    limit=None,
                    text=query.text,
                    statuses=query.statuses,
                    exclude_statuses=query.exclude_statuses,
                    task_types=query.task_types,
                    exclude_task_types=query.exclude_task_types,
                    lifecycle_state=query.lifecycle_state,
                    merge_chain_state=query.merge_chain_state,
                    exclude_merge_chain_state=query.exclude_merge_chain_state,
                    dependency_state=query.dependency_state,
                    lineage_of=query.lineage_of,
                    exclude_lineage_of=query.exclude_lineage_of,
                    root_ids=query.root_ids,
                    exclude_root_ids=query.exclude_root_ids,
                    task_ids=query.task_ids,
                    branch_owner_ids=query.branch_owner_ids,
                    branch_owner_mode=query.branch_owner_mode,
                    merge_unit_ids=query.merge_unit_ids,
                    tag_filters=query.tag_filters,
                    exclude_tag_filters=query.exclude_tag_filters,
                    any_tag=query.any_tag,
                    pickup_only=query.pickup_only,
                    date_filter=query.date_filter,
                    sort=query.sort,
                    projection=query.projection,
                    presentation=query.presentation,
                )
            ):
                owner = self._resolve_branch_owner(task, query=query)
                owner_id = owner.id
                if owner_id is None:
                    continue
                grouped.setdefault(owner_id, []).append(task)
                owner_by_id[owner_id] = owner

            rows = []
            for owner_id, owner_members in grouped.items():
                owner = owner_by_id[owner_id]
                root = _resolve_lineage_root(self._store, owner)
                full_tree = _build_lineage_tree(self._store, root, max_depth=None)
                rendered_tree = full_tree
                rendered_members: tuple[DbTask, ...] = tuple(
                    sorted(owner_members, key=lambda t: self._sort_key(t, DEFAULT_SORT), reverse=True)
                )

                if query.tag_filters is not None:
                    keep_ids = {task.id for task in owner_members if task.id is not None}
                    if owner.id is not None:
                        keep_ids.add(owner.id)
                    if root.id is not None:
                        keep_ids.add(root.id)
                    rendered_tree = _prune_lineage_tree_to_ids(full_tree, keep_ids)
                    rendered_members = tuple(_flatten_lineage_tree(rendered_tree))

                rows.append(
                    LineageRow(
                        owner_task=owner,
                        members=rendered_members,
                        tree=rendered_tree,
                    )
                )

        rows = self._apply_lineage_filters(rows, query)
        rows.sort(
            key=lambda row: self._sort_key(row.owner_task, query.sort),
            reverse=query.sort.descending,
        )
        return rows

    def _apply_limit(self, rows: list[_T], limit: int | None) -> list[_T]:
        if limit is None:
            return rows
        return rows[:limit]

    def _apply_task_filters(self, tasks: Sequence[DbTask], query: TaskQuery) -> list[DbTask]:
        filtered = list(tasks)

        if query.text is not None:
            needle = query.text.contains.casefold()
            filtered = [
                task
                for task in filtered
                if any(
                    needle in str(getattr(task, field_name, "") or "").casefold()
                    for field_name in query.text.fields
                )
            ]

        if query.statuses is not None:
            allowed = set(query.statuses)
            filtered = [task for task in filtered if task.status in allowed]

        if query.exclude_statuses is not None:
            disallowed_statuses = set(query.exclude_statuses)
            filtered = [task for task in filtered if task.status not in disallowed_statuses]

        if query.task_types is not None:
            allowed_types = set(query.task_types)
            filtered = [task for task in filtered if task.task_type in allowed_types]

        if query.exclude_task_types is not None:
            disallowed_types = set(query.exclude_task_types)
            filtered = [task for task in filtered if task.task_type not in disallowed_types]

        if query.lifecycle_state is not None:
            lifecycle = set(query.lifecycle_state)
            filtered = [task for task in filtered if self._matches_lifecycle(task, lifecycle)]

        if query.date_filter is not None:
            filtered = [task for task in filtered if self._matches_date_filter(task, query.date_filter)]

        if query.root_ids is not None:
            allowed_root_ids = set(query.root_ids)
            filtered = [
                task
                for task in filtered
                if (root := _resolve_lineage_root(self._store, task)).id in allowed_root_ids
            ]

        if query.exclude_root_ids is not None:
            disallowed_root_ids = set(query.exclude_root_ids)
            filtered = [
                task
                for task in filtered
                if (root := _resolve_lineage_root(self._store, task)).id not in disallowed_root_ids
            ]

        if query.task_ids is not None:
            allowed_task_ids = set(query.task_ids)
            filtered = [task for task in filtered if task.id in allowed_task_ids]

        if query.lineage_of is not None:
            lineage_task = self._store.get(query.lineage_of)
            if lineage_task is None:
                return []
            root = _resolve_lineage_root(self._store, lineage_task)
            root_id = root.id
            if root_id is None:
                return []
            filtered = [
                task
                for task in filtered
                if _resolve_lineage_root(self._store, task).id == root_id
            ]

        if query.exclude_lineage_of is not None:
            lineage_task = self._store.get(query.exclude_lineage_of)
            if lineage_task is not None:
                root_id = _resolve_lineage_root(self._store, lineage_task).id
                if root_id is not None:
                    filtered = [
                        task
                        for task in filtered
                        if _resolve_lineage_root(self._store, task).id != root_id
                    ]

        if query.branch_owner_ids is not None:
            allowed_owners = set(query.branch_owner_ids)
            filtered = [
                task
                for task in filtered
                if self._resolve_branch_owner(task, query=query).id in allowed_owners
            ]

        if query.merge_unit_ids is not None:
            filtered = [
                task
                for task in filtered
                if task.id is not None
                and self._store.task_is_attached_to_merge_unit_ids(task.id, query.merge_unit_ids)
            ]

        if query.tag_filters is not None or query.exclude_tag_filters is not None:
            filtered = [task for task in filtered if self._matches_tag_filters(task, query)]

        if query.merge_chain_state is not None:
            merge_states = set(query.merge_chain_state)
            filtered = [task for task in filtered if self._matches_merge_chain_state(task, merge_states)]

        if query.exclude_merge_chain_state is not None:
            excluded_merge_states = set(query.exclude_merge_chain_state)
            filtered = [
                task for task in filtered if not self._matches_merge_chain_state(task, excluded_merge_states)
            ]

        if query.dependency_state is not None:
            dep_states = set(query.dependency_state)
            filtered = [task for task in filtered if self._matches_dependency_state(task, dep_states)]

        return filtered

    def _matches_tag_filters(self, task: DbTask, query: TaskQuery) -> bool:
        if query.tag_filters is not None:
            required = normalize_tag_filters(query.tag_filters)
            if not task_matches_tag_filters(task_tags=task.tags, tag_filters=required, any_tag=query.any_tag):
                return False

        if query.exclude_tag_filters is not None:
            excluded = normalize_tag_filters(query.exclude_tag_filters)
            if task_matches_tag_filters(task_tags=task.tags, tag_filters=excluded, any_tag=query.any_tag):
                return False

        return True

    def _apply_lineage_filters(self, rows: Sequence[LineageRow], query: TaskQuery) -> list[LineageRow]:
        filtered = list(rows)

        if query.root_ids is not None:
            roots = set(query.root_ids)
            filtered = [
                row
                for row in filtered
                if _resolve_lineage_root(self._store, row.owner_task).id in roots
            ]

        if query.exclude_root_ids is not None:
            excluded_roots = set(query.exclude_root_ids)
            filtered = [
                row
                for row in filtered
                if _resolve_lineage_root(self._store, row.owner_task).id not in excluded_roots
            ]

        if query.lineage_of is not None:
            task = self._store.get(query.lineage_of)
            if task is None:
                return []
            root_id = _resolve_lineage_root(self._store, task).id
            filtered = [
                row
                for row in filtered
                if _resolve_lineage_root(self._store, row.owner_task).id == root_id
            ]

        if query.exclude_lineage_of is not None:
            task = self._store.get(query.exclude_lineage_of)
            if task is not None:
                root_id = _resolve_lineage_root(self._store, task).id
                filtered = [
                    row
                    for row in filtered
                    if _resolve_lineage_root(self._store, row.owner_task).id != root_id
                ]

        return filtered

    def _project_task_row(self, task: DbTask, query: TaskQuery, *, target_branch: str | None) -> TaskRow:
        root = _resolve_lineage_root(self._store, task)
        branch_owner = _resolve_lineage_owner_task(self._store, task)
        readiness = self._store.get_dependency_readiness(task)
        blocked = not readiness.ready
        blocking_id = readiness.blocking_task_id
        blocking_status = readiness.blocking_task_status
        review_verdict = None
        comments_count = 0
        if task.id is not None:
            comments_count = len(self._store.get_comments(task.id))
        if task.task_type == "review":
            review_verdict = task.output_content

        values: dict[str, object] = {
            "id": task.id,
            "prompt": task.prompt,
            "status": task.status,
            "task_type": task.task_type,
            "provider": task.provider,
            "model": task.model,
            "created_at": task.created_at,
            "completed_at": task.completed_at,
            "effective_at": _effective_at(task),
            "tags": list(task.tags),
            "urgent": task.urgent,
            "queue_position": task.queue_position,
            "lineage_root_id": root.id,
            "branch_owner_id": branch_owner.id,
            "branch_merge_state": self._branch_merge_state(branch_owner),
            "shares_owner_branch": _is_shared_branch_descendant_query(task, root),
            "review_verdict": review_verdict,
            "comments_count": comments_count,
            "next_action": None,
            "next_action_reason": None,
            "next_action_owner_id": None,
            "trigger_source": task.trigger_source,
            "blocked": blocked,
            "blocking_id": blocking_id,
            "blocking_status": blocking_status,
            "blocking_merge_unit_id": readiness.blocking_merge_unit_id,
            "blocking_merge_state": readiness.blocking_merge_state,
            "blocking_merge_owner_id": readiness.blocking_merge_unit_owner_task_id,
            "blocking_source_branch": readiness.blocking_source_branch,
            "blocking_target_branch": readiness.blocking_target_branch,
        }
        return TaskRow(
            task=task,
            values=self._apply_projection(values, query.projection, scope="tasks"),
        )

    def _project_lineage_row(
        self,
        row: LineageRow,
        query: TaskQuery,
        *,
        config: Any | None,
        git: Any | None,
        target_branch: str | None,
    ) -> LineageRow:
        owner = row.owner_task
        root = _resolve_lineage_root(self._store, owner)
        owner_readiness = self._store.get_dependency_readiness(owner)

        next_action_type: str | None = None
        next_action_reason: str | None = None
        next_action_owner_id: str | None = None

        if query.projection.preset == TaskProjectionPreset.INCOMPLETE_SUMMARY:
            empty_prereq_reason = blocked_by_empty_prereq_label(self._store, owner)
            if empty_prereq_reason is not None:
                next_action_type = "awaiting_human"
                next_action_reason = empty_prereq_reason
                next_action_owner_id = owner.id
            action = (
                dict(row.next_action_data)
                if row.next_action_data is not None
                else self._project_next_action(
                    row.lifecycle_action_task or row.recovery_action_task or owner,
                    config=config,
                    git=git,
                    target_branch=target_branch,
                )
            )
            action_type_value = action.get("type") if action else None
            action_reason_value = action.get("description") if action else None
            if next_action_reason is None:
                next_action_type = str(action_type_value) if action_type_value is not None else None
                next_action_reason = str(action_reason_value) if action_reason_value is not None else None
                next_action_owner_id = owner.id
                if action and _action_is_needs_attention(action):
                    from gza.advance_engine import resolve_subject_task

                    next_action_owner_id = resolve_subject_task(
                        self._store,
                        action,
                        row,
                        fallback_task=owner,
                    ).id

        values: dict[str, object] = {
            "id": owner.id,
            "prompt": owner.prompt,
            "status": owner.status,
            "task_type": owner.task_type,
            "created_at": owner.created_at,
            "completed_at": owner.completed_at,
            "effective_at": _effective_at(owner),
            "tags": list(owner.tags),
            "urgent": owner.urgent,
            "queue_position": owner.queue_position,
            "lineage_root_id": root.id,
            "branch_owner_id": owner.id,
            "branch_merge_state": self._branch_merge_state(owner),
            "shares_owner_branch": False,
            "review_verdict": self._latest_review_verdict(owner),
            "comments_count": len(self._store.get_comments(owner.id)) if owner.id else 0,
            "next_action": next_action_type,
            "next_action_reason": next_action_reason,
            "next_action_owner_id": next_action_owner_id,
            "trigger_source": owner.trigger_source,
            "blocked": not owner_readiness.ready,
            "blocking_id": owner_readiness.blocking_task_id,
            "blocking_status": owner_readiness.blocking_task_status,
            "blocking_merge_unit_id": owner_readiness.blocking_merge_unit_id,
            "blocking_merge_state": owner_readiness.blocking_merge_state,
            "blocking_merge_owner_id": owner_readiness.blocking_merge_unit_owner_task_id,
            "blocking_source_branch": owner_readiness.blocking_source_branch,
            "blocking_target_branch": owner_readiness.blocking_target_branch,
            "member_ids": [member.id for member in row.members if member.id is not None],
            "unresolved_ids": [task.id for task in row.unresolved_tasks if task.id is not None],
        }

        return LineageRow(
            owner_task=row.owner_task,
            members=row.members,
            tree=row.tree,
            unresolved_tasks=row.unresolved_tasks,
            lifecycle_action_task=row.lifecycle_action_task,
            recovery_action_task=row.recovery_action_task,
            recovery_leaf_task=row.recovery_leaf_task,
            lineage_status=row.lineage_status,
            next_action_data=row.next_action_data,
            values=self._apply_projection(values, query.projection, scope="lineages"),
        )

    def _project_next_action(
        self,
        owner_task: DbTask,
        *,
        config: Any | None,
        git: Any | None,
        target_branch: str | None,
    ) -> dict[str, Any]:
        if config is None or git is None or not target_branch:
            return {
                "type": "unknown",
                "description": "next action unavailable (missing config/git context)",
            }

        from gza.cli.advance_engine import determine_next_action

        return determine_next_action(
            config,
            self._store,
            git,
            owner_task,
            target_branch,
            persist_review_clearance=False,
        )

    def _latest_review_verdict(self, owner_task: DbTask) -> str | None:
        reviews = _get_reviews_for_root(self._store, owner_task)
        completed = [review for review in reviews if review.status == "completed"]
        if not completed:
            return None
        latest = completed[0]
        return latest.output_content

    def _resolve_branch_owner(
        self,
        task: DbTask,
        *,
        query: TaskQuery | None = None,
    ) -> DbTask:
        if query is not None and query.branch_owner_mode == "unmerged_same_branch":
            return _resolve_lineage_owner_task(self._store, task)
        root = _resolve_lineage_root(self._store, task)
        if _is_shared_branch_descendant_query(task, root):
            return root
        return task

    def _branch_merge_state(self, owner_task: DbTask) -> str:
        if owner_task.id is not None:
            unit = self._store.resolve_merge_unit_for_task(owner_task.id)
            if unit is not None:
                return effective_no_work_merge_state(owner_task, unit.state) or "n/a"
        if owner_task.merge_status == "merged":
            return "merged"
        if owner_task.merge_status == "unmerged":
            return "unmerged"
        if owner_task.status == "completed" and owner_task.has_commits:
            return "needs_merge"
        return "n/a"

    def _sort_key(self, task: DbTask, sort: SortSpec) -> tuple[datetime, int]:
        if sort.field == "created_at":
            ts = _normalize_dt(task.created_at)
        elif sort.field == "completed_at":
            ts = _normalize_dt(task.completed_at)
        else:
            ts = _normalize_dt(_effective_at(task))
        return (ts, task_id_numeric_key(task.id))

    def _matches_lifecycle(self, task: DbTask, lifecycle: set[str]) -> bool:
        if "active" in lifecycle and task.status in {"pending", "in_progress"}:
            return True
        if "terminal" in lifecycle and task.status in {"completed", "failed", "unmerged", "dropped"}:
            return True
        if "complete" in lifecycle and _is_lineage_complete(self._store, task):
            return True
        if "incomplete" in lifecycle:
            if task.status in {"pending", "in_progress"}:
                return True
            if not _is_lineage_complete(self._store, task):
                return True
        return False

    def _matches_date_filter(self, task: DbTask, date_filter: DateFilter) -> bool:
        candidate = _task_time_for_field(task, date_filter.field)
        if candidate is None:
            return False

        if date_filter.days is not None:
            start = datetime.now(UTC) - timedelta(days=date_filter.days)
            if _normalize_dt(candidate) < _normalize_dt(start):
                return False

        if date_filter.start is not None:
            start_dt = datetime.combine(date_filter.start, time.min, tzinfo=UTC)
            if _normalize_dt(candidate) < _normalize_dt(start_dt):
                return False

        if date_filter.end is not None:
            end_dt = datetime.combine(date_filter.end, time.max, tzinfo=UTC)
            if _normalize_dt(candidate) > _normalize_dt(end_dt):
                return False

        return True

    def _matches_merge_chain_state(self, task: DbTask, merge_states: set[str]) -> bool:
        owner = self._resolve_branch_owner(task)
        owner_state = self._branch_merge_state(owner)
        task_unit = self._store.resolve_merge_unit_for_task(task.id) if task.id is not None else None
        if "merged" in merge_states and owner_state == "merged":
            return True
        if "unmerged" in merge_states and (
            owner_state == "unmerged"
            or (
                task.status == "unmerged"
                and task_unit is None
                and owner_state != "merged"
            )
        ):
            return True
        if "needs_merge" in merge_states and owner_state == "needs_merge":
            return True
        if "empty" in merge_states and owner_state == "empty":
            return True
        if "redundant" in merge_states and owner_state == "redundant":
            return True

        root = _resolve_lineage_root(self._store, task)
        shared_descendant = _is_shared_branch_descendant_query(task, root)
        if "branch_shared_descendant" in merge_states and shared_descendant:
            return True
        if "branch_owner_only" in merge_states and task.id == owner.id:
            return True
        return False

    def _matches_dependency_state(self, task: DbTask, dep_states: set[str]) -> bool:
        blocked, blocking_id, blocking_status = self._store.is_task_blocked(task)
        if "blocked" in dep_states and blocked:
            return True
        if "unblocked" in dep_states and not blocked:
            return True
        if (
            "blocked_by_dropped_dep" in dep_states
            and blocked
            and blocking_id is not None
            and blocking_status == "dropped"
        ):
            return True
        return False

    def _projection_fields(self, projection: ProjectionSpec, scope: QueryScope) -> tuple[str, ...]:
        return projection_fields(projection, scope=scope)

    def _apply_projection(
        self,
        values: Mapping[str, object],
        projection: ProjectionSpec,
        *,
        scope: QueryScope,
    ) -> dict[str, object]:
        return apply_projection_values(values, projection, scope=scope)


def _normalize_dt(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _effective_at(task: DbTask) -> datetime | None:
    return task.completed_at or task.created_at


def _task_time_for_field(task: DbTask, field_name: DateField) -> datetime | None:
    if field_name == "created":
        return task.created_at
    if field_name == "completed":
        return task.completed_at
    return _effective_at(task)


def parse_csv(value: str | None) -> tuple[str, ...] | None:
    """Parse a comma-delimited list into a normalized tuple."""
    if value is None:
        return None
    items = tuple(part.strip() for part in value.split(",") if part.strip())
    return items or None


def normalize_tag_filters(tags: tuple[str, ...] | None) -> tuple[str, ...] | None:
    """Normalize tag filters to canonical lowercase matching semantics."""
    if tags is None:
        return None
    normalized = _normalize_tags(tags)
    return normalized or None


def task_matches_tag_filters(
    *,
    task_tags: Sequence[str],
    tag_filters: tuple[str, ...] | None,
    any_tag: bool = True,
) -> bool:
    """Return whether a task's tags match normalized filter tags."""
    if tag_filters is None:
        return True
    requested = set(tag_filters)
    task_values = set(task_tags)
    return bool(task_values & requested) if any_tag else requested.issubset(task_values)


def missing_scope_tags_for_task(
    task: DbTask,
    *,
    tag_filters: tuple[str, ...] | None,
    any_tag: bool,
) -> tuple[str, ...]:
    """Return the filter tags missing from ``task`` for the active matching mode."""
    normalized = normalize_tag_filters(tag_filters)
    if normalized is None:
        return ()
    if any_tag:
        return normalized
    task_tag_values = set(task.tags)
    return tuple(tag for tag in normalized if tag not in task_tag_values)


def collect_scoped_tag_scope_gaps(
    store: SqliteTaskStore,
    *,
    tag_filters: tuple[str, ...] | None,
    any_tag: bool,
    config: Any | None = None,
    git: Any | None = None,
    target_branch: str | None = None,
) -> list[ScopedTagScopeGap]:
    """Return in-scope owners blocked by detectable out-of-scope derived children."""
    normalized = normalize_tag_filters(tag_filters)
    if normalized is None:
        return []

    def _task_is_in_scope_runnable_or_running(task: DbTask) -> bool:
        if task.id is None or task.task_type == "internal":
            return False
        if not task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized, any_tag=any_tag):
            return False
        if task.status == "in_progress":
            return True
        if task.status == "pending" and not store.is_task_blocked(task)[0]:
            return True
        return False

    def _scope_gap_is_suppressed_by_in_scope_dependency(task: DbTask) -> bool:
        if task.status != "pending":
            return False
        blocked, blocking_id, _blocking_status = store.is_task_blocked(task)
        if not blocked or blocking_id is None:
            return False
        blocking_task = store.get(blocking_id)
        if blocking_task is None:
            return False
        return _task_is_in_scope_runnable_or_running(blocking_task)

    def _task_merge_state(task: DbTask) -> str | None:
        if task.id is None:
            return task.merge_status
        unit = store.resolve_merge_unit_for_task(task.id)
        if unit is None:
            return task.merge_status
        return effective_no_work_merge_state(task, unit.state)

    def _task_is_in_scope_and_unfinished(task: DbTask) -> bool:
        if task.id is None or task.task_type == "internal" or task.status == "dropped":
            return False
        if not task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized, any_tag=any_tag):
            return False
        return not task_is_complete_for_lifecycle(task, merge_state=_task_merge_state(task))

    def _scope_gap_is_suppressed_by_terminal_owner_future_scoped_child(
        owner: DbTask,
        child: DbTask,
        lineage_members: Sequence[DbTask],
    ) -> bool:
        if not child.tags:
            return False
        if not task_is_complete_for_lifecycle(owner, merge_state=_task_merge_state(owner)):
            return False
        child_id = child.id
        if child_id is None:
            return False
        for member in (owner, *lineage_members):
            member_id = member.id
            if member_id is None or member_id == child_id:
                continue
            if not _task_is_in_scope_and_unfinished(member):
                continue
            blocked, blocking_id, _blocking_status = store.is_task_blocked(member)
            if blocked and blocking_id == child_id:
                return False
        return True

    gaps: list[ScopedTagScopeGap] = []
    seen_blocking_child_ids: set[str] = set()
    owner_rows, _read_context = query_lineage_owner_rows_in_read_session(
        store,
        LineageOwnerQuery(
            limit=None,
            tags=normalized,
            any_tag=any_tag,
            include_skipped=True,
            exclude_dropped_from_planning=True,
        ),
        config=config,
        git=git,
        target_branch=target_branch,
    )
    members_by_owner_id = {}
    for row in owner_rows:
        owner_id = row.owner_task.id
        if owner_id is None:
            continue
        merged_candidates: dict[str, DbTask] = {}
        for task in row.members:
            if task.id is None or task.id == owner_id or task.task_type == "internal":
                continue
            merged_candidates[task.id] = task
        for task in lineage.walk_lineage_descendants(store, row.owner_task):
            if task.id is None or task.id == owner_id or task.task_type == "internal":
                continue
            merged_candidates[task.id] = task
        members_by_owner_id[owner_id] = tuple(merged_candidates.values())
    candidate_owners_by_id: dict[str, DbTask] = {}
    for task in store.get_all():
        if (
            task.id is None
            or task.task_type == "internal"
            or task.status == "dropped"
            or not task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized, any_tag=any_tag)
        ):
            continue
        candidate_owners_by_id[task.id] = task
    for row in owner_rows:
        owner_id = row.owner_task.id
        if owner_id is not None:
            candidate_owners_by_id.setdefault(owner_id, row.owner_task)
    candidate_owners = sorted(
        candidate_owners_by_id.values(),
        key=lambda task: (
            task.created_at or datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(task.id or ""),
        ),
    )
    for owner in candidate_owners:
        owner_id = owner.id
        if owner_id is None or owner.task_type == "internal" or owner.status == "dropped":
            continue

        explicit_members = members_by_owner_id.get(owner_id)
        if explicit_members is not None:
            candidate_members = explicit_members
        else:
            candidate_members = tuple(
                task
                for task in lineage.walk_lineage_descendants(store, owner)
                if task.id is not None and task.id != owner_id and task.task_type != "internal"
            )

        candidates = sorted(
            candidate_members,
            key=lambda task: (
                0 if task.status == "in_progress" else 1,
                task_id_numeric_key(task.id or ""),
            ),
        )
        for task in candidates:
            task_id = task.id
            assert task_id is not None
            if task_id in seen_blocking_child_ids:
                continue
            if task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized, any_tag=any_tag):
                continue
            if _scope_gap_is_suppressed_by_in_scope_dependency(task):
                continue
            if _scope_gap_is_suppressed_by_terminal_owner_future_scoped_child(owner, task, candidate_members):
                continue
            if task.status == "in_progress":
                blocking_state = "running"
            elif task.status == "pending":
                blocking_state = "blocked" if store.is_task_blocked(task)[0] else "runnable"
            else:
                continue

            missing_tags = missing_scope_tags_for_task(
                task,
                tag_filters=normalized,
                any_tag=any_tag,
            )
            if missing_tags:
                hint_tags = missing_tags if not any_tag else missing_tags[:1]
                hint_flags = " ".join(f"--add-tag {tag}" for tag in hint_tags)
                suggested = f"uv run gza edit {task_id} {hint_flags}"
            else:
                suggested = "uv run gza watch"
            gaps.append(
                ScopedTagScopeGap(
                    owner_id=owner_id,
                    blocking_child_id=task_id,
                    child_task_type=task.task_type,
                    child_status=task.status,
                    child_tags=tuple(task.tags),
                    missing_filter_tags=missing_tags,
                    suggested_next_command=suggested,
                    blocking_state=blocking_state,
                )
            )
            seen_blocking_child_ids.add(task_id)
            break
    return gaps


def projection_fields(projection: ProjectionSpec, *, scope: QueryScope) -> tuple[str, ...]:
    """Resolve the effective projection fields for a query scope."""
    if projection.fields is not None:
        return projection.fields

    preset_fields = _PROJECTION_PRESET_FIELDS.get(projection.preset)
    if preset_fields is None:
        return _LINEAGE_DEFAULT_FIELDS if scope == "lineages" else _TASK_DEFAULT_FIELDS

    if scope == "tasks":
        return tuple(
            field_name
            for field_name in preset_fields
            if field_name not in {"member_ids", "unresolved_ids"}
        )
    return preset_fields


def apply_projection_values(
    values: Mapping[str, object],
    projection: ProjectionSpec,
    *,
    scope: QueryScope,
) -> dict[str, object]:
    """Project a value mapping using the shared preset/field rules."""
    allowed_fields = set(projection_fields(projection, scope=scope))
    return {key: value for key, value in values.items() if key in allowed_fields}


def _history_filter_cls() -> Any:
    from gza.query import HistoryFilter

    return HistoryFilter


def _resolve_lineage_root(store: SqliteTaskStore, task: DbTask) -> DbTask:
    from gza.query import resolve_lineage_root

    return resolve_lineage_root(store, task)


def _action_subject_task_id(action: Mapping[str, object]) -> str | None:
    value = action.get("subject_task_id")
    if isinstance(value, str) and value.strip():
        return value
    return None


def _action_is_needs_attention(action: Mapping[str, object]) -> bool:
    value = action.get("needs_attention_reason")
    if isinstance(value, str) and value:
        return True
    action_type = str(action.get("type", ""))
    return action_type in {
        "awaiting_human",
        "needs_discussion",
        "max_cycles_reached",
        "max_improve_attempts",
        "automatic_recovery_disabled",
        "manual_review_required",
    }


def _build_lineage_tree(store: SqliteTaskStore, root_task: DbTask, *, max_depth: int | None) -> Any:
    from gza.query import build_lineage_tree

    return build_lineage_tree(store, root_task, max_depth=max_depth)


def _flatten_lineage_tree(tree: Any) -> list[DbTask]:
    from gza.query import flatten_lineage_tree

    return flatten_lineage_tree(tree)


def _query_incomplete(store: SqliteTaskStore, history_filter: Any, *, target_branch: str | None = None) -> list[Any]:
    from gza.query import query_incomplete

    return query_incomplete(store, history_filter, target_branch=target_branch)


def _prune_lineage_tree_to_ids(tree: Any, keep_ids: set[str]) -> Any:
    def _filter(node: Any, *, is_root: bool) -> Any | None:
        kept_children: list[Any] = []
        for child in node.children:
            filtered_child = _filter(child, is_root=False)
            if filtered_child is not None:
                kept_children.append(filtered_child)

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

    filtered = _filter(tree, is_root=True)
    assert filtered is not None
    return filtered


def _is_shared_branch_descendant_query(task: DbTask, root_task: DbTask) -> bool:
    from gza.query import _is_shared_branch_descendant

    return _is_shared_branch_descendant(task, root_task)


def _resolve_unmerged_branch_owner(store: SqliteTaskStore, task: DbTask) -> DbTask:
    from gza.query import resolve_unmerged_branch_owner

    return resolve_unmerged_branch_owner(store, task)


def _resolve_lineage_owner_task(store: SqliteTaskStore, task: DbTask) -> DbTask:
    from gza.query import resolve_lineage_owner_task

    return resolve_lineage_owner_task(store, task)


def _get_reviews_for_root(store: SqliteTaskStore, root_task: DbTask) -> list[DbTask]:
    from gza.query import get_reviews_for_root

    return get_reviews_for_root(store, root_task)


def _is_lineage_complete(store: SqliteTaskStore, task: DbTask) -> bool:
    from gza.query import is_lineage_complete

    return is_lineage_complete(task, store=store)


__all__ = [
    "apply_projection_values",
    "DateFilter",
    "PresentationSpec",
    "ProjectionSpec",
    "SortSpec",
    "TaskProjectionPreset",
    "TaskQuery",
    "TaskQueryPresets",
    "TaskQueryResult",
    "TaskQueryService",
    "TaskRow",
    "LineageRow",
    "TextFilter",
    "normalize_tag_filters",
    "parse_csv",
    "projection_fields",
    "task_matches_tag_filters",
]
