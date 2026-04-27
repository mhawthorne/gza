"""Unified task query service for filter/projection/presentation workflows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal

from gza.db import SqliteTaskStore, Task as DbTask, _normalize_tags, task_id_numeric_key

QueryScope = Literal["tasks", "lineages"]
DateField = Literal["created", "completed", "effective"]
PresentationMode = Literal["flat", "grouped", "tree", "one_line", "json"]


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
    task_types: tuple[str, ...] | None = None
    exclude_task_types: tuple[str, ...] | None = None
    lifecycle_state: tuple[str, ...] | None = None
    merge_chain_state: tuple[str, ...] | None = None
    dependency_state: tuple[str, ...] | None = None
    related_to: str | None = None
    lineage_of: str | None = None
    root_ids: tuple[str, ...] | None = None
    branch_owner_ids: tuple[str, ...] | None = None
    groups: tuple[str | None, ...] | None = None
    tag_filters: tuple[str, ...] | None = None
    any_tag: bool = False
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
    values: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskQueryResult:
    """Query result container with renderer helpers."""

    query: TaskQuery
    rows: tuple[TaskRow | LineageRow, ...]

    def render(self, mode: PresentationMode | None = None) -> str:
        """Render using the configured (or overridden) presentation mode."""
        from gza.task_presenters import render_query_result

        final_mode = mode or self.query.presentation.mode
        return render_query_result(self, final_mode)

    def to_json(self) -> list[dict[str, object]]:
        """Return JSON-serializable row dictionaries."""
        return [dict(row.values) for row in self.rows]


class TaskProjectionPreset:
    """Named projection presets."""

    HISTORY_DEFAULT = "history_default"
    SEARCH_DEFAULT = "search_default"
    INCOMPLETE_SUMMARY = "incomplete_summary"
    LINEAGE_FULL = "lineage_full"
    SHOW_DETAIL = "show_detail"
    JSON_MINIMAL = "json_minimal"
    QUEUE_DEFAULT = "queue_default"


_TASK_DEFAULT_FIELDS: tuple[str, ...] = (
    "id",
    "prompt",
    "status",
    "task_type",
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
        "group",
        "urgent",
        "queue_position",
        "blocked",
        "blocking_id",
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
        task_types: tuple[str, ...] | None = None,
        date_filter: DateFilter | None = None,
        related_to: str | None = None,
        lineage_of: str | None = None,
        root_ids: tuple[str, ...] | None = None,
    ) -> TaskQuery:
        return TaskQuery(
            scope="tasks",
            limit=limit,
            text=TextFilter(contains=term, fields=("prompt",)),
            statuses=statuses,
            task_types=task_types,
            related_to=related_to,
            lineage_of=lineage_of,
            root_ids=root_ids,
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
        group: str | None = None,
        tags: tuple[str, ...] | None = None,
        any_tag: bool = False,
    ) -> TaskQuery:
        group_tags = (group,) if group is not None else ()
        combined_tags = normalize_tag_filters((*group_tags, *(tags or ())))
        return TaskQuery(
            scope="tasks",
            limit=limit,
            statuses=("pending",),
            exclude_task_types=("internal",),
            dependency_state=("unblocked",),
            groups=None if group is None else (group,),
            tag_filters=combined_tags,
            any_tag=any_tag,
            pickup_only=True,
            sort=SortSpec(field="pickup_order", descending=False),
            projection=ProjectionSpec(preset=TaskProjectionPreset.QUEUE_DEFAULT),
            presentation=PresentationSpec(mode="flat"),
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
            lineages = self._collect_lineages(query)
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
            return TaskQueryResult(query=query, rows=lineage_rows)

        tasks = self._collect_tasks(query)
        task_rows = tuple(self._project_task_row(task, query) for task in tasks)
        return TaskQueryResult(query=query, rows=task_rows)

    def _collect_tasks(self, query: TaskQuery) -> list[DbTask]:
        tasks = self._base_task_candidates(query)
        tasks = self._apply_task_filters(tasks, query)
        if query.sort.field != "pickup_order":
            tasks.sort(key=lambda task: self._sort_key(task, query.sort), reverse=query.sort.descending)
        if query.limit is not None:
            tasks = tasks[: query.limit]
        return tasks

    def _base_task_candidates(self, query: TaskQuery) -> list[DbTask]:
        if query.scope != "tasks":
            return list(self._store.get_all())

        tags = query.tag_filters
        if tags is None and query.groups:
            tags = tuple(group for group in query.groups if group is not None)
        if query.pickup_only:
            return list(self._store.get_pending_pickup(limit=None, tags=tags, any_tag=query.any_tag))
        if query.sort.field == "pickup_order":
            return list(self._store.get_pending(limit=None, tags=tags, any_tag=query.any_tag))
        return list(self._store.get_all())

    def _collect_lineages(self, query: TaskQuery) -> list[LineageRow]:
        use_incomplete_rollup = bool(
            query.lifecycle_state and "incomplete" in query.lifecycle_state
        )

        rows: list[LineageRow]
        if use_incomplete_rollup:
            if query.task_types and len(query.task_types) > 1:
                raise ValueError(
                    "lineages scope with lifecycle_state=incomplete supports at most one task type"
                )
            f = _history_filter_cls()(
                limit=None,
                task_type=(query.task_types[0] if query.task_types and len(query.task_types) == 1 else None),
                days=query.date_filter.days if query.date_filter else None,
                start_date=(query.date_filter.start.isoformat() if query.date_filter and query.date_filter.start else None),
                end_date=(query.date_filter.end.isoformat() if query.date_filter and query.date_filter.end else None),
                date_field=(query.date_filter.field if query.date_filter else "effective"),
            )
            incomplete = _query_incomplete(self._store, f)
            unresolved_by_owner: dict[str, list[DbTask]] = {}
            incomplete_owner_by_id: dict[str, DbTask] = {}
            root_by_owner_id: dict[str, DbTask] = {}
            tree_by_root_id: dict[str, Any] = {}

            for item in incomplete:
                root = item.root
                if root.id is None:
                    continue
                tree_by_root_id[root.id] = item.tree

                for task in item.unresolved_tasks:
                    owner = self._resolve_branch_owner(task)
                    owner_id = owner.id
                    if owner_id is None:
                        continue
                    unresolved_by_owner.setdefault(owner_id, []).append(task)
                    incomplete_owner_by_id[owner_id] = owner
                    root_by_owner_id[owner_id] = root

            rows = []
            for owner_id, unresolved_tasks in unresolved_by_owner.items():
                owner = incomplete_owner_by_id[owner_id]
                root = root_by_owner_id[owner_id]
                root_id = root.id
                if root_id is None:
                    continue

                unresolved_by_id = {
                    task.id: task for task in unresolved_tasks if task.id is not None
                }
                owner_unresolved = tuple(
                    sorted(
                        unresolved_by_id.values(),
                        key=lambda task: self._sort_key(task, query.sort),
                        reverse=query.sort.descending,
                    )
                )

                keep_ids = {task_id for task_id in unresolved_by_id}
                if owner.id is not None:
                    keep_ids.add(owner.id)
                keep_ids.add(root_id)

                root_tree = tree_by_root_id.get(root_id) or _build_lineage_tree(
                    self._store,
                    root,
                    max_depth=None,
                )
                pruned_tree = _prune_lineage_tree_to_ids(root_tree, keep_ids)
                members = tuple(_flatten_lineage_tree(pruned_tree))

                rows.append(
                    LineageRow(
                        owner_task=owner,
                        members=members,
                        tree=pruned_tree,
                        unresolved_tasks=owner_unresolved,
                        values={},
                    )
                )
        else:
            grouped: dict[str, list[DbTask]] = {}
            owner_by_id: dict[str, DbTask] = {}
            for task in self._collect_tasks(
                TaskQuery(
                    scope="tasks",
                    limit=None,
                    text=query.text,
                    statuses=query.statuses,
                    task_types=query.task_types,
                    exclude_task_types=query.exclude_task_types,
                    lifecycle_state=query.lifecycle_state,
                    merge_chain_state=query.merge_chain_state,
                    dependency_state=query.dependency_state,
                    related_to=query.related_to,
                    lineage_of=query.lineage_of,
                    root_ids=query.root_ids,
                    branch_owner_ids=query.branch_owner_ids,
                    groups=query.groups,
                    tag_filters=query.tag_filters,
                    any_tag=query.any_tag,
                    pickup_only=query.pickup_only,
                    date_filter=query.date_filter,
                    sort=query.sort,
                    projection=query.projection,
                    presentation=query.presentation,
                )
            ):
                owner = self._resolve_branch_owner(task)
                owner_id = owner.id
                if owner_id is None:
                    continue
                grouped.setdefault(owner_id, []).append(task)
                owner_by_id[owner_id] = owner

            rows = []
            for owner_id, owner_members in grouped.items():
                owner = owner_by_id[owner_id]
                root = _resolve_lineage_root(self._store, owner)
                rows.append(
                    LineageRow(
                        owner_task=owner,
                        members=tuple(
                            sorted(owner_members, key=lambda t: self._sort_key(t, DEFAULT_SORT), reverse=True)
                        ),
                        tree=_build_lineage_tree(self._store, root, max_depth=None),
                    )
                )

        rows = self._apply_lineage_filters(rows, query)
        rows.sort(
            key=lambda row: self._sort_key(row.owner_task, query.sort),
            reverse=query.sort.descending,
        )
        if query.limit is not None:
            rows = rows[: query.limit]
        return rows

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

        if query.related_to is not None:
            related_task = self._store.get(query.related_to)
            if related_task is None:
                return []
            root = _resolve_lineage_root(self._store, related_task)
            related_ids = {
                task.id
                for task in _flatten_lineage_tree(_build_lineage_tree(self._store, root, max_depth=None))
                if task.id is not None
            }
            filtered = [task for task in filtered if task.id in related_ids]

        if query.branch_owner_ids is not None:
            allowed_owners = set(query.branch_owner_ids)
            filtered = [
                task
                for task in filtered
                if self._resolve_branch_owner(task).id in allowed_owners
            ]

        if query.groups is not None:
            allowed_groups = set(query.groups)
            filtered = [task for task in filtered if task.group in allowed_groups]

        if query.tag_filters is not None:
            required = normalize_tag_filters(query.tag_filters)
            filtered = [
                task
                for task in filtered
                if task_matches_tag_filters(task_tags=task.tags, tag_filters=required, any_tag=query.any_tag)
            ]

        if query.merge_chain_state is not None:
            merge_states = set(query.merge_chain_state)
            filtered = [task for task in filtered if self._matches_merge_chain_state(task, merge_states)]

        if query.dependency_state is not None:
            dep_states = set(query.dependency_state)
            filtered = [task for task in filtered if self._matches_dependency_state(task, dep_states)]

        return filtered

    def _apply_lineage_filters(self, rows: Sequence[LineageRow], query: TaskQuery) -> list[LineageRow]:
        filtered = list(rows)

        if query.root_ids is not None:
            roots = set(query.root_ids)
            filtered = [
                row
                for row in filtered
                if (root := _resolve_lineage_root(self._store, row.owner_task)).id in roots
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

        if query.related_to is not None:
            task = self._store.get(query.related_to)
            if task is None:
                return []
            root = _resolve_lineage_root(self._store, task)
            lineage_ids = {
                item.id
                for item in _flatten_lineage_tree(_build_lineage_tree(self._store, root, max_depth=None))
                if item.id is not None
            }
            filtered = [
                row
                for row in filtered
                if any(member.id in lineage_ids for member in row.members)
            ]

        return filtered

    def _project_task_row(self, task: DbTask, query: TaskQuery) -> TaskRow:
        root = _resolve_lineage_root(self._store, task)
        branch_owner = self._resolve_branch_owner(task)
        blocked, blocking_id, blocking_status = self._store.is_task_blocked(task)
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
            "created_at": task.created_at,
            "completed_at": task.completed_at,
            "effective_at": _effective_at(task),
            "group": task.group,
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
            "blocked": blocked,
            "blocking_id": blocking_id,
            "blocking_status": blocking_status,
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

        next_action_type: str | None = None
        next_action_reason: str | None = None
        next_action_owner_id: str | None = None

        if query.projection.preset == TaskProjectionPreset.INCOMPLETE_SUMMARY:
            action = self._project_next_action(
                owner,
                config=config,
                git=git,
                target_branch=target_branch,
            )
            next_action_type = action.get("type") if action else None
            next_action_reason = action.get("description") if action else None
            next_action_owner_id = owner.id

        values: dict[str, object] = {
            "id": owner.id,
            "prompt": owner.prompt,
            "status": owner.status,
            "task_type": owner.task_type,
            "created_at": owner.created_at,
            "completed_at": owner.completed_at,
            "effective_at": _effective_at(owner),
            "group": owner.group,
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
            "blocked": self._store.is_task_blocked(owner)[0],
            "blocking_id": self._store.is_task_blocked(owner)[1],
            "member_ids": [member.id for member in row.members if member.id is not None],
            "unresolved_ids": [task.id for task in row.unresolved_tasks if task.id is not None],
        }

        return LineageRow(
            owner_task=row.owner_task,
            members=row.members,
            tree=row.tree,
            unresolved_tasks=row.unresolved_tasks,
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
        )

    def _latest_review_verdict(self, owner_task: DbTask) -> str | None:
        reviews = _get_reviews_for_root(self._store, owner_task)
        completed = [review for review in reviews if review.status == "completed"]
        if not completed:
            return None
        latest = completed[0]
        return latest.output_content

    def _resolve_branch_owner(self, task: DbTask) -> DbTask:
        root = _resolve_lineage_root(self._store, task)
        if _is_shared_branch_descendant_query(task, root):
            return root
        return task

    def _branch_merge_state(self, owner_task: DbTask) -> str:
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
        if "complete" in lifecycle and _is_lineage_complete(task):
            return True
        if "incomplete" in lifecycle:
            if task.status in {"pending", "in_progress"}:
                return True
            if not _is_lineage_complete(task):
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
        if "merged" in merge_states and owner_state == "merged":
            return True
        if "unmerged" in merge_states and (
            owner_state == "unmerged" or task.status == "unmerged"
        ):
            return True
        if "needs_merge" in merge_states and owner_state == "needs_merge":
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

    def _apply_projection(
        self,
        values: Mapping[str, object],
        projection: ProjectionSpec,
        *,
        scope: QueryScope,
    ) -> dict[str, object]:
        allowed_fields = set(self._projection_fields(projection, scope))
        return {key: value for key, value in values.items() if key in allowed_fields}


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
    any_tag: bool,
) -> bool:
    """Return whether a task's tags match normalized filter tags."""
    if tag_filters is None:
        return True
    requested = set(tag_filters)
    task_values = set(task_tags)
    return bool(task_values & requested) if any_tag else requested.issubset(task_values)


def _history_filter_cls() -> Any:
    from gza.query import HistoryFilter

    return HistoryFilter


def _resolve_lineage_root(store: SqliteTaskStore, task: DbTask) -> DbTask:
    from gza.query import resolve_lineage_root

    return resolve_lineage_root(store, task)


def _build_lineage_tree(store: SqliteTaskStore, root_task: DbTask, *, max_depth: int | None) -> Any:
    from gza.query import build_lineage_tree

    return build_lineage_tree(store, root_task, max_depth=max_depth)


def _flatten_lineage_tree(tree: Any) -> list[DbTask]:
    from gza.query import flatten_lineage_tree

    return flatten_lineage_tree(tree)


def _query_incomplete(store: SqliteTaskStore, history_filter: Any) -> list[Any]:
    from gza.query import query_incomplete

    return query_incomplete(store, history_filter)


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


def _get_reviews_for_root(store: SqliteTaskStore, root_task: DbTask) -> list[DbTask]:
    from gza.query import get_reviews_for_root

    return get_reviews_for_root(store, root_task)


def _is_lineage_complete(task: DbTask) -> bool:
    from gza.query import is_lineage_complete

    return is_lineage_complete(task)


__all__ = [
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
    "task_matches_tag_filters",
]
