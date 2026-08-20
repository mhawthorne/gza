"""Shared query and presentation model for the task list endpoints."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal, cast
from urllib.parse import urlencode

from gza.db import SqliteTaskStore, task_updated_at
from gza.task_query import (
    SortSpec,
    TaskQueryPresets,
    TaskQueryService,
    TaskRow,
    normalize_tag_filters,
)
from gza.task_types import ALL_TASK_STATUSES, ALL_TASK_TYPES

SortField = Literal["created", "updated"]
SortDirection = Literal["asc", "desc"]

TASK_STATUSES = ALL_TASK_STATUSES


@dataclass(frozen=True)
class TaskListFilters:
    """Normalized request parameters accepted by both task list routes."""

    prompt: str = ""
    tags: tuple[str, ...] = ()
    statuses: tuple[str, ...] = ()
    task_types: tuple[str, ...] = ()
    untagged: bool = False
    sort: SortField = "updated"
    direction: SortDirection = "desc"

    @property
    def has_selection(self) -> bool:
        """Whether bulk operations are constrained by at least one real filter."""
        return bool(
            self.prompt.strip()
            or self.tags
            or self.statuses
            or self.task_types
            or self.untagged
        )

    def query_pairs(self, **overrides: str) -> list[tuple[str, str]]:
        """Return form-compatible query pairs, preserving repeated filters."""
        pairs: list[tuple[str, str]] = []
        if self.prompt:
            pairs.append(("q", self.prompt))
        pairs.extend(("tag", tag) for tag in self.tags)
        pairs.extend(("status", status) for status in self.statuses)
        pairs.extend(("type", task_type) for task_type in self.task_types)
        if self.untagged:
            pairs.append(("untagged", "true"))
        values = {"sort": self.sort, "direction": self.direction, **overrides}
        pairs.extend((key, value) for key, value in values.items())
        return pairs

    def url(self, path: str, **overrides: str) -> str:
        return f"{path}?{urlencode(self.query_pairs(**overrides))}"


@dataclass(frozen=True)
class TaskListResult:
    """One query result shared by the HTML and JSON representations."""

    rows: list[dict[str, object]]
    known_tags: tuple[str, ...]
    filters: TaskListFilters


def query_task_list(
    store: SqliteTaskStore,
    filters: TaskListFilters,
    *,
    now: datetime | None = None,
) -> TaskListResult:
    """Run the same query service and filter semantics as ``gza search``."""
    if filters.tags and filters.untagged:
        raise ValueError("tag and untagged filters cannot be combined")

    query = TaskQueryPresets.search(
        filters.prompt,
        limit=None,
        statuses=filters.statuses or None,
        task_types=filters.task_types or None,
    )
    query = replace(
        query,
        tag_filters=normalize_tag_filters(filters.tags or None),
        untagged_only=filters.untagged,
        any_tag=True,
        sort=SortSpec(
            field="created_at" if filters.sort == "created" else "updated_at",
            descending=filters.direction == "desc",
        ),
    )
    result = TaskQueryService(store).run(query, all_projects=True)
    rendered_at = now or datetime.now(UTC)
    rows = [_task_row(cast(TaskRow, row), now=rendered_at) for row in result.rows]
    return TaskListResult(
        rows=rows,
        known_tags=store.list_tags(all_projects=True),
        filters=filters,
    )


def _task_row(row: TaskRow, *, now: datetime) -> dict[str, object]:
    task = row.task
    updated_at = task_updated_at(task)
    assert task.id is not None
    detail_url = f"/projects/{row.project_id}/tasks/{task.id}"
    return {
        "id": task.id,
        "project_id": row.project_id,
        "detail_url": detail_url,
        "api_url": f"/api{detail_url}",
        "type": task.task_type,
        "status": task.status,
        "tags": list(task.tags),
        "prompt": task.prompt,
        "prompt_excerpt": _excerpt(task.prompt),
        "created_at": task.created_at,
        "updated_at": updated_at,
        "age": _format_age(updated_at, now=now),
    }


def _excerpt(prompt: str, *, limit: int = 160) -> str:
    normalized = " ".join(prompt.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def _format_age(timestamp: datetime | None, *, now: datetime) -> str:
    if timestamp is None:
        return "unknown"
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    seconds = max(0, int((now.astimezone(UTC) - timestamp.astimezone(UTC)).total_seconds()))
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 30:
        return f"{days}d ago"
    months = days // 30
    if months < 12:
        return f"{months}mo ago"
    return f"{days // 365}y ago"


TASK_TYPES = ALL_TASK_TYPES
