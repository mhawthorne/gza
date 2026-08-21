"""One page answering "what is happening on this tag right now?".

The operator runs ``gza watch --tag <tag>`` and then wants three things at once:
what has landed lately, what is running, and what is still waiting. Each of
those is a separate CLI command today, so this assembles them into one view.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from urllib.parse import urlencode

from gza.db import MergeUnit, SqliteTaskStore, Task, task_updated_at

from .merge_unit_detail import merge_unit_url


@dataclass(frozen=True)
class Window:
    """One selectable slice of recent history."""

    key: str
    label: str
    hours: float

    @property
    def delta(self) -> timedelta:
        return timedelta(hours=self.hours)


WINDOWS: tuple[Window, ...] = (
    Window("1h", "Last hour", 1),
    Window("6h", "Last 6 hours", 6),
    Window("12h", "Last 12 hours", 12),
    Window("24h", "Last 24 hours", 24),
    Window("3d", "Last 3 days", 24 * 3),
    Window("7d", "Last 7 days", 24 * 7),
)
DEFAULT_WINDOW_KEY = "24h"
_WINDOWS_BY_KEY = {window.key: window for window in WINDOWS}


def resolve_window(key: str | None) -> Window:
    """Return the requested window, falling back to the default.

    An unrecognized key lands on the default rather than erroring: this comes
    from a query string, and a stale bookmark should still show the page.
    """
    return _WINDOWS_BY_KEY.get(key or "", _WINDOWS_BY_KEY[DEFAULT_WINDOW_KEY])


@dataclass(frozen=True)
class TaskSummary:
    """A task as it appears in one of the dashboard's lists."""

    id: str
    project_id: str
    detail_url: str
    task_type: str
    status: str
    prompt_excerpt: str
    created_at: datetime | None
    started_at: datetime | None
    updated_at: datetime | None
    age: str
    merge_unit_id: str | None = None
    merge_unit_url: str | None = None

    def json_record(self) -> dict[str, object]:
        return {
            "id": self.id,
            "project_id": self.project_id,
            "detail_url": self.detail_url,
            "task_type": self.task_type,
            "status": self.status,
            "prompt_excerpt": self.prompt_excerpt,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "age": self.age,
            "merge_unit_id": self.merge_unit_id,
            "merge_unit_url": self.merge_unit_url,
        }


@dataclass(frozen=True)
class MergedSummary:
    """A merge unit that landed inside the selected window."""

    id: str
    project_id: str
    merge_unit_url: str
    source_branch: str
    target_branch: str
    merged_at: datetime | None
    merge_source: str | None
    owner_task_id: str | None
    owner_detail_url: str | None
    subject: str
    files_changed: int | None
    lines_added: int | None
    lines_removed: int | None

    def json_record(self) -> dict[str, object]:
        return {
            "id": self.id,
            "project_id": self.project_id,
            "merge_unit_url": self.merge_unit_url,
            "source_branch": self.source_branch,
            "target_branch": self.target_branch,
            "merged_at": self.merged_at,
            "merge_source": self.merge_source,
            "owner_task_id": self.owner_task_id,
            "owner_detail_url": self.owner_detail_url,
            "subject": self.subject,
            "files_changed": self.files_changed,
            "lines_added": self.lines_added,
            "lines_removed": self.lines_removed,
        }


@dataclass(frozen=True)
class WatchScope:
    """What a live watch process reports it is covering."""

    project_id: str
    owner_pid: int
    tags: tuple[str, ...]
    batch_size: int | None
    poll_seconds: float | None
    started_at: datetime

    def json_record(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "owner_pid": self.owner_pid,
            "tags": list(self.tags),
            "batch_size": self.batch_size,
            "poll_seconds": self.poll_seconds,
            "started_at": self.started_at,
        }


@dataclass(frozen=True)
class TagDashboard:
    """Everything the tag page renders."""

    tag: str
    window: Window
    merged: tuple[MergedSummary, ...]
    in_flight: tuple[TaskSummary, ...]
    queued: tuple[TaskSummary, ...]
    blocked: tuple[TaskSummary, ...]
    watch_scopes: tuple[WatchScope, ...]
    known_tags: tuple[str, ...]
    rendered_at: datetime

    @property
    def watch_is_running_on_this_tag(self) -> bool:
        return any(self.tag in scope.tags for scope in self.watch_scopes)

    def url(self, window_key: str | None = None) -> str:
        query = urlencode({"window": window_key or self.window.key})
        return f"/tags/{self.tag}?{query}"

    def json_record(self) -> dict[str, object]:
        return {
            "tag": self.tag,
            "window": self.window.key,
            "window_label": self.window.label,
            "merged": [item.json_record() for item in self.merged],
            "in_flight": [item.json_record() for item in self.in_flight],
            "queued": [item.json_record() for item in self.queued],
            "blocked": [item.json_record() for item in self.blocked],
            "watch_scopes": [scope.json_record() for scope in self.watch_scopes],
            "watch_is_running_on_this_tag": self.watch_is_running_on_this_tag,
            "rendered_at": self.rendered_at,
        }


def active_watch_scopes(store: SqliteTaskStore, *, now: datetime | None = None) -> tuple[WatchScope, ...]:
    """Return the scope of every watch currently running, newest first."""
    sessions = store.list_watch_sessions(active_only=True, all_projects=True, now=now)
    return tuple(
        WatchScope(
            project_id=session.project_id,
            owner_pid=session.owner_pid,
            tags=session.tags,
            batch_size=session.batch_size,
            poll_seconds=session.poll_seconds,
            started_at=session.started_at,
        )
        for session in sessions
    )


def default_dashboard_tag(store: SqliteTaskStore, *, now: datetime | None = None) -> str | None:
    """Return the tag a running watch is covering, if exactly one is discoverable.

    When a watch runs on several tags the first is the natural default: that is
    the order the operator typed them in.
    """
    for scope in active_watch_scopes(store, now=now):
        if scope.tags:
            return scope.tags[0]
    return None


def query_tag_dashboard(
    store: SqliteTaskStore,
    tag: str,
    *,
    window: Window | None = None,
    now: datetime | None = None,
) -> TagDashboard:
    """Assemble the merged / in-flight / queued view for one tag."""
    selected = window or _WINDOWS_BY_KEY[DEFAULT_WINDOW_KEY]
    rendered_at = _as_utc(now or datetime.now(UTC))
    since = rendered_at - selected.delta

    merged: list[MergedSummary] = []
    in_flight: list[TaskSummary] = []
    queued: list[TaskSummary] = []
    blocked: list[TaskSummary] = []

    for project_store in store.project_query_stores():
        project_id = project_store.project_id
        merged.extend(_merged_for_tag(project_store, project_id, tag, since=since))
        in_flight.extend(
            _task_summary(task, project_id, now=rendered_at)
            for task in project_store.get_in_progress()
            if tag in task.tags
        )
        # The pickup lane is what a worker would actually take next. Everything
        # else that is pending is waiting on a dependency, and reporting only
        # the lane would show "nothing queued" while real work sits behind it.
        runnable = project_store.get_pending_pickup(tags=(tag,))
        runnable_ids = {task.id for task in runnable}
        queued.extend(
            _task_summary(task, project_id, now=rendered_at) for task in runnable
        )
        blocked.extend(
            _task_summary(task, project_id, now=rendered_at)
            for task in project_store.get_pending(tags=(tag,))
            if task.id not in runnable_ids
        )

    merged.sort(key=lambda item: (item.merged_at or since), reverse=True)
    in_flight.sort(key=lambda item: (item.started_at or item.created_at or rendered_at))

    return TagDashboard(
        tag=tag,
        window=selected,
        merged=tuple(merged),
        in_flight=tuple(in_flight),
        queued=tuple(queued),
        blocked=tuple(blocked),
        watch_scopes=active_watch_scopes(store, now=rendered_at),
        known_tags=store.list_tags(all_projects=True),
        rendered_at=rendered_at,
    )


def _merged_for_tag(
    project_store: SqliteTaskStore,
    project_id: str,
    tag: str,
    *,
    since: datetime,
) -> list[MergedSummary]:
    """Return units merged since ``since`` whose owner task carries ``tag``.

    A unit's tag is its owner task's tag: the unit itself is a branch pair and
    carries no tags of its own.
    """
    units = project_store.list_merge_units_merged_since(since)
    if not units:
        return []
    owner_ids = [unit.owner_task_id for unit in units if unit.owner_task_id]
    owners = {
        task.id: task
        for task in project_store.get_many(list(dict.fromkeys(owner_ids)))
        if task.id is not None
    }

    summaries: list[MergedSummary] = []
    for unit in units:
        owner = owners.get(unit.owner_task_id or "")
        if owner is None or tag not in owner.tags:
            continue
        summaries.append(_merged_summary(unit, owner, project_id))
    return summaries


def _merged_summary(unit: MergeUnit, owner: Task, project_id: str) -> MergedSummary:
    owner_id = owner.id
    return MergedSummary(
        id=unit.id,
        project_id=project_id,
        merge_unit_url=merge_unit_url(project_id, unit.id),
        source_branch=unit.source_branch,
        target_branch=unit.target_branch,
        merged_at=_as_utc(unit.merged_at) if unit.merged_at else None,
        merge_source=unit.merge_source,
        owner_task_id=owner_id,
        owner_detail_url=(
            f"/projects/{project_id}/tasks/{owner_id}" if owner_id is not None else None
        ),
        subject=_excerpt(owner.prompt),
        files_changed=unit.diff_files_changed,
        lines_added=unit.diff_lines_added,
        lines_removed=unit.diff_lines_removed,
    )


def _task_summary(task: Task, project_id: str, *, now: datetime) -> TaskSummary:
    task_id = cast(str, task.id)
    updated_at = task_updated_at(task)
    reference = task.started_at or task.created_at
    return TaskSummary(
        id=task_id,
        project_id=project_id,
        detail_url=f"/projects/{project_id}/tasks/{task_id}",
        task_type=task.task_type,
        status=task.status,
        prompt_excerpt=_excerpt(task.prompt),
        created_at=task.created_at,
        started_at=task.started_at,
        updated_at=updated_at,
        age=_format_age(reference, now=now),
    )


def _excerpt(prompt: str, *, limit: int = 120) -> str:
    normalized = " ".join(prompt.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def _as_utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


def _format_age(timestamp: datetime | None, *, now: datetime) -> str:
    if timestamp is None:
        return "unknown"
    seconds = max(0, int((_as_utc(now) - _as_utc(timestamp)).total_seconds()))
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h {minutes % 60}m"
    days = hours // 24
    return f"{days}d {hours % 24}h"
