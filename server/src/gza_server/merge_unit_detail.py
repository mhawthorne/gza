"""Shared query and presentation model for merge-unit detail endpoints.

A merge unit — one source branch aimed at one target branch — is the real unit
of work: an implement plus the reviews, improves, rebases and re-attempts that
accumulated around it. The task pages show one task at a time; this module backs
the view that shows the whole unit and the tasks that make it up.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import cast

from gza.db import MergeUnit, SqliteTaskStore, Task, task_updated_at


@dataclass(frozen=True)
class MergeUnitMember:
    """One task attached to a merge unit, with the role it plays in it."""

    id: str
    project_id: str
    detail_url: str
    task_type: str
    status: str
    role: str
    prompt_excerpt: str
    created_at: datetime | None
    updated_at: datetime | None
    is_owner: bool

    def json_record(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MergeUnitDetail:
    """A merge unit plus its member tasks in chronological order."""

    unit: MergeUnit
    project_id: str
    detail_url: str
    members: tuple[MergeUnitMember, ...]
    owner: MergeUnitMember | None

    @property
    def has_diff_stats(self) -> bool:
        """Whether any diff stat is populated.

        The three stats are written together, but a unit whose diff was never
        measured carries ``None`` rather than zero — and "0 files changed" is a
        meaningfully different claim from "we never looked".
        """
        return any(
            value is not None
            for value in (
                self.unit.diff_files_changed,
                self.unit.diff_lines_added,
                self.unit.diff_lines_removed,
            )
        )

    @property
    def has_pr(self) -> bool:
        return self.unit.pr_number is not None

    def json_record(self) -> dict[str, object]:
        """Return the full persisted unit and its navigable membership."""
        record = cast(dict[str, object], asdict(self.unit))
        record["project_id"] = self.project_id
        record["detail_url"] = self.detail_url
        record["api_url"] = f"/api{self.detail_url}"
        record["members"] = [member.json_record() for member in self.members]
        record["owner"] = self.owner.json_record() if self.owner is not None else None
        return record


def query_merge_unit_detail(
    store: SqliteTaskStore,
    merge_unit_id: str,
    *,
    project_id: str | None = None,
) -> MergeUnitDetail | None:
    """Load one merge unit and its attached tasks with targeted queries.

    Like :func:`gza_server.task_detail.query_task_detail`, this deliberately
    avoids ``TaskQueryService``: that path projects every row it collects, which
    means asking it for one unit's members costs a whole-corpus projection.
    """
    for project_store in store.project_query_stores():
        if project_id is not None and project_store.project_id != project_id:
            continue
        unit = project_store.get_merge_unit(merge_unit_id)
        if unit is None:
            continue
        return _build_detail(project_store, unit)
    return None


def query_merge_unit_detail_for_task(
    store: SqliteTaskStore,
    task_id: str,
    *,
    project_id: str | None = None,
) -> MergeUnitDetail | None:
    """Resolve the active merge unit attached to a task, if it has one."""
    for project_store in store.project_query_stores():
        if project_id is not None and project_store.project_id != project_id:
            continue
        unit = project_store.resolve_merge_unit_for_task(task_id)
        if unit is None:
            continue
        return _build_detail(project_store, unit)
    return None


def _build_detail(project_store: SqliteTaskStore, unit: MergeUnit) -> MergeUnitDetail:
    owning_project_id = project_store.project_id
    memberships = project_store.list_merge_unit_memberships(unit.id)
    members = tuple(
        _member(task, role, owning_project_id, owner_task_id=unit.owner_task_id)
        for task, role in memberships
        if task.id is not None
    )
    owner = next((member for member in members if member.is_owner), None)
    return MergeUnitDetail(
        unit=unit,
        project_id=owning_project_id,
        detail_url=merge_unit_url(owning_project_id, unit.id),
        members=members,
        owner=owner,
    )


def _member(
    task: Task,
    role: str,
    project_id: str,
    *,
    owner_task_id: str | None,
) -> MergeUnitMember:
    assert task.id is not None
    return MergeUnitMember(
        id=task.id,
        project_id=project_id,
        detail_url=f"/projects/{project_id}/tasks/{task.id}",
        task_type=task.task_type,
        status=task.status,
        role=role,
        prompt_excerpt=_excerpt(task.prompt),
        created_at=task.created_at,
        updated_at=task_updated_at(task),
        is_owner=task.id == owner_task_id,
    )


def _excerpt(prompt: str, *, limit: int = 120) -> str:
    normalized = " ".join(prompt.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def merge_unit_url(project_id: str, merge_unit_id: str) -> str:
    return f"/projects/{project_id}/merge-units/{merge_unit_id}"
