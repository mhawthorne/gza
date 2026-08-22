"""Shared query and presentation model for task detail endpoints."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

from markdown_it import MarkdownIt
from markupsafe import Markup

from gza.db import MergeUnit, SqliteTaskStore, Task, task_updated_at
from gza.runner import get_task_output

from .merge_unit_detail import merge_unit_url

_MARKDOWN = MarkdownIt("commonmark", {"html": False})


@dataclass(frozen=True)
class LineageLink:
    """A direct lineage relationship shown on a task detail page."""

    id: str
    project_id: str
    detail_url: str
    task_type: str
    relationship: str


@dataclass(frozen=True)
class TaskDetail:
    """Full task record plus direct lineage relationships."""

    task: Task
    project_id: str
    detail_url: str
    parents: tuple[LineageLink, ...]
    children: tuple[LineageLink, ...]
    output_content: str | None
    project_root: Path | None = None
    merge_unit: MergeUnit | None = None

    @property
    def merge_unit_url(self) -> str | None:
        """Link to the merge unit this task belongs to, when it has one."""
        if self.merge_unit is None:
            return None
        return merge_unit_url(self.project_id, self.merge_unit.id)

    def json_record(self) -> dict[str, object]:
        """Return the full persisted record and its navigable lineage."""
        record = cast(dict[str, object], asdict(self.task))
        record["project_id"] = self.project_id
        record["detail_url"] = self.detail_url
        record["api_url"] = f"/api{self.detail_url}"
        record["type"] = self.task.task_type
        record["updated_at"] = task_updated_at(self.task)
        record["parents"] = [asdict(link) for link in self.parents]
        record["children"] = [asdict(link) for link in self.children]
        record["task_output"] = self.output_content
        record["plan_content"] = (
            self.output_content if self.task.task_type == "plan" else None
        )
        record["merge_unit_id"] = None if self.merge_unit is None else self.merge_unit.id
        record["merge_unit_state"] = None if self.merge_unit is None else self.merge_unit.state
        record["merge_unit_url"] = self.merge_unit_url
        record["log_url"] = f"{self.detail_url}/log"
        return record

    @property
    def prompt_html(self) -> Markup:
        return Markup(_MARKDOWN.render(self.task.prompt))

    @property
    def output_html(self) -> Markup | None:
        if not self.output_content:
            return None
        return Markup(_MARKDOWN.render(self.output_content))

    @property
    def plan_editable(self) -> bool:
        """Only plan tasks expose the in-page Markdown editor for their output."""
        return self.task.task_type == "plan" and self.output_content is not None

    @property
    def updated_at(self) -> datetime | None:
        return task_updated_at(self.task)


def query_task_detail(
    store: SqliteTaskStore,
    task_id: str,
    *,
    project_id: str | None = None,
) -> TaskDetail | None:
    """Load a task and its direct lineage with per-task queries.

    Deliberately avoids :class:`TaskQueryService`: that path projects every row
    it collects -- resolving lineage owner, merge unit and dependency readiness
    with tens of SQL queries apiece -- and this page needs none of it. Asking it
    for one task meant projecting the whole corpus, which took over a minute and
    a half against a real database.
    """
    matches: list[tuple[SqliteTaskStore, Task]] = []
    for project_store in store.project_query_stores():
        if project_id is not None and project_store.project_id != project_id:
            continue
        found = project_store.get(task_id)
        if found is not None:
            matches.append((project_store, found))

    if not matches:
        return None
    if len(matches) > 1:
        raise AmbiguousTaskIdError(task_id, tuple(match[0].project_id for match in matches))

    project_store, task = matches[0]
    owning_project_id = project_store.project_id
    detail_url = _detail_url(owning_project_id, task_id)

    parent_ids = [parent for parent in (task.based_on, task.depends_on) if parent]
    task_by_id = {
        parent.id: parent
        for parent in project_store.get_many(parent_ids)
        if parent.id is not None
    }

    parents: list[LineageLink] = []
    if task.based_on:
        parents.append(_lineage_link(task.based_on, owning_project_id, task_by_id, "based on"))
    if task.depends_on and task.depends_on != task.based_on:
        parents.append(_lineage_link(task.depends_on, owning_project_id, task_by_id, "depends on"))

    children: list[LineageLink] = []
    for child in project_store.get_lineage_children(task_id):
        if child.id is None:
            continue
        relationships: list[str] = []
        if child.based_on == task_id:
            relationships.append("based on this task")
        if child.depends_on == task_id:
            relationships.append("depends on this task")
        if relationships:
            children.append(
                LineageLink(
                    id=child.id,
                    project_id=owning_project_id,
                    detail_url=_detail_url(owning_project_id, child.id),
                    task_type=child.task_type,
                    relationship="; ".join(relationships),
                )
            )

    output_content = get_task_output(task, project_store.project_root)
    return TaskDetail(
        task=task,
        project_id=owning_project_id,
        detail_url=detail_url,
        parents=tuple(parents),
        children=tuple(children),
        output_content=output_content,
        project_root=project_store.project_root,
        merge_unit=project_store.resolve_merge_unit_for_task(task_id),
    )


def _lineage_link(
    task_id: str,
    project_id: str,
    task_by_id: dict[str, Task],
    relationship: str,
) -> LineageLink:
    related = task_by_id.get(task_id)
    return LineageLink(
        id=task_id,
        project_id=project_id,
        detail_url=_detail_url(project_id, task_id),
        task_type=related.task_type if related is not None else "unknown",
        relationship=relationship,
    )


class AmbiguousTaskIdError(ValueError):
    """A bare task ID matches more than one canonical project record."""

    def __init__(self, task_id: str, project_ids: tuple[str, ...]) -> None:
        self.task_id = task_id
        self.project_ids = tuple(sorted(project_ids))
        super().__init__(
            f"Task ID {task_id} is ambiguous across projects: {', '.join(self.project_ids)}"
        )


def _detail_url(project_id: str, task_id: str) -> str:
    return f"/projects/{project_id}/tasks/{task_id}"
