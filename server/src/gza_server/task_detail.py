"""Shared query and presentation model for task detail endpoints."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

from markdown_it import MarkdownIt
from markupsafe import Markup

from gza.db import SqliteTaskStore, Task, task_updated_at
from gza.runner import _get_task_output
from gza.task_query import TaskQuery, TaskQueryService, TaskRow

_MARKDOWN = MarkdownIt("commonmark", {"html": False})


@dataclass(frozen=True)
class LineageLink:
    """A direct lineage relationship shown on a task detail page."""

    id: str
    task_type: str
    relationship: str


@dataclass(frozen=True)
class TaskDetail:
    """Full task record plus direct lineage relationships."""

    task: Task
    parents: tuple[LineageLink, ...]
    children: tuple[LineageLink, ...]
    plan_content: str | None

    def json_record(self) -> dict[str, object]:
        """Return the full persisted record and its navigable lineage."""
        record = cast(dict[str, object], asdict(self.task))
        record["type"] = self.task.task_type
        record["updated_at"] = task_updated_at(self.task)
        record["parents"] = [asdict(link) for link in self.parents]
        record["children"] = [asdict(link) for link in self.children]
        record["plan_content"] = self.plan_content
        return record

    @property
    def prompt_html(self) -> Markup:
        return Markup(_MARKDOWN.render(self.task.prompt))

    @property
    def plan_html(self) -> Markup | None:
        if not self.plan_content:
            return None
        return Markup(_MARKDOWN.render(self.plan_content))

    @property
    def updated_at(self) -> datetime | None:
        return task_updated_at(self.task)


def query_task_detail(
    store: SqliteTaskStore,
    task_id: str,
    *,
    project_dir: Path,
) -> TaskDetail | None:
    """Load a task and direct lineage through gza's all-project query API."""
    query = TaskQuery(limit=None)
    result = TaskQueryService(store).run(query, all_projects=True)
    tasks = [cast(TaskRow, row).task for row in result.rows]
    task_by_id = {task.id: task for task in tasks if task.id is not None}
    task = task_by_id.get(task_id)
    if task is None:
        return None

    parents: list[LineageLink] = []
    if task.based_on:
        parents.append(_lineage_link(task.based_on, task_by_id, "based on"))
    if task.depends_on and task.depends_on != task.based_on:
        parents.append(_lineage_link(task.depends_on, task_by_id, "depends on"))

    children: list[LineageLink] = []
    for child in tasks:
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
                    task_type=child.task_type,
                    relationship="; ".join(relationships),
                )
            )

    plan_content = _get_task_output(task, project_dir) if task.task_type == "plan" else None
    return TaskDetail(
        task=task,
        parents=tuple(parents),
        children=tuple(children),
        plan_content=plan_content,
    )


def _lineage_link(
    task_id: str,
    task_by_id: dict[str, Task],
    relationship: str,
) -> LineageLink:
    related = task_by_id.get(task_id)
    return LineageLink(
        id=task_id,
        task_type=related.task_type if related is not None else "unknown",
        relationship=relationship,
    )
