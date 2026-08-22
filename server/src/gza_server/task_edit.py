"""Gza-backed prompt and plan content mutations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gza.db import (
    SqliteTaskStore,
    Task,
    edit_task_prompt as _edit_task_prompt,
)
from gza.report_sync import synchronize_task_report

edit_task_prompt = _edit_task_prompt


@dataclass(frozen=True)
class ContentEdit:
    """Validated request fields for one Markdown content edit."""

    content: str
    project_id: str | None


def parse_content_edit(payload: dict[str, Any], field: str) -> ContentEdit:
    """Parse a prompt or plan edit without discarding the submitted Markdown."""
    content = payload.get(field)
    if not isinstance(content, str):
        raise ValueError(f"{field} must be a string")
    project_id_value = payload.get("project_id")
    if project_id_value is None:
        project_id = None
    elif not isinstance(project_id_value, str) or not project_id_value.strip():
        raise ValueError("project_id must be a non-empty string")
    else:
        project_id = project_id_value
    return ContentEdit(content=content, project_id=project_id)


def edit_task_plan(store: SqliteTaskStore, task_id: str, content: str) -> Task:
    """Write plan Markdown through gza's shared report synchronization API."""
    return synchronize_task_report(store, task_id, content=content).task
