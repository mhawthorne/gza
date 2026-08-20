"""Gza-backed prompt and plan content mutations."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from gza.db import SqliteTaskStore, Task, validate_prompt
from gza.runner import get_task_output, get_task_output_paths


@dataclass(frozen=True)
class ContentEdit:
    """Validated request fields for one Markdown content edit."""

    content: str
    project_id: str | None


class TaskEditConflict(ValueError):
    """The task changed state and can no longer accept the requested edit."""


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


def edit_task_prompt(store: SqliteTaskStore, task_id: str, prompt: str) -> Task:
    """Persist a prompt using the validation and timestamps used by ``gza edit``."""
    task = _require_task(store, task_id)
    if task.status != "pending":
        raise TaskEditConflict(
            f"Task {task_id} is {task.status}; prompt edits are only allowed for pending tasks."
        )

    normalized = prompt.strip()
    errors = validate_prompt(normalized)
    if errors:
        raise ValueError("; ".join(errors))
    if task.prompt != normalized:
        task.prompt = normalized
        task.last_edited_at = datetime.now(UTC)
        store.update(task)
    return task


def edit_task_plan(store: SqliteTaskStore, task_id: str, content: str) -> Task:
    """Write plan Markdown to its gza report file and synchronized DB field."""
    task = _require_task(store, task_id)
    if task.task_type != "plan":
        raise ValueError(f"Task {task_id} is not a plan task")
    configured_root = store.project_root
    if configured_root is None:
        raise ValueError(f"Task {task_id} has no registered project root")
    project_root = configured_root.resolve()
    if get_task_output(task, project_root) is None:
        raise ValueError(f"Task {task_id} has no plan content to edit")
    if not content.strip():
        raise ValueError("Plan cannot be empty")

    report_path = _plan_report_path(task, project_root)
    previous_content = report_path.read_bytes() if report_path.exists() else None
    report_path.parent.mkdir(parents=True, exist_ok=True)
    _replace_text(report_path, content)

    previous_output = task.output_content
    previous_report_file = task.report_file
    previous_last_edited_at = task.last_edited_at
    try:
        task.output_content = content
        task.report_file = report_path.relative_to(project_root).as_posix()
        task.last_edited_at = datetime.now(UTC)
        store.update(task)
    except Exception:
        task.output_content = previous_output
        task.report_file = previous_report_file
        task.last_edited_at = previous_last_edited_at
        if previous_content is None:
            report_path.unlink(missing_ok=True)
        else:
            _replace_bytes(report_path, previous_content)
        raise
    return task


def _require_task(store: SqliteTaskStore, task_id: str) -> Task:
    task = store.get(task_id)
    if task is None:
        raise TaskEditConflict(f"Task {task_id} no longer exists")
    return task


def _plan_report_path(task: Task, project_root: Path) -> Path:
    if task.report_file:
        report_path = project_root / task.report_file
    else:
        report_path, _ = get_task_output_paths(task, project_root)
    if report_path is None:
        raise ValueError(f"Task {task.id} has no plan output path")
    resolved_root = project_root.resolve()
    resolved_path = report_path.resolve()
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"Task {task.id} plan output path is outside its project root") from exc
    return resolved_path


def _replace_text(path: Path, content: str) -> None:
    _replace_bytes(path, content.encode("utf-8"))


def _replace_bytes(path: Path, content: bytes) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
