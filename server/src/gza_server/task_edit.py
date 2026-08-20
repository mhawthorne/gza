"""Gza-backed prompt and plan content mutations."""

from __future__ import annotations

import fcntl
import hashlib
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from gza.db import (
    SqliteTaskStore,
    Task,
    TaskPromptEditConflict as TaskEditConflict,
    edit_task_prompt as _edit_task_prompt,
)
from gza.runner import get_task_output, get_task_output_paths

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
    """Write plan Markdown to its gza report file and synchronized DB field."""
    configured_root = store.project_root
    if configured_root is None:
        raise ValueError(f"Task {task_id} has no registered project root")
    project_root = configured_root.resolve()
    if not content.strip():
        raise ValueError("Plan cannot be empty")

    with _plan_edit_lock(project_root, task_id):
        task = _require_task(store, task_id)
        if task.task_type != "plan":
            raise ValueError(f"Task {task_id} is not a plan task")
        if get_task_output(task, project_root) is None:
            raise ValueError(f"Task {task_id} has no plan content to edit")

        report_path = _plan_report_path(task, project_root)
        previous_content = report_path.read_bytes() if report_path.exists() else None
        report_path.parent.mkdir(parents=True, exist_ok=True)
        file_replaced = False
        try:
            _replace_text(report_path, content)
            file_replaced = True
            updated = store.update_plan_content(
                task_id,
                content,
                report_path.relative_to(project_root).as_posix(),
                edited_at=datetime.now(UTC),
            )
            if updated is None:
                raise TaskEditConflict(f"Task {task_id} is no longer a plan task")
            return updated
        except Exception:
            if file_replaced:
                if previous_content is None:
                    report_path.unlink(missing_ok=True)
                else:
                    _replace_bytes(report_path, previous_content)
            raise


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


@contextmanager
def _plan_edit_lock(project_root: Path, task_id: str) -> Iterator[None]:
    """Serialize a task's complete report-file and DB content mutation."""
    lock_dir = project_root / ".gza" / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_key = hashlib.sha256(task_id.encode("utf-8")).hexdigest()
    lock_path = lock_dir / f"plan-edit-{lock_key}.lock"
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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
