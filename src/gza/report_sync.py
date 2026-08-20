"""Serialized synchronization of task report files and persisted content."""

from __future__ import annotations

import fcntl
import hashlib
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from .db import SqliteTaskStore, Task, TaskPromptEditConflict
from .runner import get_task_output, get_task_output_paths

ReportSyncStatus = Literal["synced", "unchanged", "missing", "no_report", "conflict"]


class TaskReportSyncConflict(TaskPromptEditConflict):
    """The task changed incompatibly during report synchronization."""


@dataclass(frozen=True)
class ReportSyncResult:
    """Outcome of one serialized report-file/DB synchronization."""

    status: ReportSyncStatus
    task: Task
    report_path: Path | None


def synchronize_task_report(
    store: SqliteTaskStore,
    task_id: str,
    *,
    content: str | None = None,
    dry_run: bool = False,
) -> ReportSyncResult:
    """Synchronize one task report through the shared serialization policy.

    When ``content`` is provided it is an HTTP-style plan edit and becomes the
    winning file and DB revision. Otherwise the report file is authoritative,
    matching ``gza sync-report``. Both directions refetch after taking the same
    interprocess lock and use field-scoped database persistence.
    """
    project_root = _project_root(store, task_id)
    with _task_report_lock(project_root, task_id):
        task = _require_task(store, task_id)
        if content is not None:
            return _save_plan_revision(store, task, project_root, content)
        return _sync_disk_revision(store, task, project_root, dry_run=dry_run)


def _save_plan_revision(
    store: SqliteTaskStore,
    task: Task,
    project_root: Path,
    content: str,
) -> ReportSyncResult:
    if task.task_type != "plan":
        raise ValueError(f"Task {task.id} is not a plan task")
    if not content.strip():
        raise ValueError("Plan cannot be empty")

    report_path = _report_path(task, project_root, allow_default=True)
    if get_task_output(task, project_root) is None:
        raise ValueError(f"Task {task.id} has no plan content to edit")

    previous_content = report_path.read_bytes() if report_path.exists() else None
    report_path.parent.mkdir(parents=True, exist_ok=True)
    file_replaced = False
    try:
        _replace_text(report_path, content)
        file_replaced = True
        assert task.id is not None
        updated = store.update_report_content(
            task.id,
            content,
            report_file=report_path.relative_to(project_root).as_posix(),
            edited_at=datetime.now(UTC),
            required_task_type="plan",
        )
        if updated is None:
            raise TaskReportSyncConflict(f"Task {task.id} is no longer a plan task")
        return ReportSyncResult("synced", updated, report_path)
    except Exception:
        if file_replaced:
            if previous_content is None:
                report_path.unlink(missing_ok=True)
            else:
                _replace_bytes(report_path, previous_content)
        raise


def _sync_disk_revision(
    store: SqliteTaskStore,
    task: Task,
    project_root: Path,
    *,
    dry_run: bool,
) -> ReportSyncResult:
    if not task.report_file:
        return ReportSyncResult("no_report", task, None)

    report_path = _report_path(task, project_root, allow_default=False)
    if not report_path.exists():
        return ReportSyncResult("missing", task, report_path)

    disk_content = report_path.read_text(encoding="utf-8")
    if task.output_content == disk_content:
        return ReportSyncResult("unchanged", task, report_path)
    if dry_run:
        return ReportSyncResult("synced", task, report_path)

    assert task.id is not None
    updated = store.update_report_content(
        task.id,
        disk_content,
        expected_report_file=task.report_file,
    )
    if updated is None:
        current = _require_task(store, task.id)
        current_path = (
            _report_path(current, project_root, allow_default=False)
            if current.report_file
            else None
        )
        return ReportSyncResult("conflict", current, current_path)
    return ReportSyncResult("synced", updated, report_path)


def _project_root(store: SqliteTaskStore, task_id: str) -> Path:
    configured_root = store.project_root
    if configured_root is None:
        raise ValueError(f"Task {task_id} has no registered project root")
    return configured_root.resolve()


def _require_task(store: SqliteTaskStore, task_id: str) -> Task:
    task = store.get(task_id)
    if task is None:
        raise TaskReportSyncConflict(f"Task {task_id} no longer exists")
    return task


def _report_path(task: Task, project_root: Path, *, allow_default: bool) -> Path:
    report_path: Path | None
    if task.report_file:
        report_path = project_root / task.report_file
    elif allow_default:
        report_path, _ = get_task_output_paths(task, project_root)
    else:
        report_path = None
    if report_path is None:
        raise ValueError(f"Task {task.id} has no report output path")

    resolved_path = report_path.resolve()
    try:
        resolved_path.relative_to(project_root)
    except ValueError as exc:
        raise ValueError(f"Task {task.id} report output path is outside its project root") from exc
    return resolved_path


@contextmanager
def _task_report_lock(project_root: Path, task_id: str) -> Iterator[None]:
    """Serialize a task's complete report-file and DB content mutation."""
    lock_dir = project_root / ".gza" / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_key = hashlib.sha256(task_id.encode("utf-8")).hexdigest()
    lock_path = lock_dir / f"report-sync-{lock_key}.lock"
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
