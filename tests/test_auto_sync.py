"""Tests for auto-sync of edited plan files."""

import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from gza.db import Task
from gza.runner import _get_task_output


def test_get_task_output_returns_file_when_newer_than_completed_at():
    """When report_file is modified after task completion, read from file instead of DB."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)
        report_file = project_dir / ".gza" / "plans" / "20260209-test.md"
        report_file.parent.mkdir(parents=True, exist_ok=True)

        # Create task with output_content and completed_at
        task = Task(
            id=1,
            prompt="test task",
            status="completed",
            task_type="plan",
            report_file=".gza/plans/20260209-test.md",
            output_content="original plan content",
            completed_at=datetime.now(timezone.utc),
        )

        # Write initial file content
        report_file.write_text("original plan content")

        # Wait briefly to ensure mtime will be different
        time.sleep(0.1)

        # Edit the file (simulating user edit)
        report_file.write_text("edited plan content")

        # Should return edited file content (not DB content)
        result = _get_task_output(task, project_dir)
        assert result == "edited plan content"


def test_get_task_output_returns_db_when_file_not_modified():
    """When report_file hasn't been modified, prefer DB content."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)
        report_file = project_dir / ".gza" / "plans" / "20260209-test.md"
        report_file.parent.mkdir(parents=True, exist_ok=True)

        # Write file first
        report_file.write_text("original plan content")

        # Wait briefly to ensure mtime is set
        time.sleep(0.1)

        # Create task with completed_at AFTER file mtime
        task = Task(
            id=1,
            prompt="test task",
            status="completed",
            task_type="plan",
            report_file=".gza/plans/20260209-test.md",
            output_content="db plan content",
            completed_at=datetime.now(timezone.utc),
        )

        # Should return DB content (file hasn't been modified after completion)
        result = _get_task_output(task, project_dir)
        assert result == "db plan content"


def test_get_task_output_returns_file_when_no_db_content():
    """When there's no DB content, fall back to file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)
        report_file = project_dir / ".gza" / "plans" / "20260209-test.md"
        report_file.parent.mkdir(parents=True, exist_ok=True)

        # Create task without output_content
        task = Task(
            id=1,
            prompt="test task",
            status="completed",
            task_type="plan",
            report_file=".gza/plans/20260209-test.md",
            output_content=None,
            completed_at=datetime.now(timezone.utc),
        )

        # Write file content
        report_file.write_text("file plan content")

        # Should return file content (no DB content)
        result = _get_task_output(task, project_dir)
        assert result == "file plan content"


def test_get_task_output_returns_none_when_no_content():
    """When there's no DB content or file, return None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)

        # Create task without output_content or report_file
        task = Task(
            id=1,
            prompt="test task",
            status="completed",
            task_type="plan",
            output_content=None,
            report_file=None,
            completed_at=datetime.now(timezone.utc),
        )

        # Should return None
        result = _get_task_output(task, project_dir)
        assert result is None


def test_get_task_output_handles_missing_completed_at():
    """When task has no completed_at, prefer DB content."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)
        report_file = project_dir / ".gza" / "plans" / "20260209-test.md"
        report_file.parent.mkdir(parents=True, exist_ok=True)

        # Create task without completed_at
        task = Task(
            id=1,
            prompt="test task",
            status="completed",
            task_type="plan",
            report_file=".gza/plans/20260209-test.md",
            output_content="db plan content",
            completed_at=None,
        )

        # Write file content
        report_file.write_text("file plan content")

        # Should return DB content (can't compare mtime without completed_at)
        result = _get_task_output(task, project_dir)
        assert result == "db plan content"
