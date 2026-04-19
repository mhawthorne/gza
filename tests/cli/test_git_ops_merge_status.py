"""Tests for git operations CLI commands."""


import argparse
import io
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.cli import _determine_advance_action, cmd_advance
from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import (
    make_store,
    run_gza,
    setup_config,
    setup_db_with_tasks,
    setup_git_repo_with_task_branch,
)


class TestMergeStatusTracking:
    """Tests for merge_status column tracking."""

    def _setup_git_repo(self, tmp_path: Path):
        """Set up a minimal git repo for testing."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_merge_sets_merge_status_merged(self, tmp_path: Path):
        """Successful merge sets merge_status='merged' on the task."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a task with merge_status='unmerged'
        task = store.add("Add feature")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        # Create the feature branch with a commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Run merge
        result = run_gza("merge", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        # Verify merge_status is 'merged'
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_squash_merge_sets_merge_status_merged(self, tmp_path: Path):
        """Squash merge also sets merge_status='merged'."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        task = store.add("Add feature squash")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/squash"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/squash")
        (tmp_path / "squash.txt").write_text("squash content")
        git._run("add", "squash.txt")
        git._run("commit", "-m", "Squash feature")
        git._run("checkout", "main")

        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))
        assert result.returncode == 0

        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_mark_only_sets_merge_status_merged(self, tmp_path: Path):
        """--mark-only flag sets merge_status='merged' in the database."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        task = store.add("Mark only test")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/mark-only-status"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/mark-only-status")
        (tmp_path / "mark.txt").write_text("mark content")
        git._run("add", "mark.txt")
        git._run("commit", "-m", "Mark feature")
        git._run("checkout", "main")

        result = run_gza("merge", str(task.id), "--mark-only", "--project", str(tmp_path))
        assert result.returncode == 0

        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_cmd_unmerged_uses_db_query(self, tmp_path: Path):
        """gza unmerged uses merge_status='unmerged' DB query instead of git detection."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Task with merge_status='unmerged' (branch exists)
        task1 = store.add("Unmerged task")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feature/unmerged-task"
        task1.has_commits = True
        task1.merge_status = "unmerged"
        store.update(task1)

        git._run("checkout", "-b", "feature/unmerged-task")
        (tmp_path / "unmerged.txt").write_text("content")
        git._run("add", "unmerged.txt")
        git._run("commit", "-m", "Unmerged feature")
        git._run("checkout", "main")

        # Task with merge_status='merged'
        task2 = store.add("Merged task")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        task2.branch = "feature/merged-task"
        task2.has_commits = True
        task2.merge_status = "merged"
        store.update(task2)

        # Task with merge_status=None
        task3 = store.add("No merge status")
        task3.status = "completed"
        task3.completed_at = datetime.now(UTC)
        task3.has_commits = False
        store.update(task3)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Unmerged task" in result.stdout
        assert "Merged task" not in result.stdout
        assert "No merge status" not in result.stdout

    def test_cmd_history_shows_merged_label(self, tmp_path: Path):
        """gza history shows [merged] label for tasks with merge_status='merged'."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Merged feature task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.has_commits = True
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "[merged]" in result.stdout
        assert "Merged feature task" in result.stdout

    def test_cmd_history_shows_unmerged_label_for_unmerged(self, tmp_path: Path):
        """gza history shows 'unmerged' text label for tasks with merge_status='unmerged'."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Unmerged feature")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "unmerged" in result.stdout
        assert "Unmerged feature" in result.stdout
        assert "[merged]" not in result.stdout

    def test_cmd_history_no_merge_label_without_merge_status(self, tmp_path: Path):
        """gza history shows no merge label for tasks without merge_status."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Regular completed task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "[merged]" not in result.stdout
        assert "completed" in result.stdout

    def test_cmd_show_displays_merge_status(self, tmp_path: Path):
        """gza show displays Merge Status when merge_status is set."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test show merge status")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merge Status: merged" in result.stdout

    def test_cmd_show_no_merge_status_line_when_null(self, tmp_path: Path):
        """gza show does not display Merge Status when merge_status is None."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test show no merge status")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merge Status" not in result.stdout

    def test_cmd_show_displays_skip_learnings(self, tmp_path: Path):
        """gza show displays 'Skip Learnings: yes' when skip_learnings is True."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task with skip learnings", skip_learnings=True)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Skip Learnings: yes" in result.stdout

    def test_cmd_show_no_skip_learnings_line_when_false(self, tmp_path: Path):
        """gza show does not display Skip Learnings when skip_learnings is False."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Normal task")

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Skip Learnings" not in result.stdout

    def test_cmd_show_warning_when_disk_report_newer(self, tmp_path: Path):
        """gza show displays a warning when the report file on disk is newer than task completion."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed plan task with output_content in DB
        task = store.add("Plan something", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.output_content = "Original plan content"
        task.report_file = ".gza/plans/20260101-plan-something.md"
        store.update(task)

        # Write a newer version of the report file to disk (after completed_at)
        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-something.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Modified plan content on disk")
        # Set mtime to 2 seconds after completed_at to guarantee drift detection
        future_ts = task.completed_at.timestamp() + 2
        os.utime(report_path, (future_ts, future_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Report on disk has been modified since task completion" in result.stdout

    def test_cmd_show_no_warning_when_disk_not_newer(self, tmp_path: Path):
        """gza show does not show drift warning when disk report is not newer than completion."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed task with a future completed_at
        task = store.add("Plan task", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.output_content = "Plan content"
        task.report_file = ".gza/plans/20260101-plan-task.md"
        store.update(task)

        # Write report file and set its mtime to 2 seconds BEFORE completed_at
        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-task.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Plan content")
        past_ts = task.completed_at.timestamp() - 2
        os.utime(report_path, (past_ts, past_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Report on disk has been modified since task completion" not in result.stdout

    def test_cmd_show_displays_disk_content_when_newer(self, tmp_path: Path):
        """gza show displays the disk version of the report when it is newer than DB content."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Explore something", task_type="explore")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.output_content = "Original DB content"
        task.report_file = ".gza/explorations/20260101-explore-something.md"
        store.update(task)

        # Write newer disk content with mtime 2 seconds after completed_at
        report_path = tmp_path / ".gza" / "explorations" / "20260101-explore-something.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Updated disk content")
        future_ts = task.completed_at.timestamp() + 2
        os.utime(report_path, (future_ts, future_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Updated disk content" in result.stdout
        assert "Original DB content" not in result.stdout
