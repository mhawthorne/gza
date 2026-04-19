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


class TestRefreshCommand:
    """Tests for 'gza refresh' command."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo with an initial commit."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "base.txt").write_text("base content")
        git._run("add", "base.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_refresh_single_task_with_branch(self, tmp_path: Path):
        """gza refresh <id> updates diff stats for a single task."""

        setup_config(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create a feature branch with changes
        git._run("checkout", "-b", "feat/test-task")
        (tmp_path / "new_file.py").write_text("x = 1\ny = 2\n")
        git._run("add", "new_file.py")
        git._run("commit", "-m", "Add new file")
        git._run("checkout", "main")

        store = make_store(tmp_path)

        task = store.add("Test task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/test-task"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "in 1 files" in result.stdout

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed == 1
        assert retrieved.diff_lines_added == 2
        assert retrieved.diff_lines_removed == 0

    def test_refresh_single_task_not_found(self, tmp_path: Path):
        """gza refresh <id> returns error when task doesn't exist."""
        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)
        result = run_gza("refresh", "testproject-999999", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not found" in result.stdout or "not found" in result.stderr

    def test_refresh_single_task_no_branch(self, tmp_path: Path):
        """gza refresh <id> skips task without a branch."""

        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)

        store = make_store(tmp_path)

        task = store.add("No branch task", task_type="explore")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "skipping" in result.stdout

    def test_refresh_single_task_branch_missing(self, tmp_path: Path):
        """gza refresh <id> warns and skips when branch no longer exists."""

        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)

        store = make_store(tmp_path)

        task = store.add("Task with deleted branch", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/deleted"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "skipping" in result.stdout

    def test_refresh_all_unmerged(self, tmp_path: Path):
        """gza refresh (no args) refreshes all unmerged tasks."""

        setup_config(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create two feature branches
        git._run("checkout", "-b", "feat/task-1")
        (tmp_path / "task1.py").write_text("a = 1\n")
        git._run("add", "task1.py")
        git._run("commit", "-m", "Task 1 work")
        git._run("checkout", "main")

        git._run("checkout", "-b", "feat/task-2")
        (tmp_path / "task2.py").write_text("b = 2\nc = 3\n")
        git._run("add", "task2.py")
        git._run("commit", "-m", "Task 2 work")
        git._run("checkout", "main")

        store = make_store(tmp_path)

        task1 = store.add("Task 1", task_type="implement")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feat/task-1"
        task1.merge_status = "unmerged"
        task1.has_commits = True
        store.update(task1)

        task2 = store.add("Task 2", task_type="implement")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        task2.branch = "feat/task-2"
        task2.merge_status = "unmerged"
        task2.has_commits = True
        store.update(task2)

        result = run_gza("refresh", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Refreshed 2 task(s)" in result.stdout
