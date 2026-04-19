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


class TestDiffCommand:
    """Tests for 'gza diff' command."""

    def test_diff_runs_git_diff(self, tmp_path: Path):
        """Diff command runs git diff with colored output."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes to file
        (tmp_path / "file.txt").write_text("modified")

        # Run diff command - should show the changes
        # We redirect to avoid pager issues in tests
        result = run_gza("diff", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show the diff (contains color codes when forced with --color=always)
        assert "file.txt" in result.stdout

    def test_diff_with_stat_argument(self, tmp_path: Path):
        """Diff command passes --stat to git diff."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes
        (tmp_path / "file.txt").write_text("modified")

        # Run diff with --stat (using -- separator for pass-through args)
        result = run_gza("diff", "--project", str(tmp_path), "--", "--stat")

        assert result.returncode == 0
        assert "file.txt" in result.stdout

    def test_diff_with_task_id(self, tmp_path: Path):
        """Diff command resolves task ID to branch diff."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create and checkout task branch
        git._run("checkout", "-b", "task-1-test")
        (tmp_path / "file.txt").write_text("modified")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Task changes")

        # Return to main
        git._run("checkout", "main")

        # Create task in database with branch (use same prefix as config: testproject)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path, prefix="testproject")
        task = store.add("Test task", task_type="implement")
        task.branch = "task-1-test"
        store.update(task)

        # Run diff with task ID (use full task.id so resolve_id returns it as-is)
        result = run_gza("diff", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show the diff between main and task branch
        assert "file.txt" in result.stdout
        assert "modified" in result.stdout or "initial" in result.stdout

    def test_diff_with_task_id_not_found(self, tmp_path: Path):
        """Diff falls back to git when a full prefixed task ID is not found in DB.

        This mirrors cmd_checkout behaviour: a _looks_like_task_id() match that
        doesn't resolve to a real task is passed through to git as a branch/ref.
        git will fail with a non-zero exit code when the ref is also invalid.
        """
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")

        # Create empty database
        setup_db_with_tasks(tmp_path, [])

        # Run diff with non-existent full task ID — falls back to git, which fails
        # because the ref is also invalid.
        result = run_gza("diff", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode != 0

    def test_diff_treats_bare_suffix_as_git_ref_not_task_id(self, tmp_path: Path):
        """Bare suffixes should be treated as git refs, not implicit task IDs."""
        from gza.git import Git

        setup_config(tmp_path)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        store = make_store(tmp_path)
        task = store.add("Task with branch")
        task.branch = "feature/task-branch"
        store.update(task)

        result = run_gza("diff", "000001", "--project", str(tmp_path))

        assert result.returncode != 0
        assert f"{task.branch}" not in result.stdout

    def test_diff_with_task_id_no_branch(self, tmp_path: Path):
        """Diff command shows error when task has no branch."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")

        # Create task without branch (use same prefix as config: testproject)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path, prefix="testproject")
        task = store.add("Test task", task_type="implement")
        # Don't set task.branch

        # Run diff with task ID that has no branch
        result = run_gza("diff", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {task.id} has no branch" in result.stdout

    def test_diff_with_non_numeric_argument(self, tmp_path: Path):
        """Diff command passes non-numeric arguments through to git diff."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes
        (tmp_path / "file.txt").write_text("modified")

        # Run diff with --cached (using -- separator for pass-through args)
        result = run_gza("diff", "--project", str(tmp_path), "--", "--cached")

        # Should run successfully (even if no staged changes)
        assert result.returncode == 0
