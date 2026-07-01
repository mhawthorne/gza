"""Tests for git operations CLI commands."""


from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from tests.cli.conftest import (
    make_store,
    run_gza,
    setup_config,
)

pytestmark = pytest.mark.integration


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
