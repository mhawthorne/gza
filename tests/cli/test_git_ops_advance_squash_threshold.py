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

from gza.cli import cmd_advance
from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import (
    make_store,
    run_gza,
    setup_config,
    setup_db_with_tasks,
    setup_git_repo_with_task_branch,
)


class TestAdvanceMergeSquashThreshold:
    """Tests for merge_squash_threshold feature in gza advance."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo in tmp_path with an initial commit on main."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def _create_non_implement_task_with_branch(self, store, git, tmp_path, num_commits=1, prompt="Explore the codebase"):
        """Create a completed non-implement task with a real git branch and multiple commits."""
        task = store.add(prompt, task_type="explore")
        branch = f"feat/task-{task.id}"

        git._run("checkout", "-b", branch)
        for i in range(num_commits):
            (tmp_path / f"file_{task.id}_{i}.txt").write_text(f"content {i}")
            git._run("add", f"file_{task.id}_{i}.txt")
            git._run("commit", "-m", f"Commit {i + 1} for task {task.id}")
        git._run("checkout", "main")

        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        return task

    def test_advance_no_squash_when_threshold_zero(self, tmp_path: Path):
        """When merge_squash_threshold=0, always use regular merge regardless of commit count."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 3 commits on branch, but threshold is 0 (disabled)
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=3)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            squash_threshold=None,  # use config default (0)
        )

        with patch("gza.git.Git.merge") as mock_merge:
            mock_merge.return_value = None
            rc = cmd_advance(args)

        assert rc == 0
        # merge should have been called with squash=False
        assert mock_merge.called
        _, kwargs = mock_merge.call_args
        assert kwargs.get("squash", False) is False

    def test_advance_squash_when_commits_meet_threshold(self, tmp_path: Path):
        """When commit_count >= merge_squash_threshold, use squash merge."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 3 commits, threshold=2 → should squash
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=3)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            squash_threshold=2,  # squash when >= 2 commits
        )

        with patch("gza.git.Git.merge") as mock_merge:
            mock_merge.return_value = None
            rc = cmd_advance(args)

        assert rc == 0
        assert mock_merge.called
        _, kwargs = mock_merge.call_args
        assert kwargs.get("squash", False) is True

    def test_advance_no_squash_when_commits_below_threshold(self, tmp_path: Path):
        """When commit_count < merge_squash_threshold, use regular merge."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 2 commits, threshold=3 → should NOT squash
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=2)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            squash_threshold=3,  # squash only when >= 3 commits
        )

        with patch("gza.git.Git.merge") as mock_merge:
            mock_merge.return_value = None
            rc = cmd_advance(args)

        assert rc == 0
        assert mock_merge.called
        _, kwargs = mock_merge.call_args
        assert kwargs.get("squash", False) is False

    def test_advance_squash_threshold_cli_override(self, tmp_path: Path):
        """--squash-threshold N overrides config.merge_squash_threshold: dry-run shows auto-squash."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 2 commits; passing --squash-threshold 2 on the CLI should trigger auto-squash
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=2)

        result = run_gza("advance", "--dry-run", "--squash-threshold", "2", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "auto-squash" in result.stdout

    def test_advance_dry_run_shows_squash_annotation(self, tmp_path: Path):
        """Dry-run output includes '(auto-squash, N commits)' when threshold is met."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 3 commits, threshold=2 in config → auto-squash annotation should appear
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=3)

        # Write config with merge_squash_threshold=2
        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmerge_squash_threshold: 2\n")

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "auto-squash" in result.stdout

    def test_default_merge_squash_threshold_is_zero(self, tmp_path: Path):
        """Default merge_squash_threshold is 0 (disabled)."""
        from gza.config import Config
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        assert config.merge_squash_threshold == 0
        assert config.max_resume_attempts == 1

    def test_yaml_merge_squash_threshold_parsed(self, tmp_path: Path):
        """merge_squash_threshold is correctly parsed from gza.yaml."""
        from gza.config import Config
        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmerge_squash_threshold: 3\n")
        config = Config.load(tmp_path)
        assert config.merge_squash_threshold == 3

    def test_invalid_type_raises_config_error(self, tmp_path: Path):
        """Non-integer merge_squash_threshold in yaml raises ConfigError, not bare ValueError."""
        import pytest

        from gza.config import Config, ConfigError
        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmerge_squash_threshold: two\n")
        with pytest.raises(ConfigError):
            Config.load(tmp_path)

    def test_negative_value_raises_config_error(self, tmp_path: Path):
        """Negative merge_squash_threshold in yaml raises ConfigError."""
        import pytest

        from gza.config import Config, ConfigError
        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmerge_squash_threshold: -1\n")
        with pytest.raises(ConfigError):
            Config.load(tmp_path)

    def test_validate_rejects_negative_max_resume_attempts(self, tmp_path: Path):
        from gza.config import Config

        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: -1\n")
        is_valid, errors, _warnings = Config.validate(tmp_path)
        assert is_valid is False
        assert "'max_resume_attempts' must be non-negative" in errors

    def test_validate_rejects_non_integer_max_resume_attempts(self, tmp_path: Path):
        from gza.config import Config

        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: nope\n")
        is_valid, errors, _warnings = Config.validate(tmp_path)
        assert is_valid is False
        assert "'max_resume_attempts' must be an integer" in errors

    def test_validate_rejects_non_positive_max_review_cycles(self, tmp_path: Path):
        from gza.config import Config

        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_review_cycles: 0\n")
        is_valid, errors, _warnings = Config.validate(tmp_path)
        assert is_valid is False
        assert "'max_review_cycles' must be positive" in errors

    def test_load_rejects_non_integer_max_resume_attempts(self, tmp_path: Path):
        import pytest

        from gza.config import Config, ConfigError

        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: nope\n")
        with pytest.raises(ConfigError, match="'max_resume_attempts' must be an integer"):
            Config.load(tmp_path)

    def test_load_rejects_negative_max_resume_attempts(self, tmp_path: Path):
        import pytest

        from gza.config import Config, ConfigError

        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: -1\n")
        with pytest.raises(ConfigError, match="'max_resume_attempts' must be non-negative"):
            Config.load(tmp_path)

    def test_load_rejects_non_integer_max_review_cycles(self, tmp_path: Path):
        import pytest

        from gza.config import Config, ConfigError

        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_review_cycles: nope\n")
        with pytest.raises(ConfigError, match="'max_review_cycles' must be an integer"):
            Config.load(tmp_path)

    def test_load_rejects_non_positive_max_review_cycles(self, tmp_path: Path):
        import pytest

        from gza.config import Config, ConfigError

        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_review_cycles: 0\n")
        with pytest.raises(ConfigError, match="'max_review_cycles' must be positive"):
            Config.load(tmp_path)

    @pytest.mark.parametrize(
        ("field", "value", "expected_error"),
        [
            ("max_resume_attempts", "true", "'max_resume_attempts' must be an integer"),
            ("max_resume_attempts", '"2"', "'max_resume_attempts' must be an integer"),
            ("max_review_cycles", "true", "'max_review_cycles' must be an integer"),
            ("max_review_cycles", '"3"', "'max_review_cycles' must be an integer"),
        ],
    )
    def test_load_and_validate_reject_bool_and_quoted_numeric_values(
        self, tmp_path: Path, field: str, value: str, expected_error: str
    ) -> None:
        import pytest

        from gza.config import Config, ConfigError

        (tmp_path / "gza.yaml").write_text(f"project_name: test-project\ndb_path: .gza/gza.db\n{field}: {value}\n")

        is_valid, errors, _warnings = Config.validate(tmp_path)
        assert is_valid is False
        assert expected_error in errors

        with pytest.raises(ConfigError, match=expected_error):
            Config.load(tmp_path)
