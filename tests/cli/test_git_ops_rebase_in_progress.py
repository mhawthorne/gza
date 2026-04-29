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


class TestIsRebaseInProgress:
    """Tests for the _is_rebase_in_progress helper."""

    def test_returns_false_when_no_git_dir(self, tmp_path):
        """Returns False when there's no .git directory at all."""
        from gza.cli.git_ops import _is_rebase_in_progress
        assert _is_rebase_in_progress(tmp_path) is False

    def test_returns_false_when_no_rebase_markers(self, tmp_path):
        """Returns False for a normal repository with no rebase in progress."""
        from gza.cli.git_ops import _is_rebase_in_progress
        git_dir = tmp_path / ".git"
        git_dir.mkdir()
        assert _is_rebase_in_progress(tmp_path) is False

    def test_returns_true_when_rebase_merge_present(self, tmp_path):
        """Returns True when .git/rebase-merge directory exists."""
        from gza.cli.git_ops import _is_rebase_in_progress
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "rebase-merge").mkdir()
        assert _is_rebase_in_progress(tmp_path) is True

    def test_returns_true_when_rebase_apply_present(self, tmp_path):
        """Returns True when .git/rebase-apply directory exists."""
        from gza.cli.git_ops import _is_rebase_in_progress
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "rebase-apply").mkdir()
        assert _is_rebase_in_progress(tmp_path) is True

    def test_worktree_git_file_resolved_correctly(self, tmp_path):
        """Follows the gitdir: pointer in a worktree .git file."""
        from gza.cli.git_ops import _is_rebase_in_progress
        # Simulate a real git worktree: .git is a file pointing to the gitdir
        real_git_dir = tmp_path / "main-repo" / ".git" / "worktrees" / "wt1"
        real_git_dir.mkdir(parents=True)
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        (worktree / ".git").write_text(f"gitdir: {real_git_dir}\n")
        # No rebase markers yet
        assert _is_rebase_in_progress(worktree) is False
        # Add a rebase-merge dir inside the actual git dir
        (real_git_dir / "rebase-merge").mkdir()
        assert _is_rebase_in_progress(worktree) is True
