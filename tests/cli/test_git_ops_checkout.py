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


class TestCheckoutCommand:
    """Tests for 'gza checkout' command."""

    def test_checkout_prunes_prunable_only_registration(self, tmp_path: Path):
        """Checkout succeeds when branch has only a prunable worktree registration."""
        _store, git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test checkout prunable", "feature/test-prunable-checkout",
            worktree_name="test-prunable-checkout",
        )

        assert worktree_path is not None
        assert worktree_path.exists()

        # Leave stale metadata: remove directory without pruning git's registration.
        shutil.rmtree(worktree_path)
        porcelain_before = git._run("worktree", "list", "--porcelain").stdout
        assert "feature/test-prunable-checkout" in porcelain_before
        assert "prunable" in porcelain_before

        result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Checked out" in result.stdout
        assert "Removing stale worktree" not in result.stdout

        porcelain_after = git._run("worktree", "list", "--porcelain").stdout
        assert str(worktree_path) not in porcelain_after
        assert "\nprunable" not in porcelain_after

    def test_checkout_fails_with_dirty_worktree(self, tmp_path: Path):
        """Checkout command fails if worktree has uncommitted changes."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test checkout with dirty worktree", "feature/test-dirty",
            worktree_name="test-dirty",
        )

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Checkout should fail due to dirty worktree
        result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

        # Verify failure
        assert result.returncode == 1
        assert "uncommitted changes" in result.stdout

    def test_checkout_force_removes_dirty_worktree(self, tmp_path: Path):
        """Checkout --force removes worktree even with uncommitted changes."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test checkout force", "feature/test-force",
            worktree_name="test-force",
        )

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Checkout with --force should succeed
        result = run_gza("checkout", str(task.id), "--force", "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removed worktree" in result.stdout
        assert "Checked out" in result.stdout
