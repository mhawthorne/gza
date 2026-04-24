"""Tests for git operations CLI commands."""


import argparse
from pathlib import Path
from unittest.mock import patch

from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import (
    run_gza,
    setup_git_repo_with_task_branch,
)


class TestRebaseCommand:
    """Tests for 'gza rebase' command."""

    def test_rebase_removes_clean_worktree(self, tmp_path: Path):
        """Rebase command removes clean worktree before rebasing."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase with worktree", "feature/test-rebase-wt",
            worktree_name="test-rebase-wt",
        )

        # Verify worktree exists
        assert worktree_path.exists()

        # Rebase should remove worktree first, then succeed
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removing stale worktree" in result.stdout
        assert "Removed worktree" in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_force_removes_dirty_worktree(self, tmp_path: Path):
        """Rebase always force-removes dirty worktrees and succeeds cleanly."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase force", "feature/test-rebase-force",
            worktree_name="test-rebase-force",
        )

        # Add uncommitted changes to the old worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Rebase should succeed: old dirty worktree is force-removed, fresh one created
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success — old worktree removed, rebase completed in fresh worktree
        assert result.returncode == 0
        assert "Removed worktree" in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_force_flag_accepted(self, tmp_path: Path):
        """Rebase --force flag is accepted (backward-compat no-op)."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase --force flag", "feature/test-rebase-force-flag",
            worktree_name="test-rebase-force-flag",
        )

        # Add uncommitted changes to the old worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # --force is now a no-op; rebase should still succeed
        result = run_gza("rebase", str(task.id), "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Successfully rebased" in result.stdout

    def test_rebase_without_worktree(self, tmp_path: Path):
        """Rebase works normally when no worktree exists."""
        _store, _git, task, _worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase no worktree", "feature/test-rebase-nowt",
        )

        # Rebase should work normally
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success (no worktree messages)
        assert result.returncode == 0
        assert "Removing stale worktree" not in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_logs_task_id_and_newline(self, tmp_path: Path):
        """Rebase command logs foreground rebase-task progress and ends with a newline."""
        _store, _git, task, _worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase output format", "feature/test-rebase-output",
        )

        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Rebasing task " in result.stdout
        assert str(task.id) not in result.stdout
        # Output should end with a newline (after trailing whitespace is stripped per line,
        # the last non-empty content is followed by a blank line)
        assert result.stdout.endswith("\n")

    def test_rebase_resolve_flag_accepted(self, tmp_path: Path):
        """Rebase command accepts --resolve flag."""
        _store, _git, task, _worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase with resolve", "feature/test-resolve",
        )

        # Mock the conflict resolution since we're just testing that the flag is accepted
        # and the basic flow works (we don't want to actually invoke Claude in tests)
        with patch('gza.cli.invoke_provider_resolve', return_value=False):
            # This should succeed without conflicts (no --resolve needed, but flag should work)
            result = run_gza("rebase", str(task.id), "--resolve", "--project", str(tmp_path))

            # Should succeed when there are no conflicts
            assert result.returncode == 0
            assert "Successfully rebased" in result.stdout

    def test_rebase_cleans_up_worktree_after_mechanical_success(self, tmp_path: Path):
        """Worktree is removed after a successful mechanical rebase (no conflicts)."""
        from gza.config import Config

        _store, _git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase cleanup mechanical", "feature/test-cleanup-mech",
        )
        config = Config.load(tmp_path)
        expected_worktree = config.worktree_path / str(task.id)

        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Successfully rebased" in result.stdout
        assert not expected_worktree.exists(), (
            f"Worktree at {expected_worktree} should have been removed after successful rebase"
        )

    def test_rebase_cleans_up_worktree_on_push_failure(self, tmp_path: Path):
        """Worktree is removed and command fails when force-push raises GitError."""
        from gza.cli.git_ops import cmd_rebase
        from gza.git import GitError

        _store, _git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase cleanup push fail", "feature/test-cleanup-push",
        )
        config = Config.load(tmp_path)
        expected_worktree = config.worktree_path / str(task.id)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=task.id,
            background=False,
            onto=None,
            remote=False,
            force=False,
            resolve=False,
        )

        # Patch push_force_with_lease to simulate a push failure after a provider-resolved rebase.
        # We also need invoke_provider_resolve to return True so the push is attempted.
        with patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True), \
             patch("gza.git.Git.rebase", side_effect=GitError("conflict")), \
             patch("gza.git.Git.rebase_abort"), \
             patch("gza.git.Git.push_force_with_lease", side_effect=GitError("push failed")):
            rc = cmd_rebase(args)

        assert rc == 1

        # Worktree must not exist regardless of push failure.
        assert not expected_worktree.exists(), (
            f"Worktree at {expected_worktree} should have been removed even after push failure"
        )

    def test_rebase_creates_single_rebase_task_with_canonical_log(self, tmp_path: Path):
        """Foreground rebase creates one completed rebase child with a task-owned log."""
        _store, _git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase task ownership", "feature/test-task-owned-log",
        )

        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        rebases = [t for t in store.get_based_on_children(task.id) if t.task_type == "rebase"]
        assert len(rebases) == 1
        rebase_task = rebases[0]
        assert rebase_task.status == "completed"
        assert rebase_task.log_file is not None
        log_path = config.project_dir / rebase_task.log_file
        assert log_path.exists()
        log_text = log_path.read_text()
        assert "Rebasing task" in log_text
