"""Comprehensive tests for Git operations."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from gza.git import Git, GitError, parse_diff_numstat


class TestGitInit:
    """Tests for Git initialization."""

    def test_init_with_repo_dir(self, tmp_path: Path):
        """Test Git initialization with a repository directory."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)
        assert git.repo_dir == repo_dir


class TestGitRun:
    """Tests for the _run helper method."""

    def test_run_successful_command(self, tmp_path: Path):
        """Test _run with a successful command."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="success", stderr="")
            result = git._run("status")

            mock_run.assert_called_once()
            assert result.returncode == 0
            assert result.stdout == "success"

    def test_run_with_check_false_doesnt_raise(self, tmp_path: Path):
        """Test _run with check=False doesn't raise on non-zero exit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git._run("status", check=False)

            assert result.returncode == 1

    def test_run_with_check_true_raises_on_error(self, tmp_path: Path):
        """Test _run with check=True raises GitError on non-zero exit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error message")

            with pytest.raises(GitError) as exc_info:
                git._run("invalid-command")

            assert "error message" in str(exc_info.value)

    def test_run_with_stdin(self, tmp_path: Path):
        """Test _run passes stdin correctly."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git._run("hash-object", "-w", "--stdin", stdin=b"test content")

            # Verify stdin was passed
            call_args = mock_run.call_args
            assert call_args.kwargs.get('input') == "test content"


class TestBasicOperations:
    """Tests for basic git operations."""

    def test_current_branch(self, tmp_path: Path):
        """Test getting the current branch name."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="main\n", stderr="")
            branch = git.current_branch()

            mock_run.assert_called_once_with("rev-parse", "--abbrev-ref", "HEAD")
            assert branch == "main"

    def test_default_branch_from_origin_head(self, tmp_path: Path):
        """Test default_branch gets branch from origin HEAD."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="refs/remotes/origin/main\n", stderr="")
            branch = git.default_branch()

            assert branch == "main"

    def test_default_branch_fallback_main(self, tmp_path: Path):
        """Test default_branch falls back to main when origin HEAD fails."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # First call fails (symbolic-ref), second succeeds (show-ref for main)
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # symbolic-ref fails
                MagicMock(returncode=0, stdout="", stderr=""),  # main exists
            ]
            branch = git.default_branch()

            assert branch == "main"

    def test_default_branch_fallback_master(self, tmp_path: Path):
        """Test default_branch falls back to master when main doesn't exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # First call fails (symbolic-ref), second fails (main doesn't exist), third succeeds (master exists)
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # symbolic-ref fails
                MagicMock(returncode=1, stdout="", stderr=""),  # main doesn't exist
                MagicMock(returncode=0, stdout="", stderr=""),  # master exists
            ]
            branch = git.default_branch()

            assert branch == "master"

    def test_default_branch_defaults_to_master(self, tmp_path: Path):
        """Test default_branch returns 'master' when nothing else works."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # All checks fail
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
            branch = git.default_branch()

            assert branch == "master"

    def test_checkout(self, tmp_path: Path):
        """Test checking out a branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.checkout("feature-branch")

            mock_run.assert_called_once_with("checkout", "feature-branch")

    def test_pull_success(self, tmp_path: Path):
        """Test pull returns True on success."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.pull()

            mock_run.assert_called_once_with("pull", "--ff-only", check=False)
            assert result is True

    def test_pull_failure(self, tmp_path: Path):
        """Test pull returns False on failure."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.pull()

            assert result is False

    def test_fetch_default_remote(self, tmp_path: Path):
        """Test fetch with default remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.fetch()

            mock_run.assert_called_once_with("fetch", "origin")

    def test_fetch_custom_remote(self, tmp_path: Path):
        """Test fetch with custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.fetch("upstream")

            mock_run.assert_called_once_with("fetch", "upstream")


class TestBranchOperations:
    """Tests for branch operations."""

    def test_create_branch_without_force(self, tmp_path: Path):
        """Test creating a new branch without force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.create_branch("new-branch")

            mock_run.assert_called_once_with("checkout", "-b", "new-branch")

    def test_create_branch_with_force(self, tmp_path: Path):
        """Test creating a branch with force deletes existing branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.create_branch("new-branch", force=True)

            # Verify branch deletion and creation
            assert mock_run.call_count == 2
            mock_run.assert_any_call("branch", "-D", "new-branch", check=False)
            mock_run.assert_any_call("checkout", "-b", "new-branch")

    def test_branch_exists_returns_true(self, tmp_path: Path):
        """Test branch_exists returns True for existing branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.branch_exists("main")

            mock_run.assert_called_once_with("show-ref", "--verify", "--quiet", "refs/heads/main", check=False)
            assert result is True

    def test_branch_exists_returns_false(self, tmp_path: Path):
        """Test branch_exists returns False for non-existing branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
            result = git.branch_exists("nonexistent")

            assert result is False

    def test_delete_branch_without_force(self, tmp_path: Path):
        """Test deleting a branch without force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.delete_branch("old-branch")

            mock_run.assert_called_once_with("branch", "-d", "old-branch")

    def test_delete_branch_with_force(self, tmp_path: Path):
        """Test deleting a branch with force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.delete_branch("old-branch", force=True)

            mock_run.assert_called_once_with("branch", "-D", "old-branch")


class TestChangeDetection:
    """Tests for change detection and staging."""

    def test_has_changes_with_staged_changes(self, tmp_path: Path):
        """Test has_changes detects staged changes."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # staged returns non-zero (has changes), others return zero
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is True

    def test_has_changes_with_unstaged_changes(self, tmp_path: Path):
        """Test has_changes detects unstaged changes."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # unstaged returns non-zero (has changes), others return zero
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=1, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is True

    def test_has_changes_with_untracked_files(self, tmp_path: Path):
        """Test has_changes detects untracked files."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # ls-files returns file names (untracked files exist)
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="newfile.txt\n", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is True

    def test_has_changes_no_changes(self, tmp_path: Path):
        """Test has_changes returns False when no changes exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # All commands return success with no output
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is False

    def test_has_changes_exclude_untracked(self, tmp_path: Path):
        """Test has_changes with include_untracked=False."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Only untracked files exist
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
            ]
            result = git.has_changes(include_untracked=False)

            # Should not call ls-files and should return False
            assert mock_run.call_count == 2
            assert result is False

    def test_has_changes_with_path(self, tmp_path: Path):
        """Test has_changes for a specific path."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes(path="src/")

            assert result is True
            # Verify path was passed to commands
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("src/" in call for call in calls)

    def test_add_default_path(self, tmp_path: Path):
        """Test adding changes with default path."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.add()

            mock_run.assert_called_once_with("add", ".")

    def test_add_specific_path(self, tmp_path: Path):
        """Test adding changes for a specific path."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.add("src/file.py")

            mock_run.assert_called_once_with("add", "src/file.py")

    def test_commit(self, tmp_path: Path):
        """Test creating a commit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.commit("Test commit message")

            mock_run.assert_called_once_with("commit", "-m", "Test commit message")

    def test_amend(self, tmp_path: Path):
        """Test amending the last commit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.amend()

            mock_run.assert_called_once_with("commit", "--amend", "--no-edit")


class TestWorktreeOperations:
    """Tests for worktree operations."""

    def test_worktree_add_without_base_branch(self, tmp_path: Path):
        """Test creating a worktree without specifying base branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run, \
             patch('gza.git.Git') as mock_git_class:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_worktree_git = MagicMock()
            mock_git_class.return_value = mock_worktree_git

            result = git.worktree_add(worktree_path, "feature-branch")

            # Verify worktree was created
            assert any("worktree" in str(call) and "add" in str(call) for call in mock_run.call_args_list)
            assert result == worktree_path

    def test_worktree_add_with_base_branch(self, tmp_path: Path):
        """Test creating a worktree with base branch specified."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run, \
             patch('gza.git.Git') as mock_git_class:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_worktree_git = MagicMock()
            mock_git_class.return_value = mock_worktree_git

            result = git.worktree_add(worktree_path, "feature-branch", "main")

            # Verify base branch was included
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("main" in call for call in calls)
            assert result == worktree_path

    def test_worktree_add_removes_existing(self, tmp_path: Path):
        """Test worktree_add removes existing worktree."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"
        worktree_path.mkdir(parents=True)

        with patch.object(git, '_run') as mock_run, \
             patch('gza.git.Git') as mock_git_class, \
             patch.object(git, 'worktree_remove') as mock_remove:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_worktree_git = MagicMock()
            mock_git_class.return_value = mock_worktree_git

            git.worktree_add(worktree_path, "feature-branch")

            # Verify existing worktree was removed
            mock_remove.assert_called_once_with(worktree_path, force=True)

    def test_worktree_remove_without_force(self, tmp_path: Path):
        """Test removing a worktree without force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.worktree_remove(worktree_path)

            # Verify worktree remove was called without --force
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("worktree" in call and "remove" in call and "--force" not in call for call in calls)

    def test_worktree_remove_with_force(self, tmp_path: Path):
        """Test removing a worktree with force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.worktree_remove(worktree_path, force=True)

            # Verify --force was included
            mock_run.assert_called_once()
            call_args = str(mock_run.call_args)
            assert "--force" in call_args

    def test_worktree_list_empty(self, tmp_path: Path):
        """Test listing worktrees when none exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.worktree_list()

            assert result == []

    def test_worktree_list_single(self, tmp_path: Path):
        """Test listing a single worktree."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        porcelain_output = """worktree /path/to/worktree
HEAD abc123def456
branch refs/heads/main
"""

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=porcelain_output, stderr="")
            result = git.worktree_list()

            assert len(result) == 1
            assert result[0]["path"] == "/path/to/worktree"
            assert result[0]["head"] == "abc123def456"
            assert result[0]["branch"] == "refs/heads/main"

    def test_worktree_list_multiple(self, tmp_path: Path):
        """Test listing multiple worktrees."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        porcelain_output = """worktree /path/to/main
HEAD abc123
branch refs/heads/main

worktree /path/to/feature
HEAD def456
branch refs/heads/feature
"""

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=porcelain_output, stderr="")
            result = git.worktree_list()

            assert len(result) == 2
            assert result[0]["path"] == "/path/to/main"
            assert result[1]["path"] == "/path/to/feature"


class TestRemoteOperations:
    """Tests for remote operations."""

    def test_remote_branch_exists_returns_true(self, tmp_path: Path):
        """Test remote_branch_exists returns True when branch exists."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="abc123\trefs/heads/feature\n", stderr="")
            result = git.remote_branch_exists("feature")

            mock_run.assert_called_once_with("ls-remote", "--heads", "origin", "feature", check=False)
            assert result is True

    def test_remote_branch_exists_returns_false(self, tmp_path: Path):
        """Test remote_branch_exists returns False when branch doesn't exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.remote_branch_exists("nonexistent")

            assert result is False

    def test_remote_branch_exists_custom_remote(self, tmp_path: Path):
        """Test remote_branch_exists with custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.remote_branch_exists("feature", "upstream")

            mock_run.assert_called_once_with("ls-remote", "--heads", "upstream", "feature", check=False)

    def test_needs_push_remote_doesnt_exist(self, tmp_path: Path):
        """Test needs_push returns True when remote branch doesn't exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=False):
            result = git.needs_push("feature")

            assert result is True

    def test_needs_push_has_commits_ahead(self, tmp_path: Path):
        """Test needs_push returns True when local has commits ahead."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="3\n", stderr="")
            result = git.needs_push("feature")

            assert result is True

    def test_needs_push_no_commits_ahead(self, tmp_path: Path):
        """Test needs_push returns False when no commits ahead."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="0\n", stderr="")
            result = git.needs_push("feature")

            assert result is False

    def test_needs_push_comparison_error(self, tmp_path: Path):
        """Test needs_push returns True on comparison error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.needs_push("feature")

            assert result is True

    def test_push_branch_with_upstream(self, tmp_path: Path):
        """Test pushing a branch with upstream tracking."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_branch("feature")

            mock_run.assert_called_once_with("push", "-u", "origin", "feature")

    def test_push_branch_without_upstream(self, tmp_path: Path):
        """Test pushing a branch without upstream tracking."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_branch("feature", set_upstream=False)

            mock_run.assert_called_once_with("push", "origin", "feature")

    def test_push_branch_custom_remote(self, tmp_path: Path):
        """Test pushing a branch to custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_branch("feature", remote="upstream")

            mock_run.assert_called_once_with("push", "-u", "upstream", "feature")

    def test_push_force_with_lease_default_remote(self, tmp_path: Path):
        """Test force push with lease using default remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_force_with_lease("feature")

            mock_run.assert_called_once_with("push", "--force-with-lease", "origin", "feature")

    def test_push_force_with_lease_custom_remote(self, tmp_path: Path):
        """Test force push with lease using custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_force_with_lease("feature", remote="upstream")

            mock_run.assert_called_once_with("push", "--force-with-lease", "upstream", "feature")


class TestMergeOperations:
    """Tests for merge operations."""

    def test_can_merge_returns_true(self, tmp_path: Path):
        """Test can_merge returns True for clean merge."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.can_merge("feature")

            assert result is True

    def test_can_merge_returns_false_on_conflict(self, tmp_path: Path):
        """Test can_merge returns False on conflict."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="conflict")
            result = git.can_merge("feature")

            assert result is False

    def test_can_merge_returns_false_for_nonexistent_branch(self, tmp_path: Path):
        """Test can_merge returns False for non-existent branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=False):
            result = git.can_merge("nonexistent")

            assert result is False

    def test_can_merge_with_into_parameter(self, tmp_path: Path):
        """Test can_merge with into parameter specified."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.can_merge("feature", "develop")

            # Verify merge-tree was called with correct branches
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("develop" in call and "feature" in call for call in calls)

    def test_merge_without_squash(self, tmp_path: Path):
        """Test merge without squash uses --no-ff."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.merge("feature")

            mock_run.assert_called_once_with("merge", "--no-ff", "feature")

    def test_merge_with_squash(self, tmp_path: Path):
        """Test merge with squash creates commit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.merge("feature", squash=True, commit_message="Squash merge feature")

            # Verify both merge and commit were called
            assert mock_run.call_count == 2
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("--squash" in call for call in calls)
            assert any("Squash merge feature" in call for call in calls)

    def test_merge_with_squash_requires_message(self, tmp_path: Path):
        """Test merge with squash requires commit message."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            with pytest.raises(ValueError) as exc_info:
                git.merge("feature", squash=True)

            assert "commit_message is required" in str(exc_info.value)

    def test_merge_abort(self, tmp_path: Path):
        """Test aborting a merge."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.merge_abort()

            mock_run.assert_called_once_with("merge", "--abort")

    def test_rebase(self, tmp_path: Path):
        """Test rebasing onto a branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.rebase("main")

            mock_run.assert_called_once_with("rebase", "main")

    def test_rebase_abort(self, tmp_path: Path):
        """Test aborting a rebase."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.rebase_abort()

            mock_run.assert_called_once_with("rebase", "--abort")


class TestUtilityOperations:
    """Tests for utility operations."""

    def test_get_log_with_oneline(self, tmp_path: Path):
        """Test getting log with oneline format."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="abc123 Commit message\n", stderr="")
            result = git.get_log("main..feature")

            mock_run.assert_called_once_with("log", "--oneline", "main..feature", check=False)
            assert result == "abc123 Commit message"

    def test_get_log_without_oneline(self, tmp_path: Path):
        """Test getting log without oneline format."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="Full log output\n", stderr="")
            result = git.get_log("main..feature", oneline=False)

            mock_run.assert_called_once_with("log", "main..feature", check=False)
            assert result == "Full log output"

    def test_get_diff_stat(self, tmp_path: Path):
        """Test getting diff stat."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = " file.py | 10 +++++-----\n 1 file changed, 5 insertions(+), 5 deletions(-)"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            result = git.get_diff_stat("main...feature")

            mock_run.assert_called_once_with("diff", "--stat", "main...feature", check=False)
            assert "file.py" in result
            assert "1 file changed" in result

    def test_get_diff_stat_parsed_single_file(self, tmp_path: Path):
        """Test parsing diff stat for a single file."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = " file.py | 10 +++++-----\n 1 file changed, 5 insertions(+), 5 deletions(-)"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 1
            assert insertions == 5
            assert deletions == 5

    def test_get_diff_stat_parsed_multiple_files(self, tmp_path: Path):
        """Test parsing diff stat for multiple files."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = (
            " src/a.py | 20 ++++++++++++++------\n"
            " src/b.py |  5 +++++\n"
            " 2 files changed, 25 insertions(+), 6 deletions(-)"
        )

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 2
            assert insertions == 25
            assert deletions == 6

    def test_get_diff_stat_parsed_insertions_only(self, tmp_path: Path):
        """Test parsing diff stat with only insertions."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = " new.py | 10 ++++++++++\n 1 file changed, 10 insertions(+)"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 1
            assert insertions == 10
            assert deletions == 0

    def test_get_diff_stat_parsed_empty(self, tmp_path: Path):
        """Test parsing empty diff stat returns zeros."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 0
            assert insertions == 0
            assert deletions == 0

    def test_get_diff(self, tmp_path: Path):
        """Test getting full diff."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        full_diff = """diff --git a/file.py b/file.py
index abc123..def456 100644
--- a/file.py
+++ b/file.py
@@ -1,3 +1,3 @@
-old line
+new line
"""

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=full_diff, stderr="")
            result = git.get_diff("main...feature")

            mock_run.assert_called_once_with("diff", "main...feature", check=False)
            assert "diff --git" in result
            assert "+new line" in result

    def test_count_commits_ahead(self, tmp_path: Path):
        """Test counting commits ahead."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="5\n", stderr="")
            result = git.count_commits_ahead("feature", "main")

            mock_run.assert_called_once_with("rev-list", "--count", "main..feature", check=False)
            assert result == 5

    def test_count_commits_ahead_error(self, tmp_path: Path):
        """Test count_commits_ahead returns 0 on error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.count_commits_ahead("feature", "main")

            assert result == 0


class TestParseDiffNumstat:
    """Tests for parse_diff_numstat module-level function."""

    def test_empty_output(self):
        """Empty output returns zeros."""
        assert parse_diff_numstat("") == (0, 0, 0)

    def test_single_file(self):
        """Single file with additions and deletions."""
        output = "12\t3\tpath/to/file.py"
        assert parse_diff_numstat(output) == (1, 12, 3)

    def test_multiple_files(self):
        """Multiple files are summed correctly."""
        output = "12\t3\tpath/to/file.py\n5\t0\tsrc/other.py\n0\t7\tsrc/old.py"
        files, added, removed = parse_diff_numstat(output)
        assert files == 3
        assert added == 17
        assert removed == 10

    def test_binary_files_skipped(self):
        """Binary files (- in count columns) are excluded from count."""
        output = "12\t3\tfile.py\n-\t-\timage.png"
        files, added, removed = parse_diff_numstat(output)
        assert files == 1
        assert added == 12
        assert removed == 3

    def test_all_binary_files(self):
        """All binary files results in zeros."""
        output = "-\t-\timage.png\n-\t-\tfont.woff"
        assert parse_diff_numstat(output) == (0, 0, 0)

    def test_malformed_lines_skipped(self):
        """Lines that don't have 3 tab-separated parts are skipped."""
        output = "12\t3\tfile.py\ngarbage line\n5\t1\tother.py"
        files, added, removed = parse_diff_numstat(output)
        assert files == 2
        assert added == 17
        assert removed == 4

    def test_only_additions(self):
        """Files with only additions."""
        output = "20\t0\tnew_file.py"
        assert parse_diff_numstat(output) == (1, 20, 0)


class TestGetDiffNumstat:
    """Tests for Git.get_diff_numstat method."""

    def test_get_diff_numstat_calls_git(self, tmp_path: Path):
        """get_diff_numstat calls git diff --numstat with the given range."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        numstat_output = "12\t3\tfile.py\n5\t0\tother.py"
        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=numstat_output, stderr="")
            result = git.get_diff_numstat("main...feature")

            mock_run.assert_called_once_with("diff", "--numstat", "main...feature", check=False)
            assert result == numstat_output

    def test_get_diff_numstat_strips_output(self, tmp_path: Path):
        """get_diff_numstat strips trailing whitespace from output."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="12\t3\tfile.py\n  ", stderr="")
            result = git.get_diff_numstat("main...feature")

        assert result == "12\t3\tfile.py"

    def test_get_diff_numstat_empty_on_error(self, tmp_path: Path):
        """get_diff_numstat returns empty string on error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="error")
            result = git.get_diff_numstat("main...feature")

        assert result == ""
