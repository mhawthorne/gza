"""Tests for Git worktree operations with upstream tracking."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from gza.git import Git, GitError


class TestWorktreeAdd:
    """Test worktree_add sets upstream tracking."""

    @patch('gza.git.Git')
    def test_worktree_add_pushes_with_upstream(self, mock_git_class, tmp_path: Path):
        """Test that worktree_add pushes branch with upstream tracking."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()

        git = Git(repo_dir)

        # Mock the _run method for the main Git instance
        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            # Create a mock Git instance for the worktree
            mock_worktree_git = MagicMock()
            mock_git_class.return_value = mock_worktree_git

            worktree_path = tmp_path / "worktree"
            worktree_path.mkdir(parents=True)  # Ensure directory exists
            branch_name = "test-branch"

            # Call worktree_add
            result = git.worktree_add(worktree_path, branch_name, "main")

            # Verify worktree was created with correct args
            assert any(
                'worktree' in str(call_args) and 'add' in str(call_args)
                for call_args in mock_run.call_args_list
            ), "worktree add should be called"

            # Verify push_branch was called on the worktree Git instance
            mock_worktree_git.push_branch.assert_called_once_with(
                branch_name, remote="origin", set_upstream=True
            )

            assert result == worktree_path

    @patch('gza.git.Git')
    def test_worktree_add_continues_on_push_failure(self, mock_git_class, tmp_path: Path):
        """Test that worktree_add continues even if push fails."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()

        git = Git(repo_dir)

        # Mock the _run method for the main Git instance
        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            # Create a mock Git instance for the worktree that raises GitError on push
            mock_worktree_git = MagicMock()
            mock_worktree_git.push_branch.side_effect = GitError("push failed: no remote")
            mock_git_class.return_value = mock_worktree_git

            worktree_path = tmp_path / "worktree"
            worktree_path.mkdir(parents=True)  # Ensure directory exists
            branch_name = "test-branch"

            # Call worktree_add - should not raise exception despite push failure
            result = git.worktree_add(worktree_path, branch_name, "main")

            # Verify push_branch was attempted
            mock_worktree_git.push_branch.assert_called_once()

            # Verify it still returns the path
            assert result == worktree_path
