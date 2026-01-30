"""Tests for Git.is_merged() method."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gza.git import Git


class TestIsMergedMergeTreeBased:
    """Test merge-tree based merge detection (default behavior)."""

    def test_merged_branch_detected(self, tmp_path: Path):
        """Test that a merged branch is correctly detected."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Branch exists, merge-tree produces same tree as main
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="abc123def456"),  # merge-tree --write-tree
                MagicMock(returncode=0, stdout="abc123def456"),  # rev-parse main^{tree}
            ]

            result = git.is_merged("feature-branch", "main")

            assert result is True
            # Verify merge-tree was called
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("merge-tree" in call and "--write-tree" in call for call in calls)

    def test_unmerged_branch_detected(self, tmp_path: Path):
        """Test that an unmerged branch is correctly detected."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Branch exists, merge-tree produces different tree than main
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="abc123def456"),  # merge-tree --write-tree
                MagicMock(returncode=0, stdout="different789"),  # rev-parse main^{tree}
            ]

            result = git.is_merged("feature-branch", "main")

            assert result is False

    def test_deleted_branch_considered_merged(self, tmp_path: Path):
        """Test that a deleted branch is considered merged."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Branch does not exist
            mock_run.return_value = MagicMock(returncode=1)

            result = git.is_merged("deleted-branch", "main")

            assert result is True
            # Verify only branch_exists was called
            assert mock_run.call_count == 1

    def test_squash_merge_detected(self, tmp_path: Path):
        """Test that a squash-merged branch is detected as merged."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Branch exists, merge-tree shows same result as main (content merged)
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="abc123def456"),  # merge-tree --write-tree
                MagicMock(returncode=0, stdout="abc123def456"),  # rev-parse main^{tree}
            ]

            result = git.is_merged("squashed-branch", "main")

            assert result is True

    def test_default_branch_used_when_into_not_specified(self, tmp_path: Path):
        """Test that default_branch() is called when 'into' is not specified."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run, \
             patch.object(git, 'default_branch', return_value='main') as mock_default:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="abc123def456"),  # merge-tree --write-tree
                MagicMock(returncode=0, stdout="abc123def456"),  # rev-parse main^{tree}
            ]

            git.is_merged("feature-branch")

            mock_default.assert_called_once()

    def test_merge_tree_conflict_returns_false(self, tmp_path: Path):
        """Test that merge-tree conflict (non-zero exit) returns False."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=1, stdout="", stderr="conflict"),  # merge-tree failed
            ]

            result = git.is_merged("feature-branch", "main")

            assert result is False

    def test_diverged_branch_with_same_content_is_merged(self, tmp_path: Path):
        """Test that a diverged branch with equivalent content is detected as merged."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Branch diverged from main but merging would produce same tree
            # (e.g., changes were cherry-picked or squash-merged separately)
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="same_tree_hash"),  # merge-tree --write-tree
                MagicMock(returncode=0, stdout="same_tree_hash"),  # rev-parse main^{tree}
            ]

            result = git.is_merged("diverged-branch", "main")

            assert result is True


class TestIsMergedCherryBased:
    """Test commit-based merge detection (use_cherry=True)."""

    def test_cherry_merged_all_commits_applied(self, tmp_path: Path):
        """Test cherry detection when all commits are applied."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="- abc123\n- def456\n"),  # cherry
            ]

            result = git.is_merged("feature-branch", "main", use_cherry=True)

            assert result is True

    def test_cherry_unmerged_some_commits_not_applied(self, tmp_path: Path):
        """Test cherry detection when some commits are not applied."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="- abc123\n+ def456\n"),  # cherry
            ]

            result = git.is_merged("feature-branch", "main", use_cherry=True)

            assert result is False

    def test_cherry_merged_empty_output(self, tmp_path: Path):
        """Test cherry detection with empty output (branches identical)."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout=""),  # cherry (empty = merged)
            ]

            result = git.is_merged("feature-branch", "main", use_cherry=True)

            assert result is True

    def test_cherry_error_returns_false(self, tmp_path: Path):
        """Test that cherry command error returns False."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=1, stdout="", stderr="error"),  # cherry error
            ]

            result = git.is_merged("feature-branch", "main", use_cherry=True)

            assert result is False

    def test_cherry_deleted_branch_still_considered_merged(self, tmp_path: Path):
        """Test that deleted branch is merged even with use_cherry=True."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Branch does not exist
            mock_run.return_value = MagicMock(returncode=1)

            result = git.is_merged("deleted-branch", "main", use_cherry=True)

            assert result is True


class TestIsMergedBothMethods:
    """Test scenarios where both methods should agree."""

    def test_regular_merge_detected_by_both(self, tmp_path: Path):
        """Test that a regular merge is detected by both methods."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Test merge-tree based
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="same_tree"),  # merge-tree --write-tree
                MagicMock(returncode=0, stdout="same_tree"),  # rev-parse main^{tree}
            ]
            merge_tree_result = git.is_merged("feature-branch", "main")

            # Test cherry-based
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="- abc123\n"),  # cherry
            ]
            cherry_result = git.is_merged("feature-branch", "main", use_cherry=True)

            assert merge_tree_result is True
            assert cherry_result is True

    def test_unmerged_detected_by_both(self, tmp_path: Path):
        """Test that an unmerged branch is detected by both methods."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Test merge-tree based
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="merged_tree"),  # merge-tree --write-tree
                MagicMock(returncode=0, stdout="main_tree"),  # rev-parse main^{tree} (different)
            ]
            merge_tree_result = git.is_merged("feature-branch", "main")

            # Test cherry-based
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
                MagicMock(returncode=0, stdout="+ abc123\n"),  # cherry
            ]
            cherry_result = git.is_merged("feature-branch", "main", use_cherry=True)

            assert merge_tree_result is False
            assert cherry_result is False
