"""Tests for GitHub wrapper functionality."""

import subprocess
from unittest.mock import Mock, patch

import pytest

from gza.github import GitHub, GitHubError


class TestGitHub:
    """Tests for GitHub class."""

    def test_get_pr_number_success(self):
        """get_pr_number returns PR number when PR exists."""
        gh = GitHub()

        # Mock successful gh pr view response
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "123\n"

        with patch.object(gh, '_run', return_value=mock_result):
            pr_number = gh.get_pr_number("feature/test-branch")

        assert pr_number == 123

    def test_get_pr_number_no_pr(self):
        """get_pr_number returns None when no PR exists."""
        gh = GitHub()

        # Mock failed gh pr view response (no PR)
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch.object(gh, '_run', return_value=mock_result):
            pr_number = gh.get_pr_number("feature/no-pr")

        assert pr_number is None

    def test_get_pr_number_invalid_number(self):
        """get_pr_number returns None when output is not a valid number."""
        gh = GitHub()

        # Mock response with invalid number
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "not-a-number\n"

        with patch.object(gh, '_run', return_value=mock_result):
            pr_number = gh.get_pr_number("feature/bad-response")

        assert pr_number is None

    def test_add_pr_comment_success(self):
        """add_pr_comment posts comment successfully."""
        gh = GitHub()

        # Mock successful gh pr comment
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""

        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            gh.add_pr_comment(123, "Test comment")

        # Verify gh pr comment was called with correct args
        mock_run.assert_called_once_with("pr", "comment", "123", "--body", "Test comment")

    def test_add_pr_comment_error(self):
        """add_pr_comment raises GitHubError on failure."""
        gh = GitHub()

        # Mock failed gh pr comment
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Error: PR not found"

        with patch.object(gh, '_run', return_value=mock_result, side_effect=GitHubError("Error")):
            with pytest.raises(GitHubError):
                gh.add_pr_comment(123, "Test comment")


class TestPRNumberCaching:
    """Tests for PR number caching in tasks."""

    def test_pr_number_stored_after_creation(self, tmp_path):
        """PR number is cached in task after gza pr command."""
        from gza.db import SqliteTaskStore, Task

        # Create a task with a branch
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task", task_type="implement")
        task.status = "completed"
        task.branch = "feature/test"
        task.has_commits = True
        store.update(task)

        # Simulate PR creation storing the PR number
        task.pr_number = 456
        store.update(task)

        # Verify it was stored
        loaded_task = store.get(task.id)
        assert loaded_task.pr_number == 456

    def test_pr_number_persists_across_loads(self, tmp_path):
        """PR number persists when task is loaded from database."""
        from gza.db import SqliteTaskStore

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create task with PR number
        store1 = SqliteTaskStore(db_path)
        task = store1.add("Test task")
        task.pr_number = 789
        store1.update(task)
        task_id = task.id

        # Load with new store instance
        store2 = SqliteTaskStore(db_path)
        loaded_task = store2.get(task_id)

        assert loaded_task.pr_number == 789
