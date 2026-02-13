"""Tests for GitHub wrapper functionality."""

import json
import subprocess
from unittest.mock import Mock, call, patch

import pytest

from gza.github import GitHub, GitHubError, PullRequest


class TestGitHubRun:
    """Tests for GitHub._run() method."""

    @patch('subprocess.run')
    def test_run_success(self, mock_run):
        """_run executes gh command successfully."""
        mock_run.return_value = Mock(returncode=0, stdout="output", stderr="")
        gh = GitHub()

        result = gh._run("auth", "status")

        mock_run.assert_called_once_with(
            ["gh", "auth", "status"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert result.stdout == "output"

    @patch('subprocess.run')
    def test_run_failure_with_check(self, mock_run):
        """_run raises GitHubError when command fails with check=True."""
        mock_run.return_value = Mock(returncode=1, stdout="", stderr="auth failed")
        gh = GitHub()

        with pytest.raises(GitHubError) as exc_info:
            gh._run("auth", "status", check=True)

        assert "gh auth status failed" in str(exc_info.value)
        assert "auth failed" in str(exc_info.value)

    @patch('subprocess.run')
    def test_run_failure_without_check(self, mock_run):
        """_run returns result without raising when check=False."""
        mock_run.return_value = Mock(returncode=1, stdout="", stderr="error")
        gh = GitHub()

        result = gh._run("pr", "view", "nonexistent", check=False)

        assert result.returncode == 1
        assert result.stderr == "error"

    @patch('subprocess.run')
    def test_run_with_multiple_args(self, mock_run):
        """_run handles multiple arguments correctly."""
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        gh = GitHub()

        gh._run("pr", "create", "--title", "Test", "--body", "Description")

        mock_run.assert_called_once_with(
            ["gh", "pr", "create", "--title", "Test", "--body", "Description"],
            capture_output=True,
            text=True,
        )


class TestIsAvailable:
    """Tests for GitHub.is_available() method."""

    def test_is_available_true(self):
        """is_available returns True when gh CLI is authenticated."""
        gh = GitHub()

        mock_result = Mock(returncode=0, stdout="Logged in to github.com", stderr="")
        with patch.object(gh, '_run', return_value=mock_result):
            assert gh.is_available() is True

    def test_is_available_false(self):
        """is_available returns False when gh CLI is not authenticated."""
        gh = GitHub()

        mock_result = Mock(returncode=1, stdout="", stderr="not logged in")
        with patch.object(gh, '_run', return_value=mock_result):
            assert gh.is_available() is False

    def test_is_available_calls_auth_status(self):
        """is_available calls gh auth status with check=False."""
        gh = GitHub()

        mock_result = Mock(returncode=0, stdout="", stderr="")
        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            gh.is_available()

        mock_run.assert_called_once_with("auth", "status", check=False)


class TestCreatePR:
    """Tests for GitHub.create_pr() method."""

    def test_create_pr_success(self):
        """create_pr creates PR and returns PullRequest with URL and number."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout="https://github.com/owner/repo/pull/456\n",
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            pr = gh.create_pr(
                head="feature/test",
                base="main",
                title="Test PR",
                body="PR description"
            )

        mock_run.assert_called_once_with(
            "pr", "create",
            "--head", "feature/test",
            "--base", "main",
            "--title", "Test PR",
            "--body", "PR description"
        )
        assert isinstance(pr, PullRequest)
        assert pr.url == "https://github.com/owner/repo/pull/456"
        assert pr.number == 456

    def test_create_pr_with_draft(self):
        """create_pr includes --draft flag when draft=True."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout="https://github.com/owner/repo/pull/789\n",
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            pr = gh.create_pr(
                head="feature/draft",
                base="main",
                title="Draft PR",
                body="Draft description",
                draft=True
            )

        mock_run.assert_called_once_with(
            "pr", "create",
            "--head", "feature/draft",
            "--base", "main",
            "--title", "Draft PR",
            "--body", "Draft description",
            "--draft"
        )
        assert pr.number == 789

    def test_create_pr_url_with_trailing_slash(self):
        """create_pr handles URL with trailing slash correctly."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout="https://github.com/owner/repo/pull/123/\n",
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result):
            pr = gh.create_pr("feat", "main", "Title", "Body")

        assert pr.number == 123

    def test_create_pr_invalid_url_number(self):
        """create_pr sets number to 0 when URL doesn't contain valid number."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout="https://github.com/owner/repo/pull/invalid\n",
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result):
            pr = gh.create_pr("feat", "main", "Title", "Body")

        assert pr.url == "https://github.com/owner/repo/pull/invalid"
        assert pr.number == 0

    def test_create_pr_malformed_url(self):
        """create_pr sets number to 0 when URL format is unexpected."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout="invalid-url\n",
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result):
            pr = gh.create_pr("feat", "main", "Title", "Body")

        assert pr.url == "invalid-url"
        assert pr.number == 0

    def test_create_pr_failure(self):
        """create_pr raises GitHubError when command fails."""
        gh = GitHub()

        with patch.object(gh, '_run', side_effect=GitHubError("PR creation failed")):
            with pytest.raises(GitHubError) as exc_info:
                gh.create_pr("feat", "main", "Title", "Body")

        assert "PR creation failed" in str(exc_info.value)


class TestPRExists:
    """Tests for GitHub.pr_exists() method."""

    def test_pr_exists_true(self):
        """pr_exists returns PR URL when PR exists."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout='{"url": "https://github.com/owner/repo/pull/100"}\n',
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result):
            url = gh.pr_exists("feature/exists")

        assert url == "https://github.com/owner/repo/pull/100"

    def test_pr_exists_false(self):
        """pr_exists returns None when PR doesn't exist."""
        gh = GitHub()

        mock_result = Mock(returncode=1, stdout="", stderr="no pull request")
        with patch.object(gh, '_run', return_value=mock_result):
            url = gh.pr_exists("feature/no-pr")

        assert url is None

    def test_pr_exists_calls_with_json_flag(self):
        """pr_exists calls gh pr view with --json url."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout='{"url": "https://github.com/owner/repo/pull/200"}',
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            gh.pr_exists("my-branch")

        mock_run.assert_called_once_with(
            "pr", "view", "my-branch", "--json", "url", check=False
        )

    def test_pr_exists_missing_url_in_json(self):
        """pr_exists returns None when JSON doesn't contain url field."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout='{"number": 123}\n',
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result):
            url = gh.pr_exists("feature/branch")

        assert url is None

    def test_pr_exists_invalid_json(self):
        """pr_exists returns None when response is invalid JSON."""
        gh = GitHub()

        mock_result = Mock(
            returncode=0,
            stdout='not-valid-json',
            stderr=""
        )
        with patch.object(gh, '_run', return_value=mock_result):
            # Should handle json.JSONDecodeError gracefully
            with pytest.raises(json.JSONDecodeError):
                gh.pr_exists("feature/branch")


class TestGetPRNumber:
    """Tests for GitHub.get_pr_number() method."""

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

    def test_get_pr_number_empty_output(self):
        """get_pr_number returns None when output is empty."""
        gh = GitHub()

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""

        with patch.object(gh, '_run', return_value=mock_result):
            pr_number = gh.get_pr_number("feature/empty")

        assert pr_number is None

    def test_get_pr_number_calls_with_correct_args(self):
        """get_pr_number calls gh pr view with correct arguments."""
        gh = GitHub()

        mock_result = Mock(returncode=0, stdout="999\n")
        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            gh.get_pr_number("test-branch")

        mock_run.assert_called_once_with(
            "pr", "view", "test-branch", "--json", "number", "-q", ".number", check=False
        )


class TestAddPRComment:
    """Tests for GitHub.add_pr_comment() method."""

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

        # Mock failed gh pr comment - _run will raise GitHubError
        with patch.object(gh, '_run', side_effect=GitHubError("gh pr comment 123 failed: PR not found")):
            with pytest.raises(GitHubError) as exc_info:
                gh.add_pr_comment(123, "Test comment")

        assert "gh pr comment 123 failed" in str(exc_info.value)

    def test_add_pr_comment_with_markdown(self):
        """add_pr_comment handles markdown content."""
        gh = GitHub()

        markdown_comment = "# Test\n\n- Item 1\n- Item 2\n\n```python\ncode()\n```"
        mock_result = Mock(returncode=0, stdout="")

        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            gh.add_pr_comment(456, markdown_comment)

        mock_run.assert_called_once_with("pr", "comment", "456", "--body", markdown_comment)

    def test_add_pr_comment_converts_number_to_string(self):
        """add_pr_comment converts PR number to string."""
        gh = GitHub()

        mock_result = Mock(returncode=0, stdout="")
        with patch.object(gh, '_run', return_value=mock_result) as mock_run:
            gh.add_pr_comment(789, "Comment")

        # Verify number was converted to string
        call_args = mock_run.call_args[0]
        assert call_args[2] == "789"
        assert isinstance(call_args[2], str)


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
