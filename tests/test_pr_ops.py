"""Tests for shared PR ensure/create behavior."""

from unittest.mock import Mock, patch

from gza.db import SqliteTaskStore
from gza.github import GitHubError, PullRequestDetails
from gza.pr_ops import ensure_task_pr, lookup_task_pr


class TestEnsureTaskPr:
    """Focused regressions for PR ensure/create helper behavior."""

    def test_cached_pr_is_revalidated_and_cleared_when_missing(self, tmp_path):
        """Stale cached PR numbers must not be treated as authoritative."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/stale-cached-pr"
        task.pr_number = 81
        store.update(task)

        git = Mock()
        git.default_branch.return_value = "main"
        git.needs_push.return_value = False
        git.is_merged.return_value = False

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.return_value = None
        gh.discover_pr_by_branch.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/82",
            number=82,
            state="open",
            base_ref_name="main",
        )

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = ensure_task_pr(
                task,
                store,
                git,
                title="Manual title",
                body="body",
            )

        assert result.ok is True
        assert result.status == "existing"
        assert result.pr_number == 82
        assert result.pr_url == "https://github.com/o/r/pull/82"
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.pr_number == 82

    def test_existing_pr_path_pushes_branch_before_reuse(self, tmp_path, capsys):
        """When the branch is ahead, helper should push before reusing an existing PR."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/existing-pr"
        store.update(task)

        git = Mock()
        git.default_branch.return_value = "main"
        git.needs_push.return_value = True
        git.is_merged.return_value = False

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.return_value = None
        gh.discover_pr_by_branch.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/55",
            number=55,
            state="open",
            base_ref_name="main",
        )

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = ensure_task_pr(
                task,
                store,
                git,
                title="Manual title",
                body="body",
            )

        assert result.ok is True
        assert result.status == "existing"
        git.push_branch.assert_called_once_with("feature/existing-pr")
        output = capsys.readouterr().out
        assert "Pushing branch 'feature/existing-pr' to origin..." in output

    def test_closed_cached_pr_creates_a_new_pr_for_still_unmerged_branch(self, tmp_path):
        """Closed or merged cached PRs should not block creating a replacement PR."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/reopen-pr"
        task.pr_number = 81
        store.update(task)

        git = Mock()
        git.default_branch.return_value = "main"
        git.needs_push.return_value = False
        git.is_merged.return_value = False

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/81",
            number=81,
            state="closed",
            base_ref_name="main",
        )
        gh.discover_pr_by_branch.return_value = None
        gh.create_pr.return_value = Mock(url="https://github.com/o/r/pull/82", number=82)

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = ensure_task_pr(
                task,
                store,
                git,
                title="Manual title",
                body="body",
            )

        assert result.ok is True
        assert result.status == "created"
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.pr_number == 82
        assert refreshed.pr_state == "open"

    def test_closed_cached_pr_reuses_discovered_open_pr_before_creating(self, tmp_path):
        """A closed cached PR must fall back to branch discovery before creating a new PR."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/reuse-open-pr"
        task.pr_number = 81
        store.update(task)

        git = Mock()
        git.default_branch.return_value = "main"
        git.needs_push.return_value = False
        git.is_merged.return_value = False

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/81",
            number=81,
            state="closed",
            base_ref_name="main",
        )
        gh.discover_pr_by_branch.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/82",
            number=82,
            state="open",
            base_ref_name="main",
        )

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = ensure_task_pr(
                task,
                store,
                git,
                title="Manual title",
                body="body",
            )

        assert result.ok is True
        assert result.status == "existing"
        assert result.pr_number == 82
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.pr_number == 82
        assert refreshed.pr_state == "open"
        gh.create_pr.assert_not_called()

    def test_lookup_failure_preserves_cached_pr_state_and_returns_error(self, tmp_path):
        """Lookup failures should surface as errors without clearing cached PR metadata."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/lookup-failure"
        task.pr_number = 81
        task.pr_state = "open"
        store.update(task)
        original_synced_at = task.pr_last_synced_at

        git = Mock()
        git.default_branch.return_value = "main"
        git.needs_push.return_value = False
        git.is_merged.return_value = False

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.side_effect = GitHubError("gh pr view 81 failed: authentication failed")

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = ensure_task_pr(
                task,
                store,
                git,
                title="Manual title",
                body="body",
            )

        assert result.ok is False
        assert result.status == "lookup_failed"
        assert "failed to look up cached PR #81" in result.error
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.pr_number == 81
        assert refreshed.pr_state == "open"
        assert refreshed.pr_last_synced_at == original_synced_at
        gh.create_pr.assert_not_called()


class TestLookupTaskPr:
    """Focused regressions for read-only PR lookup behavior."""

    def test_lookup_task_pr_revalidates_stale_cached_pr_and_falls_back_to_branch(self, tmp_path):
        """Read-only lookup should ignore stale cached numbers and still find the live branch PR."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/stale-cached-pr"
        task.pr_number = 81
        store.update(task)

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_url.return_value = None
        gh.pr_exists.return_value = "https://github.com/o/r/pull/82"
        gh.get_pr_number.return_value = 82

        result = lookup_task_pr(task, store=store, gh=gh, refresh_cache=True)

        assert result.found is True
        assert result.status == "existing"
        assert result.pr_number == 82
        assert result.pr_url == "https://github.com/o/r/pull/82"
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.pr_number == 82

    def test_lookup_task_pr_can_skip_pr_number_fetch_when_only_url_is_needed(self, tmp_path):
        """Unmerged-style lookup should be able to show the PR URL without an extra number query."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/existing-pr"
        store.update(task)

        gh = Mock()
        gh.is_available.return_value = True
        gh.pr_exists.return_value = "https://github.com/o/r/pull/55"

        result = lookup_task_pr(task, store=store, gh=gh, include_number=False)

        assert result.found is True
        assert result.status == "existing"
        assert result.pr_url == "https://github.com/o/r/pull/55"
        assert result.pr_number is None
        gh.get_pr_number.assert_not_called()
