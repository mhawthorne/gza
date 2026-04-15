"""Tests for shared PR ensure/create behavior."""

from unittest.mock import Mock, patch

from gza.db import SqliteTaskStore
from gza.pr_ops import ensure_task_pr


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
        gh.get_pr_url.return_value = None
        gh.pr_exists.return_value = "https://github.com/o/r/pull/82"
        gh.get_pr_number.return_value = 82

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
        gh.pr_exists.return_value = "https://github.com/o/r/pull/55"
        gh.get_pr_number.return_value = 55

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
