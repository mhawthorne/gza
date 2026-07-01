"""Tests for shared PR ensure/create behavior."""

import os
from unittest.mock import Mock, patch

from gza.concurrency import _PROCESS_LOCKS
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import GitError
from gza.github import GitHub, GitHubError, PullRequestDetails
from gza.pr_ops import _generate_pr_content, ensure_task_pr, lookup_task_pr, sync_task_branch_if_live_pr


class TestEnsureTaskPr:
    """Focused regressions for PR ensure/create helper behavior."""

    def teardown_method(self):
        GitHub.clear_pr_support_cache()

    def test_non_github_repo_short_circuits_after_cached_verdict(self, tmp_path):
        """Once gh marked the repo unsupported, ensure_task_pr should not shell out again."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/non-github"
        store.update(task)

        git = Mock()

        gh = Mock()
        gh.cached_pr_support.return_value = False

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = ensure_task_pr(
                task,
                store,
                git,
                title="Manual title",
                body="body",
            )

        assert result.ok is True
        assert result.status == "unsupported"
        gh.is_available.assert_not_called()

    def test_pr_integration_false_short_circuits_without_github(self, tmp_path):
        """Project config override should skip PR work without instantiating gh."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/config-disabled"
        store.update(task)

        git = Mock()

        with patch("gza.pr_ops.GitHub") as github_cls:
            result = ensure_task_pr(
                task,
                store,
                git,
                pr_integration=False,
                title="Manual title",
                body="body",
            )

        assert result.ok is True
        assert result.status == "disabled"
        github_cls.assert_not_called()

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

    def test_non_github_repo_discovery_skips_push_and_returns_unsupported(self, tmp_path):
        """Unsupported-repo discovery must win before any push attempt."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Implement X", task_type="implement")
        task.branch = "feature/non-github-discovery"
        store.update(task)

        git = Mock()
        git.default_branch.return_value = "main"
        git.needs_push.return_value = True

        gh = Mock()
        gh.is_available.return_value = True
        gh.cached_pr_support.side_effect = GitHub.cached_pr_support

        def _raise_unsupported(branch: str):
            GitHub._mark_pr_unsupported()
            raise GitHubError(
                f"gh pr list --head {branch} failed: "
                "none of the git remotes configured for this repository point to a known GitHub host"
            )

        gh.discover_pr_by_branch.side_effect = _raise_unsupported

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = ensure_task_pr(
                task,
                store,
                git,
                title="Manual title",
                body="body",
            )

        assert result.ok is True
        assert result.status == "unsupported"
        assert result.error == "project has no GitHub-capable remote"
        git.needs_push.assert_not_called()
        git.push_branch.assert_not_called()
        assert GitHub.cached_pr_support() is False

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


def test_generate_pr_content_refuses_at_capacity_without_creating_internal_task(
    tmp_path,
    capsys,
):
    """Capacity refusal must fall back before persisting an internal PR task row."""
    config_path = tmp_path / "gza.yaml"
    config_path.write_text("project_name: test\nproject_prefix: test\nmax_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")

    source_task = store.add("Add auth and metrics", task_type="implement")
    source_task.slug = "20260318-test-add-auth-and-metrics"
    source_task.branch = "feature/auth-metrics"
    store.update(source_task)

    running = store.add("Running task", task_type="implement")
    running.status = "in_progress"
    running.running_pid = 424242
    store.update(running)

    with (
        patch("gza.concurrency._best_effort_stale_cleanup", return_value=None),
        patch("gza.concurrency._pid_alive", side_effect=lambda pid: pid == 424242),
        patch("gza.runner.run", side_effect=AssertionError("runner should not start at capacity")),
    ):
        title, body = _generate_pr_content(
            source_task,
            commit_log="abc123 Add auth",
            diff_stat="1 file changed",
            config=config,
            store=store,
        )

    assert title == "Add auth and metrics"
    assert "## Task Prompt" in body
    assert [task for task in store.get_all() if task.task_type == "internal"] == []
    assert _PROCESS_LOCKS == {}
    assert "already at max concurrent tasks: 1 running, limit is 1" in capsys.readouterr().err


def test_generate_pr_content_allows_same_pid_reentry_at_max_concurrent_one(tmp_path):
    """Nested PR generation should reuse the current worker slot instead of falling back."""
    config_path = tmp_path / "gza.yaml"
    config_path.write_text("project_name: test\nproject_prefix: test\nmax_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")

    source_task = store.add("Add auth and metrics", task_type="implement")
    source_task.slug = "20260318-test-add-auth-and-metrics"
    source_task.branch = "feature/auth-metrics"
    store.update(source_task)

    running = store.add("Outer running task", task_type="implement")
    running.status = "in_progress"
    running.running_pid = os.getpid()
    store.update(running)

    def _complete_internal_task(_config, task_id=None, **kwargs):
        internal_task = store.get(task_id)
        assert internal_task is not None
        internal_task.status = "completed"
        internal_task.output_content = "TITLE: Better title\nBODY:\nGenerated body\n"
        store.update(internal_task)
        return 0

    with patch("gza.runner.run", side_effect=_complete_internal_task):
        title, body = _generate_pr_content(
            source_task,
            commit_log="abc123 Add auth",
            diff_stat="1 file changed",
            config=config,
            store=store,
        )

    assert title == "Better title"
    assert body == "Generated body"
    internal_tasks = [task for task in store.get_all() if task.task_type == "internal"]
    assert len(internal_tasks) == 1


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


class TestSyncTaskBranchIfLivePr:
    """Focused regressions for improve-time branch sync against existing PRs."""

    def test_open_pr_pushes_branch_before_follow_up(self, tmp_path, capsys):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement X", task_type="implement")
        impl.branch = "feature/improve-open-pr"
        impl.pr_number = 81
        store.update(impl)

        improve = store.add("Improve X", task_type="improve", based_on=impl.id, same_branch=True)
        improve.branch = impl.branch
        store.update(improve)

        git = Mock()
        git.needs_push.return_value = True

        gh = Mock()
        gh.is_available.return_value = True
        gh.get_pr_details.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/81",
            number=81,
            state="open",
            base_ref_name="main",
        )

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = sync_task_branch_if_live_pr(improve, store, git)

        assert result.ok is True
        assert result.status == "pushed"
        git.push_branch.assert_called_once_with("feature/improve-open-pr")
        output = capsys.readouterr().out
        assert "Pushing branch 'feature/improve-open-pr' to origin..." in output
        refreshed_impl = store.get(impl.id)
        refreshed_improve = store.get(improve.id)
        assert refreshed_impl is not None
        assert refreshed_improve is not None
        assert refreshed_impl.pr_number == 81
        assert refreshed_improve.pr_number == 81
        assert refreshed_impl.pr_state == "open"
        assert refreshed_improve.pr_state == "open"

    def test_no_live_pr_preserves_current_behavior(self, tmp_path, capsys):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Improve X", task_type="improve")
        task.branch = "feature/improve-no-pr"
        store.update(task)

        git = Mock()

        gh = Mock()
        gh.is_available.return_value = True
        gh.discover_pr_by_branch.return_value = None

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = sync_task_branch_if_live_pr(task, store, git)

        assert result.ok is True
        assert result.status == "no_live_pr"
        git.needs_push.assert_not_called()
        git.push_branch.assert_not_called()
        assert capsys.readouterr().out == ""

    def test_live_pr_already_synced_skips_push_without_noise(self, tmp_path, capsys):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Improve X", task_type="improve")
        task.branch = "feature/improve-synced"
        store.update(task)

        git = Mock()
        git.needs_push.return_value = False

        gh = Mock()
        gh.is_available.return_value = True
        gh.discover_pr_by_branch.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/91",
            number=91,
            state="open",
            base_ref_name="main",
        )

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = sync_task_branch_if_live_pr(task, store, git)

        assert result.ok is True
        assert result.status == "already_synced"
        git.push_branch.assert_not_called()
        assert capsys.readouterr().out == ""

    def test_push_failure_warn_path_returns_nonfatal_error(self, tmp_path):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Improve X", task_type="improve")
        task.branch = "feature/improve-push-fails"
        store.update(task)

        git = Mock()
        git.needs_push.return_value = True
        git.push_branch.side_effect = GitError("no auth")

        gh = Mock()
        gh.is_available.return_value = True
        gh.discover_pr_by_branch.return_value = PullRequestDetails(
            url="https://github.com/o/r/pull/101",
            number=101,
            state="open",
            base_ref_name="main",
        )

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = sync_task_branch_if_live_pr(task, store, git)

        assert result.ok is False
        assert result.status == "push_failed"
        assert result.pr_number == 101
        assert result.error == "no auth"

    def test_gh_unavailable_returns_without_output_or_git_activity(self, tmp_path, capsys):
        """Missing GitHub CLI should remain a quiet, non-pushable lookup result."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Improve X", task_type="improve")
        task.branch = "feature/improve-no-gh"
        store.update(task)

        git = Mock()

        gh = Mock()
        gh.is_available.return_value = False

        with patch("gza.pr_ops.GitHub", return_value=gh):
            result = sync_task_branch_if_live_pr(task, store, git)

        assert result.ok is False
        assert result.status == "gh_unavailable"
        git.needs_push.assert_not_called()
        git.push_branch.assert_not_called()
        assert capsys.readouterr().out == ""
