"""Tests for branch-scoped sync operations."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

from gza.db import SqliteTaskStore
from gza.github import GitHubError, PullRequestDetails
from gza.sync_ops import BranchCohort, build_branch_cohorts_for_task_ids, sync_branch_cohorts


def _completed_branch_task(store: SqliteTaskStore, prompt: str, branch: str):
    task = store.add(prompt, task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task


def test_build_branch_cohorts_for_task_ids_expands_same_branch_chains(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    parent = _completed_branch_task(store, "Parent task", "feature/shared")
    child = store.add("Improve task", task_type="improve")
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/shared"
    child.has_commits = True
    child.merge_status = "unmerged"
    child.based_on = parent.id
    child.same_branch = True
    store.update(child)

    cohorts, prelim = build_branch_cohorts_for_task_ids(store, [parent.id])

    assert prelim == []
    assert len(cohorts) == 1
    assert cohorts[0].branch == "feature/shared"
    assert {task.id for task in cohorts[0].tasks} == {parent.id, child.id}


def test_sync_branch_cohorts_normalizes_same_branch_rows(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    parent = _completed_branch_task(store, "Parent task", "feature/shared")
    child = store.add("Improve task", task_type="improve")
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/shared"
    child.has_commits = True
    child.merge_status = "unmerged"
    child.based_on = parent.id
    child.same_branch = True
    store.update(child)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/shared", tasks=tuple(store.get_tasks_for_branch("feature/shared")))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert results[0].diff_files_changed == 1
    refreshed_parent = store.get(parent.id)
    refreshed_child = store.get(child.id)
    assert refreshed_parent is not None
    assert refreshed_child is not None
    assert refreshed_parent.diff_files_changed == 1
    assert refreshed_child.diff_files_changed == 1
    assert refreshed_parent.merge_status == "unmerged"
    assert refreshed_child.merge_status == "unmerged"


def test_sync_branch_cohorts_pr_merged_marks_merge_status_without_git_phase(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with PR", "feature/pr-merged")
    task.pr_number = 12
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/12",
        number=12,
        state="merged",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-merged", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].pr_state == "merged"
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.pr_state == "merged"


def test_sync_branch_cohorts_prefers_discovered_open_pr_over_closed_cached_pr(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with replaced PR", "feature/reused-pr")
    task.pr_number = 12
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/12",
        number=12,
        state="closed",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/13",
        number=13,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/reused-pr", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].pr_number == 13
    assert results[0].pr_state == "open"
    assert "discovered PR #13 (open)" in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.pr_number == 13
    assert refreshed.pr_state == "open"


def test_sync_branch_cohorts_closes_stale_open_pr_when_origin_proves_merge(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with stale PR", "feature/stale-pr")
    task.pr_number = 21
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = ""

    def _is_merged(branch, into):
        return into == "origin/main"

    git.is_merged.side_effect = _is_merged

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/21",
        number=21,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/stale-pr", tasks=(task,))],
            include_git=True,
            include_pr=True,
            dry_run=False,
            fetch_remote=True,
        )

    assert partial is False
    gh.add_pr_comment.assert_called_once()
    gh.close_pr.assert_called_once_with(21)
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.pr_state == "closed"
    assert "closed stale PR #21" in results[0].actions


def test_sync_branch_cohorts_pr_only_closes_stale_open_pr_when_origin_proves_merge(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with stale PR", "feature/pr-only-close")
    task.pr_number = 31
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True

    def _is_merged(branch, into):
        return into == "origin/main"

    git.is_merged.side_effect = _is_merged

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/31",
        number=31,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-close", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=True,
        )

    assert partial is False
    gh.add_pr_comment.assert_called_once()
    gh.close_pr.assert_called_once_with(31)
    git.get_diff_numstat.assert_not_called()
    assert results[0].diff_files_changed is None
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    assert refreshed.pr_state == "closed"


def test_sync_branch_cohorts_pr_only_does_not_refresh_diff_stats(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with cached diff stats", "feature/pr-only-no-diff")
    task.pr_number = 41
    task.diff_files_changed = 7
    task.diff_lines_added = 20
    task.diff_lines_removed = 4
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/41",
        number=41,
        state="merged",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-no-diff", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    git.get_diff_numstat.assert_not_called()
    assert results[0].diff_files_changed is None
    assert results[0].diff_lines_added is None
    assert results[0].diff_lines_removed is None
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.diff_files_changed == 7
    assert refreshed.diff_lines_added == 20
    assert refreshed.diff_lines_removed == 4


def test_sync_branch_cohorts_preserves_cached_pr_state_on_lookup_failure(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with cached PR", "feature/pr-lookup-failure")
    old_synced_at = datetime.now(UTC) - timedelta(days=2)
    task.pr_number = 41
    task.pr_state = "open"
    task.pr_last_synced_at = old_synced_at
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.side_effect = GitHubError("gh pr view 41 failed: authentication failed")

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-lookup-failure", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is True
    assert results[0].errors == [
        "failed to look up cached PR #41 for branch 'feature/pr-lookup-failure': gh pr view 41 failed: authentication failed"
    ]
    assert "cleared stale cached PR" not in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.pr_number == 41
    assert refreshed.pr_state == "open"
    assert refreshed.pr_last_synced_at == old_synced_at


def test_sync_branch_cohorts_pr_only_does_not_mark_merged_from_local_git_heuristic(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with local merge heuristic", "feature/pr-only-local-merge")
    task.pr_number = 51
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = True

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/51",
        number=51,
        state="open",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-local-merge", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_sync_branch_cohorts_pr_only_does_not_mark_merged_when_branch_missing_without_pr_proof(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with missing local branch", "feature/pr-only-missing-branch")
    task.pr_number = 61
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = False
    git.is_merged.return_value = True

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/61",
        number=61,
        state="open",
        base_ref_name="main",
    )
    gh.discover_pr_by_branch.return_value = None

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/pr-only-missing-branch", tasks=(task,))],
            include_git=False,
            include_pr=True,
            dry_run=False,
            fetch_remote=False,
        )

    assert partial is False
    assert results[0].merge_status == "unmerged"
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "unmerged"


def test_sync_branch_cohorts_does_not_close_pr_when_branch_missing_locally(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task with missing branch", "feature/missing-branch")
    task.pr_number = 34
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.ref_exists.return_value = True
    git.branch_exists.return_value = False
    git.is_merged.return_value = True
    git.get_diff_numstat.return_value = ""

    gh = Mock()
    gh.is_available.return_value = True
    gh.get_pr_details.return_value = PullRequestDetails(
        url="https://github.com/o/r/pull/34",
        number=34,
        state="open",
        base_ref_name="main",
    )

    with patch("gza.sync_ops.GitHub", return_value=gh):
        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/missing-branch", tasks=(task,))],
            include_git=True,
            include_pr=True,
            dry_run=False,
            fetch_remote=True,
        )

    assert partial is False
    gh.add_pr_comment.assert_not_called()
    gh.close_pr.assert_not_called()
    assert "closed stale PR #34" not in results[0].actions


def test_sync_branch_cohorts_preserves_existing_merged_at_for_already_merged_branch(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Merged task", "feature/already-merged")
    old_merged_at = datetime.now(UTC) - timedelta(days=45)
    task.merge_status = "merged"
    task.merged_at = old_merged_at
    store.update(task)

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = True

    results, partial = sync_branch_cohorts(
        store,
        git,
        [BranchCohort(branch="feature/already-merged", tasks=(task,))],
        include_git=True,
        include_pr=False,
        dry_run=False,
        fetch_remote=False,
    )

    assert partial is False
    assert "marked merged" in results[0].actions
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merged_at == old_merged_at
