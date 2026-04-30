"""Tests for branch-scoped sync operations."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

from gza.db import SqliteTaskStore
from gza.github import PullRequestDetails
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
