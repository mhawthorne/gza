"""Functional tests for sync ops that require a real git repo."""

from datetime import UTC, datetime

from gza.db import SqliteTaskStore
from gza.sync_ops import BranchCohort, reconcile_branch_merge_truth
from tests_functional.git_helpers import init_basic_repo


def _completed_branch_task(store: SqliteTaskStore, prompt: str, branch: str):
    task = store.add(prompt, task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task


def test_reconcile_branch_merge_truth_never_diverged_branch_classifies_as_empty(tmp_path) -> None:
    """A branch created at main HEAD with no commits reconciles to 'empty'."""
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = init_basic_repo(repo_dir)

    git._run("checkout", "-b", "feature/never-diverged")
    git._run("checkout", "main")

    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/never-diverged")
    cohort = BranchCohort(branch=task.branch, tasks=(task,))

    results = reconcile_branch_merge_truth(
        git,
        [cohort],
        target_branch="main",
        include_diff_stats=False,
    )

    assert results[0].merge_status == "empty"
    assert "marked merged" not in results[0].actions
