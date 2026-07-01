"""Functional tests for sync ops that require a real git repo."""

from datetime import UTC, datetime

from gza.db import SqliteTaskStore
from gza.sync_ops import BranchCohort, reconcile_branch_merge_truth, revalidate_terminal_no_work_merge_units
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


def test_reconcile_branch_merge_truth_never_diverged_branch_classifies_as_redundant(tmp_path) -> None:
    """A never-diverged branch with task commits recorded reconciles to 'redundant'."""
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

    assert results[0].merge_status == "redundant"
    assert "marked merged" not in results[0].actions


def test_revalidate_terminal_no_work_merge_units_recovers_false_redundant_real_git_repo(tmp_path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = init_basic_repo(repo_dir)

    git._run("checkout", "-b", "feature/recover-false-redundant")
    (repo_dir / "feature.txt").write_text("feature work\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Feature commit")
    recorded_head_sha = git._run("rev-parse", "HEAD").stdout.strip()
    git._run("checkout", "main")
    git._run("branch", "-f", "feature/recover-false-redundant", "main")

    store = SqliteTaskStore(tmp_path / "test.db")
    task = _completed_branch_task(store, "Task", "feature/recover-false-redundant")
    assert task.id is not None
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.refresh_merge_unit_head(unit.id, head_sha=recorded_head_sha)
    store.set_merge_unit_state(unit.id, "redundant")

    results = revalidate_terminal_no_work_merge_units(store, git)

    assert len(results) == 1
    assert results[0].merge_status == "unmerged"
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
