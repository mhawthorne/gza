"""Functional tests for watch flows that require a real git repo."""

from gza.cli.git_ops import _execute_merge_action
from gza.config import Config

from tests_functional.git_helpers import setup_git_repo_with_task_branch


def test_execute_merge_action_marks_already_merged_task_without_error(tmp_path) -> None:
    store, git, task, _wt = setup_git_repo_with_task_branch(
        tmp_path,
        "Already merged task",
        "feature/watch-already-merged-success",
    )
    config = Config.load(tmp_path)

    assert task.id is not None
    git._run("merge", "--no-ff", task.branch)
    store.set_merge_status(task.id, "unmerged")

    merge_result = _execute_merge_action(
        config,
        store,
        git,
        task,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        already_merged_behavior="mark_merged",
    )

    assert merge_result.rc == 0
    assert merge_result.status == "already_merged"
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "merged"
