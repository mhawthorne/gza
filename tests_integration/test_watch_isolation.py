"""Integration coverage for watch merge isolation."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.cli.git_ops import _execute_merge_action, ensure_watch_main_checkout
from gza.config import Config
from tests.cli.conftest import setup_git_repo_with_task_branch

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


def test_isolated_watch_merge_advances_primary_main_checkout_cleanly(tmp_path: Path) -> None:
    """A successful isolated watch merge must land on main and keep the attached checkout clean."""
    store, git, task, _wt = setup_git_repo_with_task_branch(
        tmp_path,
        "Successful isolated merge",
        "feature/watch-isolated-success",
    )
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)

    assert task.id is not None
    assert git.current_branch() == "main"

    isolated_git = ensure_watch_main_checkout(config, git, "main")
    merge_result = _execute_merge_action(
        config,
        store,
        git,
        task,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        merge_git=isolated_git,
        merge_current_branch="main",
    )

    assert merge_result.rc == 0
    assert isolated_git.current_branch() == "HEAD"
    assert isolated_git.has_changes(include_untracked=True) is False
    assert (config.main_checkout_integration_path / "feature.txt").exists()
    assert git.current_branch() == "main"
    assert git.has_changes(include_untracked=False) is False
    assert (tmp_path / "feature.txt").exists()
    assert git.is_merged(task.branch, "main") is True
    assert git.rev_parse("main") == isolated_git.rev_parse("HEAD")
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "merged"


def test_isolated_watch_merge_promotes_real_main_before_marking_sequential_merges(tmp_path: Path) -> None:
    """Sequential isolated merges must advance the real main ref before merge_status flips."""
    store, git, task1, _wt = setup_git_repo_with_task_branch(
        tmp_path,
        "First isolated merge",
        "feature/watch-isolated-first",
    )
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "main_checkout_isolate: true\n")
    config = Config.load(tmp_path)

    assert task1.id is not None
    store.set_merge_status(task1.id, "unmerged")

    task2 = store.add("Second isolated merge", task_type="implement")
    assert task2.id is not None
    task2.status = "completed"
    task2.completed_at = datetime.now(UTC)
    task2.branch = "feature/watch-isolated-second"
    store.update(task2)
    store.set_merge_status(task2.id, "unmerged")

    git._run("checkout", "-b", task2.branch)
    (tmp_path / "second.txt").write_text("second content")
    git._run("add", "second.txt")
    git._run("commit", "-m", "Add second isolated feature")
    git._run("checkout", "main")

    isolated_git = ensure_watch_main_checkout(config, git, "main")

    first_result = _execute_merge_action(
        config,
        store,
        git,
        task1,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        merge_git=isolated_git,
        merge_current_branch="main",
    )

    assert first_result.rc == 0
    assert git.is_merged(task1.branch, "main") is True
    assert store.get(task1.id).merge_status == "merged"
    first_main_oid = git.rev_parse("main")

    second_result = _execute_merge_action(
        config,
        store,
        git,
        task2,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        merge_git=isolated_git,
        merge_current_branch="main",
    )

    assert second_result.rc == 0
    assert git.rev_parse("main") != first_main_oid
    assert git.is_merged(task2.branch, "main") is True
    assert store.get(task2.id).merge_status == "merged"
    assert (tmp_path / "feature.txt").exists()
    assert (tmp_path / "second.txt").exists()
    assert git.has_changes(include_untracked=False) is False
    assert isolated_git.current_branch() == "HEAD"
    assert isolated_git.has_changes(include_untracked=True) is False
    assert isolated_git.rev_parse("HEAD") == git.rev_parse("main")
