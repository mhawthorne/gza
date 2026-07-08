"""Functional tests for merge-state classification in a real git repo."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from gza.db import SqliteTaskStore
from gza.merge_state import resolve_task_merge_state_for_target
from tests_functional.git_helpers import init_basic_repo


def _make_completed_task(store: SqliteTaskStore, *, branch: str, has_commits: bool = True) -> object:
    task = store.add(f"Task for {branch}", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = has_commits
    task.branch = branch
    store.update(task)
    return task


def test_resolve_task_merge_state_empty_branch_real_git_repo(tmp_path: Path) -> None:
    git = init_basic_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db")

    git._run("checkout", "-b", "feature/empty")
    git._run("checkout", "main")
    task = _make_completed_task(store, branch="feature/empty", has_commits=False)

    assert resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=git,
        target_branch="main",
    ) == "empty"


def test_resolve_task_merge_state_stale_empty_branch_after_main_advances(tmp_path: Path) -> None:
    # B1: feature/stale-empty is created from main, main then advances
    # independently, and the branch never receives a commit. Its tip is an
    # ancestor of main (is_merged True) with zero commits ahead of the merge
    # base, and source_sha != target_sha because main moved. It carried no work,
    # so it must classify as `empty`, not `merged`.
    git = init_basic_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db")

    git._run("checkout", "-b", "feature/stale-empty")
    git._run("checkout", "main")
    (tmp_path / "advance.txt").write_text("main moved on\n")
    git._run("add", "advance.txt")
    git._run("commit", "-m", "Main advances after branch creation")
    task = _make_completed_task(store, branch="feature/stale-empty", has_commits=False)

    assert resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=git,
        target_branch="main",
    ) == "empty"


def test_resolve_task_merge_state_real_merged_branch_with_commits(tmp_path: Path) -> None:
    git = init_basic_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db")

    git._run("checkout", "-b", "feature/merged")
    (tmp_path / "feature.txt").write_text("feature\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Feature commit")
    git._run("checkout", "main")
    git._run("merge", "--no-ff", "feature/merged", "-m", "Merge feature")
    task = _make_completed_task(store, branch="feature/merged")

    assert resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=git,
        target_branch="main",
    ) == "merged"


def test_resolve_task_merge_state_real_squash_equivalent_branch_with_commits(tmp_path: Path) -> None:
    git = init_basic_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db")

    git._run("checkout", "-b", "feature/squash")
    (tmp_path / "feature.txt").write_text("same content\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Feature commit")
    git._run("checkout", "main")
    (tmp_path / "feature.txt").write_text("same content\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Apply equivalent content")
    task = _make_completed_task(store, branch="feature/squash")

    assert resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=git,
        target_branch="main",
    ) == "merged"


def test_resolve_task_merge_state_missing_ref_preserves_persisted_empty_state(tmp_path: Path) -> None:
    git = init_basic_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db")

    git._run("checkout", "-b", "feature/missing-empty")
    git._run("checkout", "main")
    git._run("branch", "-D", "feature/missing-empty")

    task = _make_completed_task(store, branch="feature/missing-empty")
    assert task.id is not None
    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.set_merge_unit_state(unit.id, "empty")

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert resolve_task_merge_state_for_target(
        store=store,
        task=refreshed,
        git=git,
        target_branch="main",
    ) == "empty"
