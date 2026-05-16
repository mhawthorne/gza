"""Integration tests for git-operations flows that require real git/worktrees."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.git import Git, active_worktree_path_for_branch
from tests.cli.conftest import make_store, run_gza, setup_config
from tests_functional.git_helpers import setup_git_repo_with_task_branch

pytestmark = pytest.mark.integration


def _setup_git_repo(tmp_path: Path) -> Git:
    """Initialize a git repo in tmp_path with an initial commit on main."""
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")
    return git


def test_advance_spawns_rebase_worker_on_conflicts(tmp_path: Path) -> None:
    """advance spawns a background rebase worker when a real conflict exists."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    git = _setup_git_repo(tmp_path)

    branch = "feat/conflicting"
    git._run("checkout", "-b", branch)
    (tmp_path / "README.md").write_text("feature version")
    git._run("add", "README.md")
    git._run("commit", "-m", "Conflict commit")
    git._run("checkout", "main")

    (tmp_path / "README.md").write_text("main version")
    git._run("add", "README.md")
    git._run("commit", "-m", "Main change")

    task = store.add("Conflicting feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    result = run_gza("advance", "--auto", "--project", str(tmp_path))
    assert result.returncode == 0
    assert "rebase" in result.stdout.lower()
    assert "started task" in result.stdout.lower()

    updated_task = store.get(task.id)
    assert updated_task is not None
    assert updated_task.merge_status == "unmerged"


def test_checkout_removes_clean_worktree(tmp_path: Path) -> None:
    """Checkout command removes a real clean worktree before checking out branch."""
    _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
        tmp_path,
        "Test checkout task",
        "feature/test-checkout",
        worktree_name="test-checkout",
    )

    assert worktree_path is not None
    assert worktree_path.exists()

    result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 0
    assert "Removing stale worktree" in result.stdout
    assert "Removed worktree" in result.stdout
    assert "Checked out" in result.stdout


def test_checkout_refuses_foreign_worktree_outside_managed_roots(tmp_path: Path) -> None:
    """Checkout should fail closed when the branch is attached in a foreign worktree."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = _setup_git_repo(tmp_path)

    branch = "feature/foreign-checkout"
    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("feature\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    task = store.add("Checkout foreign branch", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    store.update(task)

    foreign_path = tmp_path / "user-worktrees" / "foreign-checkout"
    foreign_path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(foreign_path), branch)
    sentinel = foreign_path / "sentinel.txt"
    sentinel.write_text("keep\n")

    result = run_gza("checkout", str(task.id), "--project", str(tmp_path))
    combined_output = result.stdout + result.stderr

    assert result.returncode == 1
    assert "Refusing to remove worktree for branch 'feature/foreign-checkout'" in combined_output
    assert "git worktree remove" in combined_output
    assert sentinel.exists()
    assert active_worktree_path_for_branch(git, branch) == foreign_path.resolve(strict=False)
    assert Git(foreign_path).current_branch() == branch


def test_rebase_refuses_foreign_worktree_outside_managed_roots(tmp_path: Path) -> None:
    """Foreground rebase should stop before recreating a foreign-managed branch checkout."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = _setup_git_repo(tmp_path)

    branch = "feature/foreign-rebase"
    git._run("checkout", "-b", branch)
    (tmp_path / "feature.txt").write_text("feature\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    task = store.add("Rebase foreign branch", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    store.update(task)

    foreign_path = tmp_path / "user-worktrees" / "foreign-rebase"
    foreign_path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(foreign_path), branch)
    sentinel = foreign_path / "sentinel.txt"
    sentinel.write_text("keep\n")

    result = run_gza("rebase", str(task.id), "--project", str(tmp_path))
    combined_output = result.stdout + result.stderr

    assert result.returncode == 1
    assert "Error setting up worktree" in combined_output
    assert "Refusing to remove worktree for branch 'feature/foreign-rebase'" in combined_output
    assert sentinel.exists()
    assert active_worktree_path_for_branch(git, branch) == foreign_path.resolve(strict=False)
    assert Git(foreign_path).current_branch() == branch
