"""Integration tests for branch-scoped worktree cleanup."""

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from gza.git import Git, GitError, cleanup_worktree_for_branch
from tests.cli.conftest import make_store
from tests.helpers.cli import run_gza
from tests_functional.git_helpers import setup_git_repo_with_task_branch

pytestmark = pytest.mark.integration


def _git_common_dir(git: Git) -> Path:
    raw = git._run("rev-parse", "--git-common-dir").stdout.strip()
    path = Path(raw)
    if path.is_absolute():
        return path
    return (git.repo_dir / path).resolve()


def _registration_dir_for_branch(git: Git, branch: str) -> Path | None:
    worktrees_dir = _git_common_dir(git) / "worktrees"
    if not worktrees_dir.exists():
        return None

    expected = f"ref: refs/heads/{branch}"
    for registration_dir in worktrees_dir.iterdir():
        if not registration_dir.is_dir():
            continue
        head_path = registration_dir / "HEAD"
        if not head_path.exists():
            continue
        if head_path.read_text().strip() == expected:
            return registration_dir
    return None


def _create_branch_with_commit(git: Git, branch: str, filename: str) -> None:
    git._run("checkout", "-b", branch)
    (git.repo_dir / filename).write_text(f"{branch}\n")
    git._run("add", filename)
    git._run("commit", "-m", f"Add {branch}")
    git._run("checkout", "main")


def _add_worktree(git: Git, branch: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(path), branch)
    return path


def test_cleanup_worktree_for_branch_preserves_unrelated_prunable_registration(tmp_path: Path) -> None:
    """Cleaning one branch must not prune a different stale registration."""
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial\n")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")

    _create_branch_with_commit(git, "feature/target", "target.txt")
    _create_branch_with_commit(git, "feature/unrelated", "unrelated.txt")

    target_path = _add_worktree(git, "feature/target", tmp_path / "worktrees" / "target")
    unrelated_path = _add_worktree(git, "feature/unrelated", tmp_path / "worktrees" / "unrelated")
    unrelated_registration = _registration_dir_for_branch(git, "feature/unrelated")

    assert unrelated_registration is not None
    assert unrelated_registration.exists()

    shutil.rmtree(unrelated_path)

    cleaned_path = cleanup_worktree_for_branch(git, "feature/target", force=True)

    assert cleaned_path == target_path
    assert _registration_dir_for_branch(git, "feature/target") is None
    assert unrelated_registration.exists()


def test_cleanup_worktree_for_branch_preserves_unrelated_stale_gitdir_registration(tmp_path: Path) -> None:
    """Unrelated registrations with stale gitdir metadata must survive cleanup."""
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial\n")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")

    _create_branch_with_commit(git, "feature/cleanup", "cleanup.txt")
    _create_branch_with_commit(git, "feature/unrelated", "unrelated.txt")

    cleanup_path = _add_worktree(git, "feature/cleanup", tmp_path / "worktrees" / "cleanup")
    unrelated_path = _add_worktree(git, "feature/unrelated", tmp_path / "worktrees" / "unrelated")
    unrelated_registration = _registration_dir_for_branch(git, "feature/unrelated")

    assert unrelated_registration is not None
    (unrelated_registration / "gitdir").write_text("/nonexistent/old/path/.git\n")

    cleaned_path = cleanup_worktree_for_branch(git, "feature/cleanup", force=True)

    assert cleaned_path == cleanup_path
    assert unrelated_path.exists()
    assert unrelated_registration.exists()


def test_rebase_retry_chain_preserves_unrelated_prunable_registration(tmp_path: Path) -> None:
    """Repeated rebase cleanup must not sweep unrelated stale registrations."""
    _store, git, task, _worktree_path = setup_git_repo_with_task_branch(
        tmp_path,
        "Test rebase cleanup scope",
        "feature/rebase-target",
    )

    _create_branch_with_commit(git, "feature/unrelated", "unrelated.txt")
    unrelated_path = _add_worktree(git, "feature/unrelated", tmp_path / "worktrees" / "unrelated")
    unrelated_registration = _registration_dir_for_branch(git, "feature/unrelated")

    assert unrelated_registration is not None
    shutil.rmtree(unrelated_path)

    attempts = {"count": 0}

    def fake_rebase(self: Git, branch: str) -> None:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise GitError(f"conflict on attempt {attempts['count']}")

    with (
        patch("gza.git.Git.rebase", new=fake_rebase),
        patch("gza.cli.git_ops.invoke_provider_resolve", return_value=False),
    ):
        first = run_gza("rebase", str(task.id), "--project", str(tmp_path))
        second = run_gza("rebase", str(task.id), "--project", str(tmp_path))
        third = run_gza("rebase", str(task.id), "--project", str(tmp_path))

    assert first.returncode == 1
    assert second.returncode == 1
    assert third.returncode == 0
    assert unrelated_registration.exists()

    store = make_store(tmp_path)
    rebases = [child for child in store.get_based_on_children(task.id) if child.task_type == "rebase"]
    assert len(rebases) == 3
