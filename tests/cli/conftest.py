"""Shared fixtures and helpers for CLI tests."""

import subprocess
from datetime import datetime, timezone
from pathlib import Path

from gza.db import SqliteTaskStore

LOG_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "logs"


def run_gza(*args: str, cwd: Path | None = None, stdin_input: str | None = None) -> subprocess.CompletedProcess:
    """Run gza command and return result."""
    return subprocess.run(
        ["uv", "run", "gza", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
        input=stdin_input,
    )


def setup_config(tmp_path: Path, project_name: str = "test-project") -> None:
    """Set up a minimal gza config file."""
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(f"project_name: {project_name}\n")


def setup_db_with_tasks(tmp_path: Path, tasks: list[dict]) -> None:
    """Set up a SQLite database with the given tasks (also creates config)."""
    # Ensure config exists
    setup_config(tmp_path)

    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path)

    for task_data in tasks:
        task = store.add(task_data["prompt"], task_type=task_data.get("task_type", "implement"))
        task.status = task_data.get("status", "pending")
        if task.status in ("completed", "failed"):
            task.completed_at = datetime.now(timezone.utc)
        store.update(task)


def setup_git_repo_with_task_branch(
    tmp_path: Path,
    task_prompt: str,
    branch_name: str,
    *,
    status: str = "completed",
    worktree_name: str | None = None,
):
    """Set up a git repo with a task on a feature branch.

    Returns (store, git, task, worktree_path). worktree_path is None when
    worktree_name is not provided.
    """
    from gza.git import Git

    setup_config(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path)

    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "file.txt").write_text("initial")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Initial commit")

    task = store.add(task_prompt)
    task.status = status
    if status in ("completed", "failed"):
        task.completed_at = datetime.now(timezone.utc)
    task.branch = branch_name
    store.update(task)

    git._run("checkout", "-b", branch_name)
    (tmp_path / "feature.txt").write_text("feature content")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    worktree_path = None
    if worktree_name is not None:
        worktree_path = tmp_path / "worktrees" / worktree_name
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), branch_name)

    return store, git, task, worktree_path


def setup_unmerged_env(
    tmp_path: Path,
    *,
    task_prompt: str = "Add feature",
    task_type: str = "implement",
    task_id: str = "20260212-add-feature",
    branch: str = "feature/test",
    merge_status: str | None = "unmerged",
    status: str = "completed",
    has_commits: bool = True,
):
    """Set up config + git repo + store + a single unmerged task with a feature branch.

    Returns (store, task, git) tuple.
    """
    from gza.git import Git

    setup_config(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path)

    # Initialize git repo with initial commit
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "file.txt").write_text("initial")
    git._run("add", "file.txt")
    git._run("commit", "-m", "Initial commit")

    # Create task
    task = store.add(task_prompt, task_type=task_type)
    task.status = status
    if status in ("completed", "failed", "dropped"):
        task.completed_at = datetime.now(timezone.utc)
    task.branch = branch
    task.has_commits = has_commits
    task.merge_status = merge_status
    task.task_id = task_id
    store.update(task)

    # Create feature branch (with a commit only if has_commits is True)
    git._run("checkout", "-b", branch)
    if has_commits:
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    return store, task, git
