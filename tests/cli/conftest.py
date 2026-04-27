"""Shared fixtures and helpers for CLI tests."""

import io
import os
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from gza.cli import main as cli_main
from gza.db import SqliteTaskStore, Task, task_id_numeric_key

LOG_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "logs"


def make_store(tmp_path: Path) -> SqliteTaskStore:
    """Create a SqliteTaskStore with the correct prefix from the project config.

    Call setup_config() before this. Ensures the store prefix matches what the
    CLI will use, so task IDs are consistent.
    """
    from gza.config import Config

    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    config = Config.load(tmp_path)
    return SqliteTaskStore(db_path, prefix=config.project_prefix)


def mark_orphaned(store: SqliteTaskStore, task: Task) -> None:
    """Put a task into an orphaned in_progress state for tests.

    Uses running_pid=None and started_at=None so reconciliation skips the task
    (see reconcile_in_progress_tasks) while orphan detection still classifies it
    as orphaned (no live PID, no worker). Avoids mark_in_progress because that
    would set running_pid to the test process's PID, which is alive.
    """
    task.status = "in_progress"
    task.running_pid = None
    task.started_at = None
    store.update(task)


def get_latest_task(
    store: SqliteTaskStore,
    *,
    task_type: str | None = None,
    based_on: str | None = None,
    depends_on: str | None = None,
    prompt: str | None = None,
) -> Task | None:
    """Get the most recently created task from the store, optionally filtered.

    Useful after a CLI command creates a new task (retry, review, improve)
    when you need to inspect the result.
    """
    tasks = store.get_all()
    if task_type is not None:
        tasks = [task for task in tasks if task.task_type == task_type]
    if based_on is not None:
        tasks = [task for task in tasks if task.based_on == based_on]
    if depends_on is not None:
        tasks = [task for task in tasks if task.depends_on == depends_on]
    if prompt is not None:
        tasks = [task for task in tasks if task.prompt == prompt]
    return max(tasks, key=lambda t: task_id_numeric_key(t.id)) if tasks else None


def run_gza(
    *args: str,
    cwd: Path | None = None,
    stdin_input: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Run gza CLI in-process and capture stdout/stderr like subprocess.run."""
    if args and args[0] in {"diff"}:
        run_env = os.environ.copy()
        if env:
            run_env.update(env)
        return subprocess.run(
            ["uv", "run", "gza", *args],
            capture_output=True,
            text=True,
            cwd=cwd,
            input=stdin_input,
            env=run_env,
        )

    stdout = io.StringIO()
    stderr = io.StringIO()
    old_cwd = Path.cwd()
    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    try:
        if cwd is not None:
            os.chdir(cwd)
        with (
            patch.dict(os.environ, run_env, clear=True),
            patch.object(sys, "argv", ["gza", *args]),
            patch("sys.stdin", io.StringIO(stdin_input or "")),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            try:
                returncode = cli_main()
            except SystemExit as exc:
                code = exc.code
                returncode = code if isinstance(code, int) else 1
    finally:
        os.chdir(old_cwd)

    return subprocess.CompletedProcess(
        args=["uv", "run", "gza", *args],
        returncode=returncode,
        stdout=stdout.getvalue(),
        stderr=stderr.getvalue(),
    )


def setup_config(tmp_path: Path, project_name: str = "test-project") -> None:
    """Set up a minimal gza config file."""
    config_path = tmp_path / "gza.yaml"
    worktree_dir = tmp_path / ".gza-test-worktrees"
    db_path = tmp_path / ".gza" / "gza.db"
    config_path.write_text(
        f"project_name: {project_name}\n"
        f"worktree_dir: {worktree_dir}\n"
        f"db_path: {db_path}\n"
    )


def setup_db_with_tasks(tmp_path: Path, tasks: list[dict], project_name: str = "test-project") -> None:
    """Set up a SQLite database with the given tasks (also creates config)."""
    # Ensure config exists
    setup_config(tmp_path, project_name=project_name)

    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Load the config to get the sanitized project_prefix, matching what the CLI uses.
    from gza.config import Config
    config = Config.load(tmp_path)
    store = SqliteTaskStore(db_path, prefix=config.project_prefix)

    for task_data in tasks:
        task = store.add(task_data["prompt"], task_type=task_data.get("task_type", "implement"))
        task.status = task_data.get("status", "pending")
        if task.status in ("completed", "failed"):
            task.completed_at = datetime.now(UTC)
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
    store = make_store(tmp_path)

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
        task.completed_at = datetime.now(UTC)
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
    store = make_store(tmp_path)

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
        task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = has_commits
    task.merge_status = merge_status
    task.slug = task_id
    store.update(task)

    # Create feature branch (with a commit only if has_commits is True)
    git._run("checkout", "-b", branch)
    if has_commits:
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    return store, task, git
