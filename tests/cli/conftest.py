"""Shared fixtures and helpers for CLI tests."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gza.db import SqliteTaskStore, Task, task_id_numeric_key
from tests.helpers.cli import invoke_gza

__all__ = ["invoke_gza"]

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


def _make_default_watch_git() -> MagicMock:
    """Return a pre-configured mock Git for the watch cycle (branch_exists=True)."""
    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.ref_exists.return_value = False
    git.can_merge.return_value = True
    git.is_merged.return_value = False
    git.count_commits_ahead.return_value = 1
    git.get_diff_stat_parsed.return_value = (1, 1, 0)
    git.get_diff_numstat.return_value = "1\t0\tfeature.txt\n"
    return git


def _make_default_runner_git() -> MagicMock:
    """Return a pre-configured mock Git for the runner (branch_exists=False for slug generation)."""
    git = MagicMock()
    git.branch_exists.return_value = False
    git.ref_exists.return_value = False
    git.default_branch.return_value = "main"
    return git


@pytest.fixture(autouse=True)
def _mock_watch_git(monkeypatch):
    """Prevent _run_cycle from creating real Git objects in the unit lane.

    _run_cycle (watch.py) constructs Git(config.project_dir) unconditionally,
    then calls git.default_branch() and passes the object to
    _query_owner_rows_with_context, which calls git.local_branch_names() via
    build_merge_context_from_git. On a non-git tmp_path those subprocess calls
    can block long enough to trip the 2-second unit watchdog under xdist.

    Also patches:
    - gza.runner.Git: called by prepare_task_startup_phase when generating slug
      for a new recovery/resume child task. branch_exists=False so generate_slug
      terminates immediately (slug not taken).
    - recovery_engine._load_merge_context: when the mock git is not an instanceof
      gza.git.Git, read_context.merge_context is left None and
      decide_failed_task_recovery falls back to _load_merge_context. Returning a
      no-git _MergeContext prevents all subsidiary git calls.

    Tests that need specific git behaviour override the patch inside their own
    ``with patch(...)`` block — unittest.mock.patch() takes precedence over
    monkeypatch for its duration.
    """
    import gza.cli.watch as _watch_module
    import gza.recovery_engine as _recovery_module
    import gza.runner as _runner_module

    monkeypatch.setattr(_watch_module, "Git", lambda _project_dir: _make_default_watch_git())
    monkeypatch.setattr(_runner_module, "Git", lambda _project_dir: _make_default_runner_git())
    monkeypatch.setattr(
        _recovery_module,
        "_load_merge_context",
        lambda _project_dir=None: _recovery_module._MergeContext(git=None, default_branch="main"),
    )
