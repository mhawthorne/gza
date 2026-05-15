"""Functional tests for clean-command flows that require a real git repo."""

import os
import time
from datetime import UTC, datetime

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git
from gza.workers import WorkerMetadata, WorkerRegistry
from tests.cli.conftest import run_gza, setup_config


def _init_git_repo(tmp_path) -> Git:
    git = Git(tmp_path)
    git._run("init")
    git._run("config", "user.email", "test@example.com")
    git._run("config", "user.name", "Test User")
    (tmp_path / "README.md").write_text("# Test")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")
    return git


def test_clean_dry_run(tmp_path) -> None:
    _init_git_repo(tmp_path)
    setup_config(tmp_path)
    config = Config.load(tmp_path)

    registry = WorkerRegistry(config.workers_path)
    worker_meta = {
        "worker_id": registry.generate_worker_id(),
        "pid": 99999,
        "task_id": None,
        "task_slug": None,
        "started_at": datetime.now(UTC).isoformat(),
        "status": "running",
        "log_file": None,
        "worktree": None,
        "is_background": True,
    }
    registry.register(WorkerMetadata.from_dict(worker_meta))

    old_log = config.log_path / "20200101-old-task.log"
    old_log.parent.mkdir(parents=True, exist_ok=True)
    old_log.write_text("old log content")
    old_time = time.time() - (60 * 24 * 60 * 60)
    os.utime(old_log, (old_time, old_time))

    result = run_gza("clean", "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "Dry run" in result.stdout
    assert old_log.exists()


def test_clean_keep_unmerged_logs(tmp_path) -> None:
    git = _init_git_repo(tmp_path)
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(config.db_path)

    unmerged_task = store.add("Unmerged feature", task_type="implement")
    unmerged_task.status = "completed"
    unmerged_task.slug = "20200101-unmerged"
    unmerged_task.branch = "feature/unmerged"
    unmerged_task.has_commits = True
    unmerged_task.completed_at = datetime.now(UTC)
    store.update(unmerged_task)
    assert unmerged_task.id is not None

    git._run("checkout", "-b", "feature/unmerged")
    (tmp_path / "feature.txt").write_text("unmerged feature")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add unmerged feature")
    git._run("checkout", "master")

    unit = store.get_or_create_merge_unit_for_task(unmerged_task)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "unmerged")
    stale_row = store.get(unmerged_task.id)
    assert stale_row is not None
    stale_row.merge_status = "merged"
    store.update(stale_row)

    unmerged_log = config.log_path / "20200101-unmerged.log"
    merged_log = config.log_path / "20200102-merged.log"
    config.log_path.mkdir(parents=True, exist_ok=True)
    unmerged_log.write_text("unmerged log")
    merged_log.write_text("merged log")
    old_time = time.time() - (60 * 24 * 60 * 60)
    os.utime(unmerged_log, (old_time, old_time))
    os.utime(merged_log, (old_time, old_time))

    result = run_gza("clean", "--logs", "--days", "30", "--keep-unmerged", "--project", str(tmp_path))

    assert result.returncode == 0
    assert unmerged_log.exists()
    assert not merged_log.exists()


def test_clean_lineage_aware_preserves_recent(tmp_path) -> None:
    git = _init_git_repo(tmp_path)
    wt_base = tmp_path / "worktrees"
    (tmp_path / "gza.yaml").write_text(
        f"project_name: test-project\n"
        "project_id: default\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {wt_base}\n"
    )
    config = Config.load(tmp_path)
    store = SqliteTaskStore(config.db_path)

    task = store.add("Recent feature", task_type="implement")
    task.slug = "20260301-recent-feature"
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    wt_path = config.worktree_path / "20260301-recent-feature"
    wt_path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(wt_path), "-b", "wt-recent")

    result = run_gza("clean", "--worktrees", "--force", "--days", "7", "--project", str(tmp_path))

    assert result.returncode == 0
    assert wt_path.exists()


def test_clean_lineage_aware_removes_old(tmp_path) -> None:
    git = _init_git_repo(tmp_path)
    wt_base = tmp_path / "worktrees"
    (tmp_path / "gza.yaml").write_text(
        f"project_name: test-project\n"
        "project_id: default\n"
        "db_path: .gza/gza.db\n"
        f"worktree_dir: {wt_base}\n"
    )
    config = Config.load(tmp_path)
    store = SqliteTaskStore(config.db_path)

    task = store.add("Old feature", task_type="implement")
    task.slug = "20250101-old-feature"
    task.status = "completed"
    task.completed_at = datetime(2025, 1, 1, tzinfo=UTC)
    store.update(task)

    wt_path = config.worktree_path / "20250101-old-feature"
    wt_path.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(wt_path), "-b", "wt-old")

    result = run_gza("clean", "--worktrees", "--force", "--days", "7", "--project", str(tmp_path))

    assert result.returncode == 0
    assert not wt_path.exists()
    assert "lineage inactive" in result.stdout


def test_clean_force_skips_prompt(tmp_path) -> None:
    _init_git_repo(tmp_path)
    wt_base = tmp_path / "worktrees"
    (tmp_path / "gza.yaml").write_text(f"project_name: test-project\nworktree_dir: {wt_base}\n")
    config = Config.load(tmp_path)

    orphan = config.worktree_path / "orphaned-dir"
    orphan.mkdir(parents=True)
    (orphan / "dummy.txt").write_text("dummy")

    result = run_gza("clean", "--worktrees", "--force", "--project", str(tmp_path))

    assert result.returncode == 0
    assert not orphan.exists()
    assert "orphaned" in result.stdout


def test_clean_no_force_denies_removal(tmp_path) -> None:
    _init_git_repo(tmp_path)
    wt_base = tmp_path / "worktrees"
    (tmp_path / "gza.yaml").write_text(f"project_name: test-project\nworktree_dir: {wt_base}\n")
    config = Config.load(tmp_path)

    orphan = config.worktree_path / "orphaned-dir"
    orphan.mkdir(parents=True)
    (orphan / "dummy.txt").write_text("dummy")

    result = run_gza("clean", "--worktrees", "--project", str(tmp_path), stdin_input="n\n")

    assert result.returncode == 0
    assert orphan.exists()
    assert "Skipped worktree removal" in result.stdout
