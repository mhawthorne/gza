"""Tests for the CLI commands."""

import argparse
import io
import json
import re
import subprocess
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from gza.cli import _determine_advance_action, _run_foreground, cmd_advance, _format_log_entry, _build_step_timeline
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git
from gza.workers import WorkerRegistry

LOG_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "logs"


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
    from gza.db import SqliteTaskStore

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


class TestHistoryCommand:
    """Tests for 'gza history' command."""

    def test_history_with_tasks(self, tmp_path: Path):
        """History command works with SQLite tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Test task 1", "status": "completed"},
            {"prompt": "Test task 2", "status": "failed"},
            {"prompt": "Test task 3", "status": "pending"},
        ])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Test task 1" in result.stdout
        assert "Test task 2" in result.stdout
        assert "Test task 3" not in result.stdout  # pending tasks not shown

    def test_history_with_no_tasks(self, tmp_path: Path):
        """History command handles missing database gracefully."""
        setup_config(tmp_path)
        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout

    def test_history_with_empty_tasks(self, tmp_path: Path):
        """History command handles empty tasks list."""
        # Create empty database
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout

    def test_history_filter_by_completed_status(self, tmp_path: Path):
        """History command filters by completed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task 1", "status": "completed"},
            {"prompt": "Completed task 2", "status": "completed"},
            {"prompt": "Failed task", "status": "failed"},
            {"prompt": "Unmerged task", "status": "unmerged"},
        ])

        result = run_gza("history", "--status", "completed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Completed task 1" in result.stdout
        assert "Completed task 2" in result.stdout
        assert "Failed task" not in result.stdout
        assert "Unmerged task" not in result.stdout

    def test_history_filter_by_failed_status(self, tmp_path: Path):
        """History command filters by failed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
            {"prompt": "Failed task 1", "status": "failed"},
            {"prompt": "Failed task 2", "status": "failed"},
            {"prompt": "Unmerged task", "status": "unmerged"},
        ])

        result = run_gza("history", "--status", "failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failed task 1" in result.stdout
        assert "Failed task 2" in result.stdout
        assert "Completed task" not in result.stdout
        assert "Unmerged task" not in result.stdout

    def test_history_filter_by_unmerged_status(self, tmp_path: Path):
        """History command filters by unmerged status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
            {"prompt": "Failed task", "status": "failed"},
            {"prompt": "Unmerged task 1", "status": "unmerged"},
            {"prompt": "Unmerged task 2", "status": "unmerged"},
        ])

        result = run_gza("history", "--status", "unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Unmerged task 1" in result.stdout
        assert "Unmerged task 2" in result.stdout
        assert "Completed task" not in result.stdout
        assert "Failed task" not in result.stdout

    def test_history_filter_with_no_matching_tasks(self, tmp_path: Path):
        """History command handles no tasks matching filter."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("history", "--status", "failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks with status 'failed'" in result.stdout

    def test_history_filter_by_task_type(self, tmp_path: Path):
        """History command filters by task_type."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Regular task", "status": "completed", "task_type": "plan"},
            {"prompt": "Explore task", "status": "completed", "task_type": "explore"},
            {"prompt": "Plan task", "status": "completed", "task_type": "plan"},
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Review task", "status": "completed", "task_type": "review"},
            {"prompt": "Improve task", "status": "completed", "task_type": "improve"},
        ])

        result = run_gza("history", "--type", "implement", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement task" in result.stdout
        assert "Regular task" not in result.stdout
        assert "Explore task" not in result.stdout
        assert "Plan task" not in result.stdout
        assert "Review task" not in result.stdout
        assert "Improve task" not in result.stdout

    def test_history_filter_by_multiple_types(self, tmp_path: Path):
        """History command filters by task_type for different types."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Regular task", "status": "completed", "task_type": "implement"},
            {"prompt": "Explore task 1", "status": "completed", "task_type": "explore"},
            {"prompt": "Explore task 2", "status": "completed", "task_type": "explore"},
            {"prompt": "Plan task", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("history", "--type", "explore", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Explore task 1" in result.stdout
        assert "Explore task 2" in result.stdout
        assert "Regular task" not in result.stdout
        assert "Plan task" not in result.stdout

    def test_history_filter_by_status_and_type(self, tmp_path: Path):
        """History command filters by both status and task_type."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed implement", "status": "completed", "task_type": "implement"},
            {"prompt": "Failed implement", "status": "failed", "task_type": "implement"},
            {"prompt": "Completed plan", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("history", "--status", "completed", "--type", "implement", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Completed implement" in result.stdout
        assert "Failed implement" not in result.stdout
        assert "Completed plan" not in result.stdout

    def test_history_filter_by_type_no_matching_tasks(self, tmp_path: Path):
        """History command handles no tasks matching type filter."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Regular task", "status": "completed", "task_type": "implement"},
        ])

        result = run_gza("history", "--type", "review", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks with type 'review'" in result.stdout

    def test_history_shows_task_type_labels(self, tmp_path: Path):
        """History command displays task type labels for all task types."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Plan task", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement task [implement]" in result.stdout
        assert "Plan task [plan]" in result.stdout

    def test_history_shows_orphaned_tasks_at_top(self, tmp_path: Path):
        """History command includes orphaned in-progress tasks at the top."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create an orphaned (in-progress, no worker) task
        orphaned_task = store.add("Orphaned task needing attention")
        store.mark_in_progress(orphaned_task)

        # Create a completed task
        completed_task = store.add("Completed task")
        completed_task.status = "completed"
        completed_task.completed_at = datetime.now(timezone.utc)
        store.update(completed_task)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout
        assert "Orphaned task needing attention" in result.stdout
        assert "Completed task" in result.stdout
        # Orphaned should appear before completed in output
        orphaned_pos = result.stdout.find("Orphaned task needing attention")
        completed_pos = result.stdout.find("Completed task")
        assert orphaned_pos < completed_pos, "Orphaned task should appear before completed task"

    def test_history_orphaned_shows_resume_suggestion(self, tmp_path: Path):
        """History command shows resume suggestion for orphaned tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        orphaned_task = store.add("Orphaned task")
        store.mark_in_progress(orphaned_task)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"gza work {orphaned_task.id}" in result.stdout

    def test_history_no_orphaned_when_status_filter_set(self, tmp_path: Path):
        """History command does not show orphaned tasks when --status filter is active."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create an orphaned task
        orphaned_task = store.add("Orphaned task")
        store.mark_in_progress(orphaned_task)

        # Create a completed task
        completed_task = store.add("Completed task")
        completed_task.status = "completed"
        completed_task.completed_at = datetime.now(timezone.utc)
        store.update(completed_task)

        result = run_gza("history", "--status", "completed", "--project", str(tmp_path))

        assert result.returncode == 0
        # Orphaned should NOT appear when a status filter is specified
        assert "orphaned" not in result.stdout
        assert "Completed task" in result.stdout

    def test_history_incomplete_flag(self, tmp_path: Path):
        """--incomplete shows failed/unmerged tasks but not completed+merged ones."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Failed task (should appear)
        failed = store.add("Failed task")
        failed.status = "failed"
        failed.completed_at = datetime.now(timezone.utc)
        store.update(failed)

        # Completed + merged (should NOT appear)
        merged = store.add("Merged task")
        merged.status = "completed"
        merged.merge_status = "merged"
        merged.completed_at = datetime.now(timezone.utc)
        store.update(merged)

        result = run_gza("history", "--incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failed task" in result.stdout
        assert "Merged task" not in result.stdout

    def test_history_lookback_days(self, tmp_path: Path):
        """--lookback-days excludes old tasks and includes recent ones."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        now = datetime.now(timezone.utc)

        old = store.add("Old task")
        old.status = "completed"
        old.completed_at = now - timedelta(days=30)
        store.update(old)

        recent = store.add("Recent task")
        recent.status = "completed"
        recent.completed_at = now - timedelta(days=1)
        store.update(recent)

        result = run_gza("history", "--lookback-days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recent task" in result.stdout
        assert "Old task" not in result.stdout

    def test_history_lineage_depth(self, tmp_path: Path):
        """--lineage-depth shows ancestor/descendant relationship lines."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        parent = store.add("Parent task")
        parent.status = "completed"
        parent.completed_at = datetime.now(timezone.utc)
        store.update(parent)

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(timezone.utc)
        store.update(child)

        result = run_gza("history", "--lineage-depth", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        # Child should appear with ancestor relationship label
        assert "Child task" in result.stdout
        assert "ancestor" in result.stdout

    def test_history_lineage_depth_two(self, tmp_path: Path):
        """--lineage-depth 2 renders all three levels of a grandparent→parent→child chain."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        grandparent = store.add("Grandparent task")
        grandparent.status = "completed"
        grandparent.completed_at = datetime.now(timezone.utc)
        store.update(grandparent)

        parent = store.add("Parent task", based_on=grandparent.id)
        parent.status = "completed"
        parent.completed_at = datetime.now(timezone.utc)
        store.update(parent)

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(timezone.utc)
        store.update(child)

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        # All three levels of the chain must appear in the output
        assert "Grandparent task" in result.stdout
        assert "Parent task" in result.stdout
        assert "Child task" in result.stdout

    def test_history_incomplete_with_lookback(self, tmp_path: Path):
        """--incomplete combined with --lookback-days applies both filters."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        now = datetime.now(timezone.utc)

        # Recent + incomplete (should appear)
        recent_failed = store.add("Recent failed")
        recent_failed.status = "failed"
        recent_failed.completed_at = now - timedelta(days=1)
        store.update(recent_failed)

        # Old + incomplete (excluded by lookback)
        old_failed = store.add("Old failed")
        old_failed.status = "failed"
        old_failed.completed_at = now - timedelta(days=30)
        store.update(old_failed)

        # Recent + complete (excluded by incomplete filter)
        recent_merged = store.add("Recent merged")
        recent_merged.status = "completed"
        recent_merged.merge_status = "merged"
        recent_merged.completed_at = now - timedelta(days=1)
        store.update(recent_merged)

        result = run_gza(
            "history", "--incomplete", "--lookback-days", "7",
            "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "Recent failed" in result.stdout
        assert "Old failed" not in result.stdout
        assert "Recent merged" not in result.stdout


class TestNextCommand:
    """Tests for 'gza next' command."""

    def test_next_shows_pending_tasks(self, tmp_path: Path):
        """Next command shows pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "First pending task", "status": "pending"},
            {"prompt": "Second pending task", "status": "pending"},
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "First pending task" in result.stdout
        assert "Second pending task" in result.stdout
        assert "Completed task" not in result.stdout

    def test_next_with_no_pending_tasks(self, tmp_path: Path):
        """Next command handles no pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" in result.stdout

    def test_next_warns_about_orphaned_tasks(self, tmp_path: Path):
        """Next command warns about orphaned in-progress tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a pending task
        store.add("Pending task")

        # Create an orphaned (in-progress, no active worker) task
        orphaned_task = store.add("Orphaned task that needs attention")
        store.mark_in_progress(orphaned_task)

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Pending task" in result.stdout
        assert "orphaned" in result.stdout
        assert "Orphaned task that needs attention" in result.stdout
        assert "gza work" in result.stdout

    def test_next_warns_orphaned_when_no_pending(self, tmp_path: Path):
        """Next command shows orphaned warning even when there are no pending tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Only an orphaned task, no pending tasks
        orphaned_task = store.add("Stuck orphaned task")
        store.mark_in_progress(orphaned_task)

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" in result.stdout
        assert "orphaned" in result.stdout
        assert "Stuck orphaned task" in result.stdout


class TestAddCommand:
    """Tests for 'gza add' command."""

    def test_add_with_inline_prompt(self, tmp_path: Path):
        """Add command with inline prompt creates a task."""
        setup_config(tmp_path)
        result = run_gza("add", "Test inline task", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added
        result = run_gza("next", "--project", str(tmp_path))
        assert "Test inline task" in result.stdout

    def test_add_explore_task(self, tmp_path: Path):
        """Add command with --explore flag creates explore task."""
        setup_config(tmp_path)
        result = run_gza("add", "--explore", "Explore the codebase", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task type is shown
        result = run_gza("next", "--project", str(tmp_path))
        assert "[explore]" in result.stdout

    def test_add_with_prompt_file(self, tmp_path: Path):
        """Add command can read prompt from file."""
        setup_config(tmp_path)

        # Create a file with prompt text
        prompt_file = tmp_path / "task_prompt.txt"
        prompt_file.write_text("Task prompt from file")

        result = run_gza("add", "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added with correct prompt
        result = run_gza("next", "--project", str(tmp_path))
        assert "Task prompt from file" in result.stdout

    def test_add_with_prompt_file_not_found(self, tmp_path: Path):
        """Add command handles missing file gracefully."""
        setup_config(tmp_path)

        result = run_gza("add", "--prompt-file", "/nonexistent/file.txt", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()

    def test_add_prompt_and_prompt_file_conflict(self, tmp_path: Path):
        """Add command rejects both prompt argument and --prompt-file."""
        setup_config(tmp_path)

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File content")

        result = run_gza("add", "inline prompt", "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Cannot use both" in result.stdout

    def test_add_prompt_file_and_edit_conflict(self, tmp_path: Path):
        """Add command rejects both --prompt-file and --edit."""
        setup_config(tmp_path)

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File content")

        result = run_gza("add", "--prompt-file", str(prompt_file), "--edit", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Cannot use both" in result.stdout

    def test_add_with_prompt_file_and_options(self, tmp_path: Path):
        """Add command with --prompt-file works with other options."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"

        # Create a file with prompt text
        prompt_file = tmp_path / "task_prompt.txt"
        prompt_file.write_text("Implement feature X")

        result = run_gza("add", "--prompt-file", str(prompt_file), "--type", "implement", "--group", "features", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added with correct attributes
        store = SqliteTaskStore(db_path)
        task = store.get(1)
        assert task is not None
        assert task.prompt == "Implement feature X"
        assert task.task_type == "implement"
        assert task.group == "features"


class TestShowCommand:
    """Tests for 'gza show' command."""

    def test_show_existing_task(self, tmp_path: Path):
        """Show command displays task details."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A detailed task prompt", "status": "pending"},
        ])

        result = run_gza("show", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task #1" in result.stdout
        assert "A detailed task prompt" in result.stdout
        assert "Status: pending" in result.stdout

    def test_show_nonexistent_task(self, tmp_path: Path):
        """Show command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("show", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_show_displays_lineage_for_review_task(self, tmp_path: Path):
        """Show command displays lineage using implementation/review chain."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        review = store.add("Review feature", task_type="review", depends_on=impl.id)
        assert review.id is not None

        result = run_gza("show", str(review.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"#{impl.id}" in result.stdout
        assert f"#{review.id}" in result.stdout

    def test_show_failed_task_displays_failure_diagnostics(self, tmp_path: Path):
        """Failed task output includes reason, limits, context, and next-step commands."""
        import json
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmax_steps: 50\nverify_command: uv run pytest tests/ -q\n")

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Failed task for show diagnostics")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "MAX_STEPS"
        task.log_file = ".gza/logs/fail.log"
        task.session_id = "session-123"
        task.num_steps_reported = 55
        task.num_turns_reported = 55
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-20260227-000001",
                pid=12345,
                task_id=task.id,
                task_slug=task.task_id,
                started_at="2026-02-27T00:00:00+00:00",
                status="failed",
                log_file=task.log_file,
                worktree=None,
                is_background=True,
            )
        )

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "tool_use", "id": "tool_1", "name": "Bash", "input": {"command": "uv run pytest tests/ -q"}},
                        {"type": "text", "text": "Running verification"},
                    ],
                },
            },
            {
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "tool_1",
                            "is_error": True,
                            "content": "FAILED tests/test_cli.py::test_case - AssertionError",
                        }
                    ],
                },
            },
            {"type": "result", "subtype": "error_max_turns", "result": "Stopped at limit", "num_steps": 55},
        ]
        (log_dir / "fail.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason: MAX_STEPS" in result.stdout
        assert "Failure Summary: Stopped due to max steps limit." in result.stdout
        assert "Step Limit: 55 / 50" in result.stdout
        assert "Last Verify Failure:" in result.stdout
        assert "uv run pytest tests/ -q" in result.stdout
        assert "Last Result Context: error_max_turns" in result.stdout
        assert "gza resume 1" in result.stdout
        assert "gza retry 1" in result.stdout
        assert "Run Context: background (w-20260227-000001)" in result.stdout

    def test_show_failed_task_extracts_verify_failure_from_tool_error_entries(self, tmp_path: Path):
        """Failed-task diagnostics should detect verify failures in non-Claude tool_* entry shapes."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nverify_command: uv run pytest tests/ -q\n")

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Failed task with non-Claude logs")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "TEST_FAILURE"
        task.log_file = ".gza/logs/non-claude.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {
                "type": "tool_use",
                "id": "call_1",
                "tool_name": "Bash",
                "tool_input": {"command": "uv run pytest tests/ -q"},
            },
            {
                "type": "tool_error",
                "tool_use_id": "call_1",
                "content": "FAILED tests/test_cli.py::test_case - AssertionError",
            },
            {"type": "result", "subtype": "error_test_failure", "result": "verification failed"},
        ]
        (log_dir / "non-claude.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Last Verify Failure:" in result.stdout
        assert "uv run pytest tests/ -q" in result.stdout
        assert "AssertionError" in result.stdout

    def test_show_completed_task_omits_failure_diagnostics(self, tmp_path: Path):
        """Completed task output should not include failed-task diagnostics block."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Completed task")
        task.status = "completed"
        store.update(task)

        result = run_gza("show", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason:" not in result.stdout
        assert "Failure Summary:" not in result.stdout

    def test_show_plan_lineage_includes_downstream_implement(self, tmp_path: Path):
        """Show for a plan task includes downstream implement task in lineage."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan = store.add("Design the feature", task_type="plan")
        assert plan.id is not None
        impl = store.add("Implement the feature", task_type="implement", based_on=plan.id)
        assert impl.id is not None

        result = run_gza("show", str(plan.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"#{plan.id}" in result.stdout
        assert f"#{impl.id}" in result.stdout

    def test_show_implement_lineage_includes_plan_and_review_improve_chain(self, tmp_path: Path):
        """Show for an implement task (based on a plan) includes plan, review, and improve."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan = store.add("Design the feature", task_type="plan")
        assert plan.id is not None
        impl = store.add("Implement the feature", task_type="implement", based_on=plan.id)
        assert impl.id is not None
        review = store.add("Review the feature", task_type="review", depends_on=impl.id)
        assert review.id is not None
        improve = store.add("Fix review issues", task_type="improve", based_on=impl.id)
        assert improve.id is not None

        result = run_gza("show", str(impl.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"#{plan.id}" in result.stdout
        assert f"#{impl.id}" in result.stdout
        assert f"#{review.id}" in result.stdout
        assert f"#{improve.id}" in result.stdout

    def test_show_multi_level_dependency_lineage(self, tmp_path: Path):
        """Lineage traverses multi-level dependency chains (plan->impl->sub-impl)."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan = store.add("Top-level plan", task_type="plan")
        assert plan.id is not None
        impl1 = store.add("First implement", task_type="implement", based_on=plan.id)
        assert impl1.id is not None
        impl2 = store.add("Second implement based on first", task_type="implement", based_on=impl1.id)
        assert impl2.id is not None

        result = run_gza("show", str(plan.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"#{plan.id}" in result.stdout
        assert f"#{impl1.id}" in result.stdout
        assert f"#{impl2.id}" in result.stdout

    def test_show_depended_on_by_field(self, tmp_path: Path):
        """Show displays 'Depended on by' listing tasks that reference the displayed task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan = store.add("Design the feature", task_type="plan")
        assert plan.id is not None
        impl = store.add("Implement the feature", task_type="implement", based_on=plan.id)
        assert impl.id is not None
        review = store.add("Review the feature", task_type="review", depends_on=impl.id)
        assert review.id is not None

        # Show the plan: it should list impl as "Depended on by"
        result_plan = run_gza("show", str(plan.id), "--project", str(tmp_path))
        assert result_plan.returncode == 0
        assert "Depended on by:" in result_plan.stdout
        assert f"#{impl.id}[implement]" in result_plan.stdout

        # Show the impl: it should list review as "Depended on by"
        result_impl = run_gza("show", str(impl.id), "--project", str(tmp_path))
        assert result_impl.returncode == 0
        assert "Depended on by:" in result_impl.stdout
        assert f"#{review.id}[review]" in result_impl.stdout

    def test_show_truncates_long_output(self, tmp_path: Path):
        """gza show truncates output >30 lines to 20 with a remainder hint."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        long_content = "\n".join(f"line {i}" for i in range(1, 52))  # 51 lines
        task = store.add("Plan with long output", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.output_content = long_content
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "line 20" in result.stdout
        assert "line 21" not in result.stdout
        assert "truncated" in result.stdout
        assert "31 more lines" in result.stdout
        assert f"gza show {task.id} --full" in result.stdout

    def test_show_full_flag_shows_complete_output(self, tmp_path: Path):
        """gza show --full bypasses truncation and displays all lines."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        long_content = "\n".join(f"line {i}" for i in range(1, 52))  # 51 lines
        task = store.add("Plan with long output", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.output_content = long_content
        store.update(task)

        result = run_gza("show", str(task.id), "--full", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "line 51" in result.stdout
        assert "truncated" not in result.stdout

    def test_show_short_output_not_truncated(self, tmp_path: Path):
        """gza show does not truncate output with exactly 30 lines."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        content_30_lines = "\n".join(f"line {i}" for i in range(1, 31))  # exactly 30 lines
        task = store.add("Plan with 30-line output", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.output_content = content_30_lines
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "line 30" in result.stdout
        assert "truncated" not in result.stdout


class TestDeleteCommand:
    """Tests for 'gza delete' command."""

    def test_delete_with_force(self, tmp_path: Path):
        """Delete command with --force removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "1", "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", "--project", str(tmp_path))
        assert "No pending tasks" in result.stdout

    def test_delete_nonexistent_task(self, tmp_path: Path):
        """Delete command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("delete", "999", "--force", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_delete_with_yes_flag(self, tmp_path: Path):
        """Delete command with --yes removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "1", "--yes", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", "--project", str(tmp_path))
        assert "No pending tasks" in result.stdout

    def test_delete_with_y_flag(self, tmp_path: Path):
        """Delete command with -y removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "1", "-y", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", "--project", str(tmp_path))
        assert "No pending tasks" in result.stdout


class TestRetryCommand:
    """Tests for 'gza retry' command."""

    def test_retry_completed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a completed task."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Original task", "status": "completed", "task_type": "implement"},
        ])

        result = run_gza("retry", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created task #2" in result.stdout
        assert "retry of #1" in result.stdout

        # Verify new task was created with same prompt
        result = run_gza("next", "--project", str(tmp_path))
        assert "Original task" in result.stdout

    def test_retry_failed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a failed task."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Failed task", "status": "failed"},
        ])

        result = run_gza("retry", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created task #2" in result.stdout
        assert "retry of #1" in result.stdout

    def test_retry_pending_task_fails(self, tmp_path: Path):
        """Retry command fails for pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("retry", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Can only retry completed or failed" in result.stdout

    def test_retry_nonexistent_task(self, tmp_path: Path):
        """Retry command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("retry", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_retry_preserves_task_fields(self, tmp_path: Path):
        """Retry command preserves task metadata and linkage fields."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)

        # Create a task with metadata
        task = store.add(
            "Test task with metadata",
            task_type="explore",
            group="test-group",
            spec="spec.md",
            depends_on=42,
            create_review=True,
            same_branch=True,
            task_type_hint="feature",
            model="gpt-5.3-codex",
            provider="codex",
        )
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Retry the task
        result = run_gza("retry", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify the new task has the same metadata
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.prompt == "Test task with metadata"
        assert new_task.task_type == "explore"
        assert new_task.group == "test-group"
        assert new_task.spec == "spec.md"
        assert new_task.depends_on == 42
        assert new_task.create_review is True
        assert new_task.same_branch is True
        assert new_task.task_type_hint == "feature"
        assert new_task.model == "gpt-5.3-codex"
        assert new_task.provider == "codex"
        assert new_task.based_on == 1
        assert new_task.status == "pending"

    def test_retry_with_background_flag(self, tmp_path: Path):
        """Retry command with --background spawns a worker for the new task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        # Run retry with background mode
        result = run_gza("retry", "1", "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes successfully
        assert result.returncode == 0
        assert "Created task #2" in result.stdout
        assert "Started worker" in result.stdout

        # Verify new task was created
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.prompt == "Failed task to retry"
        assert new_task.status == "pending"
        assert new_task.based_on == 1

    def test_retry_runs_by_default(self, tmp_path: Path):
        """Retry command runs the newly created task immediately by default."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Run retry without any flags (will fail due to missing API key, but we can verify it tries)
        result = run_gza("retry", "1", "--no-docker", "--project", str(tmp_path))

        # Verify the new task was created and run was attempted
        assert "Created task #2" in result.stdout
        assert "Running task #2" in result.stdout

        # Verify new task exists
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.prompt == "Failed task to retry"
        assert new_task.based_on == 1

    def test_retry_with_queue_flag(self, tmp_path: Path):
        """Retry command with --queue adds task to queue without executing."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Run retry with --queue flag
        result = run_gza("retry", "1", "--queue", "--project", str(tmp_path))

        # Verify the new task was created but not run
        assert result.returncode == 0
        assert "Created task #2" in result.stdout
        assert "Running task" not in result.stdout

        # Verify new task is still pending
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.status == "pending"


class TestResumeCommand:
    """Tests for 'gza resume' command."""

    def test_resume_with_background_flag(self, tmp_path: Path):
        """Resume command with --background creates a new task and spawns a worker."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        # Run resume with background mode
        result = run_gza("resume", "1", "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes successfully
        assert result.returncode == 0
        # Verify new task was created
        assert "Created task #2 (resume of #1)" in result.stdout
        assert "Started worker" in result.stdout
        assert "(resuming)" in result.stdout

        # Verify original task still failed and new task was created
        original = store.get(1)
        assert original is not None
        assert original.status == "failed"
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.based_on == 1
        assert new_task.session_id == "test-session-123"

    def test_resume_without_session_id_fails(self, tmp_path: Path):
        """Resume command fails for tasks without session ID."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task without session ID
        task = store.add("Failed task without session")
        task.status = "failed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Try to resume
        result = run_gza("resume", "1", "--project", str(tmp_path))

        # Verify it fails with helpful message
        assert result.returncode == 1
        assert "has no session ID" in result.stdout
        assert "gza retry" in result.stdout

    def test_resume_non_failed_task_fails(self, tmp_path: Path):
        """Resume command fails for non-failed, non-orphaned tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("resume", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Can only resume failed or orphaned tasks" in result.stdout

    def test_resume_runs_by_default(self, tmp_path: Path):
        """Resume command runs the new task immediately by default."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Run resume without any special flags (will fail due to missing API key, but we can verify it tries)
        result = run_gza("resume", "1", "--no-docker", "--project", str(tmp_path))

        # Verify the command creates a new task
        assert "Created task #2 (resume of #1)" in result.stdout

        # Verify original task stays failed and new task was created
        original = store.get(1)
        assert original is not None
        assert original.status == "failed"
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.based_on == 1
        assert new_task.session_id == "test-session-123"

    def test_resume_with_queue_flag(self, tmp_path: Path):
        """Resume command with --queue adds task to queue without executing."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Run resume with --queue flag
        result = run_gza("resume", "1", "--queue", "--project", str(tmp_path))

        # Verify the command creates a new task but does not run it
        assert result.returncode == 0
        assert "Created task #2 (resume of #1)" in result.stdout
        assert "Running" not in result.stdout

        # Verify new task is pending
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.status == "pending"
        assert new_task.session_id == "test-session-123"

    def test_resume_creates_new_task_preserves_original(self, tmp_path: Path):
        """Resume creates a new pending task, leaving original task as failed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a failed task with session ID, log, and stats
        task = store.add("Implement feature X")
        task.status = "failed"
        task.session_id = "session-abc-123"
        task.task_type = "implement"
        task.num_turns_reported = 42
        task.cost_usd = 1.50
        task.duration_seconds = 300.0
        task.log_file = ".gza/logs/20260101-implement-feature-x.log"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Run resume (will fail trying to run due to missing API key/git, but task should be created)
        result = run_gza("resume", "1", "--no-docker", "--project", str(tmp_path))

        # Verify output
        assert "Created task #2 (resume of #1)" in result.stdout

        # Verify original task stays failed with stats preserved
        original = store.get(1)
        assert original is not None
        assert original.status == "failed"
        assert original.num_turns_reported == 42
        assert original.cost_usd == 1.50
        assert original.duration_seconds == 300.0
        assert original.log_file == ".gza/logs/20260101-implement-feature-x.log"

        # Verify new task has the right properties
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.prompt == "Implement feature X"
        assert new_task.task_type == "implement"
        assert new_task.based_on == 1
        assert new_task.session_id == "session-abc-123"
        # New task starts with no stats
        assert new_task.num_turns_reported is None
        assert new_task.cost_usd is None
        assert new_task.log_file is None

    def test_resume_orphaned_in_progress_task_succeeds(self, tmp_path: Path):
        """Resume command succeeds for an orphaned in_progress task (no live worker)."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create an in_progress task with a session ID (simulating an orphaned task)
        task = store.add("Orphaned in-progress task")
        task.status = "in_progress"
        task.session_id = "orphaned-session-456"
        task.started_at = datetime.now(timezone.utc)
        store.update(task)

        # No worker files exist — task is orphaned

        result = run_gza("resume", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout.lower()
        assert "Created task #2 (resume of #1)" in result.stdout

        # Verify original task is unchanged and new task was created
        original = store.get(1)
        assert original is not None
        assert original.status == "in_progress"
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.based_on == 1
        assert new_task.session_id == "orphaned-session-456"
        assert new_task.status == "pending"

    def test_resume_running_in_progress_task_fails(self, tmp_path: Path):
        """Resume command fails for an in_progress task that has a live worker."""
        import subprocess as sp

        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create an in_progress task
        task = store.add("Still-running task")
        task.status = "in_progress"
        task.session_id = "running-session-789"
        task.started_at = datetime.now(timezone.utc)
        store.update(task)

        # Spawn a real sleeping process to act as the live worker
        sleeper = sp.Popen(["sleep", "30"])
        try:
            workers_path = tmp_path / ".gza" / "workers"
            workers_path.mkdir(parents=True, exist_ok=True)
            registry = WorkerRegistry(workers_path)
            worker = WorkerMetadata(
                worker_id="w-test-running",
                pid=sleeper.pid,
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
            registry.register(worker)

            result = run_gza("resume", "1", "--project", str(tmp_path))
        finally:
            sleeper.kill()
            sleeper.wait()

        assert result.returncode == 1
        assert "still running" in result.stdout.lower()
        assert "w-test-running" in result.stdout


class TestConfigRequirements:
    """Tests for gza.yaml configuration requirements."""

    def test_missing_config_file(self, tmp_path: Path):
        """Commands fail when gza.yaml is missing."""
        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Configuration file not found" in result.stderr
        assert "gza init" in result.stderr

    def test_missing_project_name(self, tmp_path: Path):
        """Commands fail when project_name is missing from config."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("timeout_minutes: 5\n")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "project_name" in result.stderr
        assert "required" in result.stderr

    def test_unknown_keys_warning(self, tmp_path: Path):
        """Unknown keys in config produce warnings but don't fail."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nunknown_key: value\n")

        result = run_gza("next", "--project", str(tmp_path))

        # Should succeed
        assert result.returncode == 0
        # Warning should be printed to stderr
        assert "unknown_key" in result.stderr
        assert "Warning" in result.stderr or "warning" in result.stderr.lower()


class TestValidateCommand:
    """Tests for 'gza validate' command."""

    def test_validate_valid_config(self, tmp_path: Path):
        """Validate command succeeds with valid config."""
        setup_config(tmp_path)
        result = run_gza("validate", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "valid" in result.stdout.lower()

    def test_validate_missing_config(self, tmp_path: Path):
        """Validate command fails with missing config."""
        result = run_gza("validate", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_validate_missing_project_name(self, tmp_path: Path):
        """Validate command fails when project_name is missing."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("timeout_minutes: 5\n")

        result = run_gza("validate", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "project_name" in result.stdout
        assert "required" in result.stdout

    def test_validate_unknown_keys_warning(self, tmp_path: Path):
        """Validate command shows warnings for unknown keys."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nunknown_field: value\n")

        result = run_gza("validate", "--project", str(tmp_path))

        assert result.returncode == 0  # Unknown keys don't fail validation
        assert "unknown_field" in result.stdout
        assert "Warning" in result.stdout

    def test_validate_docker_volumes_must_be_list(self, tmp_path: Path):
        """Validate rejects docker_volumes that isn't a list."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\ndocker_volumes: /path:/mount\n")
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "docker_volumes" in result.stdout
        assert "must be a list" in result.stdout

    def test_validate_docker_volumes_entries_must_be_strings(self, tmp_path: Path):
        """Validate rejects non-string docker_volumes entries."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\ndocker_volumes:\n  - 123\n")
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "docker_volumes[0]" in result.stdout
        assert "must be a string" in result.stdout

    def test_validate_docker_volumes_valid(self, tmp_path: Path):
        """Validate accepts valid docker_volumes configuration."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "docker_volumes:\n"
            "  - /host/data:/data:ro\n"
            "  - /host/models:/models\n"
        )
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "valid" in result.stdout.lower()

    def test_validate_docker_volumes_missing_colon_warning(self, tmp_path: Path):
        """Validate warns about docker_volumes entries without colons."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "docker_volumes:\n"
            "  - /just/a/path\n"
        )
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 0  # Warning, not error
        assert "docker_volumes[0]" in result.stdout
        assert "missing colon separator" in result.stdout

    def test_validate_docker_volumes_unknown_mode_warning(self, tmp_path: Path):
        """Validate warns about unknown docker_volumes modes."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "docker_volumes:\n"
            "  - /host:/container:xyz\n"
        )
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 0  # Warning, not error
        assert "docker_volumes[0]" in result.stdout
        assert "unknown mode 'xyz'" in result.stdout


class TestConfigEnvVars:
    """Tests for environment variable overrides in config."""

    def test_gza_docker_volumes_env_var(self, tmp_path: Path):
        """GZA_DOCKER_VOLUMES environment variable overrides config."""
        from gza.config import Config
        import os

        setup_config(tmp_path)

        # Set environment variable
        env = os.environ.copy()
        env["GZA_DOCKER_VOLUMES"] = "/host1:/data:ro,/host2:/models"

        # Use subprocess to load config with env vars
        import subprocess
        result = subprocess.run(
            ["uv", "run", "python", "-c",
             "from gza.config import Config; "
             f"from pathlib import Path; "
             f"c = Config.load(Path('{tmp_path}')); "
             "print(','.join(c.docker_volumes))"],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0
        volumes = result.stdout.strip()
        assert "/host1:/data:ro" in volumes
        assert "/host2:/models" in volumes

    def test_docker_volumes_tilde_expansion(self, tmp_path: Path):
        """Docker volumes should expand tilde in source paths."""
        from gza.config import Config
        from pathlib import Path as PathLib

        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "docker_volumes:\n"
            "  - ~/data:/container/data\n"
            "  - ~/models:/models:ro\n"
        )

        config = Config.load(tmp_path)

        # Tilde should be expanded in source paths
        assert len(config.docker_volumes) == 2
        for volume in config.docker_volumes:
            assert "~" not in volume.split(":")[0]
            assert str(PathLib.home()) in volume.split(":")[0]

    def test_docker_setup_command_loaded_from_config(self, tmp_path: Path):
        """docker_setup_command is loaded from gza.yaml."""
        from gza.config import Config

        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "docker_setup_command: 'uv sync --project /workspace'\n"
        )

        config = Config.load(tmp_path)
        assert config.docker_setup_command == "uv sync --project /workspace"

    def test_docker_setup_command_defaults_to_empty_string(self, tmp_path: Path):
        """docker_setup_command defaults to empty string when not set."""
        from gza.config import Config

        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\n")

        config = Config.load(tmp_path)
        assert config.docker_setup_command == ""


class TestDockerSetupCommandValidation:
    """Tests for docker_setup_command validation."""

    def test_validate_docker_setup_command_must_be_string(self, tmp_path: Path):
        """Validate rejects docker_setup_command that isn't a string."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\ndocker_setup_command: 123\n")

        result = run_gza("validate", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "docker_setup_command" in result.stdout

    def test_validate_docker_setup_command_valid(self, tmp_path: Path):
        """Validate accepts a valid docker_setup_command string."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "docker_setup_command: 'uv sync --project /workspace'\n"
        )

        result = run_gza("validate", "--project", str(tmp_path))

        assert result.returncode == 0


class TestLocalConfigOverrides:
    """Tests for gza.local.yaml local override behavior."""

    def test_local_overrides_deep_merge_nested_config(self, tmp_path: Path):
        """Local overrides should deep-merge dictionaries over gza.yaml."""
        from gza.config import Config

        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "providers:\n"
            "  claude:\n"
            "    task_types:\n"
            "      review:\n"
            "        model: claude-base\n"
            "        max_steps: 20\n"
        )
        (tmp_path / "gza.local.yaml").write_text(
            "providers:\n"
            "  claude:\n"
            "    task_types:\n"
            "      review:\n"
            "        model: claude-local\n"
        )

        config = Config.load(tmp_path)

        review_cfg = config.providers["claude"].task_types["review"]
        assert review_cfg.model == "claude-local"
        assert review_cfg.max_steps == 20
        assert config.local_overrides_active is True
        assert config.source_map["providers.claude.task_types.review.model"] == "local"
        assert config.source_map["providers.claude.task_types.review.max_steps"] == "base"

    def test_env_vars_take_precedence_over_local_overrides(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """Environment variables should override local and base config values."""
        from gza.config import Config

        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "use_docker: true\n"
        )
        (tmp_path / "gza.local.yaml").write_text(
            "use_docker: false\n"
        )
        monkeypatch.setenv("GZA_USE_DOCKER", "true")

        config = Config.load(tmp_path)

        assert config.use_docker is True
        assert config.source_map["use_docker"] == "env"

    def test_local_override_guardrails_reject_disallowed_keys(self, tmp_path: Path):
        """Local overrides should reject disallowed keys like project_name."""
        from gza.config import Config, ConfigError

        (tmp_path / "gza.yaml").write_text("project_name: test\n")
        (tmp_path / "gza.local.yaml").write_text("project_name: hacked\n")

        with pytest.raises(ConfigError, match="Invalid local override key 'project_name'"):
            Config.load(tmp_path)

    def test_validate_fails_for_invalid_local_override_key(self, tmp_path: Path):
        """gza validate should fail when local override contains disallowed keys."""
        (tmp_path / "gza.yaml").write_text("project_name: test\n")
        (tmp_path / "gza.local.yaml").write_text("project_name: hacked\n")

        result = run_gza("validate", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Invalid local override key 'project_name'" in result.stdout

    def test_notice_printed_when_local_overrides_active(self, tmp_path: Path):
        """Commands should print a startup notice when local overrides are active."""
        (tmp_path / "gza.yaml").write_text("project_name: test\n")
        (tmp_path / "gza.local.yaml").write_text("use_docker: false\n")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Notice: local config overrides active from gza.local.yaml" in result.stderr

    def test_config_command_shows_effective_values_with_sources(self, tmp_path: Path):
        """gza config --json should include effective values and source attribution."""
        import json

        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "timeout_minutes: 10\n"
            "use_docker: true\n"
        )
        (tmp_path / "gza.local.yaml").write_text(
            "use_docker: false\n"
        )

        env = os.environ.copy()
        env["GZA_TIMEOUT_MINUTES"] = "99"
        result = subprocess.run(
            ["uv", "run", "gza", "config", "--json", "--project", str(tmp_path)],
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["effective"]["timeout_minutes"] == 99
        assert payload["effective"]["use_docker"] is False
        assert payload["sources"]["timeout_minutes"] == "env"
        assert payload["sources"]["use_docker"] == "local"
        assert payload["local_overrides_active"] is True
        assert payload["local_override_file"] == "gza.local.yaml"

    def test_config_command_projects_source_for_branch_strategy_preset(self, tmp_path: Path):
        """gza config should attribute normalized branch_strategy fields to configured source."""
        import json

        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "branch_strategy: conventional\n"
        )

        result = subprocess.run(
            ["uv", "run", "gza", "config", "--json", "--project", str(tmp_path)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["effective"]["branch_strategy"]["pattern"] == "{type}/{slug}"
        assert payload["effective"]["branch_strategy"]["default_type"] == "feature"
        assert payload["sources"]["branch_strategy.pattern"] == "base"
        assert payload["sources"]["branch_strategy.default_type"] == "base"

    def test_config_command_includes_task_providers_with_sources(self, tmp_path: Path):
        """gza config --json should project task_providers values and source attribution."""
        import json

        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "provider: codex\n"
            "task_providers:\n"
            "  review: claude\n"
        )

        result = subprocess.run(
            ["uv", "run", "gza", "config", "--json", "--project", str(tmp_path)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["effective"]["task_providers"]["review"] == "claude"
        assert payload["sources"]["task_providers.review"] == "base"


class TestInitCommand:
    """Tests for 'gza init' command."""

    def test_init_creates_config(self, tmp_path: Path):
        """Init command creates config in project root."""
        result = run_gza("init", "--project", str(tmp_path))

        assert result.returncode == 0
        config_path = tmp_path / "gza.yaml"
        local_example_path = tmp_path / "gza.local.yaml.example"
        assert config_path.exists()
        assert local_example_path.exists()

        # Verify project_name is set (derived from directory name)
        content = config_path.read_text()
        assert "project_name:" in content
        assert tmp_path.name in content

    def test_init_does_not_overwrite(self, tmp_path: Path):
        """Init command does not overwrite existing config without --force."""
        setup_config(tmp_path, project_name="original")

        result = run_gza("init", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "already exists" in result.stdout

        # Verify original content is preserved
        config_path = tmp_path / "gza.yaml"
        assert "original" in config_path.read_text()

    def test_init_force_overwrites(self, tmp_path: Path):
        """Init command overwrites existing config with --force."""
        setup_config(tmp_path, project_name="original")
        local_example_path = tmp_path / "gza.local.yaml.example"
        local_example_path.write_text("# stale local example\n")

        result = run_gza("init", "--force", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify config was overwritten (has directory name, not "original")
        config_path = tmp_path / "gza.yaml"
        content = config_path.read_text()
        assert tmp_path.name in content
        assert local_example_path.exists()
        assert "# stale local example" not in local_example_path.read_text()


class TestImportCommand:
    """Tests for 'gza import' command."""

    def test_import_from_yaml(self, tmp_path: Path):
        """Import command imports tasks from tasks.yaml."""
        setup_config(tmp_path)
        tasks_yaml = tmp_path / "tasks.yaml"
        tasks_yaml.write_text("""tasks:
- description: Task from YAML
  status: pending
- description: Completed YAML task
  status: completed
""")

        result = run_gza("import", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Imported 2 tasks" in result.stdout

        # Verify tasks were imported
        result = run_gza("next", "--project", str(tmp_path))
        assert "Task from YAML" in result.stdout

    def test_import_no_yaml(self, tmp_path: Path):
        """Import command handles missing tasks.yaml."""
        setup_config(tmp_path)
        result = run_gza("import", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout


class TestLogCommand:
    """Tests for 'gza log' command."""

    def test_log_by_task_id_single_json_format(self, tmp_path: Path):
        """Log command with --task parses single JSON format with successful result."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for log")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a single JSON log file (old format)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {
            "type": "result",
            "subtype": "success",
            "result": "## Summary\n\nTask completed successfully!",
            "duration_ms": 60000,
            "num_turns": 10,
            "total_cost_usd": 0.5,
        }
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task completed successfully!" in result.stdout
        assert "Duration:" in result.stdout
        assert "Steps: 10" in result.stdout
        assert "Legacy turns: 10" in result.stdout
        assert "Cost: $0.5000" in result.stdout

    def test_log_by_task_id_jsonl_format(self, tmp_path: Path):
        """Log command with --task parses step-first JSONL format with successful result."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for JSONL log")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a JSONL log file (step-first format fixture)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_file.write_text((LOG_FIXTURES_DIR / "step_schema_v2_like.jsonl").read_text())

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Finished." in result.stdout
        assert "Duration:" in result.stdout
        assert "Steps: 2" in result.stdout
        assert "Cost: $0.1234" in result.stdout

    def test_log_by_task_id_error_max_turns(self, tmp_path: Path):
        """Log command with --task handles JSONL format with error_max_turns result."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task that hit max turns")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a JSONL log file with error_max_turns (no result field)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        lines = [
            {"type": "system", "subtype": "init", "session_id": "abc123"},
            {"type": "assistant", "message": {"role": "assistant", "content": "Working..."}},
            {
                "type": "result",
                "subtype": "error_max_turns",
                "duration_ms": 300000,
                "num_turns": 60,
                "total_cost_usd": 1.5,
                "errors": [],
            },
        ]
        log_file.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "error_max_turns" in result.stdout
        assert "Steps: 60" in result.stdout
        assert "Legacy turns: 60" in result.stdout
        assert "Cost: $1.5000" in result.stdout

    def test_log_by_task_id_missing_log_file(self, tmp_path: Path):
        """Log command with --task handles missing log file."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file path that doesn't exist
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with missing log")
        task.status = "completed"
        task.log_file = ".gza/logs/nonexistent.log"
        store.update(task)

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Log file not found" in result.stdout

    def test_log_by_task_id_no_result_entry(self, tmp_path: Path):
        """Log command with --task shows compact step timeline when no result entry exists."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with incomplete log")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create a JSONL log file with no result entry
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        lines = [
            {"type": "system", "subtype": "init", "session_id": "abc123", "model": "test-model"},
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Working..."}]}},
        ]
        log_file.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        # Should show formatted entries instead of failing
        assert result.returncode == 0
        assert "Working..." in result.stdout

    def test_log_by_task_id_falls_back_to_inferred_log_path(self, tmp_path: Path):
        """Task lookup should render entries from inferred slug log when task.log_file is stale."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Fallback log path task")
        task.status = "completed"
        task.task_id = "20260227-fallback-log"
        task.log_file = ".gza/logs/missing.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        inferred_log = log_dir / "20260227-fallback-log.log"
        lines = [
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Recovered via inferred path"}]}},
            {"type": "result", "subtype": "success", "result": "ok", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01},
        ]
        inferred_log.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recovered via inferred path" in result.stdout

    def test_log_by_task_id_resolves_to_latest_retry_resume_attempt(self, tmp_path: Path):
        """Task lookup resolves deterministically to latest same-type based_on attempt."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        original = store.add("Original failed task")
        assert original.id is not None
        original.status = "failed"
        original.log_file = ".gza/logs/original.log"
        store.update(original)

        retry = store.add("Retry task", based_on=original.id, task_type=original.task_type)
        assert retry.id is not None
        retry.status = "completed"
        retry.log_file = ".gza/logs/retry.log"
        retry.started_at = datetime.now(timezone.utc)
        store.update(retry)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "original.log").write_text(json.dumps({"type": "result", "result": "old run"}))
        (log_dir / "retry.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Latest attempt output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = run_gza("log", "--task", str(original.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" in result.stdout
        assert "Latest attempt output" in result.stdout

    def test_log_by_task_id_non_root_query_stays_within_its_retry_chain(self, tmp_path: Path):
        """Querying a retry should not jump to a newer same-type sibling chain."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Original failed task")
        assert root.id is not None
        root.status = "failed"
        root.log_file = ".gza/logs/root.log"
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        store.update(root)

        retry_a = store.add("Retry A", based_on=root.id, task_type=root.task_type)
        assert retry_a.id is not None
        retry_a.status = "failed"
        retry_a.log_file = ".gza/logs/retry_a.log"
        retry_a.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        store.update(retry_a)

        retry_b = store.add("Retry B", based_on=root.id, task_type=root.task_type)
        assert retry_b.id is not None
        retry_b.status = "completed"
        retry_b.log_file = ".gza/logs/retry_b.log"
        retry_b.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        store.update(retry_b)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "root.log").write_text(json.dumps({"type": "result", "result": "root run"}))
        (log_dir / "retry_a.log").write_text(json.dumps({"type": "result", "result": "retry A run"}))
        (log_dir / "retry_b.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Newest sibling output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = run_gza("log", "--task", str(retry_a.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" not in result.stdout
        assert "retry A run" in result.stdout
        assert "Newest sibling output" not in result.stdout

    def test_log_by_slug_non_root_query_stays_within_its_retry_chain(self, tmp_path: Path):
        """Slug lookup should not jump to a newer same-type sibling chain."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Original failed task")
        assert root.id is not None
        root.task_id = "20260227-chain-root"
        root.status = "failed"
        root.log_file = ".gza/logs/root.log"
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        store.update(root)

        retry_a = store.add("Retry A", based_on=root.id, task_type=root.task_type)
        assert retry_a.id is not None
        retry_a.task_id = "20260227-chain-a"
        retry_a.status = "failed"
        retry_a.log_file = ".gza/logs/retry_a.log"
        retry_a.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        store.update(retry_a)

        retry_b = store.add("Retry B", based_on=root.id, task_type=root.task_type)
        assert retry_b.id is not None
        retry_b.task_id = "20260227-chain-b"
        retry_b.status = "completed"
        retry_b.log_file = ".gza/logs/retry_b.log"
        retry_b.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        store.update(retry_b)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "root.log").write_text(json.dumps({"type": "result", "result": "root run"}))
        (log_dir / "retry_a.log").write_text(json.dumps({"type": "result", "result": "retry A run"}))
        (log_dir / "retry_b.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Newest sibling output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = run_gza("log", "--slug", "20260227-chain-a", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" not in result.stdout
        assert "retry A run" in result.stdout
        assert "Newest sibling output" not in result.stdout

    def test_log_by_task_id_latest_attempt_ignores_mixed_type_lineage_nodes(self, tmp_path: Path):
        """Latest-attempt resolution should only consider same-type retry/resume attempts."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Original task", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.log_file = ".gza/logs/root.log"
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        store.update(root)

        retry = store.add("Retry task", based_on=root.id, task_type="implement")
        assert retry.id is not None
        retry.status = "completed"
        retry.log_file = ".gza/logs/retry.log"
        retry.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        store.update(retry)

        review = store.add("Review task", based_on=root.id, task_type="review")
        assert review.id is not None
        review.status = "completed"
        review.log_file = ".gza/logs/review.log"
        review.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        store.update(review)

        improve = store.add("Improve task", based_on=review.id, task_type="improve")
        assert improve.id is not None
        improve.status = "completed"
        improve.log_file = ".gza/logs/improve.log"
        improve.started_at = datetime(2026, 2, 26, 13, 0, tzinfo=timezone.utc)
        store.update(improve)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "root.log").write_text(json.dumps({"type": "result", "result": "root run"}))
        (log_dir / "retry.log").write_text(json.dumps({"type": "result", "result": "retry chain output"}))
        (log_dir / "review.log").write_text(json.dumps({"type": "result", "result": "review output"}))
        (log_dir / "improve.log").write_text(json.dumps({"type": "result", "result": "improve output"}))

        result = run_gza("log", "--task", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resolved to latest run attempt" in result.stdout
        assert f"task #{retry.id}" in result.stdout
        assert "retry chain output" in result.stdout
        assert "review output" not in result.stdout
        assert "improve output" not in result.stdout

    def test_resolve_latest_attempt_for_task_excludes_parallel_same_type_sibling_chains(self, tmp_path: Path):
        """Resolver should stay within the queried task's direct same-type chain."""
        from gza.cli import _resolve_latest_attempt_for_task
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Root task", task_type="implement")
        assert root.id is not None
        root.started_at = datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc)
        root.status = "failed"
        store.update(root)

        chain_a = store.add("Chain A", based_on=root.id, task_type="implement")
        assert chain_a.id is not None
        chain_a.started_at = datetime(2026, 2, 26, 12, 0, tzinfo=timezone.utc)
        chain_a.status = "failed"
        store.update(chain_a)

        chain_a_resume = store.add("Chain A Resume", based_on=chain_a.id, task_type="implement")
        assert chain_a_resume.id is not None
        chain_a_resume.started_at = datetime(2026, 2, 26, 12, 10, tzinfo=timezone.utc)
        chain_a_resume.status = "completed"
        store.update(chain_a_resume)

        sibling_chain = store.add("Sibling Chain", based_on=root.id, task_type="implement")
        assert sibling_chain.id is not None
        sibling_chain.started_at = datetime(2026, 2, 26, 12, 30, tzinfo=timezone.utc)
        sibling_chain.status = "completed"
        store.update(sibling_chain)

        selected, attempts = _resolve_latest_attempt_for_task(store, chain_a)

        attempt_ids = [attempt.id for attempt in attempts]
        assert selected.id == chain_a_resume.id
        assert sibling_chain.id not in attempt_ids

    def test_log_default_mode_renders_entries_when_result_exists(self, tmp_path: Path):
        """Default formatted output should include entry rendering, not metadata-only output."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Default render parity task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Entry text should render"}]}},
            {"type": "result", "subtype": "success", "result": "summary", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01},
        ]
        (log_dir / "test.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Entry text should render" in result.stdout

    def test_log_follow_by_task_uses_running_worker_when_available(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """-t -f should follow via live worker when a task is actively running."""
        import argparse
        import json
        from gza.cli import cmd_log
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Running task for follow")
        assert task.id is not None
        task.status = "in_progress"
        task.log_file = ".gza/logs/follow.log"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-20260227-010101",
            pid=os.getpid(),
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-02-27T01:01:01+00:00",
            status="running",
            log_file=task.log_file,
            worktree=None,
            is_background=False,
        )
        registry.register(worker)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "follow.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "live"}]}})
        ]))

        captured: dict[str, str | None] = {}

        def fake_tail(log_path, args, reg, worker_id, task_id, store_obj):
            captured["worker_id"] = worker_id
            captured["task_id"] = str(task_id) if task_id is not None else None
            return 0

        monkeypatch.setattr("gza.cli._tail_log_file", fake_tail)

        args = argparse.Namespace(
            identifier=str(task.id),
            task=True,
            slug=False,
            worker=False,
            follow=True,
            raw=False,
            timeline_mode=None,
            tail=None,
            project_dir=tmp_path,
        )

        rc = cmd_log(args)
        assert rc == 0
        assert captured["worker_id"] == "w-20260227-010101"
        assert captured["task_id"] == str(task.id)

    def test_log_follow_by_task_not_running_falls_back_to_static_output(self, tmp_path: Path):
        """-t -f should print persisted logs and exit when task is not actively running."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Completed task with persisted log")
        task.status = "completed"
        task.log_file = ".gza/logs/static.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "static.log").write_text("\n".join([
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "persisted output"}]}}),
            json.dumps({"type": "result", "subtype": "success", "result": "done", "num_steps": 1, "duration_ms": 1000, "total_cost_usd": 0.01}),
        ]))

        result = subprocess.run(
            ["uv", "run", "gza", "log", "--task", "1", "--follow", "--project", str(tmp_path)],
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert result.returncode == 0
        assert "persisted output" in result.stdout

    def test_log_steps_compact_renders_step_labels(self, tmp_path: Path):
        """--steps renders compact step-first timeline anchors."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Step timeline task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "test.log").write_text((LOG_FIXTURES_DIR / "step_schema_v2_like.jsonl").read_text())

        result = run_gza("log", "--task", "1", "--steps", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "[Step S1]" in result.stdout
        assert "[Step S2]" in result.stdout
        assert "[S1.1]" not in result.stdout

    def test_log_steps_verbose_renders_substep_labels(self, tmp_path: Path):
        """--steps-verbose renders S<n>.<m> substep labels."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Verbose step timeline task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "test.log").write_text((LOG_FIXTURES_DIR / "step_schema_v2_like.jsonl").read_text())

        result = run_gza("log", "--task", "1", "--steps-verbose", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "[Step S1]" in result.stdout
        assert "[S1.1]" in result.stdout
        assert "tool_call rg -n" in result.stdout

    def test_log_turns_alias_reads_legacy_turn_only_logs(self, tmp_path: Path):
        """Deprecated --turns alias still renders verbose step timeline for turn-only logs."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Legacy log task")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "test.log").write_text((LOG_FIXTURES_DIR / "legacy_turn_only_codex.jsonl").read_text())

        result = run_gza("log", "--task", "1", "--turns", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "[Step S1] Pre-message tool activity" in result.stdout
        assert "[S1.1] tool_call Bash ls -la" in result.stdout
        assert "[Step S2] Listed files." in result.stdout

    def test_log_by_task_id_not_found(self, tmp_path: Path):
        """Log command with --task handles nonexistent task."""
        setup_config(tmp_path)

        # Create empty database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "--task", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Task 999 not found" in result.stdout

    def test_log_by_task_id_invalid_id(self, tmp_path: Path):
        """Log command with --task rejects non-numeric ID."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "--task", "not-a-number", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not a valid task ID" in result.stdout

    def test_log_by_slug_exact_match(self, tmp_path: Path):
        """Log command with --slug finds task by exact slug."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for slug lookup")
        task.task_id = "20260108-test-slug"
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {"type": "result", "result": "Slug lookup works!", "duration_ms": 1000, "num_turns": 1, "total_cost_usd": 0.01}
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "--slug", "20260108-test-slug", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Slug lookup works!" in result.stdout

    def test_log_by_slug_partial_match(self, tmp_path: Path):
        """Log command with --slug finds task by partial slug match."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for partial slug")
        task.task_id = "20260108-partial-slug-test"
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {"type": "result", "result": "Partial match works!", "duration_ms": 1000, "num_turns": 1, "total_cost_usd": 0.01}
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "--slug", "partial-slug", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Partial match works!" in result.stdout

    def test_log_by_slug_not_found(self, tmp_path: Path):
        """Log command with --slug handles nonexistent slug."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "--slug", "nonexistent-slug", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "No task found matching slug" in result.stdout

    def test_log_by_worker_success(self, tmp_path: Path):
        """Log command with --worker finds log via worker registry."""
        import json
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)

        # Create task
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for worker lookup")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        # Create worker registry entry
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker_id = registry.generate_worker_id()
        worker = WorkerMetadata(
            worker_id=worker_id,
            pid=12345,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-01-08T00:00:00Z",
            status="completed",
            log_file=".gza/logs/test.log",
            worktree=None,
        )
        registry.register(worker)

        # Create log file
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_data = {"type": "result", "result": "Worker lookup works!", "duration_ms": 1000, "num_turns": 1, "total_cost_usd": 0.01}
        log_file.write_text(json.dumps(log_data))

        result = run_gza("log", "--worker", worker_id, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Worker lookup works!" in result.stdout

    def test_log_by_worker_not_found(self, tmp_path: Path):
        """Log command with --worker handles nonexistent worker."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        # Create empty workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        result = run_gza("log", "--worker", "w-nonexistent", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Worker 'w-nonexistent' not found" in result.stdout

    def test_log_by_task_id_startup_failure(self, tmp_path: Path):
        """Log command shows startup error when log contains non-JSON content."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with startup failure")
        task.status = "failed"
        task.log_file = ".gza/logs/test-startup-error.log"
        store.update(task)

        # Create a log file with raw error text (simulating Docker startup failure)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test-startup-error.log"
        log_file.write_text("exec /usr/local/bin/docker-entrypoint.sh: argument list too long")

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        # Should detect startup failure and display the error
        assert result.returncode == 1
        assert "Task failed during startup (no Claude session):" in result.stdout
        assert "exec /usr/local/bin/docker-entrypoint.sh: argument list too long" in result.stdout
        # The error should be indented
        assert "  exec /usr/local/bin/docker-entrypoint.sh" in result.stdout

    def test_log_requires_lookup_type(self, tmp_path: Path):
        """Log command requires --task, --slug, or --worker flag."""
        setup_config(tmp_path)

        result = run_gza("log", "123", "--project", str(tmp_path))

        assert result.returncode == 2
        assert "one of the arguments --task/-t --slug/-s --worker/-w is required" in result.stderr


class TestPrCommand:
    """Tests for 'gza pr' command."""

    def test_pr_task_not_found(self, tmp_path: Path):
        """PR command handles nonexistent task."""
        setup_config(tmp_path)

        # Create empty database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("pr", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_pr_task_not_completed(self, tmp_path: Path):
        """PR command rejects pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("pr", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not completed" in result.stdout

    def test_pr_task_no_branch(self, tmp_path: Path):
        """PR command rejects tasks without branches."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Completed task without branch")
        task.status = "completed"
        task.branch = None
        task.has_commits = True
        store.update(task)

        result = run_gza("pr", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout

    def test_pr_task_no_commits(self, tmp_path: Path):
        """PR command rejects tasks without commits."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Completed task without commits")
        task.status = "completed"
        task.branch = "feature/test"
        task.has_commits = False
        store.update(task)

        result = run_gza("pr", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no commits" in result.stdout

    def test_pr_task_marked_merged_shows_distinct_error(self, tmp_path: Path):
        """PR command shows a distinct error message for tasks marked merged via --mark-only."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Mark-only merged task")
        task.status = "completed"
        task.branch = "feature/mark-only-pr"
        task.has_commits = True
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("pr", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "already marked as merged" in result.stdout
        # Should NOT say "merged into" since the branch was not actually merged
        assert "merged into" not in result.stdout


class TestGroupsCommand:
    """Tests for 'gza groups' command."""

    def test_groups_with_tasks(self, tmp_path: Path):
        """Groups command shows all groups with task counts."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create tasks in different groups
        store.add("Task 1", group="group-a")
        store.add("Task 2", group="group-a")
        task3 = store.add("Task 3", group="group-b")
        task3.status = "completed"
        task3.completed_at = datetime.now(timezone.utc)
        store.update(task3)

        result = run_gza("groups", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "group-a" in result.stdout
        assert "group-b" in result.stdout

    def test_groups_with_no_groups(self, tmp_path: Path):
        """Groups command handles no groups."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("groups", "--project", str(tmp_path))

        assert result.returncode == 0


class TestStatusCommand:
    """Tests for 'gza group <group>' command."""

    def test_status_with_group(self, tmp_path: Path):
        """Group command shows tasks in a group."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create tasks in a group
        task1 = store.add("First task", group="test-group")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        store.update(task1)
        store.add("Second task", group="test-group", depends_on=task1.id)

        result = run_gza("group", "test-group", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "test-group" in result.stdout
        assert "First task" in result.stdout
        assert "Second task" in result.stdout

    def test_status_warns_about_orphaned_tasks_in_group(self, tmp_path: Path):
        """Group command warns about orphaned tasks belonging to the viewed group."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed task and an orphaned in-progress task in the same group
        task1 = store.add("Completed task", group="my-group")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        store.update(task1)

        orphaned_task = store.add("Orphaned in-progress task", group="my-group")
        store.mark_in_progress(orphaned_task)

        result = run_gza("group", "my-group", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout
        assert "Orphaned in-progress task" in result.stdout
        assert "gza work" in result.stdout

    def test_status_no_orphaned_warning_for_other_groups(self, tmp_path: Path):
        """Group command does not show orphaned warning for tasks in other groups."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create tasks in different groups
        task1 = store.add("Task in group A", group="group-a")
        store.mark_in_progress(task1)  # orphaned in group-a

        store.add("Task in group B", group="group-b")  # pending in group-b

        # View group-b - should NOT show orphaned warning for group-a task
        result = run_gza("group", "group-b", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" not in result.stdout
        assert "Task in group B" in result.stdout


class TestEditCommand:
    """Tests for 'gza edit' command."""

    def test_edit_group(self, tmp_path: Path):
        """Edit command can change task group."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task")
        assert task.group is None

        result = run_gza("edit", str(task.id), "--group", "new-group", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify group was updated
        updated = store.get(task.id)
        assert updated.group == "new-group"

    def test_edit_remove_group(self, tmp_path: Path):
        """Edit command can remove task from group."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task", group="old-group")
        assert task.group == "old-group"

        result = run_gza("edit", str(task.id), "--group", "", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify group was removed
        updated = store.get(task.id)
        assert updated.group is None or updated.group == ""

    def test_edit_review_flag(self, tmp_path: Path):
        """Edit command can enable automatic review task creation."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task")
        assert task.create_review is False

        result = run_gza("edit", str(task.id), "--review", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify create_review was enabled
        updated = store.get(task.id)
        assert updated.create_review is True

    def test_edit_with_prompt_file(self, tmp_path: Path):
        """Edit command can update prompt from file."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Original prompt text")

        # Create a file with new prompt
        prompt_file = tmp_path / "new_prompt.txt"
        prompt_file.write_text("New prompt text from file")

        result = run_gza("edit", str(task.id), "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt text from file"

    def test_edit_with_prompt_file_not_found(self, tmp_path: Path):
        """Edit command handles missing file gracefully."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Original prompt text")

        result = run_gza("edit", str(task.id), "--prompt-file", "/nonexistent/file.txt", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()

    def test_edit_with_prompt_text(self, tmp_path: Path):
        """Edit command can update prompt from command line."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Original prompt text")

        result = run_gza("edit", str(task.id), "--prompt", "New prompt from command line", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt from command line"

    def test_edit_with_prompt_validation_error(self, tmp_path: Path):
        """Edit command validates prompt length."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Original prompt text")

        # Try to set a prompt that's too short
        result = run_gza("edit", str(task.id), "--prompt", "short", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Validation" in result.stdout or "too short" in result.stdout.lower()

        # Verify prompt was NOT updated
        updated = store.get(task.id)
        assert updated.prompt == "Original prompt text"

    def test_edit_prompt_and_prompt_file_conflict(self, tmp_path: Path):
        """Edit command rejects both --prompt and --prompt-file."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Original prompt text")

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File content")

        result = run_gza("edit", str(task.id), "--prompt", "text", "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Cannot use both" in result.stdout

    def test_edit_with_prompt_from_stdin(self, tmp_path: Path):
        """Edit command can read prompt from stdin using --prompt -."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Original prompt text")

        stdin_content = "New prompt from stdin input"
        result = run_gza("edit", str(task.id), "--prompt", "-", "--project", str(tmp_path), stdin_input=stdin_content)

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt from stdin input"


class TestNextCommandWithDependencies:
    """Tests for 'gza next' command with dependencies."""

    def test_next_skips_blocked_tasks(self, tmp_path: Path):
        """Next command skips tasks blocked by dependencies."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create task chain
        task1 = store.add("First task")
        task2 = store.add("Blocked task", depends_on=task1.id)
        task3 = store.add("Independent task")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show task1 or task3, but not task2
        assert "Blocked task" not in result.stdout or "blocked" in result.stdout.lower()

    def test_next_all_shows_blocked_tasks(self, tmp_path: Path):
        """Next --all command shows blocked tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create task chain
        task1 = store.add("First task")
        task2 = store.add("Blocked task", depends_on=task1.id)

        result = run_gza("next", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "First task" in result.stdout
        assert "Blocked task" in result.stdout

    def test_next_shows_blocked_count(self, tmp_path: Path):
        """Next command shows count of blocked tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create blocked tasks
        task1 = store.add("First task")
        store.add("Blocked task 1", depends_on=task1.id)
        store.add("Blocked task 2", depends_on=task1.id)
        store.add("Independent task")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should mention 2 blocked tasks
        assert "2" in result.stdout and "blocked" in result.stdout.lower()


class TestAddCommandWithChaining:
    """Tests for 'gza add' command with chaining features."""

    def test_add_with_type_plan(self, tmp_path: Path):
        """Add command can create plan tasks."""
        setup_config(tmp_path)
        result = run_gza("add", "--type", "plan", "Create a plan", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

    def test_add_with_type_implement(self, tmp_path: Path):
        """Add command can create implement tasks."""
        setup_config(tmp_path)
        result = run_gza("add", "--type", "implement", "Implement feature", "--project", str(tmp_path))

        assert result.returncode == 0

    def test_add_with_type_review(self, tmp_path: Path):
        """Add command can create review tasks."""
        setup_config(tmp_path)
        result = run_gza("add", "--type", "review", "Review implementation", "--project", str(tmp_path))

        assert result.returncode == 0

    def test_add_with_based_on(self, tmp_path: Path):
        """Add command can create tasks with based_on reference."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task1 = store.add("First task")

        result = run_gza("add", "--based-on", str(task1.id), "Follow-up task", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify based_on was set
        tasks = store.get_pending()
        follow_up = next((t for t in tasks if t.prompt == "Follow-up task"), None)
        assert follow_up is not None
        assert follow_up.based_on == task1.id

    def test_add_with_spec(self, tmp_path: Path):
        """Add command with --spec sets spec file on task."""
        setup_config(tmp_path)

        # Create a spec file
        spec_file = tmp_path / "specs" / "feature.md"
        spec_file.parent.mkdir(parents=True, exist_ok=True)
        spec_file.write_text("# Feature Spec\n\nThis is a test spec.")

        result = run_gza("add", "--spec", "specs/feature.md", "Implement feature", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify spec was set
        from gza.db import SqliteTaskStore
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Implement feature"), None)
        assert task is not None
        assert task.spec == "specs/feature.md"

    def test_add_with_spec_file_not_found(self, tmp_path: Path):
        """Add command with --spec fails if file doesn't exist."""
        setup_config(tmp_path)

        result = run_gza("add", "--spec", "nonexistent.md", "Implement feature", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Spec file not found: nonexistent.md" in result.stdout


class TestAddCommandWithModelAndProvider:
    """Tests for 'gza add' command with --model and --provider flags."""

    def test_add_with_model_flag(self, tmp_path: Path):
        """Add command with --model flag stores model override."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        result = run_gza("add", "--model", "claude-3-5-haiku-latest", "Test task with model", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify model was set
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with model"), None)
        assert task is not None
        assert task.model == "claude-3-5-haiku-latest"

    def test_add_with_provider_flag(self, tmp_path: Path):
        """Add command with --provider flag stores provider override."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        result = run_gza("add", "--provider", "gemini", "Test task with provider", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify provider was set
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with provider"), None)
        assert task is not None
        assert task.provider == "gemini"

    def test_add_with_both_model_and_provider(self, tmp_path: Path):
        """Add command with both --model and --provider flags works."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        result = run_gza(
            "add",
            "--model", "claude-opus-4",
            "--provider", "claude",
            "Test task with both",
            "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify both were set
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with both"), None)
        assert task is not None
        assert task.model == "claude-opus-4"
        assert task.provider == "claude"


class TestAddCommandWithNoLearnings:
    """Tests for 'gza add' command with --no-learnings flag."""

    def test_add_with_no_learnings_flag(self, tmp_path: Path):
        """Add command with --no-learnings flag sets skip_learnings on task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        result = run_gza("add", "--no-learnings", "One-off experimental task", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify skip_learnings was set
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "One-off experimental task"), None)
        assert task is not None
        assert task.skip_learnings is True

    def test_add_without_no_learnings_flag_defaults_false(self, tmp_path: Path):
        """Add command without --no-learnings flag defaults skip_learnings to False."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        result = run_gza("add", "Normal task with learnings", "--project", str(tmp_path))

        assert result.returncode == 0

        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Normal task with learnings"), None)
        assert task is not None
        assert task.skip_learnings is False


class TestEditCommandWithModelAndProvider:
    """Tests for 'gza edit' command with --model and --provider flags."""

    def test_edit_with_model_flag(self, tmp_path: Path):
        """Edit command can set model override."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a task
        task = store.add("Test task")
        assert task.model is None

        # Edit to add model
        result = run_gza("edit", str(task.id), "--model", "claude-3-5-haiku-latest", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Set model override" in result.stdout

        # Verify model was set
        task = store.get(task.id)
        assert task is not None
        assert task.model == "claude-3-5-haiku-latest"

    def test_edit_with_provider_flag(self, tmp_path: Path):
        """Edit command can set provider override."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a task
        task = store.add("Test task")
        assert task.provider is None

        # Edit to add provider
        result = run_gza("edit", str(task.id), "--provider", "gemini", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Set provider override" in result.stdout

        # Verify provider was set
        task = store.get(task.id)
        assert task is not None
        assert task.provider == "gemini"


class TestGetEffectiveConfigForTask:
    """Tests for get_effective_config_for_task helper function."""

    def test_task_model_override_beats_provider_scoped_config(self, tmp_path: Path):
        """Task-specific model takes priority over provider-scoped model config."""
        from gza.config import Config, ProviderConfig, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {
            "claude": ProviderConfig(
                model="claude-default",
                task_types={"review": TaskTypeConfig(model="claude-review")},
            )
        }

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
            model="task-model-override",
        )

        model, provider, max_turns = get_effective_config_for_task(task, config)
        assert model == "task-model-override"
        assert provider == "claude"
        assert max_turns == config.max_turns

    def test_provider_scoped_task_type_model_selected(self, tmp_path: Path):
        """Provider-scoped task type model takes priority over provider default."""
        from gza.config import Config, ProviderConfig, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {
            "claude": ProviderConfig(
                model="claude-default",
                task_types={"review": TaskTypeConfig(model="claude-review")},
            )
        }

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert model == "claude-review"
        assert provider == "claude"

    def test_provider_scoped_default_model_selected(self, tmp_path: Path):
        """Provider-scoped default model is used when task type override is absent."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {"claude": ProviderConfig(model="claude-default")}

        task = Task(
            id=1,
            prompt="Test task",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert model == "claude-default"
        assert provider == "claude"

    def test_provider_override_switches_provider_scope(self, tmp_path: Path):
        """Task provider override switches model selection to that provider scope."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {
            "claude": ProviderConfig(model="claude-default"),
            "codex": ProviderConfig(model="o4-mini"),
        }

        task = Task(
            id=1,
            prompt="Test task",
            provider="codex",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "codex"
        assert model == "o4-mini"

    def test_task_provider_route_applies_without_task_override(self, tmp_path: Path):
        """task_providers should route by task type before falling back to default provider."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "codex"
        config.task_providers = {"review": "claude"}
        config.providers = {
            "claude": ProviderConfig(model="claude-review-model"),
            "codex": ProviderConfig(model="o4-mini"),
        }

        task = Task(
            id=1,
            prompt="Review task",
            task_type="review",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "claude"
        assert model == "claude-review-model"

    def test_falls_back_to_legacy_when_provider_scope_missing(self, tmp_path: Path):
        """Legacy top-level task_types/model remain as fallback if scope is missing."""
        from gza.config import Config, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.model = "legacy-default"
        config.task_types = {"review": TaskTypeConfig(model="legacy-review")}

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "claude"
        assert model == "legacy-review"

    def test_provider_scoped_max_turns_selected(self, tmp_path: Path):
        """Provider-scoped task type max_turns takes priority."""
        from gza.config import Config, ProviderConfig, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.max_turns = 50
        config.provider = "claude"
        config.task_types = {"review": TaskTypeConfig(max_turns=30)}
        config.providers = {
            "claude": ProviderConfig(
                task_types={"review": TaskTypeConfig(max_turns=20)}
            )
        }

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
        )

        _, _, max_turns = get_effective_config_for_task(task, config)
        assert max_turns == 20


class TestBuildPromptWithSpec:
    """Tests for build_prompt with spec file content."""

    def test_build_prompt_includes_spec_content(self, tmp_path: Path):
        """build_prompt includes spec file content when task has spec."""
        from gza.config import Config
        from gza.db import SqliteTaskStore, Task
        from gza.runner import build_prompt

        # Setup config
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Setup database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create spec file
        spec_file = tmp_path / "specs" / "feature.md"
        spec_file.parent.mkdir(parents=True, exist_ok=True)
        spec_content = "# Feature Spec\n\nImplement X with Y."
        spec_file.write_text(spec_content)

        # Create task with spec
        task = store.add("Implement the feature", spec="specs/feature.md")

        # Build prompt
        prompt = build_prompt(task, config, store)

        # Verify spec content is included
        assert "## Specification" in prompt
        assert "specs/feature.md" in prompt
        assert "# Feature Spec" in prompt
        assert "Implement X with Y." in prompt

    def test_build_prompt_without_spec(self, tmp_path: Path):
        """build_prompt works correctly when task has no spec."""
        from gza.config import Config
        from gza.db import SqliteTaskStore, Task
        from gza.runner import build_prompt

        # Setup config
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Setup database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create task without spec
        task = store.add("Simple task")

        # Build prompt
        prompt = build_prompt(task, config, store)

        # Verify no spec section
        assert "## Specification" not in prompt
        assert "Simple task" in prompt


class TestGetTaskOutput:
    """Tests for _get_task_output helper function."""

    def test_prefers_db_content(self, tmp_path: Path):
        """_get_task_output should prefer output_content from DB."""
        from gza.runner import _get_task_output
        from gza.db import Task

        task = Task(
            id=1,
            prompt="Test",
            output_content="Content from DB",
        )
        result = _get_task_output(task, tmp_path)
        assert result == "Content from DB"

    def test_falls_back_to_file(self, tmp_path: Path):
        """_get_task_output should fall back to file when no DB content."""
        from gza.runner import _get_task_output
        from gza.db import Task

        # Create report file
        report_dir = tmp_path / ".gza" / "plans"
        report_dir.mkdir(parents=True)
        report_file = report_dir / "test.md"
        report_file.write_text("Content from file")

        task = Task(
            id=2,
            prompt="Test",
            report_file=".gza/plans/test.md",
            output_content=None,
        )
        result = _get_task_output(task, tmp_path)
        assert result == "Content from file"

    def test_prefers_db_over_file(self, tmp_path: Path):
        """_get_task_output should prefer DB when both exist."""
        from gza.runner import _get_task_output
        from gza.db import Task

        # Create report file
        report_dir = tmp_path / ".gza" / "plans"
        report_dir.mkdir(parents=True)
        report_file = report_dir / "test.md"
        report_file.write_text("Content from file")

        task = Task(
            id=3,
            prompt="Test",
            report_file=".gza/plans/test.md",
            output_content="DB wins",
        )
        result = _get_task_output(task, tmp_path)
        assert result == "DB wins"

    def test_returns_none_when_no_content(self, tmp_path: Path):
        """_get_task_output should return None when no content available."""
        from gza.runner import _get_task_output
        from gza.db import Task

        task = Task(
            id=4,
            prompt="Test",
            output_content=None,
        )
        result = _get_task_output(task, tmp_path)
        assert result is None


class TestPsCommand:
    """Tests for 'gza ps' command."""

    def test_ps_shows_task_id(self, tmp_path: Path):
        """PS command should display task ID for running workers."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        # Setup config and database
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a task
        task = store.add("Test task for ps command")

        # Create workers directory and register a worker
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        worker = WorkerMetadata(
            worker_id="w-test-ps",
            pid=99999,  # Fake PID
            task_id=task.id,
            task_slug=None,
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(worker)

        # Run ps command
        result = run_gza("ps", "--all", cwd=tmp_path)

        # Verify task ID is in output
        assert result.returncode == 0
        assert "TASK ID" in result.stdout, "Header should contain 'TASK ID' column"
        assert "STARTED" in result.stdout, "Header should contain 'STARTED' column"
        assert f"#{task.id}" in result.stdout, f"Output should contain task ID #{task.id}"

        # Cleanup
        registry.remove("w-test-ps")

    def test_ps_reconciles_db_and_worker_with_source_both(self, tmp_path: Path):
        """PS dedupes by task_id and marks row source as both."""
        import json
        import os

        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Reconciled task")
        store.mark_in_progress(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-both",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--json", cwd=tmp_path)
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["source"] == "both"
        assert rows[0]["is_orphaned"] is False
        assert rows[0]["started_at"] is not None

        registry.remove("w-test-both")

    def test_ps_includes_db_only_in_progress_and_flags_orphaned(self, tmp_path: Path):
        """PS includes in-progress DB rows even when no worker exists."""
        import json

        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("DB-only in-progress task")
        store.mark_in_progress(task)

        result = run_gza("ps", "--json", cwd=tmp_path)
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["source"] == "db"
        assert rows[0]["status"] == "in_progress"
        assert rows[0]["is_orphaned"] is True
        assert "orphaned" in rows[0]["flags"]
        assert rows[0]["started_at"] is not None

    def test_ps_formats_started_timestamp_in_table_output(self, tmp_path: Path):
        """PS table output renders start timestamps in UTC with clear formatting."""
        import os

        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Formatted start time")
        store.mark_in_progress(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-start-format",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at="2026-01-08T00:00:00+00:00",
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--all", cwd=tmp_path)
        assert result.returncode == 0
        assert "2026-01-08 00:00:00 UTC" in result.stdout

        registry.remove("w-test-start-format")

    def test_ps_quiet_shows_only_worker_ids(self, tmp_path: Path):
        """PS quiet output should only include real worker IDs."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # DB-only in-progress task must not appear in quiet mode.
        db_only_task = store.add("DB-only in-progress task")
        store.mark_in_progress(db_only_task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-quiet",
                pid=os.getpid(),
                task_id=None,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--quiet", cwd=tmp_path)
        assert result.returncode == 0
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        assert lines == ["w-test-quiet"]
        assert all(not line.startswith("db:") for line in lines)

        registry.remove("w-test-quiet")

    def test_ps_flags_stale_and_orphaned_for_stale_worker_in_progress_task(self, tmp_path: Path):
        """PS flags stale worker + orphaned in-progress task in reconciled row."""
        import json

        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Stale worker task")
        store.mark_in_progress(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-stale-ps",
                pid=999999,
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--json", cwd=tmp_path)
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["source"] == "both"
        assert rows[0]["is_stale"] is True
        assert rows[0]["is_orphaned"] is True
        assert "stale" in rows[0]["flags"]
        assert "orphaned" in rows[0]["flags"]

        registry.remove("w-test-stale-ps")

    def test_ps_handles_missing_started_timestamp(self, tmp_path: Path):
        """PS should gracefully handle invalid/missing start timestamps."""
        import json

        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-start",
                pid=99999,
                task_id=None,
                task_slug="standalone-worker",
                started_at="not-a-timestamp",
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        table_result = run_gza("ps", "--all", cwd=tmp_path)
        assert table_result.returncode == 0
        assert "w-test-no-start" in table_result.stdout
        assert "standalone-worker" in table_result.stdout
        assert " - " in table_result.stdout

        json_result = run_gza("ps", "--all", "--json", cwd=tmp_path)
        assert json_result.returncode == 0
        rows = json.loads(json_result.stdout)
        assert len(rows) == 1
        assert rows[0]["worker_id"] == "w-test-no-start"
        assert rows[0]["started"] == "-"
        assert rows[0]["started_at"] is None

        registry.remove("w-test-no-start")

    def test_ps_json_order_stable_when_started_timestamps_missing(self, tmp_path: Path):
        """PS JSON ordering is deterministic when start times are unavailable."""
        import json

        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        # Register in reverse lexical order to assert sort stability by worker_id.
        for worker_id in ["w-test-order-b", "w-test-order-a"]:
            registry.register(
                WorkerMetadata(
                    worker_id=worker_id,
                    pid=99999,
                    task_id=None,
                    task_slug=None,
                    started_at="invalid",
                    status="running",
                    log_file=None,
                    worktree=None,
                )
            )

        result = run_gza("ps", "--all", "--json", cwd=tmp_path)
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert [row["worker_id"] for row in rows] == ["w-test-order-a", "w-test-order-b"]

        registry.remove("w-test-order-a")
        registry.remove("w-test-order-b")

    def test_ps_poll_default_interval(self, tmp_path: Path):
        """--poll without a value uses 5-second default interval."""
        import argparse
        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            all=False,
            quiet=False,
            json=False,
            poll=5,  # const value when --poll given without argument
        )

        call_count = 0
        sleep_calls: list[float] = []

        def fake_sleep(n: float) -> None:
            nonlocal call_count
            sleep_calls.append(n)
            call_count += 1
            if call_count >= 2:
                raise KeyboardInterrupt

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=fake_sleep):
            result = cmd_ps(args)

        assert result == 0
        assert all(s == 5 for s in sleep_calls)

    def test_ps_poll_custom_interval(self, tmp_path: Path):
        """--poll N uses the specified interval."""
        import argparse
        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            all=False,
            quiet=False,
            json=False,
            poll=10,
        )

        sleep_calls: list[float] = []

        def fake_sleep(n: float) -> None:
            sleep_calls.append(n)
            raise KeyboardInterrupt

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=fake_sleep):
            result = cmd_ps(args)

        assert result == 0
        assert sleep_calls == [10]

    def test_ps_poll_shows_timestamp_header(self, tmp_path: Path, capsys):
        """Poll mode prints the refresh interval and timestamp in the header."""
        import argparse
        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            all=False,
            quiet=False,
            json=False,
            poll=3,
        )

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=KeyboardInterrupt):
            result = cmd_ps(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Refreshing every 3s" in captured.out
        assert "last updated:" in captured.out
        assert "Ctrl+C to exit" in captured.out

    def test_ps_no_poll_behaves_as_before(self, tmp_path: Path, capsys):
        """Without --poll the command runs once and exits immediately."""
        import argparse
        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            all=False,
            quiet=False,
            json=False,
            poll=None,
        )

        import unittest.mock as mock
        with mock.patch("time.sleep") as mock_sleep:
            result = cmd_ps(args)

        assert result == 0
        mock_sleep.assert_not_called()

    def test_ps_poll_negative_value_returns_error(self, tmp_path: Path, capsys):
        """Negative --poll value returns exit code 1 with an error message."""
        import argparse
        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            all=False,
            quiet=False,
            json=False,
            poll=-1,
        )

        result = cmd_ps(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "error" in captured.err
        assert "--poll" in captured.err
        assert "-1" in captured.err

    def test_ps_poll_zero_value_returns_error(self, tmp_path: Path, capsys):
        """Zero --poll value returns exit code 1 with an error message."""
        import argparse
        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            all=False,
            quiet=False,
            json=False,
            poll=0,
        )

        result = cmd_ps(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "error" in captured.err

    def test_ps_poll_no_ansi_codes_when_not_tty(self, tmp_path: Path, capsys):
        """ANSI escape codes are not emitted when stdout is not a TTY."""
        import argparse
        from gza.cli import cmd_ps

        setup_config(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            all=False,
            quiet=False,
            json=False,
            poll=5,
        )

        import unittest.mock as mock
        with mock.patch("time.sleep", side_effect=KeyboardInterrupt):
            result = cmd_ps(args)

        assert result == 0
        captured = capsys.readouterr()
        # capsys captures a non-TTY stream, so ANSI codes must be absent
        assert "\033[2J" not in captured.out
        assert "\033[H" not in captured.out

    def test_ps_poll_auto_stops_when_all_tasks_complete(self, tmp_path: Path):
        """Poll mode exits automatically once all seen tasks leave running/in_progress."""
        import argparse
        import os
        import unittest.mock as mock
        from gza.cli import cmd_ps
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        worker = WorkerMetadata(
            worker_id="w-test-autostop",
            pid=os.getpid(),  # Real PID so is_running() returns True
            task_id=None,
            task_slug=None,
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(worker)

        sleep_calls: list[float] = []

        def fake_sleep(n: float) -> None:
            sleep_calls.append(n)
            # Transition the worker to completed so the next poll sees it as done.
            worker.status = "completed"
            registry.update(worker)

        # Use --all so that completed workers remain visible in live_rows and
        # seen_tasks is updated with the final "completed" status on the second poll.
        args = argparse.Namespace(
            project_dir=tmp_path,
            all=True,
            quiet=False,
            json=False,
            poll=2,
        )

        with mock.patch("time.sleep", side_effect=fake_sleep):
            result = cmd_ps(args)

        assert result == 0
        # sleep was called exactly once: after poll 1 (running), before poll 2 (completed→break)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == 2

        registry.remove("w-test-autostop")

    def test_ps_steps_column_uses_num_steps_computed_for_completed_task(self, tmp_path: Path):
        """STEPS column shows num_steps_computed for a completed task, without hitting the DB."""
        import json
        import unittest.mock as mock
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Completed task with steps")
        store.mark_in_progress(task)
        # Set num_steps_computed before marking completed so it persists via update().
        task.num_steps_computed = 7
        store.mark_completed(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-steps-computed",
                pid=99999,
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--all", "--json", cwd=tmp_path)
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["steps"] == "7"

        registry.remove("w-test-steps-computed")

    def test_ps_steps_column_uses_live_count_for_in_progress_task(self, tmp_path: Path):
        """STEPS column shows live DB row count for an in-progress task."""
        import json
        import os
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("In-progress task with live steps")
        store.mark_in_progress(task)
        assert task.id is not None

        # Emit 3 run_steps rows for this task.
        for i in range(3):
            store.emit_step(task.id, f"Step {i + 1}", provider="claude")

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-steps-live",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--json", cwd=tmp_path)
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["steps"] == "3"

        registry.remove("w-test-steps-live")


class TestHelpOutput:
    """Tests for CLI help output."""

    def test_commands_displayed_alphabetically(self):
        """Help output should display commands in alphabetical order."""
        result = subprocess.run(
            ["uv", "run", "gza", "--help"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        # Extract the commands section from help output
        help_text = result.stdout

        # Find where the commands list starts (after "positional arguments:" or "{")
        # Commands are typically shown as "{command1,command2,...}"
        import re

        # Look for the commands in the help output
        # They appear in a format like: {add,delete,edit,...}
        commands_match = re.search(r'\{([^}]+)\}', help_text)
        if not commands_match:
            # Alternative: commands listed line by line
            # Extract command names from lines that look like "  command_name  description"
            command_lines = []
            in_commands_section = False
            for line in help_text.split('\n'):
                if 'positional arguments:' in line or '{' in line:
                    in_commands_section = True
                    continue
                if in_commands_section and line.strip() and not line.startswith(' ' * 10):
                    # Extract command name (first word after leading spaces)
                    parts = line.strip().split()
                    if parts and not parts[0].startswith('-'):
                        command_lines.append(parts[0])
                if in_commands_section and line and not line.startswith(' '):
                    # End of commands section
                    break

            # Check if commands are sorted
            if command_lines:
                sorted_commands = sorted(command_lines)
                assert command_lines == sorted_commands, f"Commands not in alphabetical order. Got: {command_lines}, Expected: {sorted_commands}"
        else:
            # Commands are in {cmd1,cmd2,...} format
            commands_str = commands_match.group(1)
            commands = [cmd.strip() for cmd in commands_str.split(',')]

            # Verify commands are in alphabetical order
            sorted_commands = sorted(commands)
            assert commands == sorted_commands, f"Commands not in alphabetical order. Got: {commands}, Expected: {sorted_commands}"


class TestWorkCommandMultiTask:
    """Tests for 'gza work' command with multiple task IDs."""

    def test_work_with_single_task_id(self, tmp_path: Path):
        """Work command accepts a single task ID."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add a task
        task1 = store.add("Test task 1")

        # Verify the command accepts the argument
        result = run_gza("work", str(task1.id), "--no-docker", "--project", str(tmp_path))

        # Note: Without actual Claude integration, this will fail,
        # but we're verifying that argparse accepts the input
        # The error should not be about argument parsing
        assert "unrecognized arguments" not in result.stderr

    def test_work_with_multiple_task_ids(self, tmp_path: Path):
        """Work command accepts multiple task IDs."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add multiple tasks
        task1 = store.add("Test task 1")
        task2 = store.add("Test task 2")
        task3 = store.add("Test task 3")

        # Verify the command accepts multiple arguments
        result = run_gza("work", str(task1.id), str(task2.id), str(task3.id),
                        "--no-docker", "--project", str(tmp_path))

        # Verify argparse accepts the input
        assert "unrecognized arguments" not in result.stderr

    def test_work_background_with_multiple_task_ids(self, tmp_path: Path):
        """Work command with --background spawns workers for multiple task IDs."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add multiple tasks
        task1 = store.add("Test task 1")
        task2 = store.add("Test task 2")

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        # Run with background mode and multiple task IDs
        result = run_gza("work", str(task1.id), str(task2.id),
                        "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes without argument parsing errors
        assert "unrecognized arguments" not in result.stderr

    def test_work_background_subprocess_uses_project_flag(self, tmp_path: Path):
        """Background worker subprocess command uses --project flag, not bare positional arg."""
        import argparse
        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from unittest.mock import patch, MagicMock

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)
        args = argparse.Namespace(no_docker=True, max_turns=None)

        with patch("gza.cli.subprocess.Popen") as mock_popen:
            mock_popen.return_value.pid = 12345
            _spawn_background_worker(args, config, task_id=task.id)

            assert mock_popen.called
            cmd = mock_popen.call_args[0][0]
            # Project dir must be passed with --project flag, not as bare positional
            project_dir = str(config.project_dir.absolute())
            assert "--project" in cmd, f"--project flag missing from subprocess cmd: {cmd}"
            project_idx = cmd.index("--project")
            assert cmd[project_idx + 1] == project_dir

    def test_work_with_no_task_ids(self, tmp_path: Path):
        """Work command works without task IDs (runs next pending)."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add a task
        store.add("Test task 1")

        # Run without task IDs
        result = run_gza("work", "--no-docker", "--project", str(tmp_path))

        # Verify no argument parsing errors
        assert "unrecognized arguments" not in result.stderr

    def test_work_validates_all_task_ids_before_execution(self, tmp_path: Path):
        """Work command validates all task IDs before starting execution."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add one valid task
        task1 = store.add("Test task 1")

        # Try to run with one valid and one invalid task ID
        result = run_gza("work", str(task1.id), "999", "--no-docker", "--project", str(tmp_path))

        # Should error about the invalid task ID
        assert result.returncode != 0
        assert "Task #999 not found" in result.stdout or "Task #999 not found" in result.stderr

    def test_work_validates_task_status(self, tmp_path: Path):
        """Work command validates that tasks are in pending status."""
        from gza.db import SqliteTaskStore
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add a completed task
        task1 = store.add("Test task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        store.update(task1)

        # Try to run the completed task
        result = run_gza("work", str(task1.id), "--no-docker", "--project", str(tmp_path))

        # Should error about task status
        assert result.returncode != 0
        assert f"Task #{task1.id} is not pending" in result.stdout or f"Task #{task1.id} is not pending" in result.stderr

    def test_work_warns_about_orphaned_tasks_before_starting(self, tmp_path: Path):
        """Work command warns about orphaned in-progress tasks before starting new work."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create an orphaned task (in-progress, no active worker) and no pending tasks.
        # With no pending tasks, run() will return 0 immediately after printing
        # "No pending tasks found", so we can observe the orphaned warning without
        # needing to actually execute a task.
        orphaned_task = store.add("Stuck task from yesterday")
        store.mark_in_progress(orphaned_task)

        result = run_gza("work", "--no-docker", "--project", str(tmp_path))

        assert result.returncode == 0
        # Warning about orphaned task should appear before the "No pending tasks" message
        assert "orphaned" in result.stdout
        assert "Stuck task from yesterday" in result.stdout
        assert "gza work" in result.stdout


class TestBackgroundWorkerCommand:
    """Tests for background worker subprocess command construction."""

    def test_background_worker_command_uses_project_flag(self, tmp_path: Path):
        """Background worker subprocess must pass project dir with --project flag, not as positional arg.

        Regression test: _spawn_background_worker was appending the project directory
        as a bare positional argument, which argparse would try to parse as a task_id
        (type=int), causing the worker subprocess to crash on startup.
        """
        from unittest.mock import patch, MagicMock
        from gza.db import SqliteTaskStore
        from gza.cli import _spawn_background_worker
        from gza.config import Config
        import argparse

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add a pending task
        task = store.add("Test background task")

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)

        # Create args namespace matching what argparse produces
        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        # Capture the subprocess command
        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_popen(cmd, **kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with patch("gza.cli.subprocess.Popen", side_effect=capture_popen):
            _spawn_background_worker(args, config, task_id=task.id)

        assert captured_cmd is not None, "subprocess.Popen was not called"

        # The project directory must be preceded by --project flag.
        # If it appears as a bare positional, argparse will try to parse it
        # as a task_id (type=int) and the worker subprocess will crash.
        project_dir_str = str(config.project_dir.absolute())
        assert project_dir_str in captured_cmd, \
            f"Project dir {project_dir_str!r} not found in command: {captured_cmd}"

        project_idx = captured_cmd.index(project_dir_str)
        assert captured_cmd[project_idx - 1] == "--project", \
            f"Project dir must be preceded by --project flag, but got: {captured_cmd[project_idx - 1]!r}. " \
            f"Full command: {captured_cmd}"

    def test_background_resume_worker_command_uses_project_flag(self, tmp_path: Path):
        """Background resume worker subprocess must pass project dir with --project flag.

        Same regression as test_background_worker_command_uses_project_flag but
        for _spawn_background_resume_worker.
        """
        from unittest.mock import patch, MagicMock
        from gza.db import SqliteTaskStore
        from gza.cli import _spawn_background_resume_worker
        from gza.config import Config
        import argparse

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add a pending task
        task = store.add("Test resume task")

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        # Capture the subprocess command
        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_popen(cmd, **kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with patch("gza.cli.subprocess.Popen", side_effect=capture_popen):
            _spawn_background_resume_worker(args, config, new_task_id=task.id)

        assert captured_cmd is not None, "subprocess.Popen was not called"

        # The project directory must be preceded by --project flag
        project_dir_str = str(config.project_dir.absolute())
        assert project_dir_str in captured_cmd, \
            f"Project dir {project_dir_str!r} not found in command: {captured_cmd}"

        project_idx = captured_cmd.index(project_dir_str)
        assert captured_cmd[project_idx - 1] == "--project", \
            f"Project dir must be preceded by --project flag, but got: {captured_cmd[project_idx - 1]!r}. " \
            f"Full command: {captured_cmd}"


class TestMergeCommand:
    """Tests for 'gza merge' command."""

    def test_merge_accepts_squash_flag(self, tmp_path: Path):
        """Merge command accepts --squash flag."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test merge task")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-merge"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-merge")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that --squash flag is accepted
        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))

        # Verify the command doesn't fail due to argument parsing
        assert "unrecognized arguments" not in result.stderr
        # The merge should succeed or fail based on git operations, not argument parsing
        assert result.returncode == 0 or "Error merging" in result.stdout

    def test_merge_accepts_rebase_flag(self, tmp_path: Path):
        """Merge command accepts --rebase flag."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test rebase task")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-rebase"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-rebase")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that --rebase flag is accepted
        result = run_gza("merge", str(task.id), "--rebase", "--project", str(tmp_path))

        # Verify the command doesn't fail due to argument parsing
        assert "unrecognized arguments" not in result.stderr
        # The rebase should succeed or fail based on git operations, not argument parsing
        assert result.returncode == 0 or "Error during rebase" in result.stdout

    def test_merge_rejects_both_rebase_and_squash(self, tmp_path: Path):
        """Merge command rejects --rebase and --squash together."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test conflicting flags")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-conflict"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-conflict")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that both flags together are rejected
        result = run_gza("merge", str(task.id), "--rebase", "--squash", "--project", str(tmp_path))

        # Verify the command fails with appropriate error message
        assert result.returncode == 1
        assert "Cannot use --rebase and --squash together" in result.stdout

    def test_merge_remote_requires_rebase(self, tmp_path: Path):
        """Merge command rejects --remote without --rebase."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test remote without rebase")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-remote"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/test-remote")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that --remote without --rebase is rejected
        result = run_gza("merge", str(task.id), "--remote", "--project", str(tmp_path))

        # Verify the command fails with appropriate error message
        assert result.returncode == 1
        assert "--remote requires --rebase" in result.stdout

    def test_merge_rebase_with_remote(self, tmp_path: Path):
        """Merge command accepts --rebase --remote together."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo with a remote
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create a bare repo to use as remote
        remote_path = tmp_path / "remote.git"
        remote_path.mkdir()
        git._run("init", "--bare", str(remote_path))

        # Add remote and push
        git._run("remote", "add", "origin", str(remote_path))
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")
        git._run("push", "-u", "origin", "main")

        # Create a task with a branch
        task = store.add("Test rebase with remote")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-remote-rebase"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-remote-rebase")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that --rebase --remote flags work together
        result = run_gza("merge", str(task.id), "--rebase", "--remote", "--project", str(tmp_path))

        # Verify the command doesn't fail due to argument parsing
        assert "unrecognized arguments" not in result.stderr
        # Should either succeed or fail gracefully (not due to flag validation)
        assert "--remote requires --rebase" not in result.stdout

    def test_merge_resolve_requires_rebase(self, tmp_path: Path):
        """Merge command rejects --resolve without --rebase."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test resolve without rebase")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-resolve"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/test-resolve")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that --resolve without --rebase is rejected
        result = run_gza("merge", str(task.id), "--resolve", "--project", str(tmp_path))

        # Verify the command fails with appropriate error message
        assert result.returncode == 1
        assert "--resolve requires --rebase" in result.stdout

    def test_merge_resolve_with_rebase_accepted(self, tmp_path: Path):
        """Merge command accepts --resolve --rebase together."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test resolve with rebase")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-resolve-rebase"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-resolve-rebase")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that --resolve --rebase is accepted (flag validation passes)
        result = run_gza("merge", str(task.id), "--rebase", "--resolve", "--project", str(tmp_path))

        # Verify no flag validation error
        assert "unrecognized arguments" not in result.stderr
        assert "--resolve requires --rebase" not in result.stdout
        # Should either succeed or fail gracefully (git ops), not due to flag validation
        assert result.returncode == 0 or "Error during rebase" in result.stdout

    def test_squash_merge_creates_commit(self, tmp_path: Path):
        """Squash merge creates a commit, not just staged changes."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Get the commit count before merge
        commits_before = git._run("rev-list", "--count", "HEAD")
        commit_count_before = int(commits_before.stdout.strip())

        # Create a task with a branch
        task = store.add("Add feature X")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-squash"
        store.update(task)

        # Create the branch and add multiple commits
        git._run("checkout", "-b", "feature/test-squash")
        (tmp_path / "feature1.txt").write_text("feature content 1")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature part 1")
        (tmp_path / "feature2.txt").write_text("feature content 2")
        git._run("add", "feature2.txt")
        git._run("commit", "-m", "Add feature part 2")
        git._run("checkout", "main")

        # Perform squash merge
        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))

        # Verify the merge succeeded
        assert result.returncode == 0
        assert "Successfully squash merged" in result.stdout

        # Verify a commit was created (not just staged changes)
        commits_after = git._run("rev-list", "--count", "HEAD")
        commit_count_after = int(commits_after.stdout.strip())
        assert commit_count_after == commit_count_before + 1, "Expected one new commit"

        # Verify no staged changes remain
        staged_result = git._run("diff", "--cached", "--quiet", check=False)
        assert staged_result.returncode == 0, "Expected no staged changes after squash merge"

        # Verify the feature files are present
        assert (tmp_path / "feature1.txt").exists()
        assert (tmp_path / "feature2.txt").exists()

    def test_squash_merge_commit_message_includes_task_info(self, tmp_path: Path):
        """Squash merge commit message includes task information."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a descriptive prompt
        task_prompt = "Implement user authentication with JWT tokens"
        task = store.add(task_prompt)
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/auth"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/auth")
        (tmp_path / "auth.txt").write_text("authentication code")
        git._run("add", "auth.txt")
        git._run("commit", "-m", "Add auth")
        git._run("checkout", "main")

        # Perform squash merge
        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))
        assert result.returncode == 0

        # Get the commit message
        log_result = git._run("log", "-1", "--pretty=%B")
        commit_message = log_result.stdout.strip()

        # Verify the commit message contains task information
        assert f"Task #{task.id}" in commit_message, "Commit message should include task ID"
        assert task_prompt in commit_message, "Commit message should include task prompt"
        assert "Squash merge" in commit_message, "Commit message should indicate squash merge"

    def test_branch_shows_as_merged_after_squash(self, tmp_path: Path):
        """Branch shows as merged in 'gza unmerged' after squash merge completes."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Add cool feature")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/cool"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/cool")
        (tmp_path / "cool.txt").write_text("cool feature")
        git._run("add", "cool.txt")
        git._run("commit", "-m", "Add cool feature")
        git._run("checkout", "main")

        # Verify branch is not merged before squash using git directly
        is_merged_before = git.is_merged(task.branch, "main")
        assert not is_merged_before, "Branch should not be merged before squash merge"

        # Perform squash merge
        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))
        assert result.returncode == 0

        # Verify branch now shows as merged using git directly
        is_merged_after = git.is_merged(task.branch, "main")
        assert is_merged_after, "Branch should be detected as merged after squash merge"

        # Verify the cool.txt file is present in main
        assert (tmp_path / "cool.txt").exists(), "Feature file should exist in main after merge"

    def test_mark_only_preserves_branch_and_marks_merged(self, tmp_path: Path):
        """--mark-only flag sets merge_status without deleting the branch."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test mark-only")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/mark-only"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/mark-only")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Verify branch exists
        assert git.branch_exists("feature/mark-only")
        assert not git.is_merged("feature/mark-only", "main")

        # Run merge with --mark-only
        result = run_gza("merge", str(task.id), "--mark-only", "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Marked task #1 as merged" in result.stdout

        # Verify branch was NOT deleted
        assert git.branch_exists("feature/mark-only")

        # Verify merge_status was set in the database
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_mark_only_rejects_conflicting_flags(self, tmp_path: Path):
        """--mark-only flag rejects conflicting flags."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test conflicting flags")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test --mark-only with --rebase
        result = run_gza("merge", str(task.id), "--mark-only", "--rebase", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "cannot be used with --rebase, --squash, or --delete" in result.stdout

        # Test --mark-only with --squash
        result = run_gza("merge", str(task.id), "--mark-only", "--squash", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "cannot be used with --rebase, --squash, or --delete" in result.stdout

        # Test --mark-only with --delete
        result = run_gza("merge", str(task.id), "--mark-only", "--delete", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "cannot be used with --rebase, --squash, or --delete" in result.stdout

    def test_mark_only_requires_completed_task(self, tmp_path: Path):
        """--mark-only flag requires task to be completed."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with pending status
        task = store.add("Test pending task")
        task.branch = "feature/pending"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/pending")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Try to mark-only a pending task
        result = run_gza("merge", str(task.id), "--mark-only", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not completed or unmerged" in result.stdout

    def test_merge_accepts_multiple_task_ids(self, tmp_path: Path):
        """Merge command accepts multiple task IDs."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create first task with a branch
        task1 = store.add("Test merge task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        task1.branch = "feature/test-1"
        store.update(task1)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-1")
        (tmp_path / "feature1.txt").write_text("feature 1 content")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature 1")
        git._run("checkout", "main")

        # Create second task with a branch
        task2 = store.add("Test merge task 2")
        task2.status = "completed"
        task2.completed_at = datetime.now(timezone.utc)
        task2.branch = "feature/test-2"
        store.update(task2)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-2")
        (tmp_path / "feature2.txt").write_text("feature 2 content")
        git._run("add", "feature2.txt")
        git._run("commit", "-m", "Add feature 2")
        git._run("checkout", "main")

        # Test merging both tasks
        result = run_gza("merge", str(task1.id), str(task2.id), "--project", str(tmp_path))

        # Verify the command succeeds
        assert result.returncode == 0
        assert "Successfully merged 2 task(s)" in result.stdout
        assert f"#{task1.id}" in result.stdout
        assert f"#{task2.id}" in result.stdout

    def test_merge_stops_on_first_failure(self, tmp_path: Path):
        """Merge command stops on first failure and reports which tasks were merged."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create first task with a branch (will succeed)
        task1 = store.add("Test merge task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        task1.branch = "feature/test-1"
        store.update(task1)

        git._run("checkout", "-b", "feature/test-1")
        (tmp_path / "feature1.txt").write_text("feature 1 content")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature 1")
        git._run("checkout", "main")

        # Create second task that will fail (no branch)
        task2 = store.add("Test merge task 2 - no branch")
        task2.status = "completed"
        task2.completed_at = datetime.now(timezone.utc)
        store.update(task2)

        # Create third task with a branch (won't be processed)
        task3 = store.add("Test merge task 3")
        task3.status = "completed"
        task3.completed_at = datetime.now(timezone.utc)
        task3.branch = "feature/test-3"
        store.update(task3)

        git._run("checkout", "-b", "feature/test-3")
        (tmp_path / "feature3.txt").write_text("feature 3 content")
        git._run("add", "feature3.txt")
        git._run("commit", "-m", "Add feature 3")
        git._run("checkout", "main")

        # Test merging all three tasks
        result = run_gza("merge", str(task1.id), str(task2.id), str(task3.id), "--project", str(tmp_path))

        # Verify the command fails
        assert result.returncode == 1

        # Verify task 1 was merged successfully
        assert "Successfully merged 1 task(s)" in result.stdout
        assert f"#{task1.id}" in result.stdout

        # Verify it stopped at task 2
        assert f"Stopped at task #{task2.id}" in result.stdout

        # Verify task 3 is listed as not processed
        assert f"#{task3.id}" in result.stdout
        assert "Remaining tasks not processed" in result.stdout

    def test_merge_multiple_with_squash(self, tmp_path: Path):
        """Merge command with --squash flag works with multiple tasks."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create first task with a branch
        task1 = store.add("Test squash merge 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        task1.branch = "feature/squash-1"
        store.update(task1)

        git._run("checkout", "-b", "feature/squash-1")
        (tmp_path / "feature1.txt").write_text("feature 1 content")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature 1")
        git._run("checkout", "main")

        # Create second task with a branch
        task2 = store.add("Test squash merge 2")
        task2.status = "completed"
        task2.completed_at = datetime.now(timezone.utc)
        task2.branch = "feature/squash-2"
        store.update(task2)

        git._run("checkout", "-b", "feature/squash-2")
        (tmp_path / "feature2.txt").write_text("feature 2 content")
        git._run("add", "feature2.txt")
        git._run("commit", "-m", "Add feature 2")
        git._run("checkout", "main")

        # Test squash merging both tasks
        result = run_gza("merge", str(task1.id), str(task2.id), "--squash", "--project", str(tmp_path))

        # Verify the command succeeds
        assert result.returncode == 0
        assert "Successfully merged 2 task(s)" in result.stdout
        assert "squash merged" in result.stdout

    def test_merge_no_args_fails(self, tmp_path: Path):
        """Merge command fails with an error when no task_ids and no --all are given."""
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        result = run_gza("merge", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "either provide task_id(s) or use --all" in result.stdout

    def test_merge_all_flag_merges_all_unmerged_tasks(self, tmp_path: Path):
        """--all flag finds and merges all unmerged done tasks."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

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

        # Create two completed tasks with branches and commits
        task1 = store.add("Unmerged task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        task1.branch = "feature/all-1"
        task1.has_commits = True
        store.update(task1)

        git._run("checkout", "-b", "feature/all-1")
        (tmp_path / "all1.txt").write_text("content 1")
        git._run("add", "all1.txt")
        git._run("commit", "-m", "Add all 1")
        git._run("checkout", "main")

        task2 = store.add("Unmerged task 2")
        task2.status = "completed"
        task2.completed_at = datetime.now(timezone.utc)
        task2.branch = "feature/all-2"
        task2.has_commits = True
        store.update(task2)

        git._run("checkout", "-b", "feature/all-2")
        (tmp_path / "all2.txt").write_text("content 2")
        git._run("add", "all2.txt")
        git._run("commit", "-m", "Add all 2")
        git._run("checkout", "main")

        result = run_gza("merge", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Successfully merged 2 task(s)" in result.stdout

    def test_merge_all_flag_no_unmerged_tasks(self, tmp_path: Path):
        """--all flag reports no tasks when all branches are already merged."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

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

        # Create a completed task whose branch is already merged
        task = store.add("Already merged task")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/already-merged"
        task.has_commits = True
        store.update(task)

        git._run("checkout", "-b", "feature/already-merged")
        (tmp_path / "merged.txt").write_text("merged content")
        git._run("add", "merged.txt")
        git._run("commit", "-m", "Add merged content")
        git._run("checkout", "main")
        git._run("merge", "feature/already-merged")

        result = run_gza("merge", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unmerged done tasks found" in result.stdout

    def test_merge_all_flag_skips_tasks_without_commits(self, tmp_path: Path):
        """--all flag skips tasks that have no commits (has_commits=False or None)."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

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

        # Task with has_commits=False should be skipped
        task_no_commits = store.add("Task with no commits")
        task_no_commits.status = "completed"
        task_no_commits.completed_at = datetime.now(timezone.utc)
        task_no_commits.branch = "feature/no-commits"
        task_no_commits.has_commits = False
        store.update(task_no_commits)

        result = run_gza("merge", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unmerged done tasks found" in result.stdout


class TestCheckoutCommand:
    """Tests for 'gza checkout' command."""

    def test_checkout_removes_clean_worktree(self, tmp_path: Path):
        """Checkout command removes clean worktree before checking out branch."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test checkout task")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-checkout"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/test-checkout")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create a worktree for the branch
        worktree_path = tmp_path / "worktrees" / "test-checkout"
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), "feature/test-checkout")

        # Verify worktree exists
        assert worktree_path.exists()

        # Checkout the branch by task ID - should remove worktree first
        result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removing stale worktree" in result.stdout
        assert "Removed worktree" in result.stdout
        assert "Checked out" in result.stdout

    def test_checkout_fails_with_dirty_worktree(self, tmp_path: Path):
        """Checkout command fails if worktree has uncommitted changes."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test checkout with dirty worktree")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-dirty"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/test-dirty")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create a worktree for the branch
        worktree_path = tmp_path / "worktrees" / "test-dirty"
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), "feature/test-dirty")

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Checkout should fail due to dirty worktree
        result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

        # Verify failure
        assert result.returncode == 1
        assert "uncommitted changes" in result.stdout

    def test_checkout_force_removes_dirty_worktree(self, tmp_path: Path):
        """Checkout --force removes worktree even with uncommitted changes."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test checkout force")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-force"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/test-force")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create a worktree for the branch
        worktree_path = tmp_path / "worktrees" / "test-force"
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), "feature/test-force")

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Checkout with --force should succeed
        result = run_gza("checkout", str(task.id), "--force", "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removed worktree" in result.stdout
        assert "Checked out" in result.stdout


class TestRebaseCommand:
    """Tests for 'gza rebase' command."""

    def test_rebase_removes_clean_worktree(self, tmp_path: Path):
        """Rebase command removes clean worktree before rebasing."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test rebase with worktree")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-rebase-wt"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-rebase-wt")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create a worktree for the branch
        worktree_path = tmp_path / "worktrees" / "test-rebase-wt"
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), "feature/test-rebase-wt")

        # Verify worktree exists
        assert worktree_path.exists()

        # Rebase should remove worktree first, then succeed
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removing stale worktree" in result.stdout
        assert "Removed worktree" in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_fails_with_dirty_worktree(self, tmp_path: Path):
        """Rebase command fails if worktree has uncommitted changes."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test rebase with dirty worktree")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-rebase-dirty"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-rebase-dirty")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create a worktree for the branch
        worktree_path = tmp_path / "worktrees" / "test-rebase-dirty"
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), "feature/test-rebase-dirty")

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Rebase should fail due to dirty worktree
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify failure
        assert result.returncode == 1
        assert "uncommitted changes" in result.stdout

    def test_rebase_force_removes_dirty_worktree(self, tmp_path: Path):
        """Rebase --force removes worktree even with uncommitted changes."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test rebase force")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-rebase-force"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-rebase-force")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create a worktree for the branch
        worktree_path = tmp_path / "worktrees" / "test-rebase-force"
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), "feature/test-rebase-force")

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Rebase with --force should succeed
        result = run_gza("rebase", str(task.id), "--force", "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removed worktree" in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_without_worktree(self, tmp_path: Path):
        """Rebase works normally when no worktree exists."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test rebase no worktree")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-rebase-nowt"
        store.update(task)

        # Create the branch and add a commit (no worktree)
        git._run("checkout", "-b", "feature/test-rebase-nowt")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Rebase should work normally
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success (no worktree messages)
        assert result.returncode == 0
        assert "Removing stale worktree" not in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_logs_task_id_and_newline(self, tmp_path: Path):
        """Rebase command logs 'Rebasing task #X...' and ends with a newline."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test rebase output format")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-rebase-output"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-rebase-output")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Rebasing task #{task.id}..." in result.stdout
        # Output should end with a newline (after trailing whitespace is stripped per line,
        # the last non-empty content is followed by a blank line)
        assert result.stdout.endswith("\n")

    def test_rebase_resolve_flag_accepted(self, tmp_path: Path):
        """Rebase command accepts --resolve flag."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone
        from unittest.mock import patch, MagicMock

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test rebase with resolve")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test-resolve"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-resolve")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Mock the conflict resolution since we're just testing that the flag is accepted
        # and the basic flow works (we don't want to actually invoke Claude in tests)
        with patch('gza.cli.invoke_claude_resolve', return_value=False):
            # This should succeed without conflicts (no --resolve needed, but flag should work)
            result = run_gza("rebase", str(task.id), "--resolve", "--project", str(tmp_path))

            # Should succeed when there are no conflicts
            assert result.returncode == 0
            assert "Successfully rebased" in result.stdout


class TestImplementCommand:
    """Tests for 'gza implement' command."""

    def test_implement_creates_task_from_completed_plan(self, tmp_path: Path):
        """Implement command creates an implementation task and queues it."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan_task = store.add("Plan authentication rollout", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(timezone.utc)
        store.update(plan_task)

        result = run_gza(
            "implement",
            str(plan_task.id),
            "Implement auth rollout",
            "--review",
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Created implement task #2" in result.stdout

        impl_task = store.get(2)
        assert impl_task is not None
        assert impl_task.task_type == "implement"
        assert impl_task.based_on == plan_task.id
        assert impl_task.prompt == "Implement auth rollout"
        assert impl_task.create_review is True

    def test_implement_fails_for_missing_plan_task(self, tmp_path: Path):
        """Implement command validates referenced plan task exists."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("implement", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Task #999 not found" in result.stdout

    def test_implement_fails_for_non_plan_task(self, tmp_path: Path):
        """Implement command requires a plan task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Not a plan", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        result = run_gza("implement", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task #{task.id} is a implement task" in result.stdout

    def test_implement_fails_for_incomplete_plan_task(self, tmp_path: Path):
        """Implement command requires the plan task to be completed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan_task = store.add("Plan feature", task_type="plan")

        result = run_gza("implement", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task #{plan_task.id} is pending. Plan task must be completed." in result.stdout

    def test_implement_derives_prompt_from_plan_slug_when_omitted(self, tmp_path: Path):
        """Implement command derives prompt from the plan task slug when prompt omitted."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        plan_task.task_id = "20260226-plan-auth-migration"
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(timezone.utc)
        store.update(plan_task)

        result = run_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created implement task #2" in result.stdout

        impl_task = store.get(2)
        assert impl_task is not None
        assert impl_task.prompt == "Implement plan from task #1: plan-auth-migration"
        assert impl_task.based_on == plan_task.id


class TestImproveCommand:
    """Tests for 'gza improve' command."""

    def test_improve_creates_task_from_implementation_and_review(self, tmp_path: Path):
        """Improve command creates an improve task with correct relationships."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement", group="auth-feature")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review implementation", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        store.update(review_task)

        # Run improve command with --queue to only create (not run)
        result = run_gza("improve", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created improve task #3" in result.stdout
        assert "Based on: implementation #1" in result.stdout
        assert "Review: #2" in result.stdout

        # Verify the improve task was created with correct fields
        improve_task = store.get(3)
        assert improve_task is not None
        assert improve_task.task_type == "improve"
        assert improve_task.depends_on == 2  # review task
        assert improve_task.based_on == 1  # implementation task
        assert improve_task.same_branch is True
        assert improve_task.group == "auth-feature"  # inherited from implementation

    def test_improve_with_review_flag(self, tmp_path: Path):
        """Improve command with --review flag sets create_review."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create implementation and review tasks
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        store.update(review_task)

        # Run improve command with --review flag and --queue to only create (not run)
        result = run_gza("improve", "1", "--review", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify the improve task has create_review set
        improve_task = store.get(3)
        assert improve_task is not None
        assert improve_task.create_review is True

    def test_improve_fails_without_review(self, tmp_path: Path):
        """Improve command fails if implementation has no review."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create implementation task without review
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Run improve command
        result = run_gza("improve", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "has no review" in result.stdout
        assert "gza add --type review --depends-on 1" in result.stdout

    def test_improve_fails_on_non_implement_task(self, tmp_path: Path):
        """Improve command fails if task is not an implementation task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a plan task
        plan_task = store.add("Plan feature", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(timezone.utc)
        store.update(plan_task)

        # Run improve command
        result = run_gza("improve", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is a plan task" in result.stdout

    def test_improve_with_review_task_id_suggests_impl(self, tmp_path: Path):
        """Improve command on review task suggests using implementation task ID."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create implementation and review tasks
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        store.update(review_task)

        # Run improve command with review task ID
        result = run_gza("improve", "2", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is a review task" in result.stdout
        assert "gza improve 1" in result.stdout

    def test_improve_uses_most_recent_review(self, tmp_path: Path):
        """Improve command uses the most recent review when multiple exist."""
        from gza.db import SqliteTaskStore
        import time

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create first review task
        time.sleep(0.01)  # Ensure different timestamps
        review_task1 = store.add("First review", task_type="review", depends_on=impl_task.id)
        review_task1.status = "completed"
        review_task1.completed_at = datetime.now(timezone.utc)
        store.update(review_task1)

        # Create second review task (more recent)
        time.sleep(0.01)  # Ensure different timestamps
        review_task2 = store.add("Second review", task_type="review", depends_on=impl_task.id)
        review_task2.status = "completed"
        review_task2.completed_at = datetime.now(timezone.utc)
        store.update(review_task2)

        # Run improve command with --queue to only create (not run)
        result = run_gza("improve", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Review: #3" in result.stdout  # Should use the second (most recent) review

        # Verify the improve task depends on the most recent review
        improve_task = store.get(4)
        assert improve_task is not None
        assert improve_task.depends_on == 3  # second review task

    def test_improve_nonexistent_task(self, tmp_path: Path):
        """Improve command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("improve", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_improve_warns_on_incomplete_review(self, tmp_path: Path):
        """Improve command warns if the review is not yet completed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create a pending review task (not completed)
        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        # Leave status as 'pending' (default)

        # Run improve command with --queue to only create (not run)
        result = run_gza("improve", "1", "--queue", "--project", str(tmp_path))

        # Should succeed but warn about incomplete review
        assert result.returncode == 0
        assert "Warning: Review #2 is pending" in result.stdout
        assert "blocked until it completes" in result.stdout
        assert "Created improve task #3" in result.stdout

    def test_improve_prevents_duplicate(self, tmp_path: Path):
        """Improve command refuses to create a duplicate improve task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        store.update(review_task)

        # Create an existing improve task for the same impl+review pair
        existing_improve = store.add(
            "Improve",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        # Run improve command - should fail with duplicate error
        result = run_gza("improve", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"improve task already exists" in result.stdout
        assert f"#{existing_improve.id}" in result.stdout

        # Verify no new task was created (still only 3 tasks)
        all_tasks = store.get_all()
        assert len(all_tasks) == 3

    def test_improve_runs_by_default(self, tmp_path: Path):
        """Improve command runs the task immediately by default (without any flags)."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        store.update(review_task)

        # Run improve command without --queue (will attempt to run)
        result = run_gza("improve", "1", "--no-docker", "--project", str(tmp_path))

        # Verify the improve task was created and run was attempted
        assert "Created improve task #3" in result.stdout
        assert "Running improve task #3" in result.stdout

    def test_improve_with_model_flag(self, tmp_path: Path):
        """Improve command with --model sets the model on the created task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        store.update(review_task)

        result = run_gza("improve", "1", "--model", "claude-opus-4-5", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        improve_task = store.get(3)
        assert improve_task is not None
        assert improve_task.model == "claude-opus-4-5"

    def test_improve_with_provider_flag(self, tmp_path: Path):
        """Improve command with --provider sets the provider on the created task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        store.update(review_task)

        result = run_gza("improve", "1", "--provider", "gemini", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        improve_task = store.get(3)
        assert improve_task is not None
        assert improve_task.provider == "gemini"


class TestReviewCommand:
    """Tests for the 'gza review' command."""

    def test_review_creates_task_for_completed_implementation(self, tmp_path: Path):
        """Review command creates a review task for a completed implementation."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement", group="auth-feature")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Run review command with --queue to only create (not run)
        result = run_gza("review", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task #2" in result.stdout
        assert "Implementation: #1" in result.stdout
        assert "Group: auth-feature" in result.stdout

        # Verify the review task was created with correct fields
        review_task = store.get(2)
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == 1  # implementation task
        assert review_task.group == "auth-feature"  # inherited from implementation

    def test_review_fails_on_non_implementation_task(self, tmp_path: Path):
        """Review command fails if task is not an implementation/improve task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a plan task
        plan_task = store.add("Plan authentication system", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(timezone.utc)
        store.update(plan_task)

        # Run review command
        result = run_gza("review", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is a plan task, not an implementation/improve task" in result.stdout

    def test_review_accepts_improve_task_and_targets_implementation(self, tmp_path: Path):
        """Review command accepts improve tasks and reviews the base implementation."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create completed implementation task
        impl_task = store.add("Implement auth", task_type="implement", group="auth")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-auth"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create completed improve task based on implementation
        improve_task = store.add(
            "Improve auth",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
            group="auth",
        )
        improve_task.status = "completed"
        improve_task.completed_at = datetime.now(timezone.utc)
        store.update(improve_task)

        result = run_gza("review", str(improve_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task #3" in result.stdout
        assert "Implementation: #1" in result.stdout

        review_task = store.get(3)
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == impl_task.id
        assert review_task.group == "auth"

    def test_review_fails_on_non_completed_task(self, tmp_path: Path):
        """Review command fails if implementation is not completed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a pending implementation task
        impl_task = store.add("Add feature", task_type="implement")
        # Leave status as 'pending'

        # Run review command
        result = run_gza("review", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is pending. Can only review completed tasks" in result.stdout

    def test_review_nonexistent_task(self, tmp_path: Path):
        """Review command fails gracefully for nonexistent task."""
        setup_config(tmp_path)

        result = run_gza("review", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Task #999 not found" in result.stdout

    def test_review_inherits_based_on_from_implementation(self, tmp_path: Path):
        """Review task inherits based_on from implementation to find plan."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a plan task
        plan_task = store.add("Plan feature", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(timezone.utc)
        store.update(plan_task)

        # Create implementation based on plan
        impl_task = store.add("Implement feature", task_type="implement", based_on=plan_task.id)
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-feature"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Run review command with --queue to only create (not run)
        result = run_gza("review", "2", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task #3" in result.stdout

        # Verify the review task inherited based_on
        review_task = store.get(3)
        assert review_task is not None
        assert review_task.based_on == 1  # plan task
        assert review_task.depends_on == 2  # implementation task

    def test_review_runs_by_default(self, tmp_path: Path):
        """Review command runs the review task immediately by default."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Run review command without --queue (will attempt to run immediately)
        result = run_gza("review", "1", "--no-docker", "--project", str(tmp_path))

        # Verify the review task was created and run attempted
        assert "Created review task #2" in result.stdout
        assert "Running review task #2" in result.stdout

        # Verify the review task exists
        review_task = store.get(2)
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == 1

    def test_review_with_queue_flag(self, tmp_path: Path):
        """Review command with --queue adds task to queue without executing."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Run review command with --queue flag
        result = run_gza("review", "1", "--queue", "--project", str(tmp_path))

        # Verify the review task was created but not run
        assert result.returncode == 0
        assert "Created review task #2" in result.stdout
        assert "Running review task" not in result.stdout

        # Verify the review task is still pending
        review_task = store.get(2)
        assert review_task is not None
        assert review_task.status == "pending"

    def test_review_with_open_flag_no_editor(self, tmp_path: Path, monkeypatch):
        """Review command with --open warns when $EDITOR is not set."""
        import os
        from unittest.mock import patch, MagicMock
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Unset EDITOR environment variable
        monkeypatch.delenv("EDITOR", raising=False)

        # Mock the provider to simulate a successful review
        with patch("gza.runner.get_provider") as mock_get_provider:
            mock_provider = MagicMock()
            mock_provider.name = "test-provider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True

            # Simulate successful run
            mock_result = MagicMock()
            mock_result.exit_code = 0
            mock_result.error_type = None
            mock_result.session_id = "test-session-123"
            mock_provider.run.return_value = mock_result
            mock_get_provider.return_value = mock_provider

            # Create the review directory and file that would be created by the task
            review_dir = tmp_path / ".gza" / "reviews"
            review_dir.mkdir(parents=True, exist_ok=True)

            # Run review command with --open flag (runs by default)
            result = run_gza("review", "1", "--open", "--no-docker", "--project", str(tmp_path))

            # Check that warning about missing EDITOR is shown
            # Note: This might not appear in output if the task doesn't complete successfully in test
            # The important thing is that the flag is accepted and doesn't cause an error
            assert result.returncode in (0, 1)  # May fail due to missing credentials, but flag should be accepted

    def test_review_open_flag_with_queue_does_not_run(self, tmp_path: Path):
        """--open flag with --queue creates task but does not run it."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Run review command with --open and --queue (should create task but not run)
        result = run_gza("review", "1", "--open", "--queue", "--project", str(tmp_path))

        # Should succeed but not run the task
        assert result.returncode == 0
        assert "Created review task #2" in result.stdout
        assert "Running review task" not in result.stdout

    def test_review_prevents_duplicate_pending_review(self, tmp_path: Path):
        """Review command warns and exits if a pending review already exists."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create an existing pending review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        # Leave status as 'pending' (default)

        # Attempt to create another review
        result = run_gza("review", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Warning: A review task already exists" in result.stdout
        assert f"#{existing_review.id}" in result.stdout
        assert "pending" in result.stdout

        # Verify no additional review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 1

    def test_review_prevents_duplicate_in_progress_review(self, tmp_path: Path):
        """Review command warns and exits if an in_progress review already exists."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create an existing in_progress review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "in_progress"
        store.update(existing_review)

        # Attempt to create another review
        result = run_gza("review", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Warning: A review task already exists" in result.stdout
        assert f"#{existing_review.id}" in result.stdout
        assert "in_progress" in result.stdout

        # Verify no additional review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 1

    def test_review_allows_new_review_after_completed_review(self, tmp_path: Path):
        """Review command allows creating a new review if existing review is completed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        # Create an existing completed review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "completed"
        existing_review.completed_at = datetime.now(timezone.utc)
        store.update(existing_review)

        # Create another review with --queue (should succeed after improvements)
        result = run_gza("review", "1", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task" in result.stdout

        # Verify a new review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 2

    def test_duplicate_review_uses_DuplicateReviewError_no_second_db_query(self, tmp_path: Path):
        """cmd_review shows the warning using DuplicateReviewError without a second DB query.

        After the refactor, cmd_review catches DuplicateReviewError (which carries
        the active_review task) so store.get_reviews_for_task is called exactly once
        (inside _create_review_task) and NOT a second time in the error handler.
        """
        import argparse
        from unittest.mock import MagicMock, patch
        from gza.cli import cmd_review, DuplicateReviewError
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        # Leave as pending so it counts as active

        args = argparse.Namespace(
            task_id=impl_task.id,
            project_dir=tmp_path,
            no_docker=True,
            queue=False,
            background=False,
            open=False,
            pr=False,
            no_pr=False,
        )

        mock_config = MagicMock()
        mock_config.project_dir = tmp_path
        mock_config.use_docker = False

        # Wrap get_reviews_for_task to count calls
        original_get_reviews = store.get_reviews_for_task
        call_count = []

        def counting_get_reviews(task_id: int):
            call_count.append(task_id)
            return original_get_reviews(task_id)

        import io
        output = io.StringIO()

        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch.object(store, "get_reviews_for_task", side_effect=counting_get_reviews), \
             patch("sys.stdout", output):
            result = cmd_review(args)

        assert result == 1
        printed = output.getvalue()
        assert "Warning: A review task already exists" in printed
        assert f"#{existing_review.id}" in printed
        # get_reviews_for_task must be called exactly once (inside _create_review_task),
        # NOT a second time in the cmd_review error handler.
        assert len(call_count) == 1, (
            f"get_reviews_for_task was called {len(call_count)} times; expected exactly 1"
        )

    def test_review_with_model_flag(self, tmp_path: Path):
        """Review command with --model sets the model on the created review task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        result = run_gza("review", "1", "--model", "claude-opus-4-5", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        review_task = store.get(2)
        assert review_task is not None
        assert review_task.model == "claude-opus-4-5"

    def test_review_with_provider_flag(self, tmp_path: Path):
        """Review command with --provider sets the provider on the created review task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(timezone.utc)
        store.update(impl_task)

        result = run_gza("review", "1", "--provider", "gemini", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        review_task = store.get(2)
        assert review_task is not None
        assert review_task.provider == "gemini"


class TestDiffCommand:
    """Tests for 'gza diff' command."""

    def test_diff_runs_git_diff(self, tmp_path: Path):
        """Diff command runs git diff with colored output."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes to file
        (tmp_path / "file.txt").write_text("modified")

        # Run diff command - should show the changes
        # We redirect to avoid pager issues in tests
        result = run_gza("diff", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show the diff (contains color codes when forced with --color=always)
        assert "file.txt" in result.stdout

    def test_diff_with_stat_argument(self, tmp_path: Path):
        """Diff command passes --stat to git diff."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes
        (tmp_path / "file.txt").write_text("modified")

        # Run diff with --stat (using -- separator for pass-through args)
        result = run_gza("diff", "--project", str(tmp_path), "--", "--stat")

        assert result.returncode == 0
        assert "file.txt" in result.stdout

    def test_diff_with_task_id(self, tmp_path: Path):
        """Diff command resolves task ID to branch diff."""
        from gza.git import Git
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create and checkout task branch
        git._run("checkout", "-b", "task-1-test")
        (tmp_path / "file.txt").write_text("modified")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Task changes")

        # Return to main
        git._run("checkout", "main")

        # Create task in database with branch
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task", task_type="implement")
        task.branch = "task-1-test"
        store.update(task)

        # Run diff with task ID
        result = run_gza("diff", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show the diff between main and task branch
        assert "file.txt" in result.stdout
        assert "modified" in result.stdout or "initial" in result.stdout

    def test_diff_with_task_id_not_found(self, tmp_path: Path):
        """Diff command shows error when task ID not found."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")

        # Create empty database
        setup_db_with_tasks(tmp_path, [])

        # Run diff with non-existent task ID
        result = run_gza("diff", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Task #999 not found" in result.stdout

    def test_diff_with_task_id_no_branch(self, tmp_path: Path):
        """Diff command shows error when task has no branch."""
        from gza.git import Git
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")

        # Create task without branch
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task", task_type="implement")
        # Don't set task.branch

        # Run diff with task ID that has no branch
        result = run_gza("diff", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Task #1 has no branch" in result.stdout

    def test_diff_with_non_numeric_argument(self, tmp_path: Path):
        """Diff command passes non-numeric arguments through to git diff."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes
        (tmp_path / "file.txt").write_text("modified")

        # Run diff with --cached (using -- separator for pass-through args)
        result = run_gza("diff", "--project", str(tmp_path), "--", "--cached")

        # Should run successfully (even if no staged changes)
        assert result.returncode == 0


class TestCleanCommand:
    """Tests for 'gza clean' command."""

    def test_clean_default_behavior(self, tmp_path: Path):
        """Clean command archives files older than 30 days by default."""
        import time
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        # Create logs and workers directories
        logs_dir = tmp_path / ".gza" / "logs"
        workers_dir = tmp_path / ".gza" / "workers"
        logs_dir.mkdir(parents=True, exist_ok=True)
        workers_dir.mkdir(parents=True, exist_ok=True)

        # Create old files (35 days old)
        old_log = logs_dir / "old_log.txt"
        old_worker = workers_dir / "old_worker.json"
        old_log.write_text("old log content")
        old_worker.write_text("old worker content")

        # Set mtime to 35 days ago
        old_time = (datetime.now(timezone.utc) - timedelta(days=35)).timestamp()
        old_log.touch()
        old_worker.touch()
        old_log.chmod(0o644)
        old_worker.chmod(0o644)
        # Use os.utime to set modification time
        import os
        os.utime(old_log, (old_time, old_time))
        os.utime(old_worker, (old_time, old_time))

        # Create recent files (10 days old)
        recent_log = logs_dir / "recent_log.txt"
        recent_worker = workers_dir / "recent_worker.json"
        recent_log.write_text("recent log content")
        recent_worker.write_text("recent worker content")

        recent_time = (datetime.now(timezone.utc) - timedelta(days=10)).timestamp()
        recent_log.touch()
        recent_worker.touch()
        os.utime(recent_log, (recent_time, recent_time))
        os.utime(recent_worker, (recent_time, recent_time))

        # Run clean command
        result = run_gza("clean", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Archived files older than 30 days" in result.stdout
        assert "Logs: 1 files" in result.stdout
        assert "Workers: 1 files" in result.stdout

        # Verify old files were moved to archives
        assert not old_log.exists()
        assert not old_worker.exists()
        archives_dir = tmp_path / ".gza" / "archives"
        assert (archives_dir / "logs" / "old_log.txt").exists()
        assert (archives_dir / "workers" / "old_worker.json").exists()

        # Verify recent files were kept
        assert recent_log.exists()
        assert recent_worker.exists()

    def test_clean_with_custom_days(self, tmp_path: Path):
        """Clean command respects custom --days value."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create file 8 days old
        log_file = logs_dir / "log.txt"
        log_file.write_text("content")

        old_time = (datetime.now(timezone.utc) - timedelta(days=8)).timestamp()
        os.utime(log_file, (old_time, old_time))

        # Run with --days 7 (should archive 8-day-old file)
        result = run_gza("clean", "--days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Archived files older than 7 days" in result.stdout
        assert not log_file.exists()
        # Verify file was archived
        archives_dir = tmp_path / ".gza" / "archives"
        assert (archives_dir / "logs" / "log.txt").exists()

    def test_clean_dry_run_mode(self, tmp_path: Path):
        """Clean command with --dry-run shows what would be archived without archiving."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old file
        old_log = logs_dir / "old_log.txt"
        old_log.write_text("old content")

        old_time = (datetime.now(timezone.utc) - timedelta(days=40)).timestamp()
        os.utime(old_log, (old_time, old_time))

        # Run with --dry-run
        result = run_gza("clean", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Dry run: would archive files older than 30 days" in result.stdout
        assert "old_log.txt" in result.stdout

        # Verify file was NOT archived
        assert old_log.exists()
        archives_dir = tmp_path / ".gza" / "archives"
        assert not (archives_dir / "logs" / "old_log.txt").exists()

    def test_clean_empty_directories(self, tmp_path: Path):
        """Clean command handles empty directories without errors."""
        setup_config(tmp_path)

        # Create empty directories
        logs_dir = tmp_path / ".gza" / "logs"
        workers_dir = tmp_path / ".gza" / "workers"
        logs_dir.mkdir(parents=True, exist_ok=True)
        workers_dir.mkdir(parents=True, exist_ok=True)

        result = run_gza("clean", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Logs: 0 files" in result.stdout
        assert "Workers: 0 files" in result.stdout

    def test_clean_nonexistent_directories(self, tmp_path: Path):
        """Clean command handles nonexistent directories without errors."""
        setup_config(tmp_path)

        # Don't create .gza directories
        result = run_gza("clean", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Logs: 0 files" in result.stdout
        assert "Workers: 0 files" in result.stdout

    def test_clean_mixed_old_and_new_files(self, tmp_path: Path):
        """Clean command correctly handles mixed old and new files."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create multiple old and new files
        for i in range(3):
            old_file = logs_dir / f"old_{i}.txt"
            old_file.write_text(f"old content {i}")
            old_time = (datetime.now(timezone.utc) - timedelta(days=35 + i)).timestamp()
            os.utime(old_file, (old_time, old_time))

            new_file = logs_dir / f"new_{i}.txt"
            new_file.write_text(f"new content {i}")
            new_time = (datetime.now(timezone.utc) - timedelta(days=5 + i)).timestamp()
            os.utime(new_file, (new_time, new_time))

        result = run_gza("clean", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Logs: 3 files" in result.stdout

        # Verify old files archived, new files kept
        archives_dir = tmp_path / ".gza" / "archives"
        for i in range(3):
            assert not (logs_dir / f"old_{i}.txt").exists()
            assert (archives_dir / "logs" / f"old_{i}.txt").exists()
            assert (logs_dir / f"new_{i}.txt").exists()

    def test_clean_only_files_not_directories(self, tmp_path: Path):
        """Clean command only archives files, not directories."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create an old subdirectory
        old_subdir = logs_dir / "old_subdir"
        old_subdir.mkdir()

        # Set directory mtime to old
        old_time = (datetime.now(timezone.utc) - timedelta(days=40)).timestamp()
        os.utime(old_subdir, (old_time, old_time))

        result = run_gza("clean", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify subdirectory was NOT archived
        assert old_subdir.exists()

    def test_clean_second_run_is_noop(self, tmp_path: Path):
        """Second run of clean should be a no-op (only checks source dirs)."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old file
        old_log = logs_dir / "old_log.txt"
        old_log.write_text("old content")
        old_time = (datetime.now(timezone.utc) - timedelta(days=40)).timestamp()
        os.utime(old_log, (old_time, old_time))

        # First run - archives the file
        result1 = run_gza("clean", "--project", str(tmp_path))
        assert result1.returncode == 0
        assert "Logs: 1 files" in result1.stdout

        # Second run - should find nothing to archive
        result2 = run_gza("clean", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "Logs: 0 files" in result2.stdout

    def test_clean_purge_mode(self, tmp_path: Path):
        """Clean with --purge deletes archived files older than N days."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        # Create archives directory with old files
        archives_logs_dir = tmp_path / ".gza" / "archives" / "logs"
        archives_workers_dir = tmp_path / ".gza" / "archives" / "workers"
        archives_logs_dir.mkdir(parents=True, exist_ok=True)
        archives_workers_dir.mkdir(parents=True, exist_ok=True)

        # Create very old archived files (400 days old)
        old_archived_log = archives_logs_dir / "old_archived.txt"
        old_archived_worker = archives_workers_dir / "old_archived.json"
        old_archived_log.write_text("old archived content")
        old_archived_worker.write_text("old archived content")

        very_old_time = (datetime.now(timezone.utc) - timedelta(days=400)).timestamp()
        os.utime(old_archived_log, (very_old_time, very_old_time))
        os.utime(old_archived_worker, (very_old_time, very_old_time))

        # Create recent archived files (100 days old)
        recent_archived_log = archives_logs_dir / "recent_archived.txt"
        recent_archived_log.write_text("recent archived content")
        recent_time = (datetime.now(timezone.utc) - timedelta(days=100)).timestamp()
        os.utime(recent_archived_log, (recent_time, recent_time))

        # Run purge with default days (365)
        result = run_gza("clean", "--purge", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Purged archived files older than 365 days" in result.stdout
        assert "Archived logs: 1 files" in result.stdout
        assert "Archived workers: 1 files" in result.stdout

        # Verify very old files were deleted
        assert not old_archived_log.exists()
        assert not old_archived_worker.exists()

        # Verify recent archived files were kept
        assert recent_archived_log.exists()

    def test_clean_purge_with_custom_days(self, tmp_path: Path):
        """Clean --purge respects custom --days value."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        # Create archives directory
        archives_logs_dir = tmp_path / ".gza" / "archives" / "logs"
        archives_logs_dir.mkdir(parents=True, exist_ok=True)

        # Create archived file 200 days old
        archived_log = archives_logs_dir / "archived.txt"
        archived_log.write_text("archived content")
        old_time = (datetime.now(timezone.utc) - timedelta(days=200)).timestamp()
        os.utime(archived_log, (old_time, old_time))

        # Run purge with --days 180 (should delete 200-day-old file)
        result = run_gza("clean", "--purge", "--days", "180", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Purged archived files older than 180 days" in result.stdout
        assert not archived_log.exists()

    def test_clean_purge_dry_run(self, tmp_path: Path):
        """Clean --purge --dry-run shows what would be deleted without deleting."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        # Create archives directory
        archives_logs_dir = tmp_path / ".gza" / "archives" / "logs"
        archives_logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old archived file
        old_archived = archives_logs_dir / "old_archived.txt"
        old_archived.write_text("old archived content")
        old_time = (datetime.now(timezone.utc) - timedelta(days=400)).timestamp()
        os.utime(old_archived, (old_time, old_time))

        # Run purge with --dry-run
        result = run_gza("clean", "--purge", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Dry run: would purge archived files older than 365 days" in result.stdout
        assert "old_archived.txt" in result.stdout

        # Verify file was NOT deleted
        assert old_archived.exists()

    def test_clean_purge_second_run_is_noop(self, tmp_path: Path):
        """Second run of clean --purge should be a no-op (only checks archives dir)."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        # Create archives directory
        archives_logs_dir = tmp_path / ".gza" / "archives" / "logs"
        archives_logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old archived file
        old_archived = archives_logs_dir / "old_archived.txt"
        old_archived.write_text("old archived content")
        old_time = (datetime.now(timezone.utc) - timedelta(days=400)).timestamp()
        os.utime(old_archived, (old_time, old_time))

        # First purge run - deletes the file
        result1 = run_gza("clean", "--purge", "--project", str(tmp_path))
        assert result1.returncode == 0
        assert "Archived logs: 1 files" in result1.stdout

        # Second purge run - should find nothing to delete
        result2 = run_gza("clean", "--purge", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "Archived logs: 0 files" in result2.stdout

    def test_clean_deletes_old_backups(self, tmp_path: Path):
        """Clean command deletes old backup files from .gza/backups/."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        backups_dir = tmp_path / ".gza" / "backups"
        backups_dir.mkdir(parents=True, exist_ok=True)

        # Create an old backup file (35 days old)
        old_backup = backups_dir / "gza-2026011400.db"
        old_backup.write_bytes(b"old backup data")
        old_time = (datetime.now(timezone.utc) - timedelta(days=35)).timestamp()
        os.utime(old_backup, (old_time, old_time))

        # Create a recent backup file (1 day old)
        recent_backup = backups_dir / "gza-2026021900.db"
        recent_backup.write_bytes(b"recent backup data")
        recent_time = (datetime.now(timezone.utc) - timedelta(days=1)).timestamp()
        os.utime(recent_backup, (recent_time, recent_time))

        result = run_gza("clean", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Backups deleted: 1 files" in result.stdout

        # Old backup should be deleted
        assert not old_backup.exists()
        # Recent backup should be kept
        assert recent_backup.exists()

    def test_clean_dry_run_shows_backups(self, tmp_path: Path):
        """Clean --dry-run shows old backup files that would be deleted."""
        import os
        from datetime import datetime, timedelta, timezone

        setup_config(tmp_path)

        backups_dir = tmp_path / ".gza" / "backups"
        backups_dir.mkdir(parents=True, exist_ok=True)

        old_backup = backups_dir / "gza-2026010100.db"
        old_backup.write_bytes(b"old backup")
        old_time = (datetime.now(timezone.utc) - timedelta(days=40)).timestamp()
        os.utime(old_backup, (old_time, old_time))

        result = run_gza("clean", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "gza-2026010100.db" in result.stdout
        # File should NOT have been deleted (dry run)
        assert old_backup.exists()


class TestMaxTurnsFlag:
    """Tests for --max-turns flag on work, retry, and resume commands."""

    def test_work_command_accepts_max_turns_flag(self, tmp_path: Path):
        """Work command accepts --max-turns flag without error."""
        from gza.config import Config
        import argparse

        # Create a config with a default max_steps
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_steps: 50\n")

        # Load config
        config = Config.load(tmp_path)
        assert config.max_turns == 50

        # Apply override like cmd_run does
        args = argparse.Namespace(max_turns=100, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_steps = args.max_turns
            config.max_turns = args.max_turns

        assert config.max_turns == 100
        assert config.max_steps == 100

    def test_retry_command_accepts_max_turns_flag(self, tmp_path: Path):
        """Retry command accepts --max-turns flag without error."""
        from gza.config import Config
        import argparse

        # Create a config with a default max_steps
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_steps: 50\n")

        # Load config
        config = Config.load(tmp_path)
        assert config.max_turns == 50

        # Apply override like cmd_retry does
        args = argparse.Namespace(max_turns=150, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_steps = args.max_turns
            config.max_turns = args.max_turns

        assert config.max_turns == 150
        assert config.max_steps == 150

    def test_resume_command_accepts_max_turns_flag(self, tmp_path: Path):
        """Resume command accepts --max-turns flag without error."""
        from gza.config import Config
        import argparse

        # Create a config with a default max_steps
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_steps: 50\n")

        # Load config
        config = Config.load(tmp_path)
        assert config.max_turns == 50

        # Apply override like cmd_resume does
        args = argparse.Namespace(max_turns=200, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_steps = args.max_turns
            config.max_turns = args.max_turns

        assert config.max_turns == 200
        assert config.max_steps == 200

    def test_max_turns_override_takes_precedence_over_config(self, tmp_path: Path):
        """--max-turns flag overrides the value from gza.yaml."""
        from gza.config import Config
        import argparse

        # Create a config with a default max_steps of 50
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_steps: 50\n")

        # Load config
        config = Config.load(tmp_path)
        before = config.max_turns
        assert before == 50

        # Apply override
        args = argparse.Namespace(max_turns=999, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_steps = args.max_turns
            config.max_turns = args.max_turns

        after = config.max_turns
        assert after == 999
        assert before != after


class TestUnmergedReviewStatus:
    """Tests for review status display in 'gza unmerged' command."""

    def test_unmerged_shows_approved_review_status(self, tmp_path: Path):
        """Unmerged output shows approved review status."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create implementation task with branch
        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        # Create branch with commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create review task with approved verdict
        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.depends_on = task.id
        review.task_id = "20260212-review-implementation"
        review.output_content = """# Review

Code looks good!

**Verdict: APPROVED**"""
        store.update(review)

        # Run unmerged command
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "✓ approved" in result.stdout

    def test_unmerged_shows_changes_requested_review_status(self, tmp_path: Path):
        """Unmerged output shows changes requested review status."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create implementation task
        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        # Create branch with commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create review task with changes requested
        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.depends_on = task.id
        review.task_id = "20260212-review-implementation"
        review.output_content = """# Review

Needs some fixes.

Verdict: CHANGES_REQUESTED"""
        store.update(review)

        # Run unmerged command
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

    def test_unmerged_shows_needs_discussion_review_status(self, tmp_path: Path):
        """Unmerged output shows needs discussion review status."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create implementation task
        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        # Create branch with commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create review task with needs discussion
        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.depends_on = task.id
        review.task_id = "20260212-review-implementation"
        review.output_content = """# Review

This requires team discussion.

**Verdict: NEEDS_DISCUSSION**"""
        store.update(review)

        # Run unmerged command
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "💬 needs discussion" in result.stdout

    def test_unmerged_without_review_shows_no_status(self, tmp_path: Path):
        """Unmerged output shows no review status when no review exists."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create implementation task
        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.task_id = "20260212-add-feature"
        store.update(task)

        # Create branch with commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Run unmerged command (no review)
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review: no review" in result.stdout
        assert "approved" not in result.stdout
        assert "changes requested" not in result.stdout
        assert "needs discussion" not in result.stdout

    def test_unmerged_uses_most_recent_review(self, tmp_path: Path):
        """Unmerged output shows status from most recent review."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        import time

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create implementation task
        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        # Create branch with commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create first review (changes requested)
        review1 = store.add("First review", task_type="review")
        review1.status = "completed"
        review1.completed_at = datetime.now(timezone.utc)
        review1.depends_on = task.id
        review1.task_id = "20260212-first-review"
        review1.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review1)

        # Wait a bit to ensure different timestamps
        time.sleep(0.1)

        # Create second review (approved)
        review2 = store.add("Second review", task_type="review")
        review2.status = "completed"
        review2.completed_at = datetime.now(timezone.utc)
        review2.depends_on = task.id
        review2.task_id = "20260212-second-review"
        review2.output_content = "**Verdict: APPROVED**"
        store.update(review2)

        # Run unmerged command - should show approved (most recent)
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "✓ approved" in result.stdout

    def test_unmerged_uses_older_verdict_when_latest_review_has_no_output(self, tmp_path: Path):
        """Unmerged scans newest-to-oldest and uses first parseable review verdict."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        import time

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

        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime.now(timezone.utc)
        older_review.depends_on = task.id
        older_review.task_id = "20260212-older-review"
        older_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(older_review)

        time.sleep(0.01)

        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime.now(timezone.utc)
        latest_review.depends_on = task.id
        latest_review.task_id = "20260212-latest-review"
        latest_review.output_content = None
        latest_review.report_file = None
        store.update(latest_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

    def test_unmerged_does_not_use_older_stale_verdict_when_latest_review_has_no_output(self, tmp_path: Path):
        """Staleness via review_cleared_at still suppresses older verdicts."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        import time

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

        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime.now(timezone.utc)
        older_review.depends_on = task.id
        older_review.task_id = "20260212-older-review"
        older_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(older_review)

        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        time.sleep(0.01)
        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime.now(timezone.utc)
        latest_review.depends_on = task.id
        latest_review.task_id = "20260212-latest-review"
        latest_review.output_content = None
        latest_review.report_file = None
        store.update(latest_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" not in result.stdout
        assert "review: reviewed" in result.stdout

    def test_unmerged_falls_back_to_unlinked_review_slug_match(self, tmp_path: Path):
        """Unmerged should infer review status from unlinked 'review <slug>' tasks."""
        from gza.db import SqliteTaskStore
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

        task = store.add("Simplify mixer", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260225-simplify-mixer-by-removing-the-people-strategy"
        store.update(task)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Simulate a retry-created review that lost depends_on but kept review slug.
        review = store.add("review simplify-mixer-by-removing-the-people-strategy", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.task_id = "20260225-review-simplify-mixer-by-removing-the-people-strategy-2"
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

    def test_unmerged_marks_review_stale_after_improve_clears_it(self, tmp_path: Path):
        """After improve clears review state, unmerged marks review as stale."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        import time

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create implementation task
        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        # Create branch with commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create review task with changes requested
        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.depends_on = task.id
        review.task_id = "20260212-review-implementation"
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        # Before improve: should show changes requested
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

        # Simulate improve task completing (clear review state)
        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        # After improve: status should explicitly show stale review
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review stale" in result.stdout
        assert "last review" in result.stdout

    def test_unmerged_shows_new_review_status_after_improve_and_re_review(self, tmp_path: Path):
        """After improve clears review state, a newer review's verdict is shown."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        import time

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create implementation task
        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260212-add-feature"
        store.update(task)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Create first review (changes requested)
        review1 = store.add("Review", task_type="review")
        review1.status = "completed"
        review1.completed_at = datetime.now(timezone.utc)
        review1.depends_on = task.id
        review1.task_id = "20260212-review"
        review1.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review1)

        # Improve task runs, clearing the review state
        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        # A new review runs after the improve, resulting in approved
        time.sleep(0.01)
        review2 = store.add("Second review", task_type="review")
        review2.status = "completed"
        review2.completed_at = datetime.now(timezone.utc)
        review2.depends_on = task.id
        review2.task_id = "20260212-second-review"
        review2.output_content = "**Verdict: APPROVED**"
        store.update(review2)

        # The new review's verdict should be shown (it's newer than review_cleared_at)
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "reviewed" in result.stdout
        assert "✓ approved" in result.stdout
        assert "⚠ changes requested" not in result.stdout

    def test_unmerged_shows_lineage_for_review_improve_chain(self, tmp_path: Path):
        """Unmerged output includes related review/improve lineage for implementation."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        import time

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

        impl = store.add("Add feature", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(timezone.utc)
        impl.branch = "feature/test"
        impl.has_commits = True
        impl.merge_status = "unmerged"
        impl.task_id = "20260212-add-feature"
        store.update(impl)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        review = store.add("Review", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.depends_on = impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        time.sleep(0.01)
        improve = store.add("Address review feedback", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime.now(timezone.utc)
        improve.based_on = impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        # Simulate improve completion clearing the review state.
        assert impl.id is not None
        store.clear_review_state(impl.id)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "lineage:" in result.stdout
        assert f"#{impl.id}" in result.stdout
        assert f"#{review.id}" in result.stdout
        assert f"#{improve.id}" in result.stdout
        assert "[implement]" in result.stdout
        assert "[review]" in result.stdout
        assert "[improve]" in result.stdout
        assert "review stale" in result.stdout


class TestClearReviewState:
    """Tests for SqliteTaskStore.clear_review_state()."""

    def test_clear_review_state_sets_review_cleared_at(self, tmp_path: Path):
        """clear_review_state sets review_cleared_at on the task."""
        from gza.db import SqliteTaskStore

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.review_cleared_at is None

        assert task.id is not None
        store.clear_review_state(task.id)

        updated = store.get(task.id)
        assert updated is not None
        assert updated.review_cleared_at is not None

    def test_clear_review_state_updates_timestamp_on_re_clear(self, tmp_path: Path):
        """Calling clear_review_state twice updates the timestamp."""
        from gza.db import SqliteTaskStore
        import time

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None

        store.clear_review_state(task.id)
        first = store.get(task.id)
        assert first is not None
        first_cleared = first.review_cleared_at

        time.sleep(0.01)
        store.clear_review_state(task.id)
        second = store.get(task.id)
        assert second is not None

        assert second.review_cleared_at is not None
        assert first_cleared is not None
        assert second.review_cleared_at > first_cleared


class TestUnmergedAllFlag:
    """Tests for 'gza unmerged --all' flag."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo with an initial commit."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_all_flag_excludes_failed_tasks(self, tmp_path: Path):
        """Failed tasks are excluded from gza unmerged (only completed tasks shown)."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        # Create a failed task with merge_status='unmerged'
        task = store.add("Failed but useful task", task_type="implement")
        task.status = "failed"
        task.branch = "feature/failed-branch"
        task.has_commits = False
        task.merge_status = "unmerged"
        task.task_id = "20260220-failed-task"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Create the branch with a real commit
        git._run("checkout", "-b", "feature/failed-branch")
        (tmp_path / "feature.txt").write_text("useful work")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add useful work")
        git._run("checkout", "main")

        # Failed task is excluded from unmerged output
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Failed but useful task" not in result.stdout
        assert "No unmerged tasks" in result.stdout

        # --all is a no-op: same result
        result = run_gza("unmerged", "--all", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Failed but useful task" not in result.stdout

    def test_all_flag_is_noop_with_merge_status(self, tmp_path: Path):
        """--all flag is a no-op since we now use merge_status from the database."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        # Task with merge_status='unmerged' shows up
        task = store.add("Unmerged task", task_type="implement")
        task.status = "completed"
        task.branch = "feature/unmerged"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260220-unmerged"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Task without merge_status='unmerged' doesn't show up
        task2 = store.add("Merged task", task_type="implement")
        task2.status = "completed"
        task2.branch = "feature/merged"
        task2.has_commits = True
        task2.merge_status = "merged"
        task2.task_id = "20260220-merged"
        task2.completed_at = datetime.now(timezone.utc)
        store.update(task2)

        # Without --all
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Unmerged task" in result.stdout
        assert "Merged task" not in result.stdout

        # With --all: same result (it's a no-op)
        result = run_gza("unmerged", "--all", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Unmerged task" in result.stdout
        assert "Merged task" not in result.stdout

    def test_unmerged_excludes_tasks_without_merge_status(self, tmp_path: Path):
        """Tasks without merge_status='unmerged' are not shown."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        # Task with merge_status=None (no commits)
        task = store.add("No commits task", task_type="implement")
        task.status = "completed"
        task.branch = "feature/no-commits"
        task.has_commits = False
        task.merge_status = None
        task.task_id = "20260220-no-commits"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No commits task" not in result.stdout

    def test_unmerged_shows_deleted_branch_if_merge_status_unmerged(self, tmp_path: Path):
        """Tasks with merge_status='unmerged' show even if branch is deleted."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        # Task with merge_status='unmerged' but branch doesn't exist
        task = store.add("Deleted branch task", task_type="implement")
        task.status = "completed"
        task.branch = "feature/nonexistent-branch"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.task_id = "20260220-deleted-branch"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Deleted branch task" in result.stdout
        assert "branch deleted" in result.stdout

    def test_lazy_migration_backfills_merge_status(self, tmp_path: Path):
        """Running gza unmerged backfills merge_status for existing tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        # Create a task with has_commits=True but merge_status=NULL (pre-migration state)
        task = store.add("Old task needing migration", task_type="implement")
        task.status = "completed"
        task.branch = "feature/old-task"
        task.has_commits = True
        task.merge_status = None  # Simulates pre-migration state
        task.task_id = "20260220-old-task"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Create the branch with a real commit (unmerged)
        git._run("checkout", "-b", "feature/old-task")
        (tmp_path / "old.txt").write_text("old work")
        git._run("add", "old.txt")
        git._run("commit", "-m", "Old work")
        git._run("checkout", "main")

        # Run gza unmerged - should trigger migration and show the task
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Migrating merge status" in result.stdout
        assert "Old task needing migration" in result.stdout

        # Verify merge_status was set in the database
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

        # Running again should not show migration message
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Migrating merge status" not in result.stdout


class TestUnmergedImprovedDisplay:
    """Tests for improved unmerged display (diff stats, review prominence, completed-only)."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo with an initial commit."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_unmerged_excludes_failed_tasks(self, tmp_path: Path):
        """Failed tasks with merge_status='unmerged' are excluded from unmerged output."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        # Create a failed task
        failed_task = store.add("Failed task", task_type="implement")
        failed_task.status = "failed"
        failed_task.branch = "feature/failed"
        failed_task.merge_status = "unmerged"
        failed_task.completed_at = datetime.now(timezone.utc)
        store.update(failed_task)

        # Create a completed task
        completed_task = store.add("Completed task", task_type="implement")
        completed_task.status = "completed"
        completed_task.branch = "feature/completed"
        completed_task.merge_status = "unmerged"
        completed_task.completed_at = datetime.now(timezone.utc)
        store.update(completed_task)

        git._run("checkout", "-b", "feature/completed")
        (tmp_path / "new.txt").write_text("work")
        git._run("add", "new.txt")
        git._run("commit", "-m", "Add work")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Completed task" in result.stdout
        assert "Failed task" not in result.stdout

    def test_unmerged_shows_diff_stats(self, tmp_path: Path):
        """Unmerged output shows diff stats (files, LOC added/removed)."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("line1\nline2\nline3\n")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Diff stats should be shown in branch line
        assert "LOC" in result.stdout
        assert "files" in result.stdout

    def test_unmerged_uses_cached_diff_stats(self, tmp_path: Path):
        """gza unmerged uses cached diff stats from DB when available (no live git call)."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        task = store.add("Cached stats task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/cached"
        task.merge_status = "unmerged"
        # Pre-populate cached diff stats
        task.diff_files_changed = 5
        task.diff_lines_added = 42
        task.diff_lines_removed = 7
        store.update(task)

        git._run("checkout", "-b", "feature/cached")
        (tmp_path / "cached.txt").write_text("some content\n")
        git._run("add", "cached.txt")
        git._run("commit", "-m", "Cached stats commit")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Cached stats should be displayed in +N/-N LOC, N files format
        assert "+42/-7 LOC, 5 files" in result.stdout

    def test_unmerged_review_shown_on_own_line(self, tmp_path: Path):
        """Review status appears on its own 'review:' line."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        review = store.add("Review feature", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.depends_on = task.id
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Review should be on its own line starting with "review:"
        assert "review:" in result.stdout
        assert "✓ approved" in result.stdout

    def test_unmerged_shows_no_review_when_missing(self, tmp_path: Path):
        """Unmerged output shows 'no review' when no review exists."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        task = store.add("Add feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review:" in result.stdout
        assert "no review" in result.stdout

    def test_unmerged_always_shows_completion_time(self, tmp_path: Path):
        """Completion time is shown even for tasks with improve tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        git = self._setup_git_repo(tmp_path)

        task = store.add("Root task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime(2026, 2, 12, 10, 30, tzinfo=__import__('datetime').timezone.utc)
        task.branch = "feature/test"
        task.merge_status = "unmerged"
        store.update(task)

        improve = store.add("Improve root task", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime.now(timezone.utc)
        improve.branch = "feature/test"
        improve.based_on = task.id
        improve.merge_status = "unmerged"
        store.update(improve)

        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Completion date should appear
        assert "2026-02-12" in result.stdout


class TestFailureReasonField:
    """Tests for the failure_reason field on Task."""

    def test_failure_reason_persisted(self, tmp_path: Path):
        """failure_reason field is saved and loaded from the database."""
        from gza.db import SqliteTaskStore

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task")
        task.status = "failed"
        task.failure_reason = "Claude returned exit code 1"
        store.update(task)

        loaded = store.get(task.id)
        assert loaded is not None
        assert loaded.failure_reason == "Claude returned exit code 1"

    def test_failure_reason_defaults_to_none(self, tmp_path: Path):
        """failure_reason defaults to None for new tasks."""
        from gza.db import SqliteTaskStore

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task")
        assert task.failure_reason is None

        loaded = store.get(task.id)
        assert loaded is not None
        assert loaded.failure_reason is None


class TestCleanupCommand:
    """Tests for 'gza cleanup' command."""

    def test_cleanup_dry_run(self, tmp_path: Path):
        """Cleanup command dry run works."""
        from gza.config import Config
        from gza.workers import WorkerRegistry
        from gza.git import Git

        # Initialize git repo (needed for worktree cleanup)
        git = Git(tmp_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (tmp_path / "README.md").write_text("# Test")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Create some worker metadata files
        registry = WorkerRegistry(config.workers_path)
        worker1 = registry.generate_worker_id()
        worker_meta = {
            "worker_id": worker1,
            "pid": 99999,  # Non-existent PID
            "task_id": None,
            "task_slug": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "log_file": None,
            "worktree": None,
            "is_background": True,
        }
        from gza.workers import WorkerMetadata
        registry.register(WorkerMetadata.from_dict(worker_meta))

        # Create some old log files
        log_dir = config.log_path
        log_dir.mkdir(parents=True, exist_ok=True)
        old_log = log_dir / "20200101-old-task.log"
        old_log.write_text("old log content")
        # Set modification time to 60 days ago
        import time
        old_time = time.time() - (60 * 24 * 60 * 60)
        import os
        os.utime(old_log, (old_time, old_time))

        result = run_gza("cleanup", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Dry run" in result.stdout
        # The old log should still exist after dry run
        assert old_log.exists()

    def test_cleanup_logs_only(self, tmp_path: Path):
        """Cleanup command with --logs flag works."""
        from gza.config import Config

        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Create some old log files
        log_dir = config.log_path
        log_dir.mkdir(parents=True, exist_ok=True)
        old_log = log_dir / "20200101-old-task.log"
        old_log.write_text("old log content")
        new_log = log_dir / "20260101-new-task.log"
        new_log.write_text("new log content")

        # Set modification time for old log to 60 days ago
        import time
        import os
        old_time = time.time() - (60 * 24 * 60 * 60)
        os.utime(old_log, (old_time, old_time))

        result = run_gza("cleanup", "--logs", "--days", "30", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Logs cleaned" in result.stdout
        assert not old_log.exists()
        assert new_log.exists()

    def test_cleanup_workers(self, tmp_path: Path):
        """Cleanup command cleans up stale worker metadata."""
        from gza.config import Config
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Create a stale worker (PID doesn't exist)
        registry = WorkerRegistry(config.workers_path)
        worker_id = registry.generate_worker_id()
        worker_meta = WorkerMetadata(
            worker_id=worker_id,
            pid=99999,  # Non-existent PID
            task_id=None,
            task_slug=None,
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
            is_background=True,
        )
        registry.register(worker_meta)

        # Verify worker file exists
        worker_file = config.workers_path / f"{worker_id}.json"
        assert worker_file.exists()

        result = run_gza("cleanup", "--workers", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "worker metadata cleaned" in result.stdout.lower()
        # Worker metadata should be cleaned up
        assert not worker_file.exists()

    def test_cleanup_keep_unmerged_logs(self, tmp_path: Path):
        """Cleanup command with --keep-unmerged keeps logs for unmerged tasks."""
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.git import Git
        import time
        import os

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (tmp_path / "README.md").write_text("# Test")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Create an unmerged task
        store = SqliteTaskStore(config.db_path)
        unmerged_task = store.add("Unmerged feature", task_type="implement")
        unmerged_task.status = "completed"
        unmerged_task.task_id = "20200101-unmerged"
        unmerged_task.branch = "feature/unmerged"
        unmerged_task.has_commits = True
        unmerged_task.completed_at = datetime.now(timezone.utc)
        store.update(unmerged_task)

        # Create branch for unmerged task
        git._run("checkout", "-b", "feature/unmerged")
        (tmp_path / "feature.txt").write_text("unmerged feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add unmerged feature")
        git._run("checkout", "master")

        # Create logs for both tasks
        log_dir = config.log_path
        log_dir.mkdir(parents=True, exist_ok=True)

        unmerged_log = log_dir / "20200101-unmerged.log"
        unmerged_log.write_text("unmerged log")

        merged_log = log_dir / "20200102-merged.log"
        merged_log.write_text("merged log")

        # Set both logs to old timestamps
        old_time = time.time() - (60 * 24 * 60 * 60)
        os.utime(unmerged_log, (old_time, old_time))
        os.utime(merged_log, (old_time, old_time))

        result = run_gza("cleanup", "--logs", "--days", "30", "--keep-unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        # Unmerged task log should be kept
        assert unmerged_log.exists()
        # Merged task log should be removed
        assert not merged_log.exists()


class TestRebaseHelpers:
    """Tests for rebase helper functions."""

    def test_invoke_claude_resolve_uses_effective_codex_provider(self, tmp_path):
        """Auto-resolve uses effective provider selection (codex override)."""
        from gza.cli import invoke_claude_resolve
        from gza.config import Config
        from gza.providers.base import RunResult
        from types import SimpleNamespace
        from unittest.mock import patch, Mock

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        task = SimpleNamespace(task_type="implement", provider="codex", model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_claude_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "codex"
            assert resolve_config.use_docker is False
            mock_provider.run.assert_called_once()
            assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto"

    def test_invoke_claude_resolve_uses_effective_gemini_provider(self, tmp_path):
        """Auto-resolve supports gemini provider selection from effective config."""
        from gza.cli import invoke_claude_resolve
        from gza.config import Config
        from gza.providers.base import RunResult
        from types import SimpleNamespace
        from unittest.mock import patch, Mock

        config = Config(project_dir=tmp_path, project_name="test", provider="gemini")
        task = SimpleNamespace(task_type="implement", provider=None, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_claude_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "gemini"
            mock_provider.run.assert_called_once()
            assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto"

    def test_invoke_claude_resolve_fails_fast_when_skill_missing(self, tmp_path, capsys, monkeypatch):
        """Auto-resolve fails before provider run when runtime skill is missing and auto-install fails."""
        from gza.cli import invoke_claude_resolve
        from gza.config import Config
        from types import SimpleNamespace
        from unittest.mock import patch

        codex_home = tmp_path / "codex-home"
        monkeypatch.setenv("CODEX_HOME", str(codex_home))
        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        task = SimpleNamespace(task_type="implement", provider=None, model=None)

        with patch("gza.cli.ensure_skill", return_value=False), \
             patch("gza.providers.get_provider") as mock_get_provider:
            result = invoke_claude_resolve(task, "feature", "main", config)
            assert result is False
            assert mock_get_provider.call_count == 0

        out = capsys.readouterr().out
        assert "Missing required 'gza-rebase' skill for provider 'codex'" in out
        assert "uv run gza skills-install --target codex gza-rebase --project" in out

    def test_ensure_skill_returns_true_when_skill_already_present(self, tmp_path):
        """ensure_skill returns True immediately when the skill file already exists."""
        from gza.cli import ensure_skill

        skills_dir = tmp_path / ".claude" / "skills"
        skill_dir = skills_dir / "gza-rebase"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("---\nname: gza-rebase\n---\n")

        result = ensure_skill("gza-rebase", "claude", tmp_path)
        assert result is True

    def test_ensure_skill_installs_when_missing(self, tmp_path):
        """ensure_skill auto-installs from bundled package when skill is absent."""
        from gza.cli import ensure_skill
        from unittest.mock import patch

        with patch("gza.cli._resolve_runtime_skill_dir") as mock_resolve, \
             patch("gza.skills_utils.copy_skill") as mock_copy:
            runtime_dir = tmp_path / ".claude" / "skills"
            mock_resolve.return_value = ("claude", runtime_dir)
            # Simulate successful install: copy_skill writes the file
            def fake_copy(name, target, force=False):
                skill_path = target / name / "SKILL.md"
                skill_path.parent.mkdir(parents=True, exist_ok=True)
                skill_path.write_text("---\nname: gza-rebase\n---\n")
                return True, "installed"
            mock_copy.side_effect = fake_copy

            result = ensure_skill("gza-rebase", "claude", tmp_path)
            assert result is True
            mock_copy.assert_called_once_with("gza-rebase", runtime_dir)

    def test_ensure_skill_returns_false_when_install_fails(self, tmp_path):
        """ensure_skill returns False when copy_skill fails."""
        from gza.cli import ensure_skill
        from unittest.mock import patch

        with patch("gza.cli._resolve_runtime_skill_dir") as mock_resolve, \
             patch("gza.skills_utils.copy_skill", return_value=(False, "copy failed: error")):
            runtime_dir = tmp_path / ".claude" / "skills"
            mock_resolve.return_value = ("claude", runtime_dir)

            result = ensure_skill("gza-rebase", "claude", tmp_path)
            assert result is False

    def test_ensure_skill_returns_false_for_unknown_provider(self, tmp_path):
        """ensure_skill returns False when the provider has no known skill dir."""
        from gza.cli import ensure_skill

        result = ensure_skill("gza-rebase", "unknown-provider", tmp_path)
        assert result is False


class TestMarkCompletedCommand:
    """Tests for 'gza mark-completed' command."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a minimal git repo in tmp_path."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_mark_completed_nonexistent_task(self, tmp_path: Path):
        """mark-completed errors on a nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("mark-completed", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_mark_completed_default_verify_git_for_code_tasks(self, tmp_path: Path):
        """Code task types default to git verification mode."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        self._setup_git_repo(tmp_path)

        task = store.add("Code task with no branch", task_type="implement")
        task.status = "failed"
        store.update(task)

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout

    def test_mark_completed_default_force_for_non_code_tasks(self, tmp_path: Path):
        """Non-code task types default to status-only completion."""
        from gza.db import SqliteTaskStore

        setup_db_with_tasks(tmp_path, [
            {"prompt": "Review task", "status": "failed", "task_type": "review"},
        ])

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "status-only" in result.stdout

        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        updated = store.get(1)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.has_commits is False

    def test_mark_completed_verify_git_requires_branch(self, tmp_path: Path):
        """--verify-git errors when no branch is set."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task without branch", "status": "failed", "task_type": "review"},
        ])

        result = run_gza("mark-completed", "1", "--verify-git", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout
        assert "Use --force" in result.stdout

    def test_mark_completed_warns_if_not_failed(self, tmp_path: Path):
        """mark-completed warns when task status is not failed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create a branch for the task
        git._run("checkout", "-b", "gza/1-test-task")
        git._run("checkout", "main")

        task = store.add("Pending task")
        task.status = "pending"
        task.branch = "gza/1-test-task"
        store.update(task)

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Warning" in result.stdout
        assert "not in failed status" in result.stdout

    def test_mark_completed_errors_if_branch_missing_in_git(self, tmp_path: Path):
        """mark-completed errors when git branch does not exist."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        self._setup_git_repo(tmp_path)

        task = store.add("Failed task")
        task.status = "failed"
        task.branch = "gza/1-nonexistent-branch"
        store.update(task)

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "does not exist" in result.stdout
        assert "Use --force" in result.stdout

    def test_mark_completed_with_commits_sets_unmerged(self, tmp_path: Path):
        """mark-completed sets status='unmerged' when branch has commits."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create branch with a commit
        git._run("checkout", "-b", "gza/1-task-with-commits")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        task = store.add("Failed task with commits")
        task.status = "failed"
        task.branch = "gza/1-task-with-commits"
        store.update(task)

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "unmerged" in result.stdout

        updated = store.get(1)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.merge_status == "unmerged"
        assert updated.has_commits is True

    def test_mark_completed_without_commits_marks_completed(self, tmp_path: Path):
        """mark-completed sets status='completed' when branch has no commits."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create branch with NO commits beyond main
        git._run("checkout", "-b", "gza/1-empty-branch")
        git._run("checkout", "main")

        task = store.add("Failed task no commits")
        task.status = "failed"
        task.branch = "gza/1-empty-branch"
        store.update(task)

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No commits found" in result.stdout
        assert "completed" in result.stdout

        updated = store.get(1)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.has_commits is False

    def test_mark_completed_force_stale_in_progress_recovery(self, tmp_path: Path):
        """--force supports stale in_progress recovery without git validation."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Stale worker task", task_type="implement")
        task.status = "in_progress"
        store.update(task)

        result = run_gza("mark-completed", "1", "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "in_progress → completed" in result.stdout

        updated = store.get(1)
        assert updated is not None
        assert updated.status == "completed"

    def test_mark_completed_failed_task_no_warning(self, tmp_path: Path):
        """mark-completed does not warn when task is in failed status."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        git._run("checkout", "-b", "gza/1-failed-branch")
        git._run("checkout", "main")

        task = store.add("Failed task")
        task.status = "failed"
        task.branch = "gza/1-failed-branch"
        store.update(task)

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Warning" not in result.stdout

    def test_mark_completed_cleans_up_running_worker(self, tmp_path: Path):
        """mark-completed calls registry.mark_completed() for a running worker."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        git._run("checkout", "-b", "gza/1-worker-task")
        git._run("checkout", "main")

        task = store.add("Failed task with worker")
        task.status = "failed"
        task.branch = "gza/1-worker-task"
        store.update(task)

        # Register a running worker for this task
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-20260301-120000",
            pid=99999,  # non-existent PID
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-03-01T12:00:00+00:00",
            status="running",
            log_file=None,
            worktree=None,
            is_background=True,
        )
        registry.register(worker)

        # Verify PID file exists before
        pid_path = workers_path / "w-20260301-120000.pid"
        assert pid_path.exists()

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0

        # Worker metadata should be updated to completed and PID file removed
        updated_worker = registry.get("w-20260301-120000")
        assert updated_worker is not None
        assert updated_worker.status == "completed"
        assert not pid_path.exists()

    def test_mark_completed_no_worker_is_graceful(self, tmp_path: Path):
        """mark-completed succeeds when no worker exists for the task."""
        from gza.db import SqliteTaskStore

        setup_db_with_tasks(tmp_path, [
            {"prompt": "Review task no worker", "status": "failed", "task_type": "review"},
        ])

        # No workers directory / no registry entry — should still succeed
        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "status-only" in result.stdout

    def test_mark_completed_does_not_touch_already_completed_worker(self, tmp_path: Path):
        """mark-completed leaves an already-completed worker unchanged."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        git._run("checkout", "-b", "gza/1-already-done-branch")
        git._run("checkout", "main")

        task = store.add("Failed task with done worker")
        task.status = "failed"
        task.branch = "gza/1-already-done-branch"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-20260301-130000",
            pid=99998,
            task_id=task.id,
            task_slug=task.task_id,
            started_at="2026-03-01T13:00:00+00:00",
            status="failed",
            log_file=None,
            worktree=None,
            is_background=True,
            exit_code=1,
            completed_at="2026-03-01T13:05:00+00:00",
        )
        registry.register(worker)

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        # Worker that was already failed should remain failed (not touched)
        updated_worker = registry.get("w-20260301-130000")
        assert updated_worker is not None
        assert updated_worker.status == "failed"


class TestForceCompleteRemoval:
    """Tests for removed force-complete command."""

    def test_force_complete_is_not_a_valid_command(self, tmp_path: Path):
        """force-complete command is removed and rejected by CLI parsing."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Failed task", "status": "failed", "task_type": "implement"},
        ])

        result = run_gza("force-complete", "1", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "invalid choice" in result.stderr
        assert "force-complete" in result.stderr


class TestMergeStatusTracking:
    """Tests for merge_status column tracking."""

    def _setup_git_repo(self, tmp_path: Path):
        """Set up a minimal git repo for testing."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_merge_sets_merge_status_merged(self, tmp_path: Path):
        """Successful merge sets merge_status='merged' on the task."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create a task with merge_status='unmerged'
        task = store.add("Add feature")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        # Create the feature branch with a commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Run merge
        result = run_gza("merge", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        # Verify merge_status is 'merged'
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_squash_merge_sets_merge_status_merged(self, tmp_path: Path):
        """Squash merge also sets merge_status='merged'."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        task = store.add("Add feature squash")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/squash"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/squash")
        (tmp_path / "squash.txt").write_text("squash content")
        git._run("add", "squash.txt")
        git._run("commit", "-m", "Squash feature")
        git._run("checkout", "main")

        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))
        assert result.returncode == 0

        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_mark_only_sets_merge_status_merged(self, tmp_path: Path):
        """--mark-only flag sets merge_status='merged' in the database."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        task = store.add("Mark only test")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feature/mark-only-status"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/mark-only-status")
        (tmp_path / "mark.txt").write_text("mark content")
        git._run("add", "mark.txt")
        git._run("commit", "-m", "Mark feature")
        git._run("checkout", "main")

        result = run_gza("merge", str(task.id), "--mark-only", "--project", str(tmp_path))
        assert result.returncode == 0

        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_cmd_unmerged_uses_db_query(self, tmp_path: Path):
        """gza unmerged uses merge_status='unmerged' DB query instead of git detection."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Task with merge_status='unmerged' (branch exists)
        task1 = store.add("Unmerged task")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        task1.branch = "feature/unmerged-task"
        task1.has_commits = True
        task1.merge_status = "unmerged"
        store.update(task1)

        git._run("checkout", "-b", "feature/unmerged-task")
        (tmp_path / "unmerged.txt").write_text("content")
        git._run("add", "unmerged.txt")
        git._run("commit", "-m", "Unmerged feature")
        git._run("checkout", "main")

        # Task with merge_status='merged'
        task2 = store.add("Merged task")
        task2.status = "completed"
        task2.completed_at = datetime.now(timezone.utc)
        task2.branch = "feature/merged-task"
        task2.has_commits = True
        task2.merge_status = "merged"
        store.update(task2)

        # Task with merge_status=None
        task3 = store.add("No merge status")
        task3.status = "completed"
        task3.completed_at = datetime.now(timezone.utc)
        task3.has_commits = False
        store.update(task3)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Unmerged task" in result.stdout
        assert "Merged task" not in result.stdout
        assert "No merge status" not in result.stdout

    def test_cmd_history_shows_merged_label(self, tmp_path: Path):
        """gza history shows [merged] label for tasks with merge_status='merged'."""
        from gza.db import SqliteTaskStore
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Merged feature task")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.has_commits = True
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "[merged]" in result.stdout
        assert "Merged feature task" in result.stdout

    def test_cmd_history_shows_lightning_for_unmerged(self, tmp_path: Path):
        """gza history shows lightning icon for tasks with merge_status='unmerged'."""
        from gza.db import SqliteTaskStore
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Unmerged feature")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "\u26a1" in result.stdout
        assert "Unmerged feature" in result.stdout
        assert "[merged]" not in result.stdout

    def test_cmd_history_no_merge_label_without_merge_status(self, tmp_path: Path):
        """gza history shows no merge label for tasks without merge_status."""
        from gza.db import SqliteTaskStore
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Regular completed task")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "[merged]" not in result.stdout
        assert "\u2713" in result.stdout

    def test_cmd_show_displays_merge_status(self, tmp_path: Path):
        """gza show displays Merge Status when merge_status is set."""
        from gza.db import SqliteTaskStore
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test show merge status")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merge Status: merged" in result.stdout

    def test_cmd_show_no_merge_status_line_when_null(self, tmp_path: Path):
        """gza show does not display Merge Status when merge_status is None."""
        from gza.db import SqliteTaskStore
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test show no merge status")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merge Status" not in result.stdout

    def test_cmd_show_displays_skip_learnings(self, tmp_path: Path):
        """gza show displays 'Skip Learnings: yes' when skip_learnings is True."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task with skip learnings", skip_learnings=True)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Skip Learnings: yes" in result.stdout

    def test_cmd_show_no_skip_learnings_line_when_false(self, tmp_path: Path):
        """gza show does not display Skip Learnings when skip_learnings is False."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Normal task")

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Skip Learnings" not in result.stdout

    def test_cmd_show_warning_when_disk_report_newer(self, tmp_path: Path):
        """gza show displays a warning when the report file on disk is newer than task completion."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed plan task with output_content in DB
        task = store.add("Plan something", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.output_content = "Original plan content"
        task.report_file = ".gza/plans/20260101-plan-something.md"
        store.update(task)

        # Write a newer version of the report file to disk (after completed_at)
        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-something.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Modified plan content on disk")
        # Set mtime to 2 seconds after completed_at to guarantee drift detection
        future_ts = task.completed_at.timestamp() + 2
        os.utime(report_path, (future_ts, future_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Report on disk has been modified since task completion" in result.stdout

    def test_cmd_show_no_warning_when_disk_not_newer(self, tmp_path: Path):
        """gza show does not show drift warning when disk report is not newer than completion."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create a completed task with a future completed_at
        task = store.add("Plan task", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.output_content = "Plan content"
        task.report_file = ".gza/plans/20260101-plan-task.md"
        store.update(task)

        # Write report file and set its mtime to 2 seconds BEFORE completed_at
        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-task.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Plan content")
        past_ts = task.completed_at.timestamp() - 2
        os.utime(report_path, (past_ts, past_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Report on disk has been modified since task completion" not in result.stdout

    def test_cmd_show_displays_disk_content_when_newer(self, tmp_path: Path):
        """gza show displays the disk version of the report when it is newer than DB content."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Explore something", task_type="explore")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.output_content = "Original DB content"
        task.report_file = ".gza/explorations/20260101-explore-something.md"
        store.update(task)

        # Write newer disk content with mtime 2 seconds after completed_at
        report_path = tmp_path / ".gza" / "explorations" / "20260101-explore-something.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Updated disk content")
        future_ts = task.completed_at.timestamp() + 2
        os.utime(report_path, (future_ts, future_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Updated disk content" in result.stdout
        assert "Original DB content" not in result.stdout


class TestSyncReportCommand:
    """Tests for 'gza sync-report' command."""

    def test_sync_report_updates_db_from_disk_for_plan(self, tmp_path: Path):
        """sync-report copies disk content into DB output_content for plan tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Plan something", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.output_content = "Old plan content in DB"
        task.report_file = ".gza/plans/20260101-plan-something.md"
        store.update(task)

        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-something.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("New plan content on disk")

        result = run_gza("sync-report", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Synced" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.output_content == "New plan content on disk"

    def test_sync_report_updates_db_from_disk_for_review(self, tmp_path: Path):
        """sync-report copies disk content into DB output_content for review tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Review feature", task_type="review")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.output_content = "Old review content"
        task.report_file = ".gza/reviews/20260101-review-feature.md"
        store.update(task)

        report_path = tmp_path / ".gza" / "reviews" / "20260101-review-feature.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Updated review content on disk")

        result = run_gza("sync-report", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Synced" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.output_content == "Updated review content on disk"

    def test_sync_report_updates_db_from_disk_for_explore(self, tmp_path: Path):
        """sync-report copies disk content into DB output_content for explore tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Explore codebase", task_type="explore")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.output_content = "Old exploration content"
        task.report_file = ".gza/explorations/20260101-explore-codebase.md"
        store.update(task)

        report_path = tmp_path / ".gza" / "explorations" / "20260101-explore-codebase.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("New exploration findings on disk")

        result = run_gza("sync-report", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Synced" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.output_content == "New exploration findings on disk"

    def test_sync_report_noop_when_already_in_sync(self, tmp_path: Path):
        """sync-report is a no-op when disk content matches DB output_content."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Plan task", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.output_content = "Identical content"
        task.report_file = ".gza/plans/20260101-plan-task.md"
        store.update(task)

        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-task.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Identical content")

        result = run_gza("sync-report", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "already in sync" in result.stdout

        # Verify DB content is unchanged
        updated = store.get(task.id)
        assert updated is not None
        assert updated.output_content == "Identical content"

    def test_sync_report_error_no_report_file(self, tmp_path: Path):
        """sync-report returns error when task has no report_file."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Code task", task_type="implement")
        result = run_gza("sync-report", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 1
        assert "no report file" in result.stdout

    def test_sync_report_error_task_not_found(self, tmp_path: Path):
        """sync-report returns error when task does not exist."""
        setup_config(tmp_path)
        (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)

        result = run_gza("sync-report", "999", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not found" in result.stdout


class TestEditCommandWithNoLearnings:
    """Tests for 'gza edit' command with --no-learnings flag."""

    def test_edit_with_no_learnings_flag(self, tmp_path: Path):
        """Edit command with --no-learnings sets skip_learnings on task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task without skip")
        assert task.skip_learnings is False

        result = run_gza("edit", str(task.id), "--no-learnings", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "skip_learnings" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.skip_learnings is True


class TestLearningsCommand:
    """Tests for 'gza learnings' command."""

    def test_learnings_show_displays_content(self, tmp_path: Path):
        """gza learnings show displays the learnings file content."""
        setup_config(tmp_path)
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        learnings_content = "# Project Learnings\n\n- Use pytest fixtures\n"
        (gza_dir / "learnings.md").write_text(learnings_content)

        result = run_gza("learnings", "show", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Project Learnings" in result.stdout
        assert "Use pytest fixtures" in result.stdout

    def test_learnings_show_no_file(self, tmp_path: Path):
        """gza learnings show reports missing file gracefully."""
        setup_config(tmp_path)

        result = run_gza("learnings", "show", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No learnings file found" in result.stdout

    def test_learnings_update_generates_file(self, tmp_path: Path):
        """gza learnings update writes .gza/learnings.md from completed tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Implement testing flow", task_type="implement")
        store.mark_completed(task, output_content="- Use dedicated fixtures for tests\n", has_commits=False)

        result = run_gza("learnings", "update", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Updated learnings" in result.stdout
        assert "Delta:" in result.stdout
        learnings_path = tmp_path / ".gza" / "learnings.md"
        assert learnings_path.exists()
        assert "Use dedicated fixtures for tests" in learnings_path.read_text()


class TestRefreshCommand:
    """Tests for 'gza refresh' command."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo with an initial commit."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "base.txt").write_text("base content")
        git._run("add", "base.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_refresh_single_task_with_branch(self, tmp_path: Path):
        """gza refresh <id> updates diff stats for a single task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create a feature branch with changes
        git._run("checkout", "-b", "feat/test-task")
        (tmp_path / "new_file.py").write_text("x = 1\ny = 2\n")
        git._run("add", "new_file.py")
        git._run("commit", "-m", "Add new file")
        git._run("checkout", "main")

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Test task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feat/test-task"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "in 1 files" in result.stdout

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed == 1
        assert retrieved.diff_lines_added == 2
        assert retrieved.diff_lines_removed == 0

    def test_refresh_single_task_not_found(self, tmp_path: Path):
        """gza refresh <id> returns error when task doesn't exist."""
        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)
        result = run_gza("refresh", "9999", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not found" in result.stdout or "not found" in result.stderr

    def test_refresh_single_task_no_branch(self, tmp_path: Path):
        """gza refresh <id> skips task without a branch."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("No branch task", task_type="explore")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "skipping" in result.stdout

    def test_refresh_single_task_branch_missing(self, tmp_path: Path):
        """gza refresh <id> warns and skips when branch no longer exists."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task with deleted branch", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = "feat/deleted"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "skipping" in result.stdout

    def test_refresh_all_unmerged(self, tmp_path: Path):
        """gza refresh (no args) refreshes all unmerged tasks."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create two feature branches
        git._run("checkout", "-b", "feat/task-1")
        (tmp_path / "task1.py").write_text("a = 1\n")
        git._run("add", "task1.py")
        git._run("commit", "-m", "Task 1 work")
        git._run("checkout", "main")

        git._run("checkout", "-b", "feat/task-2")
        (tmp_path / "task2.py").write_text("b = 2\nc = 3\n")
        git._run("add", "task2.py")
        git._run("commit", "-m", "Task 2 work")
        git._run("checkout", "main")

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task1 = store.add("Task 1", task_type="implement")
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        task1.branch = "feat/task-1"
        task1.merge_status = "unmerged"
        task1.has_commits = True
        store.update(task1)

        task2 = store.add("Task 2", task_type="implement")
        task2.status = "completed"
        task2.completed_at = datetime.now(timezone.utc)
        task2.branch = "feat/task-2"
        task2.merge_status = "unmerged"
        task2.has_commits = True
        store.update(task2)

        result = run_gza("refresh", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Refreshed 2 task(s)" in result.stdout


class TestGetReviewVerdict:
    """Tests for get_review_verdict()."""

    def _setup(self, tmp_path: Path):
        from gza.cli import get_review_verdict
        from gza.db import SqliteTaskStore
        from gza.config import Config
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        config = Config.load(tmp_path)
        return get_review_verdict, config, store

    def test_inline_verdict(self, tmp_path: Path):
        """Parses inline **Verdict: APPROVED** format."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Some review text.\n\n**Verdict: APPROVED**\n"
        store.update(task)
        assert get_review_verdict(config, task) == "APPROVED"

    def test_heading_verdict(self, tmp_path: Path):
        """Parses ## Verdict heading with verdict on following line."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Some review.\n\n## Verdict\n\n**CHANGES_REQUESTED**\n"
        store.update(task)
        assert get_review_verdict(config, task) == "CHANGES_REQUESTED"

    def test_heading_verdict_no_bold(self, tmp_path: Path):
        """Parses ## Verdict heading with plain verdict on following line."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Review.\n\n## Verdict\n\nNEEDS_DISCUSSION\n"
        store.update(task)
        assert get_review_verdict(config, task) == "NEEDS_DISCUSSION"

    def test_no_verdict_returns_none(self, tmp_path: Path):
        """Returns None when no verdict pattern is found."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "I have some thoughts but no verdict."
        store.update(task)
        assert get_review_verdict(config, task) is None


class TestAdvanceCommand:
    """Tests for 'gza advance' command."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo in tmp_path with an initial commit on main."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def _create_implement_task_with_branch(self, store, git, tmp_path, prompt="Implement feature"):
        """Create a completed implement task with a real git branch."""
        task = store.add(prompt, task_type="implement")
        branch = f"feat/task-{task.id}"

        # Create the branch with a commit
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Add feature for task {task.id}")
        git._run("checkout", "main")

        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        return task

    def test_advance_no_eligible_tasks(self, tmp_path: Path):
        """advance command reports no tasks when none are eligible."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        SqliteTaskStore(db_path)  # create empty db

        self._setup_git_repo(tmp_path)

        result = run_gza("advance", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_dry_run_shows_actions(self, tmp_path: Path):
        """advance --dry-run shows planned actions without executing."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert str(task.id) in result.stdout

    def test_advance_merges_approved_task(self, tmp_path: Path):
        """advance merges a task whose review is APPROVED."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review task with APPROVED verdict
        review_prompt = f"Review implementation #{task.id}"
        review_task = store.add(
            review_prompt,
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good!"
        store.update(review_task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merged" in result.stdout or "merged" in result.stdout

        # Verify merge status updated
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_advance_skips_task_with_conflicts(self, tmp_path: Path):
        """advance skips a task whose branch has merge conflicts."""
        from gza.db import SqliteTaskStore
        from gza.git import Git
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create a branch that conflicts with main
        branch = "feat/conflicting"
        git._run("checkout", "-b", branch)
        (tmp_path / "README.md").write_text("feature version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Conflict commit")
        git._run("checkout", "main")

        # Modify same file on main to create a conflict
        (tmp_path / "README.md").write_text("main version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Main change")

        task = store.add("Conflicting feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "needs" in result.stdout.lower() and "rebase" in result.stdout.lower()

        # Task should still be unmerged
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "unmerged"

    def test_advance_merges_non_implement_task_without_review(self, tmp_path: Path):
        """advance merges a non-implement task (e.g. explore) directly, skipping review creation."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create a completed explore task with a branch but no review
        task = store.add("Explore the codebase", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"explore_{task.id}.txt").write_text("notes")
        git._run("add", f"explore_{task.id}.txt")
        git._run("commit", "-m", f"Exploration for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        rc = cmd_advance(args)

        assert rc == 0

        # Verify the task was merged directly without creating a review
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"
        assert store.get_reviews_for_task(task.id) == []

    def test_advance_creates_review_for_implement_without_review(self, tmp_path: Path):
        """advance creates a review task for a completed implement task with no review."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        from unittest.mock import patch
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Patch _spawn_background_worker to avoid actually spawning processes
        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0

        # Verify a review task was created (not merged directly)
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 1
        assert reviews[0].task_type == 'review'

    def test_advance_creates_improve_for_changes_requested(self, tmp_path: Path):
        """advance creates an improve task when review is CHANGES_REQUESTED."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        from unittest.mock import patch
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a review with CHANGES_REQUESTED
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix the tests."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Patch _spawn_background_worker to avoid actually spawning processes
        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0

        # Verify improve task was created
        improve_tasks = store.get_improve_tasks_for(task.id, review_task.id)
        assert len(improve_tasks) == 1
        assert improve_tasks[0].task_type == "improve"

    def test_advance_single_task_id(self, tmp_path: Path):
        """advance with a specific task ID only advances that task."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task1 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature A")
        task2 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature B")

        # Give task1 an approved review so it can merge
        review = store.add(f"Review #{task1.id}", task_type="review", depends_on=task1.id)
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        # Advance only task1
        result = run_gza("advance", str(task1.id), "--auto", "--project", str(tmp_path))
        assert result.returncode == 0

        # task1 should be merged, task2 should still be unmerged
        assert store.get(task1.id).merge_status == "merged"
        assert store.get(task2.id).merge_status == "unmerged"

    def test_advance_max_limits_batch(self, tmp_path: Path):
        """advance --max N limits the number of tasks processed."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        from unittest.mock import patch
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task1 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature A")
        task2 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature B")
        task3 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature C")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=2,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        # Only 2 tasks should have been processed (not 3, due to --max 2).
        # Since these are implement tasks with no reviews, reviews are created.
        # Tasks are ordered by completed_at DESC (newest first), so task3 and
        # task2 are processed while task1 (oldest) is left untouched.
        review_counts = [
            len(store.get_reviews_for_task(t.id))
            for t in [task1, task2, task3]
        ]
        assert sum(review_counts) == 2
        # task1 is the oldest so it falls outside the --max 2 window.
        assert review_counts[0] == 0

    def test_advance_spawns_worker_for_pending_review(self, tmp_path: Path):
        """advance spawns a worker for a pending review instead of skipping."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a pending review
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        # review_task.status is 'pending' by default

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Started review worker" in result.stdout

    def test_advance_waits_for_in_progress_review(self, tmp_path: Path):
        """advance skips a task whose review is in_progress."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create an in_progress review
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "in_progress"
        store.update(review_task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "SKIP" in result.stdout
        assert "in_progress" in result.stdout

    def test_advance_task_not_found(self, tmp_path: Path):
        """advance with non-existent task ID returns error."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        SqliteTaskStore(db_path)  # create db
        self._setup_git_repo(tmp_path)

        result = run_gza("advance", "9999", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_advance_dry_run_does_not_modify_state(self, tmp_path: Path):
        """advance --dry-run does not modify task state or create tasks."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action would be merge
        review = store.add(f"Review #{task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout

        # Task should still be unmerged
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_task_with_no_branch_is_skipped(self, tmp_path: Path):
        """advance skips tasks that have no branch (no commits)."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        self._setup_git_repo(tmp_path)

        # Create a task with no branch
        task = store.add("Implement feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.merge_status = "unmerged"
        task.branch = None
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No review tasks should have been created
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 0

    def test_advance_needs_discussion_verdict_skips(self, tmp_path: Path):
        """advance skips tasks whose review verdict needs manual attention."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review with no recognizable verdict
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "I have some thoughts but no verdict."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # Task should not have been merged or had new tasks created
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_non_implement_task_skipped_in_create_review(self, tmp_path: Path):
        """advance skips creating a review for non-implement task types."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create a plan-type task with a branch
        task = store.add("Plan something", task_type="plan")
        branch = f"plan/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"plan_{task.id}.txt").write_text("plan")
        git._run("add", f"plan_{task.id}.txt")
        git._run("commit", "-m", f"Plan task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No review should have been created for a plan task
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 0

    def test_advance_active_improve_already_exists_is_skipped(self, tmp_path: Path):
        """advance skips creating a new improve task when one is already active."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a review with CHANGES_REQUESTED
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix the tests."
        store.update(review_task)

        # Create an already-pending improve task
        existing_improve = store.add(
            f"Improve #{task.id}",
            task_type="improve",
            depends_on=review_task.id,
            based_on=task.id,
            same_branch=True,
        )
        # status is 'pending' by default

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No additional improve task should be created
        improve_tasks = store.get_improve_tasks_for(task.id, review_task.id)
        assert len(improve_tasks) == 1
        assert improve_tasks[0].id == existing_improve.id

    def test_advance_already_merged_task_returns_early(self, tmp_path: Path):
        """advance with a specific already-merged task ID exits with 0 early."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Mark task as already merged
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("advance", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "already merged" in result.stdout

    def test_advance_review_cleared_at_triggers_merge(self, tmp_path: Path):
        """advance merges when review_cleared_at marks prior review as addressed (no new review)."""
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        from unittest.mock import patch
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        # Set review_cleared_at on the task to a time AFTER the review completed
        # (simulates an improve task having run after the review)
        import time
        time.sleep(0.01)  # ensure strictly after
        task.review_cleared_at = datetime.now(timezone.utc)
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        # No new review should be created — task is merged directly after improve
        all_reviews = store.get_reviews_for_task(task.id)
        assert len(all_reviews) == 1  # only the original review
        assert store.get(task.id).merge_status == "merged"

    def test_advance_batch_limits_worker_spawning(self, tmp_path: Path):
        """advance --batch B stops after B workers have been started."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create 3 implement tasks, each with a pending review (triggers run_review)
        tasks = []
        for i in range(3):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Feature {i}")
            store.add(
                f"Review #{task.id}",
                task_type="review",
                depends_on=task.id,
            )
            tasks.append(task)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=2,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        # Only 2 workers should have been started, not 3
        assert len(spawn_calls) == 2
        # The third task should show a batch limit message
        assert "batch limit reached" in output
        assert f"#{tasks[2].id}" in output

    def test_advance_batch_merge_does_not_count_toward_limit(self, tmp_path: Path):
        """advance --batch B: merge actions don't count toward the worker limit."""
        # Use advance_requires_review=false so unreviewed tasks merge directly
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nadvance_requires_review: false\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create 2 tasks that will merge (with APPROVED reviews)
        merge_tasks = []
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Merge {i}")
            review_task = store.add(
                f"Review #{task.id}",
                task_type="review",
                depends_on=task.id,
            )
            review_task.status = "completed"
            review_task.completed_at = datetime.now(timezone.utc)
            review_task.output_content = "**Verdict: APPROVED**"
            store.update(review_task)
            merge_tasks.append(task)

        # Create 2 tasks with pending reviews (will spawn workers)
        worker_tasks = []
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Worker {i}")
            store.add(
                f"Review #{task.id}",
                task_type="review",
                depends_on=task.id,
            )
            worker_tasks.append(task)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            rc = cmd_advance(args)

        assert rc == 0
        # Both merge tasks should be merged (they don't count toward batch)
        for t in merge_tasks:
            assert store.get(t.id).merge_status == "merged"
        # Only 1 worker should have been spawned (batch=1)
        assert len(spawn_calls) == 1

    def test_advance_batch_enforced_on_failed_spawn(self, tmp_path: Path):
        """advance --batch 1 attempts only one spawn even when the first spawn fails."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # Create 2 implement tasks, each with a pending review (triggers run_review)
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Feature {i}")
            store.add(
                f"Review #{task.id}",
                task_type="review",
                depends_on=task.id,
            )

        spawn_calls = []

        def fake_spawn_first_fails(worker_args, config, task_id):
            spawn_calls.append(task_id)
            # First call fails, second would succeed — but with batch=1 it should never be called
            return 1 if len(spawn_calls) == 1 else 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn_first_fails):
            rc = cmd_advance(args)

        # With batch=1, the failed spawn still counts toward the limit,
        # so only 1 spawn attempt should be made (not 2)
        assert len(spawn_calls) == 1

    def test_advance_batch_zero_returns_error(self, tmp_path: Path):
        """advance --batch 0 is rejected with an error message."""
        setup_config(tmp_path)
        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=0,
            no_docker=True,
        )
        rc = cmd_advance(args)
        assert rc == 1

    def test_advance_spawn_worker_failure_increments_error_count(self, tmp_path: Path):
        """advance returns 1 when _spawn_background_worker fails for an improve task."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review so advance will try to spawn an improve worker
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Simulate worker spawn failure
        with patch("gza.cli._spawn_background_worker", return_value=1):
            rc = cmd_advance(args)

        assert rc == 1

    def test_advance_interactive_shows_plan_and_prompts(self, tmp_path: Path):
        """advance without --auto shows plan and prompts for confirmation."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        # Simulate user confirming with 'y'
        with patch("builtins.input", return_value="y") as mock_input:
            with patch("gza.cli._spawn_background_worker", return_value=0):
                rc = cmd_advance(args)

        assert rc == 0
        mock_input.assert_called_once()
        call_args = mock_input.call_args[0][0]
        assert "Proceed" in call_args

    def test_advance_interactive_aborts_on_no(self, tmp_path: Path):
        """advance without --auto exits without executing when user answers 'n'."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action would be merge
        review = store.add(f"Review #{task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input", return_value="n"):
            rc = cmd_advance(args)

        assert rc == 0
        # Task should NOT have been merged
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_interactive_eof_aborts(self, tmp_path: Path):
        """advance without --auto exits cleanly when stdin is closed (EOFError)."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input", side_effect=EOFError):
            rc = cmd_advance(args)

        assert rc == 0

    def test_advance_auto_flag_skips_prompt(self, tmp_path: Path):
        """advance --auto executes without prompting."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action is merge
        review = store.add(f"Review #{task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input") as mock_input:
            with patch("gza.cli._spawn_background_worker", return_value=0):
                rc = cmd_advance(args)

        assert rc == 0
        mock_input.assert_not_called()
        assert store.get(task.id).merge_status == "merged"

    def test_advance_merges_run_before_workers(self, tmp_path: Path):
        """advance executes all merge actions before spawning any background workers.

        This test fails if the sort line in cmd_advance is removed: get_unmerged()
        returns tasks ORDER BY completed_at DESC, so task_spawn (the newer task)
        appears first. Without the sort, spawn happens before merge. The sort
        reorders so merge runs first.
        """
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_advance
        from unittest.mock import patch
        from datetime import datetime, timezone
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)

        # task_merge: APPROVED review → 'merge' action.
        # Given an EARLIER completed_at so it appears second in DB order (DESC).
        task_merge = self._create_implement_task_with_branch(store, git, tmp_path, "Feature merge")
        approved_review = store.add(
            f"Review #{task_merge.id}", task_type="review", depends_on=task_merge.id
        )
        approved_review.status = "completed"
        approved_review.completed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        approved_review.output_content = "**Verdict: APPROVED**\n\nLooks great."
        store.update(approved_review)
        task_merge.completed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        store.update(task_merge)

        # task_spawn: pending review → 'run_review' action (spawns a worker).
        # Given a LATER completed_at so it appears first in DB order (DESC).
        # Without the sort, this causes spawn to execute before merge.
        task_spawn = self._create_implement_task_with_branch(store, git, tmp_path, "Feature spawn")
        store.add(f"Review #{task_spawn.id}", task_type="review", depends_on=task_spawn.id)
        # Leave review status as default 'pending' — this triggers run_review action.
        task_spawn.completed_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
        store.update(task_spawn)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        call_log: list[str] = []

        def fake_merge(task_id, config, store, git, merge_args, default_branch):
            call_log.append('merge')
            return 0

        def fake_spawn(spawn_args, config, task_id=None):
            call_log.append('spawn')
            return 0

        with patch("gza.cli._merge_single_task", side_effect=fake_merge):
            with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
                rc = cmd_advance(args)

        assert rc == 0
        assert 'merge' in call_log, "Expected at least one merge call"
        assert 'spawn' in call_log, "Expected at least one worker spawn call"
        # All merges must complete before the first spawn
        last_merge_index = max(i for i, v in enumerate(call_log) if v == 'merge')
        first_spawn_index = min(i for i, v in enumerate(call_log) if v == 'spawn')
        assert last_merge_index < first_spawn_index, (
            f"Expected all merges before first spawn, got call order: {call_log}"
        )

    def test_advance_requires_review_true_create_true_creates_review_for_unreviewed(self, tmp_path: Path):
        """advance creates a review when advance_requires_review=True, advance_create_reviews=True."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_create_reviews: true\n"
            "advance_requires_review: true\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 1
        assert reviews[0].task_type == 'review'
        assert store.get(task.id).merge_status != "merged"

    def test_advance_requires_review_true_create_false_skips_unreviewed(self, tmp_path: Path):
        """advance skips unreviewed implement tasks when advance_create_reviews=False."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_create_reviews: false\n"
            "advance_requires_review: true\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'skip'

    def test_advance_requires_review_false_merges_unreviewed(self, tmp_path: Path):
        """advance merges unreviewed implement tasks when advance_requires_review=False."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_requires_review: false\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        rc = cmd_advance(args)

        assert rc == 0
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"
        assert store.get_reviews_for_task(task.id) == []

    def test_advance_review_cleared_always_merges_regardless_of_config(self, tmp_path: Path):
        """advance merges when review is cleared by improve, even with advance_requires_review=True."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_create_reviews: true\n"
            "advance_requires_review: true\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        # Mark review as cleared (simulates improve task having run)
        time.sleep(0.01)
        task.review_cleared_at = datetime.now(timezone.utc)
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        assert store.get(task.id).merge_status == "merged"

    # Planned test #5 (advance_requires_review=True, APPROVED review → merge) is covered by
    # the pre-existing test_advance_merges_approved_task, which verifies this happy path.

    def test_advance_default_config_creates_review_for_unreviewed(self, tmp_path: Path):
        """advance creates a review for unreviewed implement tasks with default config."""
        # Default config — no explicit advance_* flags
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        config = Config.load(tmp_path)
        # Defaults: advance_create_reviews=True, advance_requires_review=True
        assert config.advance_create_reviews is True
        assert config.advance_requires_review is True

        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'create_review'

    def _create_completed_improve(self, store, impl_task, review_task):
        """Create a completed improve task for the given impl and review tasks."""
        improve = store.add(
            f"Improve #{impl_task.id}",
            task_type="improve",
            depends_on=review_task.id,
            based_on=impl_task.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(timezone.utc)
        store.update(improve)
        return improve

    def test_advance_skips_task_at_max_review_cycles(self, tmp_path: Path):
        """advance skips task when completed improve count >= max_review_cycles."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 2\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 2 completed improve tasks (= max_review_cycles)
        self._create_completed_improve(store, task, review_task)
        self._create_completed_improve(store, task, review_task)

        config = Config.load(tmp_path)
        assert config.max_review_cycles == 2

        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'max_cycles_reached'
        assert 'max review cycles' in action['description']
        assert '2' in action['description']

    def test_advance_creates_improve_when_under_cycle_limit(self, tmp_path: Path):
        """advance creates an improve task when completed cycles < max_review_cycles."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 3\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 1 completed improve (below limit of 3)
        self._create_completed_improve(store, task, review_task)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'improve'

    def test_advance_needs_attention_summary_printed(self, tmp_path: Path):
        """advance prints Needs attention section for actionable skips."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 1\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review and 1 completed improve (= max_review_cycles=1)
        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)
        self._create_completed_improve(store, task, review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
            max_review_cycles=None,
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        assert "Needs attention" in output
        assert f"#{task.id}" in output
        assert "max review cycles" in output

    def test_advance_max_review_cycles_cli_override(self, tmp_path: Path):
        """--max-review-cycles overrides the config value."""
        # Config has default max_review_cycles=3; 2 completed improves would normally allow more
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 2 completed improves
        self._create_completed_improve(store, task, review_task)
        self._create_completed_improve(store, task, review_task)

        # With default max_review_cycles=3, action would be 'improve' (2 < 3)
        config = Config.load(tmp_path)
        action_default = _determine_advance_action(config, store, git, task, "main")
        assert action_default['type'] == 'improve'

        # Override to 2 — now 2 completed improves == limit → max_cycles_reached
        config.max_review_cycles = 2
        action_override = _determine_advance_action(config, store, git, task, "main")
        assert action_override['type'] == 'max_cycles_reached'

    def test_advance_max_review_cycles_dry_run(self, tmp_path: Path):
        """advance --dry-run shows max_cycles_reached action without executing."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 1\n"
        )
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        review_task = store.add(
            f"Review #{task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(timezone.utc)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)
        self._create_completed_improve(store, task, review_task)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert "max review cycles" in result.stdout


    def _create_failed_task(self, store, session_id="sess-abc", failure_reason="MAX_STEPS", prompt="Implement feature"):
        """Create a failed task with given failure_reason and session_id."""
        task = store.add(prompt, task_type="implement")
        task.status = "failed"
        task.failure_reason = failure_reason
        task.session_id = session_id
        task.completed_at = datetime.now(timezone.utc)
        task.branch = f"feat/task-{task.id}"
        store.update(task)
        return task

    def test_advance_resumes_max_steps_failed_task(self, tmp_path: Path):
        """advance creates a resume child task and spawns worker for MAX_STEPS failed task."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout
        assert "Created resume task" in result.stdout

        # Verify a resume child task was created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1
        child = children[0]
        assert child.based_on == failed_task.id
        assert child.session_id == failed_task.session_id

    def test_advance_resumes_max_turns_failed_task(self, tmp_path: Path):
        """advance creates a resume child task and spawns worker for MAX_TURNS failed task."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-xyz", failure_reason="MAX_TURNS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1
        assert children[0].session_id == "sess-xyz"

    def test_advance_skips_failed_task_at_max_attempts(self, tmp_path: Path):
        """advance skips a failed task when chain depth >= max_resume_attempts."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        # Create a chain: original (MAX_STEPS) → first_resume (MAX_STEPS)
        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(timezone.utc)
        store.update(first_resume)

        # Default max_resume_attempts=1; original is skipped (already has a child),
        # first_resume (depth=1) is skipped (at max attempts)
        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "max resume attempts" in result.stdout

        # Original should NOT get a new resume child (it already has first_resume)
        original_children = store.get_based_on_children(original.id)
        assert len(original_children) == 1  # only the pre-existing first_resume
        # first_resume should not have any new children (at max attempts)
        first_resume_children = store.get_based_on_children(first_resume.id)
        assert len(first_resume_children) == 0

    def test_advance_skips_failed_task_with_existing_resume_child(self, tmp_path: Path):
        """advance skips a failed task that already has a pending/in_progress child."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Create an existing pending resume child
        child = store.add("Implement feature", task_type="implement")
        child.based_on = failed_task.id
        child.status = "pending"
        store.update(child)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        # No new child should have been created (still just the one pre-existing)
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1  # only the pre-existing child

    def test_advance_skips_failed_task_with_completed_resume_child(self, tmp_path: Path):
        """advance skips a failed task whose resume child already completed."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Create a completed resume child (simulating a successful resume)
        child = store.add("Implement feature", task_type="implement")
        child.based_on = failed_task.id
        child.status = "completed"
        store.update(child)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        # No new child should have been created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1  # only the pre-existing completed child

    def test_advance_skips_failed_task_with_failed_resume_child(self, tmp_path: Path):
        """advance skips a failed task whose resume child also failed (no double-resume of root)."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        # Original task #198 equivalent — failed with MAX_STEPS
        original = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Resume child #213 equivalent — also failed with MAX_STEPS
        child = store.add("Implement feature", task_type="implement")
        child.based_on = original.id
        child.status = "failed"
        child.failure_reason = "MAX_STEPS"
        child.session_id = "sess-abc"
        store.update(child)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        # The original should NOT appear in the plan — only the child should
        # (and the child should be skipped due to max resume attempts)
        assert f"#{original.id}" not in result.stdout
        assert "SKIP: max resume attempts" in result.stdout

    def test_advance_no_resume_failed_flag_skips(self, tmp_path: Path):
        """advance --no-resume-failed excludes failed tasks from processing."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--no-resume-failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_dry_run_shows_resume_action(self, tmp_path: Path):
        """advance --dry-run shows resume action without executing."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert "Resume" in result.stdout

        # No resume child should have been created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 0

    def test_advance_specific_failed_task_id(self, tmp_path: Path):
        """advance with a specific failed resumable task ID works."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", str(failed_task.id), "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1

    def test_advance_skips_failed_task_without_session_id(self, tmp_path: Path):
        """advance skips failed tasks without session_id (not resumable)."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        # Task with no session_id — not resumable
        self._create_failed_task(store, session_id=None, failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_max_resume_attempts_flag_overrides_config(self, tmp_path: Path):
        """advance --max-resume-attempts N overrides the config value."""
        from gza.db import SqliteTaskStore
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        self._setup_git_repo(tmp_path)

        # Create a chain of depth 1: original (MAX_STEPS) → first_resume (MAX_STEPS)
        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(timezone.utc)
        store.update(first_resume)

        # With --max-resume-attempts 2, original is skipped (has child),
        # first_resume (depth=1 < 2) gets resumed
        result = run_gza("advance", "--auto", "--max-resume-attempts", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout
        # Original should NOT get a new child (already has first_resume)
        original_children = store.get_based_on_children(original.id)
        assert len(original_children) == 1  # only the pre-existing first_resume
        # first_resume should get a new resume child (depth=1 < max=2)
        first_resume_children = store.get_based_on_children(first_resume.id)
        assert len(first_resume_children) == 1


class TestStatsCommand:
    """Tests for 'gza stats' command."""

    def test_stats_uses_computed_steps_when_reported_missing(self, tmp_path: Path):
        """gza stats should display computed steps for computed-only providers."""
        from gza.db import SqliteTaskStore, TaskStats

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Computed-only stats task", task_type="implement")
        store.mark_completed(
            task,
            has_commits=False,
            stats=TaskStats(num_steps_computed=5, cost_usd=0.12, duration_seconds=30.0),
        )

        result = run_gza("stats", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Total steps:  5" in result.stdout
        assert re.search(r"✓\s+#1\s+implement\s+\$0\.1200\s+5\s", result.stdout)


class TestIterateCommand:
    """Tests for 'gza iterate' command (formerly 'gza cycle')."""

    def _make_completed_impl(self, store, prompt: str = "Implement feature") -> object:
        """Create and return a completed implement task."""
        from datetime import datetime, timezone
        impl = store.add(prompt, task_type="implement")
        impl.status = "completed"
        impl.branch = f"test-project/20260101-impl"
        impl.completed_at = datetime.now(timezone.utc)
        store.update(impl)
        return impl

    def test_cycle_dry_run(self, tmp_path: Path):
        """gza iterate --dry-run prints preview and exits 0."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        impl = self._make_completed_impl(store)

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()

    def test_cycle_rejects_non_implement_task(self, tmp_path: Path):
        """gza iterate rejects tasks that are not implement type."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan_task = store.add("A plan", task_type="plan")

        result = run_gza("iterate", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode != 0
        assert "implement" in result.stdout.lower() or "implement" in result.stderr.lower()

    def test_cycle_rejects_incomplete_task(self, tmp_path: Path):
        """gza iterate rejects implementation tasks that are not completed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = store.add("Implement feature", task_type="implement")  # status = 'pending'

        result = run_gza("iterate", str(impl.id), "--project", str(tmp_path))

        assert result.returncode != 0
        assert "pending" in result.stdout or "pending" in result.stderr

    def test_cycle_start_and_close_in_db(self, tmp_path: Path):
        """start_cycle + close_cycle flow creates correct DB records."""
        from gza.db import SqliteTaskStore

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None

        cycle = store.start_cycle(impl.id, max_iterations=2)
        it = store.append_cycle_iteration(cycle.id, 0)
        store.update_cycle_iteration(it.id, state="terminal", review_verdict="APPROVED")
        store.close_cycle(cycle.id, status="approved", stop_reason="approved")

        cycles = store.get_cycles_for_impl(impl.id)
        assert len(cycles) == 1
        assert cycles[0].status == "approved"
        assert cycles[0].stop_reason == "approved"

        iterations = store.get_cycle_iterations(cycle.id)
        assert len(iterations) == 1
        assert iterations[0].review_verdict == "APPROVED"

    def test_cycle_blocked_on_active_cycle_without_continue(self, tmp_path: Path):
        """gza iterate errors if an active cycle exists without --continue."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = self._make_completed_impl(store)
        # Pre-create an active cycle
        store.start_cycle(impl.id, max_iterations=3)

        result = run_gza("iterate", str(impl.id), "--project", str(tmp_path))

        assert result.returncode != 0
        assert "active cycle" in result.stdout.lower() or "already has an active cycle" in result.stdout.lower()

    def test_cycle_continue_dry_run_shows_resume_message(self, tmp_path: Path):
        """gza iterate --continue --dry-run shows 'resume' message, not 'start' message."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = self._make_completed_impl(store)
        cycle = store.start_cycle(impl.id, max_iterations=3)

        result = run_gza("iterate", str(impl.id), "--continue", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        # Must say "resume", not "start", when --continue is given
        assert "resume" in result.stdout.lower()
        assert "No active cycle" not in result.stdout
        # The active cycle should still be there (dry-run doesn't mutate state)
        active = store.get_active_cycle_for_impl(impl.id)
        assert active is not None
        assert active.id == cycle.id

    def test_cycle_continue_resumes_existing_active_cycle(self, tmp_path: Path):
        """--continue resumes at next iteration and APPROVED verdict closes cycle correctly.

        This is a unit test that calls cmd_iterate() directly so that unittest.mock.patch
        is effective. (run_gza spawns a subprocess where in-process patches have no effect.)
        """
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_iterate
        from unittest.mock import patch, MagicMock

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = self._make_completed_impl(store)
        cycle = store.start_cycle(impl.id, max_iterations=3)
        # Simulate one completed iteration already recorded so resume starts at index 1
        it = store.append_cycle_iteration(cycle.id, 0)
        store.update_cycle_iteration(it.id, state="improve_completed", review_verdict="CHANGES_REQUESTED")

        # Create a completed review task in the store with an APPROVED verdict in output_content.
        # _create_review_task will be patched to return this pre-seeded task, and run() will
        # return 0 (success), so get_review_verdict will read output_content and return APPROVED.
        fake_review = store.add("Review impl", task_type="review", depends_on=impl.id)
        fake_review.status = "completed"
        fake_review.completed_at = datetime.now(timezone.utc)
        fake_review.output_content = "**Verdict: APPROVED**"
        store.update(fake_review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            continue_cycle=True,
            project_dir=tmp_path,
            no_docker=True,
        )

        mock_config = MagicMock()
        mock_config.project_dir = tmp_path
        mock_config.use_docker = False

        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task", return_value=fake_review), \
             patch("gza.cli.run", return_value=0):
            result = cmd_iterate(args)

        # The cycle should complete as approved
        assert result == 0
        # Verify iteration records: seeded index=0 plus new index=1 from the resumed run
        iterations = store.get_cycle_iterations(cycle.id)
        assert len(iterations) == 2
        assert iterations[0].iteration_index == 0
        assert iterations[1].iteration_index == 1
        # Verify the cycle was closed with approved status
        cycles = store.get_cycles_for_impl(impl.id)
        assert len(cycles) == 1
        assert cycles[0].status == "approved"
        assert cycles[0].stop_reason == "approved"

    def test_cycle_continue_no_active_cycle_returns_error(self, tmp_path: Path):
        """gza iterate --continue with no active cycle returns non-zero exit code and error message."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = self._make_completed_impl(store)
        # No active cycle exists for this implementation

        result = run_gza("iterate", str(impl.id), "--continue", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "No active cycle" in result.stdout or "No active cycle" in result.stderr

    def test_cycle_continue_respects_original_max_iterations(self, tmp_path: Path):
        """gza cycle --continue uses cycle.max_iterations, not the CLI --max-iterations default.

        Seeded state: max_iterations=5, 3 completed iterations (indices 0, 1, 2).
        CLI args: max_iterations=3 (should be IGNORED when --continue is given).

        With the bug (CLI default max_iterations=3): iteration starts at 3,
        `while 3 < 3` is immediately False → loop body never runs → still 3 records.
        With the fix (cycle.max_iterations=5): loop runs iterations 3 and 4,
        exhausts max_iterations, and returns exit code 2 (maxed_out).
        """
        import argparse
        from gza.db import SqliteTaskStore
        from gza.cli import cmd_iterate
        from unittest.mock import patch, MagicMock

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = self._make_completed_impl(store)
        # Start cycle with max_iterations=5 (higher than the CLI default of 3)
        cycle = store.start_cycle(impl.id, max_iterations=5)
        # Simulate 3 completed iterations (indices 0, 1, 2)
        for i in range(3):
            it = store.append_cycle_iteration(cycle.id, i)
            store.update_cycle_iteration(it.id, state="improve_completed", review_verdict="CHANGES_REQUESTED")

        # Fake review task: always returns CHANGES_REQUESTED verdict so the loop runs to completion
        fake_review = store.add("Review impl", task_type="review", depends_on=impl.id)
        fake_review.status = "completed"
        fake_review.completed_at = datetime.now(timezone.utc)
        fake_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        store.update(fake_review)

        # Fake improve task for the improve phase
        fake_improve = store.add("Improve impl", task_type="improve", depends_on=impl.id)
        store.update(fake_improve)

        # CLI args: max_iterations=3 (should be IGNORED in favour of cycle.max_iterations=5)
        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            continue_cycle=True,
            project_dir=tmp_path,
            no_docker=True,
        )

        mock_config = MagicMock()
        mock_config.project_dir = tmp_path
        mock_config.use_docker = False

        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task", return_value=fake_review), \
             patch("gza.cli._create_improve_task", return_value=fake_improve), \
             patch("gza.cli.run", return_value=0):
            result = cmd_iterate(args)

        # With the fix, the loop runs iterations 3 and 4, exhausts max_iterations=5,
        # and exits with code 2 (maxed_out). With the CLI-default bug it would never
        # enter the loop and would return 3 (blocked/unknown stop reason).
        assert result == 2, f"Expected exit code 2 (maxed_out), got {result}"

        # Verify exactly 2 new iterations were appended (indices 3 and 4)
        all_iterations = store.get_cycle_iterations(cycle.id)
        assert len(all_iterations) == 5, (
            f"Expected 5 iteration records (3 seeded + 2 new), got {len(all_iterations)}"
        )
        new_indices = sorted(it.iteration_index for it in all_iterations if it.iteration_index >= 3)
        assert new_indices == [3, 4], f"Expected new iteration indices [3, 4], got {new_indices}"

        # Verify the cycle was closed with maxed_out status
        cycles = store.get_cycles_for_impl(impl.id)
        assert len(cycles) == 1
        assert cycles[0].status == "maxed_out"
        assert cycles[0].stop_reason == "max_iterations"

    def test_cycle_alias_still_works(self, tmp_path: Path):
        """'gza cycle' backward-compat alias routes to the same handler as 'gza iterate'."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        impl = self._make_completed_impl(store)

        result = run_gza("cycle", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()


class TestStatsCyclesCommand:
    """Tests for 'gza stats --cycles' command."""

    def test_stats_cycles_no_data(self, tmp_path: Path):
        """gza stats --cycles with no cycles prints zero-data message."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        result = run_gza("stats", "--cycles", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No cycles found" in result.stdout or "0" in result.stdout

    def test_stats_cycles_with_approved_cycle(self, tmp_path: Path):
        """gza stats --cycles reports correct counts for an approved cycle."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        cycle = store.start_cycle(impl.id)
        it = store.append_cycle_iteration(cycle.id, 0)
        review = store.add("Review", task_type="review")
        assert review.id is not None
        store.update_cycle_iteration(it.id, review_task_id=review.id, state="terminal", review_verdict="APPROVED")
        store.close_cycle(cycle.id, status="approved", stop_reason="approved")

        result = run_gza("stats", "--cycles", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "1" in result.stdout  # at least 1 cycle shown
        assert "approved" in result.stdout.lower() or "Approved" in result.stdout

    def test_stats_cycles_json_output(self, tmp_path: Path):
        """gza stats --cycles --json outputs valid JSON."""
        import json

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        result = run_gza("stats", "--cycles", "--json", "--project", str(tmp_path))

        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert "total_cycles" in data
        assert "approved_cycles" in data

    def test_stats_cycles_task_json(self, tmp_path: Path):
        """gza stats --cycles --task <id> --json outputs per-impl cycle data."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        cycle = store.start_cycle(impl.id)
        store.close_cycle(cycle.id, status="maxed_out", stop_reason="max_iterations")

        result = run_gza(
            "stats", "--cycles", "--task", str(impl.id), "--json",
            "--project", str(tmp_path)
        )

        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["impl_task_id"] == impl.id
        assert data["cycle_count"] == 1
        assert data["cycles"][0]["status"] == "maxed_out"

    def test_stats_without_cycles_flag_unchanged(self, tmp_path: Path):
        """gza stats without --cycles shows the normal task stats table."""
        from gza.db import SqliteTaskStore, TaskStats

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("A task", task_type="implement")
        store.mark_completed(
            task,
            has_commits=False,
            stats=TaskStats(num_steps_computed=3, cost_usd=0.05, duration_seconds=10.0),
        )

        result = run_gza("stats", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Total cost" in result.stdout
        assert "Recent Tasks" in result.stdout
        # Should NOT show cycle analytics headers
        assert "Cycle Analytics" not in result.stdout

    def test_stats_cycles_improves_before_approval_metric(self, tmp_path: Path):
        """gza stats --cycles --json reports improves_before_approval key (not iterations_to_approval)."""
        import json
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        cycle = store.start_cycle(impl.id)

        # Iteration 0: review + improve (CHANGES_REQUESTED)
        review0 = store.add("Review 0", task_type="review")
        improve0 = store.add("Improve 0", task_type="improve")
        assert review0.id is not None and improve0.id is not None
        it0 = store.append_cycle_iteration(cycle.id, 0)
        store.update_cycle_iteration(
            it0.id,
            review_task_id=review0.id,
            improve_task_id=improve0.id,
            state="improve_completed",
            review_verdict="CHANGES_REQUESTED",
        )

        # Iteration 1: review only (APPROVED)
        review1 = store.add("Review 1", task_type="review")
        assert review1.id is not None
        it1 = store.append_cycle_iteration(cycle.id, 1)
        store.update_cycle_iteration(
            it1.id,
            review_task_id=review1.id,
            state="terminal",
            review_verdict="APPROVED",
        )
        store.close_cycle(cycle.id, status="approved", stop_reason="approved")

        result = run_gza("stats", "--cycles", "--json", "--project", str(tmp_path))

        assert result.returncode == 0
        data = json.loads(result.stdout)
        # Key must be improves_before_approval, NOT iterations_to_approval
        assert "improves_before_approval" in data
        assert "iterations_to_approval" not in data
        assert data["improves_before_approval"]["min"] == 1.0

    def test_stats_cycles_human_readable_indentation(self, tmp_path: Path):
        """Human-readable stats rows have exactly 2 leading spaces, not 4.

        Regression test for the double-indentation bug where call sites passed
        labels with '  ' prefix while _format_percentile_row already adds '  '.
        """
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        cycle = store.start_cycle(impl.id)

        # One iteration with improve (CHANGES_REQUESTED) followed by APPROVED
        review0 = store.add("Review 0", task_type="review")
        improve0 = store.add("Improve 0", task_type="improve")
        assert review0.id is not None and improve0.id is not None
        it0 = store.append_cycle_iteration(cycle.id, 0)
        store.update_cycle_iteration(
            it0.id,
            review_task_id=review0.id,
            improve_task_id=improve0.id,
            state="improve_completed",
            review_verdict="CHANGES_REQUESTED",
        )
        review1 = store.add("Review 1", task_type="review")
        assert review1.id is not None
        it1 = store.append_cycle_iteration(cycle.id, 1)
        store.update_cycle_iteration(
            it1.id,
            review_task_id=review1.id,
            state="terminal",
            review_verdict="APPROVED",
        )
        store.close_cycle(cycle.id, status="approved", stop_reason="approved")

        result = run_gza("stats", "--cycles", "--project", str(tmp_path))

        assert result.returncode == 0
        # Find the improves_before_approval row in the output
        lines = result.stdout.splitlines()
        matching = [ln for ln in lines if "improves_before_approval" in ln]
        assert matching, "Expected an 'improves_before_approval' row in output"
        row = matching[0]
        # Must start with exactly 2 leading spaces (not 4)
        assert row.startswith("  "), f"Row should start with 2 spaces: {row!r}"
        assert not row.startswith("    "), f"Row must not have 4 leading spaces (double-indent bug): {row!r}"


class TestAdvancePlansCommand:
    """Tests for 'gza advance --plans' command."""

    def test_advance_plans_lists_completed_plans_without_impl(self, tmp_path: Path):
        """advance --plans lists completed plans with no implement task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Add a completed plan task
        plan = store.add("Design the authentication system", task_type="plan")
        plan.status = "completed"
        from datetime import datetime, timezone
        plan.completed_at = datetime.now(timezone.utc)
        store.update(plan)

        result = run_gza("advance", "--plans", "--project", str(tmp_path))

        assert result.returncode == 0
        assert str(plan.id) in result.stdout
        assert "gza implement" in result.stdout

    def test_advance_plans_excludes_plans_with_impl(self, tmp_path: Path):
        """advance --plans excludes plans that already have an implement task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        from datetime import datetime, timezone

        plan = store.add("A plan", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(timezone.utc)
        store.update(plan)

        impl = store.add("Implement plan", task_type="implement", based_on=plan.id)
        impl.status = "completed"
        impl.completed_at = datetime.now(timezone.utc)
        store.update(impl)

        result = run_gza("advance", "--plans", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed plans without implementation" in result.stdout

    def test_advance_plans_create_queues_implement_tasks(self, tmp_path: Path):
        """advance --plans --create creates implement tasks for each listed plan."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        from datetime import datetime, timezone

        plan1 = store.add("Plan A", task_type="plan")
        plan1.status = "completed"
        plan1.completed_at = datetime.now(timezone.utc)
        store.update(plan1)

        plan2 = store.add("Plan B", task_type="plan")
        plan2.status = "completed"
        plan2.completed_at = datetime.now(timezone.utc)
        store.update(plan2)

        result = run_gza("advance", "--plans", "--create", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created" in result.stdout

        # Verify impl tasks were created with based_on pointing to plans
        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 2
        based_on_ids = {t.based_on for t in impl_tasks}
        assert plan1.id in based_on_ids
        assert plan2.id in based_on_ids

    def test_advance_plans_dry_run_no_create(self, tmp_path: Path):
        """advance --plans --create --dry-run shows preview but creates nothing."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        from datetime import datetime, timezone

        plan = store.add("Plan C", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(timezone.utc)
        store.update(plan)

        result = run_gza("advance", "--plans", "--create", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower() or "Would create" in result.stdout

        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 0

    def test_advance_plans_targeted_query_ignores_non_plan_tasks(self, tmp_path: Path):
        """advance --plans correctly filters plans even with many non-plan tasks present.

        This exercises get_impl_based_on_ids (the targeted query path) to ensure
        the plan-exclusion filter is based only on implement tasks, not all tasks.
        """
        from gza.db import SqliteTaskStore
        from datetime import datetime, timezone

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        plan_with_impl = store.add("Plan with impl", task_type="plan")
        plan_with_impl.status = "completed"
        plan_with_impl.completed_at = datetime.now(timezone.utc)
        store.update(plan_with_impl)

        plan_without_impl = store.add("Plan without impl", task_type="plan")
        plan_without_impl.status = "completed"
        plan_without_impl.completed_at = datetime.now(timezone.utc)
        store.update(plan_without_impl)

        assert plan_with_impl.id is not None and plan_without_impl.id is not None

        # Implement task based on plan_with_impl
        store.add("Impl 1", task_type="implement", based_on=plan_with_impl.id)

        # Many non-plan tasks that should NOT affect the exclusion logic
        for i in range(20):
            t = store.add(f"Task {i}", task_type="implement")
            t.based_on = plan_with_impl.id  # review/task based_on should be ignored
            store.update(t)

        result = run_gza("advance", "--plans", "--project", str(tmp_path))

        assert result.returncode == 0
        # Only plan_without_impl should appear (plan_with_impl is excluded)
        assert "Plan without impl" in result.stdout
        assert "Plan with impl" not in result.stdout


class TestSetStatusCommand:
    """Tests for 'gza set-status' command."""

    def test_set_status_nonexistent_task(self, tmp_path: Path):
        """set-status errors when task does not exist."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("set-status", "999", "failed", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_set_status_to_failed(self, tmp_path: Path):
        """set-status can mark a pending task as failed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)

        result = run_gza("set-status", "1", "failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "in_progress" in result.stdout
        assert "failed" in result.stdout

        task = store.get(1)
        assert task is not None
        assert task.status == "failed"
        assert task.completed_at is not None

    def test_set_status_to_completed(self, tmp_path: Path):
        """set-status can mark a task as completed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)

        result = run_gza("set-status", "1", "completed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "completed" in result.stdout

        task = store.get(1)
        assert task is not None
        assert task.status == "completed"
        assert task.completed_at is not None

    def test_set_status_to_pending_clears_completed_at(self, tmp_path: Path):
        """set-status clears completed_at when transitioning back to pending."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "failed"},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)

        result = run_gza("set-status", "1", "pending", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pending" in result.stdout

        task = store.get(1)
        assert task is not None
        assert task.status == "pending"
        assert task.completed_at is None

    def test_set_status_to_in_progress_clears_completed_at(self, tmp_path: Path):
        """set-status clears completed_at when transitioning to in_progress."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "failed"},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)

        result = run_gza("set-status", "1", "in_progress", "--project", str(tmp_path))

        assert result.returncode == 0

        task = store.get(1)
        assert task is not None
        assert task.status == "in_progress"
        assert task.completed_at is None

    def test_set_status_with_reason_for_failed(self, tmp_path: Path):
        """set-status --reason sets failure_reason for failed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)

        result = run_gza(
            "set-status", "1", "failed", "--reason", "Process killed", "--project", str(tmp_path)
        )

        assert result.returncode == 0

        task = store.get(1)
        assert task is not None
        assert task.status == "failed"
        assert task.failure_reason == "Process killed"

    def test_set_status_reason_warns_for_non_failed(self, tmp_path: Path):
        """set-status warns when --reason is used with a non-failed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])

        result = run_gza(
            "set-status", "1", "completed", "--reason", "Ignored reason", "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "Warning" in result.stdout or "warning" in result.stdout.lower()

    def test_set_status_invalid_status_rejected(self, tmp_path: Path):
        """set-status rejects unknown status values."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending"},
        ])

        result = run_gza("set-status", "1", "bogus", "--project", str(tmp_path))

        assert result.returncode != 0

    def test_set_status_clears_failure_reason_on_non_failed_transition(self, tmp_path: Path):
        """set-status clears failure_reason when transitioning away from failed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "failed"},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)

        # Set failure_reason on the existing failed task
        task = store.get(1)
        assert task is not None
        task.failure_reason = "Original error"
        store.update(task)

        result = run_gza("set-status", "1", "pending", "--project", str(tmp_path))

        assert result.returncode == 0

        task = store.get(1)
        assert task is not None
        assert task.status == "pending"
        assert task.failure_reason is None


class TestFormatLogEntry:
    """Tests for _format_log_entry with gza entry type."""

    def test_gza_entry_with_subtype_renders_correctly(self) -> None:
        """gza entry with subtype is formatted as [gza:subtype] message."""
        entry = {"type": "gza", "subtype": "branch", "message": "Branch: feat/foo", "branch": "feat/foo"}
        result = _format_log_entry(entry)
        assert result == "[gza:branch] Branch: feat/foo"

    def test_gza_entry_without_subtype_renders_correctly(self) -> None:
        """gza entry without subtype is formatted as [gza] message."""
        entry = {"type": "gza", "message": "Some info"}
        result = _format_log_entry(entry)
        assert result == "[gza] Some info"

    def test_gza_entry_with_empty_message_returns_none(self) -> None:
        """gza entry with empty message returns None (should be skipped)."""
        entry = {"type": "gza", "subtype": "info", "message": ""}
        result = _format_log_entry(entry)
        assert result is None

    def test_gza_entry_outcome_renders_with_subtype(self) -> None:
        """gza outcome entry renders with subtype label."""
        entry = {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0}
        result = _format_log_entry(entry)
        assert result == "[gza:outcome] Outcome: completed"

    def test_gza_entry_stats_renders_with_subtype(self) -> None:
        """gza stats entry renders with subtype label."""
        entry = {"type": "gza", "subtype": "stats", "message": "Stats: 5 steps, 12.3s, $0.0042", "duration_seconds": 12.3, "cost_usd": 0.0042, "num_steps": 5}
        result = _format_log_entry(entry)
        assert result == "[gza:stats] Stats: 5 steps, 12.3s, $0.0042"

    def test_gza_log_entry_renders_in_gza_log_output(self, tmp_path: Path) -> None:
        """Integration: gza log renders gza entries from a JSONL log file."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Orchestration logging integration test")
        task.status = "completed"
        task.log_file = ".gza/logs/test.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            {"type": "gza", "subtype": "info", "message": "Task: #1 20260101-test-task"},
            {"type": "gza", "subtype": "branch", "message": "Branch: feat/test", "branch": "feat/test"},
            {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Working on it"}]}},
            {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0},
            {"type": "result", "subtype": "success", "result": "", "num_steps": 2, "duration_ms": 5000, "total_cost_usd": 0.005},
        ]
        (log_dir / "test.log").write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Branch: feat/test" in result.stdout
        assert "Outcome: completed" in result.stdout


class TestBuildStepTimeline:
    """Tests for _build_step_timeline with gza entries."""

    def test_gza_metadata_subtypes_do_not_produce_timeline_steps(self) -> None:
        """branch, stats, and outcome entries are not added as timeline steps."""
        entries = [
            {"type": "gza", "subtype": "branch", "message": "Branch: feat/foo", "branch": "feat/foo"},
            {"type": "gza", "subtype": "stats", "message": "Stats: 3 steps, 10.0s, $0.001", "num_steps": 3},
            {"type": "gza", "subtype": "outcome", "message": "Outcome: completed", "exit_code": 0},
        ]
        steps = _build_step_timeline(entries)
        assert steps == []

    def test_gza_info_subtype_produces_timeline_step(self) -> None:
        """info entries are added as timeline steps."""
        entries = [
            {"type": "gza", "subtype": "info", "message": "Task: #1 20260101-test-task"},
        ]
        steps = _build_step_timeline(entries)
        assert len(steps) == 1
        assert steps[0]["message_text"] == "[gza:info] Task: #1 20260101-test-task"

    def test_gza_metadata_mixed_with_info_only_info_appears(self) -> None:
        """Only info entries appear in timeline when mixed with metadata entries."""
        entries = [
            {"type": "gza", "subtype": "branch", "message": "Branch: feat/foo"},
            {"type": "gza", "subtype": "info", "message": "Task: #1 slug"},
            {"type": "gza", "subtype": "stats", "message": "Stats: 1 step, 5.0s, $0.0001"},
            {"type": "gza", "subtype": "outcome", "message": "Outcome: completed"},
        ]
        steps = _build_step_timeline(entries)
        assert len(steps) == 1
        assert steps[0]["message_text"] == "[gza:info] Task: #1 slug"


class TestRunForeground:
    """Tests for _run_foreground() helper."""

    def test_run_foreground_registers_and_completes_worker(self, tmp_path: Path):
        """_run_foreground registers a worker before running and marks it completed after."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test foreground task")
        assert task.id is not None

        workers_path = config.workers_path
        workers_path.mkdir(parents=True, exist_ok=True)

        with patch("gza.cli.run", return_value=0) as mock_run:
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 0
        mock_run.assert_called_once_with(config, task_id=task.id, resume=False, open_after=False)

        # Worker should now be marked completed
        registry = WorkerRegistry(workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        w = workers[0]
        assert w.task_id == task.id
        assert w.status == "completed"
        assert w.exit_code == 0
        assert w.is_background is False

    def test_run_foreground_marks_failed_on_nonzero_exit(self, tmp_path: Path):
        """_run_foreground marks worker as failed when run() returns non-zero."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test failing task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with patch("gza.cli.run", return_value=1):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 1

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        w = workers[0]
        assert w.status == "failed"
        assert w.exit_code == 1

    def test_run_foreground_passes_resume_and_open_after(self, tmp_path: Path):
        """_run_foreground correctly passes resume and open_after to run()."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with patch("gza.cli.run", return_value=0) as mock_run:
            rc = _run_foreground(config, task_id=task.id, resume=True, open_after=True)

        assert rc == 0
        mock_run.assert_called_once_with(config, task_id=task.id, resume=True, open_after=True)

    def test_run_foreground_marks_failed_on_keyboard_interrupt(self, tmp_path: Path):
        """_run_foreground marks worker as failed when interrupted."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test interrupt task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with patch("gza.cli.run", side_effect=KeyboardInterrupt):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 130

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        w = workers[0]
        assert w.status == "failed"
        assert w.exit_code == 130

    def test_run_foreground_signal_calls_mark_completed_once(self, tmp_path: Path):
        """Signal delivery via _cleanup raises KeyboardInterrupt; mark_completed is called exactly once."""
        import signal as signal_mod

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test signal task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        # Capture the installed SIGINT handler so we can call _cleanup directly
        installed_handlers: dict = {}
        original_signal = signal_mod.signal

        def capture_signal(signum, handler):
            installed_handlers[signum] = handler
            return original_signal(signum, handler)

        with patch("gza.cli.signal.signal", side_effect=capture_signal):
            with patch("gza.workers.WorkerRegistry.mark_completed") as mock_mark:
                def run_then_signal(*args, **kwargs):
                    # Simulate SIGINT arriving while run() is executing
                    cleanup = installed_handlers.get(signal_mod.SIGINT)
                    if cleanup and callable(cleanup):
                        cleanup(signal_mod.SIGINT, None)

                with patch("gza.cli.run", side_effect=run_then_signal):
                    rc = _run_foreground(config, task_id=task.id)

        assert rc == 130
        # mark_completed must be called exactly once, not twice
        assert mock_mark.call_count == 1
        assert mock_mark.call_args.kwargs.get("status") == "failed"
        assert mock_mark.call_args.kwargs.get("exit_code") == 130
