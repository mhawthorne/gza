"""Tests for task query and display CLI commands."""


import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from .conftest import run_gza, setup_config, setup_db_with_tasks, setup_unmerged_env, LOG_FIXTURES_DIR


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
        """--days excludes old tasks and includes recent ones."""
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

        result = run_gza("history", "--days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recent task" in result.stdout
        assert "Old task" not in result.stdout

    def test_history_lineage_depth(self, tmp_path: Path):
        """--lineage-depth shows a branch-rendered tree."""
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
        assert "Parent task" in result.stdout
        assert "Child task" in result.stdout
        assert "└──" in result.stdout or "├──" in result.stdout

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

    def test_history_lineage_orders_completed_root_before_pending_descendants(self, tmp_path: Path):
        """Lineage rendering keeps ancestor-first order even when descendants are pending."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Root completed task", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
        store.update(root)

        child = store.add("Child pending task", task_type="implement", based_on=root.id)
        grandchild = store.add("Grandchild pending task", task_type="implement", based_on=child.id)
        assert child.id is not None
        assert grandchild.id is not None

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        root_idx = result.stdout.index("Root completed task")
        child_idx = result.stdout.index("Child pending task")
        grandchild_idx = result.stdout.index("Grandchild pending task")
        assert root_idx < child_idx < grandchild_idx

    def test_history_incomplete_with_lookback(self, tmp_path: Path):
        """--incomplete combined with --days applies both filters."""
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
            "history", "--incomplete", "--days", "7",
            "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "Recent failed" in result.stdout
        assert "Old failed" not in result.stdout
        assert "Recent merged" not in result.stdout

    def test_history_last_flag(self, tmp_path: Path):
        """--last limits the number of tasks shown."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task 1", "status": "completed"},
            {"prompt": "Task 2", "status": "completed"},
            {"prompt": "Task 3", "status": "completed"},
        ])

        result = run_gza("history", "--last", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        # Exactly 2 tasks shown (the 2 most recent)
        assert result.stdout.count("Task ") == 2

    def test_history_n_shorthand(self, tmp_path: Path):
        """-n is shorthand for --last."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task A", "status": "completed"},
            {"prompt": "Task B", "status": "completed"},
            {"prompt": "Task C", "status": "completed"},
        ])

        result = run_gza("history", "-n", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert result.stdout.count("Task ") == 1

    def test_history_start_date(self, tmp_path: Path):
        """--start-date excludes tasks before the given date."""
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

        # Use a date 7 days ago as start date
        start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        result = run_gza("history", "--start-date", start, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Recent task" in result.stdout
        assert "Old task" not in result.stdout

    def test_history_end_date(self, tmp_path: Path):
        """--end-date excludes tasks after the given date."""
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

        # Use a date 7 days ago as end date — only old task should appear
        end = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        result = run_gza("history", "--end-date", end, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Old task" in result.stdout
        assert "Recent task" not in result.stdout


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

    def test_show_indicates_worker_startup_failure(self, tmp_path: Path):
        """Show surfaces startup failure when worker failed before main log existed."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Task with startup failure")
        assert task.id is not None
        task.status = "failed"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-20260318-startup-failure",
                pid=12345,
                task_id=task.id,
                task_slug=task.task_id,
                started_at="2026-03-18T00:00:00+00:00",
                status="failed",
                log_file=None,
                worktree=None,
                is_background=True,
                startup_log_file=".gza/workers/w-20260318-startup-failure-startup.log",
            )
        )

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Worker Failure: failed during startup" in result.stdout
        assert "Startup Log: .gza/workers/w-20260318-startup-failure-startup.log" in result.stdout

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

    def test_show_lineage_orders_completed_root_before_pending_descendants(self, tmp_path: Path):
        """Show lineage keeps root first even when downstream tasks are still pending."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        root = store.add("Root task", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
        store.update(root)

        child = store.add("Child task", task_type="implement", based_on=root.id)
        grandchild = store.add("Grandchild task", task_type="implement", based_on=child.id)

        result = run_gza("show", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        root_idx = result.stdout.index(f"#{root.id}")
        child_idx = result.stdout.index(f"#{child.id}")
        grandchild_idx = result.stdout.index(f"#{grandchild.id}")
        assert root_idx < child_idx < grandchild_idx

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
        result = run_gza("ps", "--project", str(tmp_path))

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

        result = run_gza("ps", "--json", "--project", str(tmp_path))
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

        result = run_gza("ps", "--json", "--project", str(tmp_path))
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

        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "2026-01-08 00:00:00 UTC" in result.stdout

        registry.remove("w-test-start-format")

    def test_ps_quiet_shows_only_task_ids(self, tmp_path: Path):
        """PS quiet output should include task IDs (not worker IDs)."""
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Task with an associated worker.
        task = store.add("Task with worker")
        store.mark_in_progress(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-quiet",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--quiet", "--project", str(tmp_path))
        assert result.returncode == 0
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        assert lines == [str(task.id)]

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

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["source"] == "both"
        assert rows[0]["is_stale"] is True
        assert rows[0]["is_orphaned"] is True
        assert "stale" in rows[0]["flags"]
        assert "orphaned" in rows[0]["flags"]

        registry.remove("w-test-stale-ps")

    def test_ps_marks_startup_failure_for_failed_worker_without_main_log(self, tmp_path: Path):
        """PS marks startup failures in table and JSON output."""
        import json
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-startup-ps",
                pid=99999,
                task_id=None,
                task_slug="startup-failed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
                startup_log_file=".gza/workers/w-test-startup-ps-startup.log",
            )
        )

        table_result = run_gza("ps", "--project", str(tmp_path))
        assert table_result.returncode == 0
        assert "failed(startup)" in table_result.stdout

        json_result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert json_result.returncode == 0
        rows = json.loads(json_result.stdout)
        assert len(rows) == 1
        assert rows[0]["startup_failure"] is True
        assert rows[0]["startup_log_file"] == ".gza/workers/w-test-startup-ps-startup.log"

        registry.remove("w-test-startup-ps")

    def test_ps_default_includes_startup_failure_but_filters_other_terminal_rows(self, tmp_path: Path):
        """Default ps keeps startup failures visible while filtering other terminal rows."""
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-running",
                pid=99998,
                task_id=None,
                task_slug="running-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-startup-failed",
                pid=99997,
                task_id=None,
                task_slug="startup-failed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
                startup_log_file=".gza/workers/w-test-ps-startup-failed-startup.log",
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-failed",
                pid=99996,
                task_id=None,
                task_slug="ordinary-failed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-test-ps-completed",
                pid=99995,
                task_id=None,
                task_slug="completed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(timezone.utc).isoformat(),
            )
        )

        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "running-worker" in result.stdout
        assert "failed(startup)" in result.stdout
        assert "ordinary-failed-worker" not in result.stdout
        assert "completed-worker" not in result.stdout

        registry.remove("w-test-ps-running")
        registry.remove("w-test-ps-startup-failed")
        registry.remove("w-test-ps-failed")
        registry.remove("w-test-ps-completed")

    def test_ps_all_flag_includes_completed_and_failed_rows(self, tmp_path: Path):
        """ps --all includes ordinary completed/failed rows that default ps filters out."""
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        registry.register(
            WorkerMetadata(
                worker_id="w-all-running",
                pid=99998,
                task_id=None,
                task_slug="running-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-all-failed",
                pid=99996,
                task_id=None,
                task_slug="ordinary-failed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
            )
        )
        registry.register(
            WorkerMetadata(
                worker_id="w-all-completed",
                pid=99995,
                task_id=None,
                task_slug="completed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(timezone.utc).isoformat(),
            )
        )

        # Default ps: filters out ordinary completed/failed
        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "running-worker" in result.stdout
        assert "ordinary-failed-worker" not in result.stdout
        assert "completed-worker" not in result.stdout

        # ps --all: includes everything
        result_all = run_gza("ps", "--all", "--project", str(tmp_path))
        assert result_all.returncode == 0
        assert "running-worker" in result_all.stdout
        assert "ordinary-failed-worker" in result_all.stdout
        assert "completed-worker" in result_all.stdout

        # status --all (alias) also works
        result_status = run_gza("status", "--all", "--project", str(tmp_path))
        assert result_status.returncode == 0
        assert "completed-worker" in result_status.stdout

        registry.remove("w-all-running")
        registry.remove("w-all-failed")
        registry.remove("w-all-completed")

    def test_ps_all_json_includes_terminal_rows(self, tmp_path: Path):
        """ps --all --json includes completed/failed workers in JSON output."""
        import json as json_lib
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        registry.register(
            WorkerMetadata(
                worker_id="w-json-completed",
                pid=99994,
                task_id=None,
                task_slug="json-completed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(timezone.utc).isoformat(),
            )
        )

        result = run_gza("ps", "--all", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        data = json_lib.loads(result.stdout)
        slugs = [r["task"] for r in data]
        assert any("json-completed-worker" in s for s in slugs)

        registry.remove("w-json-completed")

    def test_print_ps_output_poll_adopts_first_seen_startup_failure(self, tmp_path: Path, capsys):
        """Poll path keeps startup-failed workers visible on first observation."""
        import argparse
        from gza.cli import _print_ps_output
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-startup-poll",
                pid=99999,
                task_id=None,
                task_slug="startup-failed-worker",
                started_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                log_file=None,
                worktree=None,
                startup_log_file=".gza/workers/w-test-startup-poll-startup.log",
            )
        )

        args = argparse.Namespace(quiet=False, json=True)
        seen_tasks: dict[str, dict] = {}
        _print_ps_output(args, registry, store, seen_tasks=seen_tasks)

        captured = capsys.readouterr()
        assert '"worker_id": "w-test-startup-poll"' in captured.out
        assert '"status": "failed"' in captured.out
        assert '"startup_failure": true' in captured.out
        assert "w-test-startup-poll" in seen_tasks

        registry.remove("w-test-startup-poll")

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

        table_result = run_gza("ps", "--project", str(tmp_path))
        assert table_result.returncode == 0
        assert "standalone-worker" in table_result.stdout
        assert " - " in table_result.stdout

        json_result = run_gza("ps", "--json", "--project", str(tmp_path))
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

        result = run_gza("ps", "--json", "--project", str(tmp_path))
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

    def test_ps_poll_keeps_completed_tasks_visible(self, tmp_path: Path):
        """Poll mode keeps completed tasks visible so users see transitions.

        Workers that transition to completed remain in the display (via
        seen_tasks) instead of vanishing. The poll loop continues until
        interrupted with Ctrl+C.
        """
        import argparse
        import json
        import os
        import unittest.mock as mock
        from gza.cli import cmd_ps
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        worker = WorkerMetadata(
            worker_id="w-test-transition",
            pid=os.getpid(),
            task_id=None,
            task_slug=None,
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(worker)

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # Transition worker to completed during the sleep after poll 1.
                worker.status = "completed"
                registry.update(worker)
            elif sleep_count >= 2:
                # Stop after seeing the completed state
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        # Find JSON outputs (lines that start with '[') and parse them.
        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 2, f"Expected at least 2 JSON snapshots, got: {json_outputs}"

        first_snapshot = json.loads(json_outputs[0])
        assert len(first_snapshot) == 1
        assert first_snapshot[0]["status"] == "running"

        # Second poll: completed worker remains visible (not filtered out).
        second_snapshot = json.loads(json_outputs[1])
        assert len(second_snapshot) == 1
        assert second_snapshot[0]["status"] == "completed"

        registry.remove("w-test-transition")

    def test_ps_poll_shows_tasks_from_start_to_end(self, tmp_path: Path):
        """Poll mode tracks task lifecycle via DB status alone (workerless tasks).

        Scenario:
        - 3 tasks: pending (#1), in_progress (#2), completed (#3)
        - Poll 1: only #2 (in_progress) is visible
        - Transition #1 to in_progress during sleep
        - Poll 2: #1 and #2 visible (2 in_progress tasks)
        - Transition #2 to completed during sleep
        - Poll 3: #1 (in_progress) and #2 (completed, still visible)
        - Transition #1 to completed during sleep
        - Poll 4: both completed, poll continues until Ctrl+C
        """
        import argparse
        import json
        import unittest.mock as mock
        from gza.cli import cmd_ps
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create 3 tasks with different initial states
        task_pending = store.add("Pending task")
        task_running = store.add("Running task")
        task_completed = store.add("Already completed task")

        store.mark_in_progress(task_running)
        store.mark_in_progress(task_completed)
        store.mark_completed(task_completed)

        # Workers dir must exist for WorkerRegistry even though we don't use workers
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # After poll 1: start the pending task
                store.mark_in_progress(task_pending)
            elif sleep_count == 2:
                # After poll 2: complete the originally running task
                store.mark_completed(task_running)
            elif sleep_count == 3:
                # After poll 3: complete the other task too
                store.mark_completed(task_pending)
            elif sleep_count >= 4:
                # All transitions done — stop the poll loop
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 4, f"Expected at least 4 JSON snapshots, got {len(json_outputs)}: {json_outputs}"

        # Poll 1: only the in_progress task
        snap1 = json.loads(json_outputs[0])
        assert len(snap1) == 1, f"Poll 1: expected 1 task, got {len(snap1)}: {snap1}"
        assert snap1[0]["status"] == "in_progress"

        # Poll 2: 2 in_progress tasks (original + newly started)
        snap2 = json.loads(json_outputs[1])
        assert len(snap2) == 2, f"Poll 2: expected 2 tasks, got {len(snap2)}: {snap2}"
        statuses2 = {r["status"] for r in snap2}
        assert statuses2 == {"in_progress"}, f"Poll 2: expected all in_progress, got {statuses2}"

        # Poll 3: 1 in_progress + 1 completed (completed stays visible)
        snap3 = json.loads(json_outputs[2])
        assert len(snap3) == 2, f"Poll 3: expected 2 tasks, got {len(snap3)}: {snap3}"
        statuses3 = sorted(r["status"] for r in snap3)
        assert statuses3 == ["completed", "in_progress"], f"Poll 3: expected completed+in_progress, got {statuses3}"

        # Poll 4: both completed, poll continues until interrupted
        snap4 = json.loads(json_outputs[3])
        assert len(snap4) == 2, f"Poll 4: expected 2 tasks, got {len(snap4)}: {snap4}"
        statuses4 = {r["status"] for r in snap4}
        assert statuses4 == {"completed"}, f"Poll 4: expected all completed, got {statuses4}"

    def test_ps_poll_detects_completion_with_workers(self, tmp_path: Path):
        """Poll mode detects task completion even when workers are present.

        When a task has a worker registered, it appears in live_rows via the
        worker loop (not just get_in_progress). The poll must still detect
        when the DB task status transitions to completed, even though the
        task never "vanishes" from live_rows.

        Scenario:
        - 2 tasks running with workers (task_id set on worker)
        - Poll 1: both show as running
        - Both tasks complete in DB during sleep
        - Poll 2: both show as completed (not running)
        """
        import argparse
        import json
        import os
        import unittest.mock as mock
        from gza.cli import cmd_ps
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        # Create 2 in_progress tasks
        task1 = store.add("Internal task 1", task_type="internal")
        task2 = store.add("Internal task 2", task_type="internal")
        store.mark_in_progress(task1)
        store.mark_in_progress(task2)

        # Register workers for both tasks (simulates gza work running them)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        w1 = WorkerMetadata(
            worker_id="w-learn-1",
            pid=os.getpid(),
            task_id=task1.id,
            task_slug=None,
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        w2 = WorkerMetadata(
            worker_id="w-learn-2",
            pid=os.getpid(),
            task_id=task2.id,
            task_slug=None,
            started_at=datetime.now(timezone.utc).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(w1)
        registry.register(w2)

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # After poll 1: both tasks complete in DB
                store.mark_completed(task1)
                store.mark_completed(task2)
            elif sleep_count >= 2:
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 2, f"Expected at least 2 JSON snapshots, got {len(json_outputs)}"

        # Poll 1: both tasks running
        snap1 = json.loads(json_outputs[0])
        assert len(snap1) == 2, f"Poll 1: expected 2 tasks, got {len(snap1)}: {snap1}"
        statuses1 = {r["status"] for r in snap1}
        assert statuses1 == {"running"}, f"Poll 1: expected all running, got {statuses1}"

        # Poll 2: both tasks completed (DB is source of truth)
        snap2 = json.loads(json_outputs[1])
        assert len(snap2) == 2, f"Poll 2: expected 2 tasks, got {len(snap2)}: {snap2}"
        statuses2 = {r["status"] for r in snap2}
        assert statuses2 == {"completed"}, f"Poll 2: expected all completed, got {statuses2}"

        # Cleanup
        registry.remove("w-learn-1")
        registry.remove("w-learn-2")

    def test_ps_poll_shows_steps_for_completed_task(self, tmp_path: Path):
        """STEPS column shows num_steps_computed for a completed task in poll mode."""
        import argparse
        import json
        import os
        import unittest.mock as mock
        from gza.cli import cmd_ps
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerRegistry, WorkerMetadata

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task with steps")
        store.mark_in_progress(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-steps-poll",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        sleep_count = 0

        def fake_sleep(n: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 1:
                # Complete the task with num_steps_computed
                task.num_steps_computed = 7
                store.mark_completed(task)
                w = registry.get("w-test-steps-poll")
                w.status = "completed"
                registry.update(w)
            else:
                raise KeyboardInterrupt

        args = argparse.Namespace(
            project_dir=tmp_path,
            quiet=False,
            json=True,
            poll=2,
        )

        captured_outputs: list[str] = []
        original_print = print

        def capturing_print(*a, **kw):
            if a:
                captured_outputs.append(str(a[0]))
            original_print(*a, **kw)

        with mock.patch("builtins.print", side_effect=capturing_print):
            with mock.patch("time.sleep", side_effect=fake_sleep):
                result = cmd_ps(args)

        assert result == 0

        json_outputs = [o for o in captured_outputs if o.startswith("[")]
        assert len(json_outputs) >= 2, f"Expected at least 2 snapshots, got {len(json_outputs)}"

        # Second poll: task is completed and should show num_steps_computed
        snap2 = json.loads(json_outputs[1])
        assert len(snap2) == 1
        assert snap2[0]["status"] == "completed"
        assert snap2[0]["steps"] == "7"

        registry.remove("w-test-steps-poll")

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

        result = run_gza("ps", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        rows = json.loads(result.stdout)
        assert len(rows) == 1
        assert rows[0]["steps"] == "3"

        registry.remove("w-test-steps-live")


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


class TestUnmergedReviewStatus:
    """Tests for review status display in 'gza unmerged' command."""

    @pytest.mark.parametrize(
        "review_output, expected_text",
        [
            ("# Review\n\nCode looks good!\n\n**Verdict: APPROVED**", "✓ approved"),
            ("# Review\n\nNeeds some fixes.\n\nVerdict: CHANGES_REQUESTED", "⚠ changes requested"),
            ("# Review\n\nThis requires team discussion.\n\n**Verdict: NEEDS_DISCUSSION**", "💬 needs discussion"),
        ],
        ids=["approved", "changes_requested", "needs_discussion"],
    )
    def test_unmerged_shows_review_verdict(self, tmp_path: Path, review_output, expected_text):
        """Unmerged output shows the correct review verdict."""
        store, task, git = setup_unmerged_env(tmp_path)

        review = store.add("Review implementation", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(timezone.utc)
        review.depends_on = task.id
        review.task_id = "20260212-review-implementation"
        review.output_content = review_output
        store.update(review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert expected_text in result.stdout

    def test_unmerged_without_review_shows_no_status(self, tmp_path: Path):
        """Unmerged output shows no review status when no review exists."""
        store, task, git = setup_unmerged_env(tmp_path)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review: no review" in result.stdout
        assert "approved" not in result.stdout
        assert "changes requested" not in result.stdout
        assert "needs discussion" not in result.stdout

    def test_unmerged_uses_most_recent_review(self, tmp_path: Path):
        """Unmerged output shows status from most recent review."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

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
        import time

        store, task, git = setup_unmerged_env(tmp_path)

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
        import time

        store, task, git = setup_unmerged_env(tmp_path)

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
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Simplify mixer",
            task_id="20260225-simplify-mixer-by-removing-the-people-strategy",
        )

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
        import time

        store, task, git = setup_unmerged_env(tmp_path)

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
        import time

        store, task, git = setup_unmerged_env(tmp_path)

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
        import time

        store, impl, git = setup_unmerged_env(tmp_path)

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


class TestUnmergedAllFlag:
    """Tests for 'gza unmerged --all' flag."""

    def test_all_flag_excludes_failed_tasks(self, tmp_path: Path):
        """Failed tasks are excluded from gza unmerged (only completed tasks shown)."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Failed but useful task",
            task_id="20260220-failed-task",
            branch="feature/failed-branch",
            status="failed",
            has_commits=False,
        )

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
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Unmerged task",
            task_id="20260220-unmerged",
            branch="feature/unmerged",
        )

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
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="No commits task",
            task_id="20260220-no-commits",
            branch="feature/no-commits",
            has_commits=False,
            merge_status=None,
        )

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

        # We need a git repo but the task's branch won't exist
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

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
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Old task needing migration",
            task_id="20260220-old-task",
            branch="feature/old-task",
            merge_status=None,
        )

        git._run("checkout", "feature/old-task")
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

    def test_unmerged_excludes_failed_tasks(self, tmp_path: Path):
        """Failed tasks with merge_status='unmerged' are excluded from unmerged output."""
        from gza.db import SqliteTaskStore

        # Use setup_unmerged_env for the completed task (creates config, git, store)
        store, completed_task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Completed task",
            branch="feature/completed",
        )

        # Add a failed task to the same store
        failed_task = store.add("Failed task", task_type="implement")
        failed_task.status = "failed"
        failed_task.branch = "feature/failed"
        failed_task.merge_status = "unmerged"
        failed_task.completed_at = datetime.now(timezone.utc)
        store.update(failed_task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Completed task" in result.stdout
        assert "Failed task" not in result.stdout

    def test_unmerged_shows_diff_stats(self, tmp_path: Path):
        """Unmerged output shows diff stats (files, LOC added/removed)."""
        store, task, git = setup_unmerged_env(tmp_path)

        # Add more content on the feature branch for visible diff stats
        git._run("checkout", "feature/test")
        (tmp_path / "feature.txt").write_text("line1\nline2\nline3\n")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Expand feature content")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Diff stats should be shown in branch line
        assert "LOC" in result.stdout
        assert "files" in result.stdout

    def test_unmerged_uses_cached_diff_stats(self, tmp_path: Path):
        """gza unmerged uses cached diff stats from DB when available (no live git call)."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Cached stats task",
            branch="feature/cached",
        )

        # Pre-populate cached diff stats
        task.diff_files_changed = 5
        task.diff_lines_added = 42
        task.diff_lines_removed = 7
        store.update(task)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Cached stats should be displayed in +N/-N LOC, N files format
        assert "+42/-7 LOC, 5 files" in result.stdout

    def test_unmerged_review_shown_on_own_line(self, tmp_path: Path):
        """Review status appears on its own 'review:' line."""
        store, task, git = setup_unmerged_env(tmp_path)

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
        store, task, git = setup_unmerged_env(tmp_path)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review:" in result.stdout
        assert "no review" in result.stdout

    def test_unmerged_always_shows_completion_time(self, tmp_path: Path):
        """Completion time is shown even for tasks with improve tasks."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Root task",
        )

        # Override completion time to a specific date
        task.completed_at = datetime(2026, 2, 12, 10, 30, tzinfo=timezone.utc)
        store.update(task)

        improve = store.add("Improve root task", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime.now(timezone.utc)
        improve.branch = "feature/test"
        improve.based_on = task.id
        improve.merge_status = "unmerged"
        store.update(improve)

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


class TestStopCommandSafety:
    """Safety tests for stopping workers."""

    def test_stop_refuses_non_running_worker_even_if_pid_is_live(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """A non-running worker record should never be signaled."""
        from gza.cli.query import cmd_stop
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-stop-safety",
                task_id=None,
                pid=os.getpid(),
                status="completed",
            )
        )

        args = argparse.Namespace(
            project_dir=tmp_path,
            worker_id="w-stop-safety",
            all=False,
            force=False,
        )
        with patch("gza.cli.query.WorkerRegistry.stop") as mock_stop:
            rc = cmd_stop(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert "Refusing to stop worker w-stop-safety" in captured.out
        mock_stop.assert_not_called()
