"""Tests for task query and display CLI commands."""


import argparse
import json
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from .conftest import (
    make_store,
    run_gza,
    setup_config,
    setup_db_with_tasks,
    setup_git_repo_with_task_branch,
    setup_unmerged_env,
)


def _mock_unmerged_git() -> MagicMock:
    git = MagicMock()
    git.default_branch.return_value = "main"
    git.current_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.count_commits_ahead.return_value = 1
    git.get_diff_stat_parsed.return_value = (1, 1, 0)
    git.can_merge.return_value = True
    git.is_merged.return_value = False
    return git


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

    def test_history_excludes_internal_tasks_by_default(self, tmp_path: Path):
        """Default history output omits internal tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Internal task", "status": "completed", "task_type": "internal"},
        ])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement task" in result.stdout
        assert "Internal task" not in result.stdout

    def test_history_internal_type_includes_internal_tasks(self, tmp_path: Path):
        """Explicit --type internal includes internal tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Internal task", "status": "completed", "task_type": "internal"},
        ])

        result = run_gza("history", "--type", "internal", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Internal task" in result.stdout
        assert "Implement task" not in result.stdout

    def test_history_shows_task_type_labels(self, tmp_path: Path):
        """History command displays task type labels for all task types."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Implement task", "status": "completed", "task_type": "implement"},
            {"prompt": "Plan task", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        # Type labels are now on a separate line
        assert "Implement task" in result.stdout
        assert "[implement]" in result.stdout
        assert "Plan task" in result.stdout
        assert "[plan]" in result.stdout

    def test_history_shows_orphaned_tasks_at_top(self, tmp_path: Path):
        """History command includes orphaned in-progress tasks at the top."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.config import Config
        config = Config.load(tmp_path)
        store = SqliteTaskStore(db_path, prefix=config.project_prefix)

        # Create an orphaned (in-progress, no worker) task
        orphaned_task = store.add("Orphaned task needing attention")
        store.mark_in_progress(orphaned_task)

        # Create a completed task
        completed_task = store.add("Completed task")
        completed_task.status = "completed"
        completed_task.completed_at = datetime.now(UTC)
        store.update(completed_task)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout
        # Prompt may be truncated — assert a prefix short enough to survive
        # layout changes as task IDs widen (e.g. the v25 padding to width 6).
        assert "Orphaned task" in result.stdout
        assert "Completed task" in result.stdout
        # Orphaned should appear before completed in output
        orphaned_pos = result.stdout.find("Orphaned task")
        completed_pos = result.stdout.find("Completed task")
        assert orphaned_pos < completed_pos, "Orphaned task should appear before completed task"

    def test_history_orphaned_shows_resume_suggestion(self, tmp_path: Path):
        """History command shows resume suggestion for orphaned tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        orphaned_task = store.add("Orphaned task")
        store.mark_in_progress(orphaned_task)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"gza work {orphaned_task.id}" in result.stdout

    def test_history_no_orphaned_when_status_filter_set(self, tmp_path: Path):
        """History command does not show orphaned tasks when --status filter is active."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create an orphaned task
        orphaned_task = store.add("Orphaned task")
        store.mark_in_progress(orphaned_task)

        # Create a completed task
        completed_task = store.add("Completed task")
        completed_task.status = "completed"
        completed_task.completed_at = datetime.now(UTC)
        store.update(completed_task)

        result = run_gza("history", "--status", "completed", "--project", str(tmp_path))

        assert result.returncode == 0
        # Orphaned should NOT appear when a status filter is specified
        assert "orphaned" not in result.stdout
        assert "Completed task" in result.stdout

    def test_history_incomplete_flag(self, tmp_path: Path):
        """--incomplete shows failed/unmerged tasks but not completed+merged ones."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Failed task (should appear)
        failed = store.add("Failed task")
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        # Completed + merged (should NOT appear)
        merged = store.add("Merged task")
        merged.status = "completed"
        merged.merge_status = "merged"
        merged.completed_at = datetime.now(UTC)
        store.update(merged)

        result = run_gza("history", "--incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failed task" in result.stdout
        assert "Merged task" not in result.stdout

    def test_history_lookback_days(self, tmp_path: Path):
        """--days excludes old tasks and includes recent ones."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Parent task")
        parent.status = "completed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--lineage-depth", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Parent task" in result.stdout
        assert "Child task" in result.stdout
        assert "└──" in result.stdout or "├──" in result.stdout

    def test_history_lineage_depth_two(self, tmp_path: Path):
        """--lineage-depth 2 renders all three levels of a grandparent→parent→child chain."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        grandparent = store.add("Grandparent task")
        grandparent.status = "completed"
        grandparent.completed_at = datetime.now(UTC)
        store.update(grandparent)

        parent = store.add("Parent task", based_on=grandparent.id)
        parent.status = "completed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        # All three levels of the chain must appear in the output
        assert "Grandparent task" in result.stdout
        assert "Parent task" in result.stdout
        assert "Child task" in result.stdout

    def test_history_lineage_orders_completed_root_before_pending_descendants(self, tmp_path: Path):
        """Lineage rendering keeps ancestor-first order even when descendants are pending."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Root done", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime(2026, 3, 1, tzinfo=UTC)
        store.update(root)

        child = store.add("Child pend", task_type="implement", based_on=root.id)
        grandchild = store.add("Gchild pend", task_type="implement", based_on=child.id)
        assert child.id is not None
        assert grandchild.id is not None

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        root_idx = result.stdout.index("Root done")
        child_idx = result.stdout.index("Child pend")
        grandchild_idx = result.stdout.index("Gchild pend")
        assert root_idx < child_idx < grandchild_idx

    def test_history_lineage_same_branch_children_render_compact_without_repeated_connectors(
        self,
        tmp_path: Path,
    ):
        """Same-branch review/improve children render compactly and avoid connector/status/branch bugs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement root", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        impl.branch = "20260412-impl-history-lineage"
        impl.merge_status = "merged"
        store.update(impl)
        assert impl.id is not None

        review = store.add(
            "Review root",
            task_type="review",
            based_on=impl.id,
            depends_on=impl.id,
            same_branch=True,
        )
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.branch = impl.branch
        review.merge_status = "unmerged"
        review.report_file = "reviews/review.md"
        review.output_content = "Verdict: CHANGES_REQUESTED\n\nNeeds revisions."
        review.duration_seconds = 99
        review.cost_usd = 0.25
        store.update(review)
        assert review.id is not None

        improve = store.add(
            "Improve root",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.branch = impl.branch
        improve.merge_status = "unmerged"
        improve.report_file = "reviews/improve.md"
        improve.duration_seconds = 111
        improve.cost_usd = 0.33
        store.update(improve)
        assert improve.id is not None

        result = run_gza("history", "--lineage-depth", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert re.search(rf"completed\s+{re.escape(review.id)}", result.stdout)
        assert re.search(rf"completed\s+{re.escape(improve.id)}", result.stdout)
        assert f"unmerged  {review.id}" not in result.stdout
        assert f"unmerged  {improve.id}" not in result.stdout
        assert "verdict:" in result.stdout
        assert "CHANGES_REQUESTED" in result.stdout
        assert "| stats:" in result.stdout
        assert result.stdout.count("branch: ") == 1
        assert "└──     [review]" not in result.stdout
        assert "├──     [review]" not in result.stdout
        assert "└──     [improve]" not in result.stdout
        assert "├──     [improve]" not in result.stdout
        assert "report:" not in result.stdout

    def test_history_incomplete_with_lookback(self, tmp_path: Path):
        """--incomplete combined with --days applies both filters."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

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

    def test_history_shows_text_status_labels(self, tmp_path: Path):
        """History command shows text labels instead of icons for status."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        completed = store.add("Completed task")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)

        failed = store.add("Failed task")
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        dropped = store.add("Dropped task")
        dropped.status = "dropped"
        dropped.completed_at = datetime.now(UTC)
        store.update(dropped)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "completed" in result.stdout
        assert "failed" in result.stdout
        assert "dropped" in result.stdout

    def test_history_shows_failure_reason_including_unknown(self, tmp_path: Path):
        """History shows failure_reason for all failed tasks, including UNKNOWN."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_unknown = store.add("Failed unknown reason")
        failed_unknown.status = "failed"
        failed_unknown.failure_reason = "UNKNOWN"
        failed_unknown.completed_at = datetime.now(UTC)
        store.update(failed_unknown)

        failed_known = store.add("Failed known reason")
        failed_known.status = "failed"
        failed_known.failure_reason = "MAX_STEPS"
        failed_known.completed_at = datetime.now(UTC)
        store.update(failed_known)

        failed_none = store.add("Failed no reason")
        failed_none.status = "failed"
        failed_none.completed_at = datetime.now(UTC)
        store.update(failed_none)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        # Failure reason is now on a separate line
        assert result.stdout.count("reason: UNKNOWN") >= 2  # explicit UNKNOWN and None both map to UNKNOWN
        assert "reason: MAX_STEPS" in result.stdout

    def test_history_annotates_failed_task_with_retry_outcome(self, tmp_path: Path):
        """Failed tasks show a retry annotation with final retry outcome."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        assert original.id is not None
        retry = store.add("Retry succeeded", task_type="implement", based_on=original.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        store.update(retry)
        assert retry.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {retry.id} ✓" in result.stdout

    def test_history_annotates_failed_task_with_resume_outcome(self, tmp_path: Path):
        """Failed tasks show 'resumed' when the next attempt reused session + branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.branch = "20260415-impl-resume-test"
        original.session_id = "session-123"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        assert original.id is not None
        resumed = store.add("Resumed failed again", task_type="implement", based_on=original.id)
        resumed.status = "failed"
        resumed.failure_reason = "TEST_FAILURE"
        resumed.branch = "20260415-impl-resume-test"
        resumed.session_id = "session-123"
        resumed.completed_at = datetime.now(UTC)
        store.update(resumed)
        assert resumed.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ resumed as {resumed.id} ✗" in result.stdout

    def test_history_annotates_failed_task_with_queued_retry_without_outcome_marker(self, tmp_path: Path):
        """Queued retries are annotated without a terminal outcome marker."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        queued_retry = store.add("Queued retry", task_type="implement", based_on=original.id)
        queued_retry.status = "pending"
        store.update(queued_retry)
        assert queued_retry.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {queued_retry.id}" in result.stdout
        assert f"→ retried as {queued_retry.id} ✗" not in result.stdout
        assert f"→ retried as {queued_retry.id} ✓" not in result.stdout

    def test_history_annotates_failed_task_with_queued_resume_without_outcome_marker(self, tmp_path: Path):
        """Queued resume attempts are annotated without a terminal outcome marker."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.branch = "20260415-impl-resume-queued"
        original.session_id = "session-queued"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        queued_resumed = store.add("Queued resumed", task_type="implement", based_on=original.id)
        queued_resumed.status = "pending"
        queued_resumed.branch = "20260415-impl-resume-queued"
        queued_resumed.session_id = "session-queued"
        store.update(queued_resumed)
        assert queued_resumed.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ resumed as {queued_resumed.id}" in result.stdout
        assert f"→ resumed as {queued_resumed.id} ✗" not in result.stdout
        assert f"→ resumed as {queued_resumed.id} ✓" not in result.stdout

    def test_history_retry_annotation_follows_chain_and_ignores_other_task_types(self, tmp_path: Path):
        """Retry annotation follows same-type based_on chains to the final attempt."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        # Different task type child should not be considered a retry/resume.
        review = store.add("Review child", task_type="review", based_on=original.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        retry_1 = store.add("Retry one", task_type="implement", based_on=original.id)
        retry_1.status = "failed"
        retry_1.failure_reason = "MAX_TURNS"
        retry_1.completed_at = datetime.now(UTC)
        store.update(retry_1)
        assert retry_1.id is not None

        retry_2 = store.add("Retry two", task_type="implement", based_on=retry_1.id)
        retry_2.status = "completed"
        retry_2.completed_at = datetime.now(UTC)
        store.update(retry_2)
        assert retry_2.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {retry_2.id} ✓" in result.stdout
        assert f"→ retried as {review.id}" not in result.stdout

    def test_history_retry_annotation_resolves_latest_descendant_across_sibling_branches(self, tmp_path: Path):
        """Retry annotation resolves from the full same-type descendant tree, not one direct-child branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        older_branch = store.add("Older retry branch", task_type="implement", based_on=original.id)
        older_branch.status = "failed"
        older_branch.failure_reason = "MAX_TURNS"
        older_branch.completed_at = datetime.now(UTC)
        store.update(older_branch)
        assert older_branch.id is not None

        newer_direct_child = store.add("Newest direct child", task_type="implement", based_on=original.id)
        newer_direct_child.status = "pending"
        store.update(newer_direct_child)
        assert newer_direct_child.id is not None

        final_attempt = store.add("Final success on older branch", task_type="implement", based_on=older_branch.id)
        final_attempt.status = "completed"
        final_attempt.completed_at = datetime.now(UTC)
        store.update(final_attempt)
        assert final_attempt.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ retried as {final_attempt.id} ✓" in result.stdout
        assert f"→ retried as {newer_direct_child.id}" not in result.stdout

    def test_history_retry_annotation_keeps_resume_label_in_mixed_sibling_branches(self, tmp_path: Path):
        """Mixed retry/resume siblings should keep action label from the resolved descendant path."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original failed", task_type="implement")
        original.status = "failed"
        original.failure_reason = "MAX_STEPS"
        original.branch = "20260415-impl-branching"
        original.session_id = "session-branching"
        original.completed_at = datetime.now(UTC)
        store.update(original)
        assert original.id is not None

        resume_child = store.add("Resume sibling", task_type="implement", based_on=original.id)
        resume_child.status = "failed"
        resume_child.failure_reason = "MAX_TURNS"
        resume_child.branch = "20260415-impl-branching"
        resume_child.session_id = "session-branching"
        resume_child.completed_at = datetime.now(UTC)
        store.update(resume_child)
        assert resume_child.id is not None

        retry_child = store.add("Retry sibling", task_type="implement", based_on=original.id)
        retry_child.status = "pending"
        retry_child.branch = "20260415-impl-other-branch"
        retry_child.session_id = "session-other"
        store.update(retry_child)
        assert retry_child.id is not None

        resumed_terminal = store.add(
            "Terminal resumed attempt",
            task_type="implement",
            based_on=resume_child.id,
        )
        resumed_terminal.status = "completed"
        resumed_terminal.completed_at = datetime.now(UTC)
        resumed_terminal.branch = "20260415-impl-branching"
        resumed_terminal.session_id = "session-branching"
        store.update(resumed_terminal)
        assert resumed_terminal.id is not None

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"→ resumed as {resumed_terminal.id} ✓" in result.stdout
        assert f"→ retried as {resumed_terminal.id}" not in result.stdout

    def test_history_shows_parent_task_id(self, tmp_path: Path):
        """History shows parent task ID when based_on or depends_on is set."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent = store.add("Parent task")
        parent.status = "completed"
        parent.completed_at = datetime.now(UTC)
        store.update(parent)
        assert parent.id is not None

        child = store.add("Child task", based_on=parent.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"← {parent.id}" in result.stdout

    def test_history_shows_both_based_on_and_depends_on(self, tmp_path: Path):
        """History shows both based_on and depends_on when a task has both set."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan task")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        blocker = store.add("Blocker task")
        blocker.status = "completed"
        blocker.completed_at = datetime.now(UTC)
        store.update(blocker)

        assert plan.id is not None
        assert blocker.id is not None

        child = store.add("Child task", based_on=plan.id, depends_on=blocker.id)
        child.status = "completed"
        child.completed_at = datetime.now(UTC)
        store.update(child)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"← {plan.id} (dep {blocker.id})" in result.stdout

    def test_history_reconciles_in_progress_tasks(self, tmp_path: Path):
        """History command reconciles orphaned in_progress tasks to failed (WORKER_DIED)."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a task and mark it in_progress with a non-existent PID so reconciliation triggers
        orphaned = store.add("Orphaned in_progress task")
        orphaned.status = "in_progress"
        orphaned.started_at = datetime.now(UTC)
        orphaned.running_pid = 999999999  # non-existent PID
        store.update(orphaned)

        result = run_gza("history", "--project", str(tmp_path))

        assert result.returncode == 0
        # Reconciliation must have run: task should be shown as failed with WORKER_DIED reason
        assert "failed" in result.stdout
        assert "WORKER_DIED" in result.stdout


class TestSearchCommand:
    """Tests for 'gza search' command."""

    def test_search_matches_pending_and_history_excludes_pending(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("needle pending task")
        assert pending.id is not None

        completed = store.add("needle completed task")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)

        search_result = run_gza("search", "needle", "--project", str(tmp_path))
        history_result = run_gza("history", "--project", str(tmp_path))

        assert search_result.returncode == 0
        assert "needle pending task" in search_result.stdout
        assert "needle completed task" in search_result.stdout
        assert history_result.returncode == 0
        assert "needle pending task" not in history_result.stdout

    def test_search_shows_actual_status_labels_for_in_progress_and_pending(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("label pending needle")
        assert pending.id is not None

        in_progress = store.add("label in progress needle")
        store.mark_in_progress(in_progress)

        result = run_gza("search", "needle", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pending" in result.stdout
        assert "in_progress" in result.stdout

    def test_search_aligns_status_column_for_pending_and_in_progress(self, tmp_path: Path):
        """Search rows should keep task IDs in the same column across status label lengths."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("align pending needle")
        assert pending.id is not None

        in_progress = store.add("align in progress needle")
        assert in_progress.id is not None
        store.mark_in_progress(in_progress)

        result = run_gza("search", "align", "--last", "0", "--project", str(tmp_path))

        assert result.returncode == 0
        pending_line = next(line for line in result.stdout.splitlines() if "align pending needle" in line)
        in_progress_line = next(line for line in result.stdout.splitlines() if "align in progress needle" in line)
        assert pending_line.index(pending.id) == in_progress_line.index(in_progress.id)

    def test_search_empty_state_message(self, tmp_path: Path):
        setup_db_with_tasks(tmp_path, [
            {"prompt": "alpha task", "status": "completed"},
        ])

        result = run_gza("search", "missing", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No tasks found matching 'missing'" in result.stdout

    def test_search_last_limit(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("limit needle one")
        store.add("limit needle two")
        store.add("limit needle three")

        result = run_gza("search", "needle", "--last", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "limit needle three" in result.stdout
        assert "limit needle two" in result.stdout
        assert "limit needle one" not in result.stdout

    def test_search_last_zero_shows_all_matches(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("all needle one")
        store.add("all needle two")
        store.add("all needle three")

        result = run_gza("search", "needle", "--last", "0", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "all needle three" in result.stdout
        assert "all needle two" in result.stdout
        assert "all needle one" in result.stdout

    def test_search_last_rejects_negative_values(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("negative needle one")

        result = run_gza("search", "needle", "--last", "-1", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "--last must be >= 0 (use 0 for all matches)" in result.stderr

    def test_search_uses_history_style_row_and_detail_rendering(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("style needle task", task_type="implement")
        task.branch = "feat/search-style"
        store.update(task)

        result = run_gza("search", "style needle", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "style needle task" in result.stdout
        assert "[implement]" in result.stdout
        assert "branch: feat/search-style" in result.stdout


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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Only an orphaned task, no pending tasks
        orphaned_task = store.add("Stuck orphaned task")
        store.mark_in_progress(orphaned_task)

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" in result.stdout
        assert "orphaned" in result.stdout
        assert "Stuck orphaned task" in result.stdout

    def test_next_orphaned_hint_requires_full_task_id(self, tmp_path: Path):
        """Orphaned-task hint should tell users to pass full prefixed task IDs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        orphaned_task = store.add("Orphaned task")
        store.mark_in_progress(orphaned_task)

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "gza work <full-task-id>" in result.stdout
        assert "gza mark-completed --force" in result.stdout
        assert "<full-task-id>" in result.stdout


class TestQueueCommand:
    """Tests for `gza queue` ordering and urgent-lane controls."""

    def test_queue_lists_pending_in_urgent_then_fifo_order(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        normal_1 = store.add("Normal 1")
        normal_2 = store.add("Normal 2")
        urgent = store.add("Urgent")
        assert urgent.id is not None
        store.set_urgent(urgent.id, True)

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        assert any("Urgent" in line and "[urgent]" in line for line in lines)
        assert any("Normal 1" in line and "[normal]" in line for line in lines)
        assert any("Normal 2" in line and "[normal]" in line for line in lines)
        urgent_line = next(i for i, line in enumerate(lines) if "Urgent" in line)
        normal_1_line = next(i for i, line in enumerate(lines) if "Normal 1" in line)
        normal_2_line = next(i for i, line in enumerate(lines) if "Normal 2" in line)
        assert urgent_line < normal_1_line < normal_2_line
        assert str(normal_1.id) in lines[normal_1_line]
        assert str(normal_2.id) in lines[normal_2_line]

    def test_queue_bump_and_unbump(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Need soon")
        assert task.id is not None
        assert task.urgent is False

        bump = run_gza("queue", "bump", task.id, "--project", str(tmp_path))
        assert bump.returncode == 0
        assert "Bumped task" in bump.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.urgent is True

        unbump = run_gza("queue", "unbump", task.id, "--project", str(tmp_path))
        assert unbump.returncode == 0
        assert "Removed task" in unbump.stdout
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.urgent is False

    def test_queue_bump_rejects_internal_pending_task(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        internal = store.add("Internal pending", task_type="internal")
        assert internal.id is not None

        result = run_gza("queue", "bump", internal.id, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is internal and not part of the runnable queue" in result.stdout

    def test_queue_bump_blocked_pending_task_clarifies_non_runnable_status(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        blocker = store.add("Blocking task")
        blocked = store.add("Blocked pending", depends_on=blocker.id)
        assert blocked.id is not None

        result = run_gza("queue", "bump", blocked.id, "--project", str(tmp_path))

        assert result.returncode == 0
        assert "not currently runnable" in result.stdout
        refreshed = store.get(blocked.id)
        assert refreshed is not None
        assert refreshed.urgent is True

    def test_queue_bump_moves_task_to_front_of_urgent_lane(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        older_urgent = store.add("Older urgent", urgent=True)
        newer_urgent = store.add("Newer urgent", urgent=True)
        bumped = store.add("Bumped now")
        assert older_urgent.id is not None
        assert newer_urgent.id is not None
        assert bumped.id is not None

        bump = run_gza("queue", "bump", bumped.id, "--project", str(tmp_path))
        assert bump.returncode == 0

        queue = run_gza("queue", "--project", str(tmp_path))
        assert queue.returncode == 0
        lines = [line for line in queue.stdout.splitlines() if line.strip()]
        bumped_line = next(i for i, line in enumerate(lines) if "Bumped now" in line)
        older_line = next(i for i, line in enumerate(lines) if "Older urgent" in line)
        newer_line = next(i for i, line in enumerate(lines) if "Newer urgent" in line)
        assert bumped_line < older_line < newer_line

    def test_next_shows_bumped_task_first(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        store.add("Older urgent", urgent=True)
        store.add("Newer urgent", urgent=True)
        bumped = store.add("Bumped now")
        assert bumped.id is not None
        run_gza("queue", "bump", bumped.id, "--project", str(tmp_path))

        result = run_gza("next", "--project", str(tmp_path))
        assert result.returncode == 0
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        bumped_line = next(i for i, line in enumerate(lines) if "Bumped now" in line)
        older_line = next(i for i, line in enumerate(lines) if "Older urgent" in line)
        newer_line = next(i for i, line in enumerate(lines) if "Newer urgent" in line)
        assert bumped_line < older_line < newer_line

    def test_queue_excludes_non_pickable_internal_and_blocked_pending_tasks(self, tmp_path: Path):
        """Queue pickup order output should only include runnable pending tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        runnable = store.add("Runnable")
        assert runnable.id is not None
        store.add("Internal pending", task_type="internal")
        blocker = store.add("Dependency blocker")
        store.add("Blocked pending", depends_on=blocker.id)

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Runnable" in result.stdout
        assert "Internal pending" not in result.stdout
        assert "Blocked pending" not in result.stdout

    def test_queue_shows_no_runnable_tasks_when_only_non_pickable_pending_exist(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Internal blocker", task_type="internal")
        store.add("Blocked pending", depends_on=blocker.id)

        result = run_gza("queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No runnable tasks" in result.stdout


class TestShowCommand:
    """Tests for 'gza show' command."""

    def test_show_existing_task(self, tmp_path: Path):
        """Show command displays task details."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A detailed task prompt", "status": "pending"},
        ])

        result = run_gza("show", "testproject-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Task " in result.stdout
        assert "A detailed task prompt" in result.stdout
        assert "Status: pending" in result.stdout

    def test_show_displays_execution_mode_when_set(self, tmp_path: Path):
        """Show command includes execution provenance mode when present."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Task with execution mode")
        assert task.id is not None
        task.execution_mode = "skill_inline"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Execution Mode: skill_inline" in result.stdout

    def test_show_nonexistent_task(self, tmp_path: Path):
        """Show command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("show", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_show_displays_lineage_for_review_task(self, tmp_path: Path):
        """Show command displays lineage using implementation/review chain."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        review = store.add("Review feature", task_type="review", depends_on=impl.id)
        assert review.id is not None

        result = run_gza("show", str(review.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout

    def test_show_displays_active_worktree_path_for_task_branch(self, tmp_path: Path):
        """Show command includes active worktree path when task branch is checked out in a worktree."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path,
            task_prompt="Task with worktree",
            branch_name="feature/show-worktree",
            worktree_name="show-worktree",
        )
        assert task.id is not None
        assert worktree_path is not None

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Branch: feature/show-worktree" in result.stdout
        compact_output = "".join(result.stdout.split())
        assert f"Worktree: {worktree_path}".replace(" ", "") in compact_output

    def test_show_omits_worktree_path_when_branch_has_no_active_worktree(self, tmp_path: Path):
        """Show command omits worktree line when no active worktree is registered for the task branch."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path,
            task_prompt="Task without active worktree",
            branch_name="feature/no-worktree",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Branch: feature/no-worktree" in result.stdout
        assert "Worktree:" not in result.stdout

    def test_show_warns_when_worktree_lookup_raises_git_error(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Show command emits a warning when worktree lookup fails with GitError."""
        from gza.cli.query import cmd_show
        from gza.git import GitError

        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path,
            task_prompt="Task with lookup failure",
            branch_name="feature/worktree-lookup-giterror",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        with patch("gza.cli.query.Git.worktree_list", side_effect=GitError("simulated worktree list failure")):
            args = argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(task.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
            )
            exit_code = cmd_show(args)
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Branch: feature/worktree-lookup-giterror" in output
        assert "Warning: Worktree lookup failed:" in output
        assert "simulated worktree list failure" in output

    def test_show_warns_when_worktree_lookup_raises_os_error(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Show command emits a warning when worktree lookup fails with OSError."""
        from gza.cli.query import cmd_show

        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path,
            task_prompt="Task with lookup os error",
            branch_name="feature/worktree-lookup-oserror",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        with patch("gza.cli.query.Git.worktree_list", side_effect=OSError("simulated os error")):
            args = argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(task.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
            )
            exit_code = cmd_show(args)
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Branch: feature/worktree-lookup-oserror" in output
        assert "Warning: Worktree lookup failed:" in output
        assert "simulated os error" in output

    def test_show_omits_prunable_worktree_path_for_task_branch(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Show command should not treat prunable worktrees as active paths."""
        from gza.cli.query import cmd_show

        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path,
            task_prompt="Task with stale worktree registration",
            branch_name="feature/prunable-worktree",
            worktree_name=None,
        )
        assert task.id is not None
        assert worktree_path is None

        with patch("gza.cli.query.Git.worktree_list", return_value=[
            {
                "path": "/tmp/stale-worktree",
                "branch": "refs/heads/feature/prunable-worktree",
                "prunable": "gone",
            }
        ]):
            args = argparse.Namespace(
                project_dir=tmp_path,
                task_id=str(task.id),
                prompt=False,
                path=False,
                output=False,
                page=False,
                full=False,
            )
            exit_code = cmd_show(args)
        output = capsys.readouterr().out

        assert exit_code == 0
        assert "Branch: feature/prunable-worktree" in output
        assert "Worktree:" not in output

    def test_show_failed_task_displays_failure_diagnostics(self, tmp_path: Path):
        """Failed task output includes reason, limits, context, and next-step commands."""
        import json

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmax_steps: 50\nverify_command: uv run pytest tests/ -q\n")

        store = make_store(tmp_path)
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
                task_slug=task.slug,
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
        assert f"gza resume {task.id}" in result.stdout
        assert f"gza retry {task.id}" in result.stdout
        assert "Run Context: background (w-20260227-000001)" in result.stdout

    def test_show_failed_task_extracts_verify_failure_from_tool_error_entries(self, tmp_path: Path):
        """Failed-task diagnostics should detect verify failures in non-Claude tool_* entry shapes."""
        import json


        setup_config(tmp_path)
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nverify_command: uv run pytest tests/ -q\n")

        store = make_store(tmp_path)
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

    def test_show_failed_test_failure_excludes_resume_next_step(self, tmp_path: Path):
        """TEST_FAILURE guidance should not advertise gza resume."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Failed verification")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "TEST_FAILURE"
        task.session_id = "sess-test-failure"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"gza retry {task.id}" in result.stdout
        assert f"gza resume {task.id}" not in result.stdout

    def test_show_failed_task_prerequisite_unmerged_next_steps(self, tmp_path: Path):
        """PREREQUISITE_UNMERGED should show merge+retry guidance."""
        setup_config(tmp_path)

        store = make_store(tmp_path)
        dep = store.add("Upstream dependency")
        task = store.add("Failed downstream", depends_on=dep.id)
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "PREREQUISITE_UNMERGED"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason: PREREQUISITE_UNMERGED" in result.stdout
        assert "Failure Summary: Dependency is not yet merged to main." in result.stdout
        assert f"gza merge {dep.id}" in result.stdout
        assert f"gza retry {task.id}" in result.stdout

    def test_show_prerequisite_unmerged_prefers_resolved_dependency_from_log(self, tmp_path: Path):
        """PREREQUISITE_UNMERGED next steps should use resolved dependency_task_id from outcome log."""
        setup_config(tmp_path)

        store = make_store(tmp_path)
        direct_dep = store.add("Original failed dependency")
        retry_dep = store.add("Completed retry dependency", based_on=direct_dep.id)
        task = store.add("Failed downstream", depends_on=direct_dep.id)
        assert task.id is not None
        assert retry_dep.id is not None
        task.status = "failed"
        task.failure_reason = "PREREQUISITE_UNMERGED"
        task.log_file = ".gza/logs/prereq-unmerged.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "prereq-unmerged.log").write_text(
            json.dumps(
                {
                    "type": "gza",
                    "subtype": "outcome",
                    "failure_reason": "PREREQUISITE_UNMERGED",
                    "dependency_task_id": retry_dep.id,
                }
            )
            + "\n"
        )

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"gza merge {retry_dep.id}" in result.stdout
        assert f"gza merge {direct_dep.id}" not in result.stdout

    def test_show_indicates_worker_startup_failure(self, tmp_path: Path):
        """Show surfaces startup failure when worker failed before main log existed."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
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
                task_slug=task.slug,
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

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed task")
        task.status = "completed"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Failure Reason:" not in result.stdout
        assert "Failure Summary:" not in result.stdout

    def test_show_plan_lineage_includes_downstream_implement(self, tmp_path: Path):
        """Show for a plan task includes downstream implement task in lineage."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design the feature", task_type="plan")
        assert plan.id is not None
        impl = store.add("Implement the feature", task_type="implement", based_on=plan.id)
        assert impl.id is not None

        result = run_gza("show", str(plan.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{plan.id}" in result.stdout
        assert f"{impl.id}" in result.stdout

    def test_show_implement_lineage_includes_plan_and_review_improve_chain(self, tmp_path: Path):
        """Show for an implement task (based on a plan) includes plan, review, and improve."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        assert f"{plan.id}" in result.stdout
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout
        assert f"{improve.id}" in result.stdout

    def test_show_multi_level_dependency_lineage(self, tmp_path: Path):
        """Lineage traverses multi-level dependency chains (plan->impl->sub-impl)."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Top-level plan", task_type="plan")
        assert plan.id is not None
        impl1 = store.add("First implement", task_type="implement", based_on=plan.id)
        assert impl1.id is not None
        impl2 = store.add("Second implement based on first", task_type="implement", based_on=impl1.id)
        assert impl2.id is not None

        result = run_gza("show", str(plan.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        assert f"{plan.id}" in result.stdout
        assert f"{impl1.id}" in result.stdout
        assert f"{impl2.id}" in result.stdout

    def test_show_lineage_orders_completed_root_before_pending_descendants(self, tmp_path: Path):
        """Show lineage keeps root first even when downstream tasks are still pending."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Root task", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime(2026, 3, 1, tzinfo=UTC)
        store.update(root)

        child = store.add("Child task", task_type="implement", based_on=root.id)
        grandchild = store.add("Grandchild task", task_type="implement", based_on=child.id)

        result = run_gza("show", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Lineage:" in result.stdout
        root_idx = result.stdout.index(f"{root.id}")
        child_idx = result.stdout.index(f"{child.id}")
        grandchild_idx = result.stdout.index(f"{grandchild.id}")
        assert root_idx < child_idx < grandchild_idx

    def test_show_depended_on_by_field(self, tmp_path: Path):
        """Show displays 'Depended on by' listing tasks that reference the displayed task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        assert f"{impl.id}[implement]" in result_plan.stdout

        # Show the impl: it should list review as "Depended on by"
        result_impl = run_gza("show", str(impl.id), "--project", str(tmp_path))
        assert result_impl.returncode == 0
        assert "Depended on by:" in result_impl.stdout
        assert f"{review.id}[review]" in result_impl.stdout

    def test_show_truncates_long_output(self, tmp_path: Path):
        """gza show truncates output >30 lines to 20 with a remainder hint."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create tasks in different groups
        store.add("Task 1", group="group-a")
        store.add("Task 2", group="group-a")
        task3 = store.add("Task 3", group="group-b")
        task3.status = "completed"
        task3.completed_at = datetime.now(UTC)
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
        make_store(tmp_path)

        result = run_gza("groups", "--project", str(tmp_path))

        assert result.returncode == 0


class TestStatusCommand:
    """Tests for 'gza group <group>' command."""

    def test_status_with_group(self, tmp_path: Path):
        """Group command shows tasks in a group."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create tasks in a group
        task1 = store.add("First task", group="test-group")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        store.update(task1)
        store.add("Second task", group="test-group", depends_on=task1.id)

        result = run_gza("group", "test-group", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "test-group" in result.stdout
        assert "First task" in result.stdout
        assert "Second task" in result.stdout

    def test_status_warns_about_orphaned_tasks_in_group(self, tmp_path: Path):
        """Group command warns about orphaned tasks belonging to the viewed group."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed task and an orphaned in-progress task in the same group
        task1 = store.add("Completed task", group="my-group")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        from gza.workers import WorkerMetadata, WorkerRegistry

        # Setup config and database
        setup_config(tmp_path)
        store = make_store(tmp_path)

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
            started_at=datetime.now(UTC).isoformat(),
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
        assert f"{task.id}" in result.stdout, f"Output should contain task ID {task.id}"

        # Cleanup
        registry.remove("w-test-ps")

    def test_ps_reconciles_db_and_worker_with_source_both(self, tmp_path: Path):
        """PS dedupes by task_id and marks row source as both."""
        import json
        import os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
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
                started_at=datetime.now(UTC).isoformat(),
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

    def test_ps_prunes_dead_worker_for_terminal_task(self, tmp_path: Path):
        """ps/status should prune stale worker entries once their task is terminal."""
        import subprocess

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Terminal task with dead worker")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        proc = subprocess.Popen(["true"])
        proc.wait()
        dead_pid = proc.pid

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-prune-on-ps",
                pid=dead_pid,
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        result = run_gza("ps", "--all", "--json", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No in-progress tasks" in result.stdout
        assert registry.get("w-prune-on-ps") is None

    def test_ps_no_id_background_claim_reconciles_single_active_row(self, tmp_path: Path):
        """No-id background claim should reconcile into one active non-orphaned task row."""
        import os

        from gza.cli.query import _build_ps_rows
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("No-id claimed task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.slug = "20260319-claim-no-id"
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-id-claim",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        rows, _ = _build_ps_rows(registry, store, include_completed=False)
        assert len(rows) == 1
        assert rows[0]["source"] == "both"
        assert rows[0]["task_id"] == task.id
        assert rows[0]["status"] == "in_progress"
        assert rows[0]["is_orphaned"] is False

    def test_no_orphan_warning_for_healthy_no_id_background_claim(self, tmp_path: Path):
        """Healthy claimed no-id background runs should not be classified as orphaned."""
        import os

        from gza.cli.query import _get_orphaned_tasks
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Healthy running task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.slug = "20260319-healthy-no-id"
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-id-healthy",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        orphaned = _get_orphaned_tasks(registry, store)
        assert orphaned == []

    def test_ps_task_label_maps_to_claimed_task_for_no_id_background_claim(self, tmp_path: Path):
        """No-id claimed worker rows should render the claimed task label, not a worker placeholder."""
        import os

        from gza.cli.query import _build_ps_rows
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Claimed label task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.slug = "20260319-claimed-label"
        store.update(task)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-id-label",
                pid=os.getpid(),
                task_id=task.id,
                task_slug=None,
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
        )

        rows, _ = _build_ps_rows(registry, store, include_completed=False)
        assert len(rows) == 1
        assert rows[0]["task_id"] == task.id
        assert rows[0]["task"] == task.slug
        assert rows[0]["task"] != ""
        assert not rows[0]["task"].startswith("task ")

    def test_ps_includes_db_only_in_progress_and_flags_orphaned(self, tmp_path: Path):
        """PS includes in-progress DB rows even when no worker exists."""
        import json


        setup_config(tmp_path)
        store = make_store(tmp_path)
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

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
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
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
                started_at=datetime.now(UTC).isoformat(),
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

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
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
                started_at=datetime.now(UTC).isoformat(),
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

        from gza.workers import WorkerMetadata, WorkerRegistry

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
                started_at=datetime.now(UTC).isoformat(),
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
        from gza.workers import WorkerMetadata, WorkerRegistry

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
                started_at=datetime.now(UTC).isoformat(),
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
                started_at=datetime.now(UTC).isoformat(),
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
                started_at=datetime.now(UTC).isoformat(),
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
                started_at=datetime.now(UTC).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(UTC).isoformat(),
            )
        )

        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        # Stale running worker (dead PID, no task_id) is pruned automatically
        assert "running-worker" not in result.stdout
        assert "failed(startup)" in result.stdout
        assert "ordinary-failed-worker" not in result.stdout
        assert "completed-worker" not in result.stdout

        registry.remove("w-test-ps-startup-failed")
        registry.remove("w-test-ps-failed")
        registry.remove("w-test-ps-completed")

    def test_ps_all_flag_includes_completed_and_failed_rows(self, tmp_path: Path):
        """ps --all includes ordinary completed/failed rows that default ps filters out."""
        from gza.workers import WorkerMetadata, WorkerRegistry

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
                started_at=datetime.now(UTC).isoformat(),
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
                started_at=datetime.now(UTC).isoformat(),
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
                started_at=datetime.now(UTC).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(UTC).isoformat(),
            )
        )

        # Default ps: stale running worker (dead PID, no task_id) is pruned;
        # ordinary completed/failed are filtered out
        result = run_gza("ps", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "running-worker" not in result.stdout
        assert "ordinary-failed-worker" not in result.stdout
        assert "completed-worker" not in result.stdout

        # ps --all: includes completed/failed (running was already pruned)
        result_all = run_gza("ps", "--all", "--project", str(tmp_path))
        assert result_all.returncode == 0
        assert "ordinary-failed-worker" in result_all.stdout
        assert "completed-worker" in result_all.stdout

        # status --all (alias) also works
        result_status = run_gza("status", "--all", "--project", str(tmp_path))
        assert result_status.returncode == 0
        assert "completed-worker" in result_status.stdout

        registry.remove("w-all-failed")
        registry.remove("w-all-completed")

    def test_ps_all_json_includes_terminal_rows(self, tmp_path: Path):
        """ps --all --json includes completed/failed workers in JSON output."""
        import json as json_lib

        from gza.workers import WorkerMetadata, WorkerRegistry

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
                started_at=datetime.now(UTC).isoformat(),
                status="completed",
                log_file=None,
                worktree=None,
                exit_code=0,
                completed_at=datetime.now(UTC).isoformat(),
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
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-startup-poll",
                pid=99999,
                task_id=None,
                task_slug="startup-failed-worker",
                started_at=datetime.now(UTC).isoformat(),
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
        import os as _os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)
        registry.register(
            WorkerMetadata(
                worker_id="w-test-no-start",
                pid=_os.getpid(),  # use real PID so prune doesn't remove it
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
        import os as _os

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        # Register in reverse lexical order to assert sort stability by worker_id.
        for worker_id in ["w-test-order-b", "w-test-order-a"]:
            registry.register(
                WorkerMetadata(
                    worker_id=worker_id,
                    pid=_os.getpid(),  # use real PID so prune doesn't remove it
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
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_dir)

        worker = WorkerMetadata(
            worker_id="w-test-transition",
            pid=os.getpid(),
            task_id=None,
            task_slug=None,
            started_at=datetime.now(UTC).isoformat(),
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
        assert first_snapshot[0]["status"] == "in_progress"

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
            started_at=datetime.now(UTC).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        w2 = WorkerMetadata(
            worker_id="w-learn-2",
            pid=os.getpid(),
            task_id=task2.id,
            task_slug=None,
            started_at=datetime.now(UTC).isoformat(),
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

        # Poll 1: both tasks in_progress
        snap1 = json.loads(json_outputs[0])
        assert len(snap1) == 2, f"Poll 1: expected 2 tasks, got {len(snap1)}: {snap1}"
        statuses1 = {r["status"] for r in snap1}
        assert statuses1 == {"in_progress"}, f"Poll 1: expected all in_progress, got {statuses1}"

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
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
                started_at=datetime.now(UTC).isoformat(),
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

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
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
                started_at=datetime.now(UTC).isoformat(),
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

        result = run_gza("delete", "testproject-1", "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", "--project", str(tmp_path))
        assert "No pending tasks" in result.stdout

    def test_delete_nonexistent_task(self, tmp_path: Path):
        """Delete command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("delete", "testproject-999999", "--force", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_delete_with_yes_flag(self, tmp_path: Path):
        """Delete command with --yes removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "testproject-1", "--yes", "--project", str(tmp_path))

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

        result = run_gza("delete", "testproject-1", "-y", "--project", str(tmp_path))

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
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.slug = "20260212-review-implementation"
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

    def test_unmerged_failed_reviews_do_not_count_as_reviewed(self, tmp_path: Path):
        """Failed reviews with completed_at set should not mark an impl as reviewed."""
        store, task, git = setup_unmerged_env(tmp_path)

        failed_review_1 = store.add("Failed review 1", task_type="review")
        failed_review_1.status = "failed"
        failed_review_1.completed_at = datetime.now(UTC)
        failed_review_1.depends_on = task.id
        failed_review_1.output_content = "Verdict: APPROVED"
        store.update(failed_review_1)

        failed_review_2 = store.add("Failed review 2", task_type="review")
        failed_review_2.status = "failed"
        failed_review_2.completed_at = datetime.now(UTC)
        failed_review_2.depends_on = task.id
        failed_review_2.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(failed_review_2)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review: no review" in result.stdout
        assert "review: reviewed" not in result.stdout
        assert "✓ approved" not in result.stdout
        assert "⚠ changes requested" not in result.stdout

    def test_unmerged_ignores_failed_review_when_completed_review_exists(self, tmp_path: Path):
        """Only completed reviews should drive unmerged review classification and verdict."""
        store, task, git = setup_unmerged_env(tmp_path)

        completed_review = store.add("Completed review", task_type="review")
        completed_review.status = "completed"
        completed_review.completed_at = datetime.now(UTC)
        completed_review.depends_on = task.id
        completed_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(completed_review)

        failed_review = store.add("Failed review", task_type="review")
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        failed_review.depends_on = task.id
        failed_review.output_content = "Verdict: APPROVED"
        store.update(failed_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "review: reviewed" in result.stdout
        assert "⚠ changes requested" in result.stdout
        assert "✓ approved" not in result.stdout

    def test_unmerged_uses_most_recent_review(self, tmp_path: Path):
        """Unmerged output shows status from most recent review."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

        # Create first review (changes requested)
        review1 = store.add("First review", task_type="review")
        review1.status = "completed"
        review1.completed_at = datetime.now(UTC)
        review1.depends_on = task.id
        review1.slug = "20260212-first-review"
        review1.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review1)

        # Wait a bit to ensure different timestamps
        time.sleep(0.1)

        # Create second review (approved)
        review2 = store.add("Second review", task_type="review")
        review2.status = "completed"
        review2.completed_at = datetime.now(UTC)
        review2.depends_on = task.id
        review2.slug = "20260212-second-review"
        review2.output_content = "**Verdict: APPROVED**"
        store.update(review2)

        # Run unmerged command - should show approved (most recent)
        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
            result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "✓ approved" in result.stdout

    def test_unmerged_uses_older_verdict_when_latest_review_has_no_output(self, tmp_path: Path):
        """Unmerged scans newest-to-oldest and uses first parseable review verdict."""
        import time

        store, task, git = setup_unmerged_env(tmp_path)

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime.now(UTC)
        older_review.depends_on = task.id
        older_review.slug = "20260212-older-review"
        older_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(older_review)

        time.sleep(0.01)

        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime.now(UTC)
        latest_review.depends_on = task.id
        latest_review.slug = "20260212-latest-review"
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
        older_review.completed_at = datetime.now(UTC)
        older_review.depends_on = task.id
        older_review.slug = "20260212-older-review"
        older_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(older_review)

        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        time.sleep(0.01)
        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime.now(UTC)
        latest_review.depends_on = task.id
        latest_review.slug = "20260212-latest-review"
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
        review.completed_at = datetime.now(UTC)
        review.slug = "20260225-review-simplify-mixer-by-removing-the-people-strategy-2"
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
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.slug = "20260212-review-implementation"
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        # Before improve: should show changes requested
        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
            result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" in result.stdout

        # Simulate improve task completing (clear review state)
        time.sleep(0.01)
        assert task.id is not None
        store.clear_review_state(task.id)

        # After improve: status should explicitly show stale review
        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
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
        review1.completed_at = datetime.now(UTC)
        review1.depends_on = task.id
        review1.slug = "20260212-review"
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
        review2.completed_at = datetime.now(UTC)
        review2.depends_on = task.id
        review2.slug = "20260212-second-review"
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
        review.completed_at = datetime.now(UTC)
        review.depends_on = impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        time.sleep(0.01)
        improve = store.add("Address review feedback", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
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
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout
        assert f"{improve.id}" in result.stdout
        assert "[implement]" in result.stdout
        assert "[review]" in result.stdout
        assert "[improve]" in result.stdout
        assert "review stale" in result.stdout

    def test_unmerged_lineage_shows_full_canonical_tree_and_annotations(self, tmp_path: Path):
        """Unmerged lineage uses the canonical tree and keeps node annotations."""
        store, impl, git = setup_unmerged_env(tmp_path)

        review = store.add("Review", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        review.depends_on = impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        improve = store.add("Improve", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        improve.based_on = impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        downstream_impl = store.add("Downstream implement noise", task_type="implement")
        downstream_impl.status = "completed"
        downstream_impl.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        downstream_impl.based_on = impl.id
        downstream_impl.branch = "feature/test"
        downstream_impl.same_branch = True
        store.update(downstream_impl)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "lineage:" in result.stdout
        assert f"{impl.id}" in result.stdout
        assert f"{review.id}" in result.stdout
        assert f"{improve.id}" in result.stdout
        assert f"{downstream_impl.id}" in result.stdout
        assert "| completed |" in result.stdout
        assert "changes_requested" in result.stdout
        # The "← latest" annotation may wrap across lines at narrow terminal widths
        assert "latest" in " ".join(result.stdout.split())

    def test_unmerged_uses_latest_branch_implementation_for_summary_and_review(self, tmp_path: Path):
        """Unmerged summarizes the latest implementation on a shared branch, not an older retry."""
        store, root_impl, git = setup_unmerged_env(tmp_path)

        retry_impl = store.add("Retry implementation", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/test"
        retry_impl.same_branch = True
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        store.update(retry_impl)

        review = store.add("Review retry", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        review.depends_on = retry_impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        improve = store.add("Improve retry", task_type="improve")
        improve.status = "failed"
        improve.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        improve.based_on = retry_impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        sibling_impl = store.add("Sibling retry", task_type="implement")
        sibling_impl.status = "completed"
        sibling_impl.completed_at = datetime(2026, 2, 12, 14, 0, tzinfo=UTC)
        sibling_impl.based_on = root_impl.id
        sibling_impl.branch = "feature/test"
        sibling_impl.same_branch = True
        sibling_impl.has_commits = True
        sibling_impl.merge_status = "unmerged"
        store.update(sibling_impl)

        sibling_review = store.add("Review sibling retry", task_type="review")
        sibling_review.status = "completed"
        sibling_review.completed_at = datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
        sibling_review.depends_on = sibling_impl.id
        sibling_review.output_content = "Verdict: APPROVED"
        store.update(sibling_review)

        unmerged_result = run_gza("unmerged", "--project", str(tmp_path))
        assert unmerged_result.returncode == 0

        unmerged_output = " ".join(unmerged_result.stdout.split())
        assert f"⚡ {sibling_impl.id}" in unmerged_output
        assert "review: reviewed [✓ approved]" in unmerged_output
        assert root_impl.id in unmerged_output
        assert retry_impl.id in unmerged_output
        assert sibling_impl.id in unmerged_output
        assert sibling_review.id in unmerged_output

    def test_unmerged_lineage_matches_lineage_command_root_for_retry_chain(self, tmp_path: Path):
        """Unmerged lineage keeps the same canonical root as `gza lineage` for retried implementations."""
        store, root_impl, git = setup_unmerged_env(tmp_path)

        retry_impl = store.add("Retry implementation", task_type="implement")
        retry_impl.status = "completed"
        retry_impl.completed_at = datetime(2026, 2, 12, 11, 0, tzinfo=UTC)
        retry_impl.based_on = root_impl.id
        retry_impl.branch = "feature/test"
        retry_impl.same_branch = True
        retry_impl.has_commits = True
        retry_impl.merge_status = "unmerged"
        store.update(retry_impl)

        review = store.add("Review retry", task_type="review")
        review.status = "completed"
        review.completed_at = datetime(2026, 2, 12, 12, 0, tzinfo=UTC)
        review.depends_on = retry_impl.id
        review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(review)

        improve = store.add("Improve retry", task_type="improve")
        improve.status = "failed"
        improve.completed_at = datetime(2026, 2, 12, 13, 0, tzinfo=UTC)
        improve.based_on = retry_impl.id
        improve.depends_on = review.id
        improve.branch = "feature/test"
        improve.same_branch = True
        store.update(improve)

        sibling_impl = store.add("Sibling retry", task_type="implement")
        sibling_impl.status = "completed"
        sibling_impl.completed_at = datetime(2026, 2, 12, 14, 0, tzinfo=UTC)
        sibling_impl.based_on = root_impl.id
        sibling_impl.branch = "feature/test"
        sibling_impl.same_branch = True
        store.update(sibling_impl)

        lineage_result = run_gza("lineage", retry_impl.id, "--project", str(tmp_path))
        assert lineage_result.returncode == 0

        unmerged_result = run_gza("unmerged", "--project", str(tmp_path))
        assert unmerged_result.returncode == 0

        lineage_output = " ".join(lineage_result.stdout.split())
        unmerged_output = " ".join(unmerged_result.stdout.split())
        assert root_impl.id in lineage_output
        assert root_impl.id in unmerged_output
        assert retry_impl.id in lineage_output
        assert retry_impl.id in unmerged_output
        assert sibling_impl.id in lineage_output
        assert sibling_impl.id in unmerged_output

    def test_unmerged_lineage_marks_only_latest_review_node(self, tmp_path: Path):
        """The most recent review node is annotated with the latest marker."""
        store, impl, git = setup_unmerged_env(tmp_path)

        older_review = store.add("Older review", task_type="review")
        older_review.status = "completed"
        older_review.completed_at = datetime(2026, 2, 12, 9, 0, tzinfo=UTC)
        older_review.depends_on = impl.id
        older_review.output_content = "Verdict: APPROVED"
        store.update(older_review)

        latest_review = store.add("Latest review", task_type="review")
        latest_review.status = "completed"
        latest_review.completed_at = datetime(2026, 2, 12, 10, 0, tzinfo=UTC)
        latest_review.depends_on = impl.id
        latest_review.output_content = "Verdict: CHANGES_REQUESTED"
        store.update(latest_review)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        # Normalize output since "← latest" may wrap across lines at narrow terminal widths
        normalized = " ".join(result.stdout.split())
        older_idx = normalized.index(f"{older_review.id}")
        latest_idx = normalized.index(f"{latest_review.id}")
        marker_idx = normalized.index("latest")
        assert marker_idx > latest_idx
        assert not (older_idx < marker_idx < latest_idx)


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
        task2.slug = "20260220-merged"
        task2.completed_at = datetime.now(UTC)
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        task.slug = "20260220-deleted-branch"
        task.completed_at = datetime.now(UTC)
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

    def test_unmerged_update_marks_stale_merged_task_as_merged(self, tmp_path: Path):
        """--update reconciles stale unmerged tasks whose branch is already merged."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Stale unmerged task",
            task_id="20260220-stale-unmerged",
            branch="feature/stale-unmerged",
        )

        git._run("merge", "--no-ff", "feature/stale-unmerged", "-m", "Merge stale branch")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Stale unmerged task" in result.stdout

        result = run_gza("unmerged", "--update", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Reconciled unmerged tasks: 1 merged, 0 refreshed" in result.stdout
        assert "No unmerged tasks" in result.stdout

        updated_task = store.get(task.id)
        assert updated_task.merge_status == "merged"

    def test_unmerged_update_refreshes_diff_stats_for_live_branch(self, tmp_path: Path):
        """--update refreshes cached diff stats for branches that are still unmerged."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Refresh diff stats task",
            task_id="20260220-refresh-diff-stats",
            branch="feature/refresh-diff-stats",
        )

        task.diff_files_changed = 99
        task.diff_lines_added = 999
        task.diff_lines_removed = 111
        store.update(task)

        git._run("checkout", "feature/refresh-diff-stats")
        (tmp_path / "feature.txt").write_text("line1\nline2\n")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Update diff stats")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--update", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Reconciled unmerged tasks: 0 merged, 1 refreshed" in result.stdout
        assert "+2/-0 LOC, 1 files" in result.stdout

        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"
        assert updated_task.diff_files_changed == 1
        assert updated_task.diff_lines_added == 2
        assert updated_task.diff_lines_removed == 0

    def test_unmerged_into_current_uses_current_branch(self, tmp_path: Path):
        """--into-current uses live git state against the current branch."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Branch local task",
            task_id="20260220-branch-local-task",
            branch="feature/branch-local-task",
        )

        git._run("checkout", "-b", "integration")
        git._run("merge", "--no-ff", "feature/branch-local-task", "-m", "Merge feature into integration")

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Branch local task" in result.stdout

        result = run_gza("unmerged", "--into-current", "--project", str(tmp_path), cwd=tmp_path)
        assert result.returncode == 0
        assert "Showing tasks unmerged relative to integration" in result.stdout
        assert "No unmerged tasks" in result.stdout

    def test_unmerged_target_uses_specified_branch(self, tmp_path: Path):
        """--target uses live git state against the specified branch."""
        store, task, git = setup_unmerged_env(
            tmp_path,
            task_prompt="Target branch task",
            task_id="20260220-target-branch-task",
            branch="feature/target-branch-task",
        )

        git._run("checkout", "-b", "integration")
        git._run("merge", "--no-ff", "feature/target-branch-task", "-m", "Merge feature into integration")
        git._run("checkout", "main")

        result = run_gza("unmerged", "--target", "integration", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Showing tasks unmerged relative to integration" in result.stdout
        assert "No unmerged tasks" in result.stdout


class TestUnmergedImprovedDisplay:
    """Tests for improved unmerged display (diff stats, review prominence, completed-only)."""

    def test_unmerged_excludes_failed_tasks(self, tmp_path: Path):
        """Failed tasks with merge_status='unmerged' are excluded from unmerged output."""

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
        failed_task.completed_at = datetime.now(UTC)
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
        review.completed_at = datetime.now(UTC)
        review.depends_on = task.id
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        with patch("gza.cli.Git", return_value=_mock_unmerged_git()):
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
        task.completed_at = datetime(2026, 2, 12, 10, 30, tzinfo=UTC)
        store.update(task)

        improve = store.add("Improve root task", task_type="improve")
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        task.status = "failed"
        task.failure_reason = "Claude returned exit code 1"
        store.update(task)

        loaded = store.get(task.id)
        assert loaded is not None
        assert loaded.failure_reason == "Claude returned exit code 1"

    def test_failure_reason_defaults_to_none(self, tmp_path: Path):
        """failure_reason defaults to None for new tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.failure_reason is None

        loaded = store.get(task.id)
        assert loaded is not None
        assert loaded.failure_reason is None


class TestNextCommandWithDependencies:
    """Tests for 'gza next' command with dependencies."""

    def test_next_skips_blocked_tasks(self, tmp_path: Path):
        """Next command skips tasks blocked by dependencies."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create task chain
        task1 = store.add("First task")
        store.add("Blocked task", depends_on=task1.id)
        store.add("Independent task")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show task1 or task3, but not task2
        assert "Blocked task" not in result.stdout or "blocked" in result.stdout.lower()

    def test_next_all_shows_blocked_tasks(self, tmp_path: Path):
        """Next --all command shows blocked tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create task chain
        task1 = store.add("First task")
        store.add("Blocked task", depends_on=task1.id)

        result = run_gza("next", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "First task" in result.stdout
        assert "Blocked task" in result.stdout

    def test_next_shows_blocked_count(self, tmp_path: Path):
        """Next command shows count of blocked tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create blocked tasks
        task1 = store.add("First task")
        store.add("Blocked task 1", depends_on=task1.id)
        store.add("Blocked task 2", depends_on=task1.id)
        store.add("Independent task")

        result = run_gza("next", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should mention 2 blocked tasks
        assert "2" in result.stdout and "blocked" in result.stdout.lower()

    def test_next_excludes_internal_and_only_shows_blocked_via_blocked_path(self, tmp_path: Path):
        """Internal pending tasks should not appear in runnable or blocked output."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Dependency blocker")
        store.add("Blocked task", depends_on=blocker.id)
        store.add("Internal pending", task_type="internal")
        store.add("Runnable task")

        result = run_gza("next", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Runnable task" in result.stdout
        assert "Internal pending" not in result.stdout
        assert "Blocked task" not in result.stdout
        assert "blocked by dependencies" in result.stdout

        result_all = run_gza("next", "--all", "--project", str(tmp_path))
        assert result_all.returncode == 0
        assert "Runnable task" in result_all.stdout
        assert "Blocked task" in result_all.stdout
        assert "Internal pending" not in result_all.stdout


class TestKillCommand:
    """Tests for the 'gza kill' command."""

    def test_kill_refuses_non_in_progress_task(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """kill must reject tasks that are not in_progress."""
        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Completed task")
        assert task.id is not None

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=False)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert "not running" in captured.out
        mock_kill.assert_not_called()

    def test_kill_task_not_found(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """kill must report an error for unknown task IDs."""
        from gza.cli.query import cmd_kill

        setup_config(tmp_path)

        args = argparse.Namespace(project_dir=tmp_path, task_id="testproject-99999", all=False, force=False)
        rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert "not found" in captured.out

    def test_kill_signals_via_worker_record(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """kill sends SIGTERM to the worker PID found in the registry."""
        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Running task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 12345
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-kill-1", task_id=task.id, pid=12345, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=False)
        # Patch os.kill so SIGTERM appears sent and process dies immediately (OSError on signal 0)
        def fake_kill(pid: int, sig: int) -> None:
            if sig == 0:
                raise OSError("no such process")
        with patch("gza.cli.query.os.kill", side_effect=fake_kill) as mock_kill:
            with patch("gza.cli.query.time.sleep"):
                rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 0
        assert "Task " in captured.out and "killed" in captured.out
        # Confirm SIGTERM was sent
        import signal
        mock_kill.assert_any_call(12345, signal.SIGTERM)
        # Task status must be KILLED
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "KILLED"

    def test_kill_escalates_to_sigkill_when_process_survives_sigterm(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """If the process survives SIGTERM for 3 seconds, kill escalates to SIGKILL."""

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Stubborn task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 22222
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-kill-2", task_id=task.id, pid=22222, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=False)
        # signal 0 succeeds (process still alive after SIGTERM), others succeed silently
        def fake_kill(pid: int, sig: int) -> None:
            pass  # process "survives" every signal check
        with patch("gza.cli.query.os.kill", side_effect=fake_kill):
            with patch("gza.cli.query.time.sleep"):
                rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 0
        assert "SIGKILL" in captured.out or "escalated" in captured.out.lower()

    def test_kill_force_sends_sigkill_immediately(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """--force skips SIGTERM and sends SIGKILL immediately."""
        import signal as _signal

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Force kill task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 33333
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-kill-3", task_id=task.id, pid=33333, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=True)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        assert rc == 0
        mock_kill.assert_called_once_with(33333, _signal.SIGKILL)

    def test_kill_uses_running_pid_when_no_worker_record(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """tmux bug case: no worker record, kill falls back to task.running_pid."""
        import signal as _signal

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        task = store.add("Orphaned task")
        assert task.id is not None
        task.status = "in_progress"
        task.running_pid = 44444
        store.update(task)

        args = argparse.Namespace(project_dir=tmp_path, task_id=task.id, all=False, force=True)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        capsys.readouterr()
        assert rc == 0
        mock_kill.assert_called_once_with(44444, _signal.SIGKILL)
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.failure_reason == "KILLED"

    def test_kill_all_kills_all_in_progress_tasks(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """--all kills every in-progress task."""
        import signal as _signal

        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")

        task1 = store.add("Task A")
        assert task1.id is not None
        task1.status = "in_progress"
        task1.running_pid = 55555
        store.update(task1)

        task2 = store.add("Task B")
        assert task2.id is not None
        task2.status = "in_progress"
        task2.running_pid = 66666
        store.update(task2)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(WorkerMetadata(worker_id="w-all-1", task_id=task1.id, pid=55555, status="running"))
        registry.register(WorkerMetadata(worker_id="w-all-2", task_id=task2.id, pid=66666, status="running"))

        args = argparse.Namespace(project_dir=tmp_path, task_id=None, all=True, force=True)
        with patch("gza.cli.query.os.kill") as mock_kill:
            rc = cmd_kill(args)

        assert rc == 0
        killed_pids = {c.args[0] for c in mock_kill.call_args_list if c.args[1] == _signal.SIGKILL}
        assert {55555, 66666} == killed_pids

    def test_kill_all_no_running_tasks(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """--all exits cleanly with a message when no tasks are running."""
        from gza.cli.query import cmd_kill

        setup_config(tmp_path)

        args = argparse.Namespace(project_dir=tmp_path, task_id=None, all=True, force=False)
        rc = cmd_kill(args)

        captured = capsys.readouterr()
        assert rc == 0
        assert "No running tasks" in captured.out

    def test_kill_all_returns_nonzero_when_some_kills_fail(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """--all returns 1 if any task could not be killed (e.g. no PID)."""
        from gza.cli.query import cmd_kill
        from gza.db import SqliteTaskStore
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")

        # Task with a valid PID — kill will succeed.
        task_ok = store.add("Task with PID")
        assert task_ok.id is not None
        task_ok.status = "in_progress"
        task_ok.running_pid = 55555
        store.update(task_ok)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        registry.register(
            WorkerMetadata(worker_id="w-ok-1", task_id=task_ok.id, pid=55555, status="running")
        )

        # Task with no PID and no worker record — kill will fail.
        task_no_pid = store.add("Task without PID")
        assert task_no_pid.id is not None
        task_no_pid.status = "in_progress"
        task_no_pid.running_pid = None
        store.update(task_no_pid)

        args = argparse.Namespace(project_dir=tmp_path, task_id=None, all=True, force=True)
        with patch("gza.cli.query.os.kill"):
            rc = cmd_kill(args)

        assert rc == 1


class TestLineageCommand:
    """Tests for 'gza lineage <task-id>' command."""

    def test_lineage_single_root_task(self, tmp_path: Path):
        """Lineage command shows a single root task with no children."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Design auth system", "status": "completed", "task_type": "plan"},
        ])

        result = run_gza("lineage", "testproject-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Design auth system" in result.stdout
        assert "plan" in result.stdout
        assert "completed" in result.stdout

    def test_lineage_task_not_found(self, tmp_path: Path):
        """Lineage command returns error for missing task ID."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("lineage", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower() or "not found" in result.stderr.lower()

    def test_lineage_highlights_target_task(self, tmp_path: Path):
        """Lineage command highlights the requested task with an arrow marker."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        root = store.add("Design auth system", task_type="plan")
        root.status = "completed"
        root.completed_at = now
        store.update(root)

        child = store.add("Implement auth per plan", task_type="implement", based_on=root.id)
        child.status = "completed"
        child.completed_at = now
        store.update(child)

        result = run_gza("lineage", str(child.id), "--project", str(tmp_path))

        assert result.returncode == 0
        # Arrow marker for the target task
        assert "→" in result.stdout
        # Both tasks shown — collapse whitespace since Rich may wrap long lines
        normalized = " ".join(result.stdout.split())
        assert "Design auth system" in normalized
        assert "Implement auth per plan" in normalized

    def test_lineage_shows_failed_task_with_reason(self, tmp_path: Path):
        """Lineage command shows failure_reason for failed tasks."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        task = store.add("Implement feature", task_type="implement")
        task.status = "failed"
        task.failure_reason = "MAX_STEPS"
        task.completed_at = now
        store.update(task)

        result = run_gza("lineage", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "MAX_STEPS" in result.stdout
        normalized = " ".join(result.stdout.split())
        assert "Implement feature" in normalized

    def test_lineage_full_tree(self, tmp_path: Path):
        """Lineage command renders a multi-level tree with parent and children."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        root = store.add("Design auth system", task_type="plan")
        root.status = "completed"
        root.completed_at = now
        store.update(root)

        impl = store.add("Implement auth per plan", task_type="implement", based_on=root.id)
        impl.status = "completed"
        impl.completed_at = now
        store.update(impl)

        review = store.add("Review implementation", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = now
        store.update(review)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))

        assert result.returncode == 0
        normalized = " ".join(result.stdout.split())
        assert "Design auth system" in normalized
        assert "Implement auth per plan" in normalized
        assert "Review implementation" in normalized

    def test_lineage_shows_stats_when_available(self, tmp_path: Path):
        """Lineage command shows duration and cost when available."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        task = store.add("Implement feature", task_type="implement")
        task.status = "completed"
        task.completed_at = now
        task.duration_seconds = 120.0  # 2 minutes
        task.cost_usd = 0.1234
        store.update(task)

        result = run_gza("lineage", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "2m" in result.stdout
        assert "0.1234" in result.stdout

    def test_lineage_child_shows_relationship_label(self, tmp_path: Path):
        """Relationship label is shown only when it differs from the task type.

        A review child of type 'review' has rel='review' == type_str='review', so
        the bracket label is suppressed (redundant).  A 'task'-typed child with
        depends_on has rel='depends' != type_str='task', so [depends] IS shown.
        """
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.completed_at = now
        store.update(impl)

        # rel="review" == type_str="review" → label suppressed
        review = store.add("Review feature impl", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = now
        store.update(review)

        # rel="depends" != type_str="task" → label shown
        dep = store.add("Dependent task", task_type="task", depends_on=impl.id)
        dep.status = "pending"
        store.update(dep)

        result = run_gza("lineage", str(impl.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Review feature impl" in result.stdout
        assert "[review]" not in result.stdout
        assert "[depends]" in result.stdout

    def test_lineage_rel_label_brackets_are_rendered_literally(self, tmp_path: Path):
        """Relationship labels render as [rel] text, not as Rich markup tags."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        now = datetime.now(UTC)

        root = store.add("Root task", task_type="implement")
        root.status = "completed"
        root.completed_at = now
        store.update(root)

        # task_type="task" with depends_on → _classify_child_relationship returns "depends"
        # → _LINEAGE_REL_LABELS maps to "depends" → rendered as [depends]
        child = store.add("Dependent task", task_type="task", depends_on=root.id)
        child.status = "pending"
        store.update(child)

        result = run_gza("lineage", str(root.id), "--project", str(tmp_path))

        # returncode == 0 catches MarkupError crashes
        assert result.returncode == 0
        # "[depends]" in stdout catches silent text loss of the relationship label
        assert "[depends]" in result.stdout

    def test_cli_lineage_uses_shared_relationship_label_map(self):
        from gza import query as query_module
        from gza.cli import query as cli_query

        cli_query_path = Path(cli_query.__file__)

        assert cli_query._LINEAGE_REL_LABELS is query_module._LINEAGE_REL_LABELS
        assert "_LINEAGE_REL_LABELS:" not in cli_query_path.read_text()


class TestPsSortKey:
    """Tests for _ps_sort_key sort-key function."""

    def _make_row(
        self,
        task_id: str | int | None = None,
        status: str = "running",
        sort_timestamp: str = "2026-01-01T00:00:00",
        worker_id: str = "w-001",
    ) -> dict:
        return {
            "task_id": task_id,
            "status": status,
            "sort_timestamp": sort_timestamp,
            "worker_id": worker_id,
        }

    def test_string_task_ids_sort_in_numeric_order(self):
        """String task IDs sort numerically by decimal suffix."""
        from gza.cli.query import _ps_sort_key

        rows = [
            self._make_row(task_id="gza-100"),
            self._make_row(task_id="gza-10"),
            self._make_row(task_id="gza-2"),  # 2
            self._make_row(task_id="gza-1"),  # 1
        ]
        sorted_rows = sorted(rows, key=_ps_sort_key)
        assert [r["task_id"] for r in sorted_rows] == [
            "gza-1",
            "gza-2",
            "gza-10",
            "gza-100",
        ]

    def test_none_task_id_sorts_last(self):
        """Worker-only rows (task_id=None) must sort after all tasks."""
        import sys

        from gza.cli.query import _ps_sort_key

        row_with_task = self._make_row(task_id="gza-1")
        row_no_task = self._make_row(task_id=None)

        key_with_task = _ps_sort_key(row_with_task)
        key_no_task = _ps_sort_key(row_no_task)

        # task_id component (index 3) of the no-task row should be sys.maxsize
        assert key_no_task[3] == sys.maxsize
        assert key_with_task[3] < sys.maxsize

    def test_status_group_ordering(self):
        """in_progress sorts before failed, failed before completed."""
        from gza.cli.query import _ps_sort_key

        in_progress_row = self._make_row(task_id="gza-1", status="in_progress")
        failed_row = self._make_row(task_id="gza-2", status="failed")
        completed_row = self._make_row(task_id="gza-3", status="completed")

        assert _ps_sort_key(in_progress_row)[0] < _ps_sort_key(failed_row)[0]
        assert _ps_sort_key(failed_row)[0] < _ps_sort_key(completed_row)[0]

    def test_integer_task_id_backward_compat(self):
        """Integer task_id values sort numerically (backward compat)."""
        from gza.cli.query import _ps_sort_key

        row_int = self._make_row(task_id=5)
        key = _ps_sort_key(row_int)
        assert key[3] == 5

    def test_completed_sort_by_start_time_descending(self):
        """Completed tasks sort most-recently-started first."""
        from gza.cli.query import _ps_sort_key

        early = self._make_row(task_id="gza-1", status="completed", sort_timestamp="2026-01-01T00:00:00")
        late = self._make_row(task_id="gza-2", status="completed", sort_timestamp="2026-01-02T00:00:00")

        sorted_rows = sorted([early, late], key=_ps_sort_key)
        assert [r["task_id"] for r in sorted_rows] == ["gza-2", "gza-1"]

    def test_in_progress_sort_by_start_time_ascending(self):
        """In-progress tasks sort longest-running (earliest start) first."""
        from gza.cli.query import _ps_sort_key

        early = self._make_row(task_id="gza-1", status="in_progress", sort_timestamp="2026-01-01T00:00:00")
        late = self._make_row(task_id="gza-2", status="in_progress", sort_timestamp="2026-01-02T00:00:00")

        sorted_rows = sorted([early, late], key=_ps_sort_key)
        assert [r["task_id"] for r in sorted_rows] == ["gza-1", "gza-2"]


class TestIncompleteCommand:
    """Tests for `gza incomplete` command."""

    def test_incomplete_hides_merged_root_with_completed_review_improve(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement root", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        root.merge_status = "merged"
        store.update(root)
        assert root.id is not None

        review = store.add("Review done", task_type="review", based_on=root.id, depends_on=root.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.merge_status = "unmerged"
        store.update(review)
        assert review.id is not None

        improve = store.add("Improve done", task_type="improve", based_on=root.id, depends_on=review.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.merge_status = "unmerged"
        store.update(improve)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unresolved task lineages" in result.stdout
        assert "Implement root" not in result.stdout

    def test_incomplete_shows_failed_improve_under_merged_root(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement root", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        root.merge_status = "merged"
        store.update(root)
        assert root.id is not None

        improve = store.add("Improve failed", task_type="improve", based_on=root.id, same_branch=True)
        improve.status = "failed"
        improve.completed_at = datetime.now(UTC)
        improve.failure_reason = "TEST_FAILURE"
        store.update(improve)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement root" in result.stdout
        assert "Improve failed" in result.stdout

    def test_incomplete_shows_unmerged_root_once_without_completed_improve_child(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement root", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        root.has_commits = True
        root.merge_status = "unmerged"
        store.update(root)
        assert root.id is not None

        improve = store.add("Improve completed", task_type="improve", based_on=root.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        improve.has_commits = True
        improve.merge_status = "unmerged"
        store.update(improve)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement root" in result.stdout
        assert "Improve completed" not in result.stdout

    def test_incomplete_retry_chain_is_root_anchored_with_latest_attempt_visible(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        first = store.add("Attempt one", task_type="implement")
        first.status = "failed"
        first.completed_at = datetime.now(UTC)
        store.update(first)
        assert first.id is not None

        second = store.add("Attempt two", task_type="implement", based_on=first.id)
        second.status = "failed"
        second.completed_at = datetime.now(UTC)
        store.update(second)
        assert second.id is not None

        third = store.add("Attempt three", task_type="implement", based_on=second.id)
        third.status = "completed"
        third.completed_at = datetime.now(UTC)
        third.has_commits = True
        third.merge_status = "unmerged"
        store.update(third)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Attempt one" in result.stdout
        assert "Attempt two" in result.stdout
        assert "Attempt three" in result.stdout

    def test_incomplete_includes_legacy_unmerged_status_rows(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        legacy = store.add("Legacy unmerged", task_type="implement")
        legacy.status = "unmerged"
        legacy.completed_at = datetime.now(UTC)
        legacy.has_commits = True
        store.update(legacy)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Legacy unmerged" in result.stdout

    def test_incomplete_branching_retry_shows_all_unresolved_siblings_under_one_root(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Root failed", task_type="implement")
        root.status = "failed"
        root.completed_at = datetime.now(UTC)
        root.failure_reason = "ROOT_TEST_FAILURE"
        store.update(root)
        assert root.id is not None

        retry_completed = store.add("Retry completed", task_type="implement", based_on=root.id)
        retry_completed.status = "completed"
        retry_completed.completed_at = datetime.now(UTC)
        retry_completed.has_commits = True
        retry_completed.merge_status = "unmerged"
        store.update(retry_completed)
        assert retry_completed.id is not None

        retry_failed = store.add("Retry failed", task_type="implement", based_on=root.id)
        retry_failed.status = "failed"
        retry_failed.completed_at = datetime.now(UTC)
        retry_failed.failure_reason = "RETRY_TEST_FAILURE"
        store.update(retry_failed)
        assert retry_failed.id is not None

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Root failed" in result.stdout
        assert "Retry completed" in result.stdout
        assert "Retry failed" in result.stdout
        # The resolved root anchor must not be rendered as a normal failed row:
        # it should not repeat its own failure reason or retry-as annotation.
        assert "ROOT_TEST_FAILURE" not in result.stdout
        # The unresolved retry's own failure reason should still be rendered.
        assert "RETRY_TEST_FAILURE" in result.stdout

    def test_incomplete_completed_unmerged_root_hidden_when_retry_merges(self, tmp_path: Path):
        """Regression: a completed-unmerged root should disappear from
        `gza incomplete` once a later same-lineage retry has merged."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        first = store.add("First attempt unmerged", task_type="implement")
        first.status = "completed"
        first.completed_at = datetime.now(UTC)
        first.has_commits = True
        first.merge_status = "unmerged"
        store.update(first)
        assert first.id is not None

        second = store.add("Second attempt merged", task_type="implement", based_on=first.id)
        second.status = "completed"
        second.completed_at = datetime.now(UTC)
        second.has_commits = True
        second.merge_status = "merged"
        store.update(second)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unresolved task lineages" in result.stdout
        assert "First attempt unmerged" not in result.stdout

    def test_incomplete_hides_completed_rebase_under_merged_root(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement root", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        root.merge_status = "merged"
        store.update(root)
        assert root.id is not None

        rebase = store.add("Rebase done", task_type="rebase", based_on=root.id, same_branch=True)
        rebase.status = "completed"
        rebase.completed_at = datetime.now(UTC)
        rebase.merge_status = "unmerged"
        store.update(rebase)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unresolved task lineages" in result.stdout
        assert "Rebase done" not in result.stdout

    def test_incomplete_shows_failed_rebase_under_merged_root(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement root", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        root.merge_status = "merged"
        store.update(root)
        assert root.id is not None

        rebase = store.add("Rebase failed", task_type="rebase", based_on=root.id, same_branch=True)
        rebase.status = "failed"
        rebase.completed_at = datetime.now(UTC)
        rebase.failure_reason = "TEST_FAILURE"
        store.update(rebase)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement root" in result.stdout
        assert "Rebase failed" in result.stdout

    def test_incomplete_ignores_completed_no_commit_plan_root(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan complete", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        plan.has_commits = False
        plan.merge_status = None
        store.update(plan)

        result = run_gza("incomplete", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unresolved task lineages" in result.stdout
        assert "Plan complete" not in result.stdout

    def test_incomplete_days_filter_hides_old_failed_root_with_recent_merged_retry(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Old failed root", task_type="implement")
        root.status = "failed"
        root.created_at = datetime.now(UTC) - timedelta(days=30)
        root.completed_at = datetime.now(UTC) - timedelta(days=30)
        root.failure_reason = "TEST_FAILURE"
        store.update(root)
        assert root.id is not None

        retry = store.add("Recent merged retry", task_type="implement", based_on=root.id)
        retry.status = "completed"
        retry.created_at = datetime.now(UTC) - timedelta(hours=12)
        retry.completed_at = datetime.now(UTC) - timedelta(hours=12)
        retry.has_commits = True
        retry.merge_status = "merged"
        store.update(retry)

        result = run_gza("incomplete", "--days", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unresolved task lineages" in result.stdout
        assert "Old failed root" not in result.stdout

    def test_incomplete_type_filter_hides_failed_root_when_same_type_retry_succeeds(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Failed implement root", task_type="implement")
        root.status = "failed"
        root.completed_at = datetime.now(UTC) - timedelta(hours=2)
        root.failure_reason = "TEST_FAILURE"
        store.update(root)
        assert root.id is not None

        improve = store.add("Intervening improve", task_type="improve", based_on=root.id, same_branch=True)
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC) - timedelta(hours=1)
        improve.has_commits = True
        improve.merge_status = "unmerged"
        store.update(improve)

        retry = store.add("Successful implement retry", task_type="implement", based_on=root.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        retry.has_commits = True
        retry.merge_status = "merged"
        store.update(retry)

        result = run_gza("incomplete", "--type", "implement", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unresolved task lineages" in result.stdout
        assert "Failed implement root" not in result.stdout
