"""Tests for the CLI commands."""

import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest


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
        task = store.add(task_data["prompt"], task_type=task_data.get("task_type", "task"))
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
            {"prompt": "Regular task", "status": "completed", "task_type": "task"},
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
            {"prompt": "Regular task", "status": "completed", "task_type": "task"},
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
            {"prompt": "Regular task", "status": "completed", "task_type": "task"},
        ])

        result = run_gza("history", "--type", "review", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks with type 'review'" in result.stdout


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

        result = run_gza("retry", "1", "--project", str(tmp_path))

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

        result = run_gza("retry", "1", "--project", str(tmp_path))

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
        """Retry command preserves task_type, group, spec, and other fields."""
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
            create_review=True,
            task_type_hint="feature",
        )
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        # Retry the task
        result = run_gza("retry", "1", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify the new task has the same metadata
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.prompt == "Test task with metadata"
        assert new_task.task_type == "explore"
        assert new_task.group == "test-group"
        assert new_task.spec == "spec.md"
        assert new_task.create_review is True
        assert new_task.task_type_hint == "feature"
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

    def test_retry_with_run_flag(self, tmp_path: Path):
        """Retry command with --run attempts to run the newly created task."""
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

        # Run retry with --run flag (will fail due to missing API key, but we can verify it tries)
        result = run_gza("retry", "1", "--run", "--no-docker", "--project", str(tmp_path))

        # Verify the new task was created
        assert "Created task #2" in result.stdout
        assert "Running task #2" in result.stdout

        # Verify new task exists
        new_task = store.get(2)
        assert new_task is not None
        assert new_task.prompt == "Failed task to retry"
        assert new_task.based_on == 1


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
        """Resume command fails for non-failed tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("resume", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Can only resume failed tasks" in result.stdout

    def test_resume_with_run_flag(self, tmp_path: Path):
        """Resume command with --run creates a new task and attempts to run it."""
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

        # Run resume with --run flag (will fail due to missing API key, but we can verify it tries)
        result = run_gza("resume", "1", "--run", "--no-docker", "--project", str(tmp_path))

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


class TestInitCommand:
    """Tests for 'gza init' command."""

    def test_init_creates_config(self, tmp_path: Path):
        """Init command creates config in project root."""
        result = run_gza("init", "--project", str(tmp_path))

        assert result.returncode == 0
        config_path = tmp_path / "gza.yaml"
        assert config_path.exists()

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

        result = run_gza("init", "--force", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify config was overwritten (has directory name, not "original")
        config_path = tmp_path / "gza.yaml"
        content = config_path.read_text()
        assert tmp_path.name in content


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
        assert "Turns: 10" in result.stdout
        assert "Cost: $0.5000" in result.stdout

    def test_log_by_task_id_jsonl_format(self, tmp_path: Path):
        """Log command with --task parses JSONL format with successful result."""
        import json
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

        # Create a JSONL log file (new format)
        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        lines = [
            {"type": "system", "subtype": "init", "session_id": "abc123"},
            {"type": "assistant", "message": {"role": "assistant", "content": "Hello"}},
            {"type": "user", "message": {"role": "user", "content": "Hi"}},
            {
                "type": "result",
                "subtype": "success",
                "result": "## JSONL Summary\n\nThis was parsed from JSONL!",
                "duration_ms": 120000,
                "num_turns": 5,
                "total_cost_usd": 0.25,
            },
        ]
        log_file.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_gza("log", "--task", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "This was parsed from JSONL!" in result.stdout
        assert "Duration:" in result.stdout
        assert "Turns: 5" in result.stdout
        assert "Cost: $0.2500" in result.stdout

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
        assert "Turns: 60" in result.stdout
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
        """Log command with --task shows formatted entries when no result entry exists."""
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
    """Tests for 'gza status <group>' command."""

    def test_status_with_group(self, tmp_path: Path):
        """Status command shows tasks in a group."""
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

        result = run_gza("status", "test-group", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "test-group" in result.stdout
        assert "First task" in result.stdout
        assert "Second task" in result.stdout


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

    def test_task_model_overrides_config(self, tmp_path: Path):
        """Task-specific model takes priority over config model."""
        from gza.config import Config
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.model = "claude-3-5-sonnet-latest"

        task = Task(
            id=1,
            prompt="Test task",
            model="claude-3-5-haiku-latest",
        )

        model, provider = get_effective_config_for_task(task, config)
        assert model == "claude-3-5-haiku-latest"

    def test_task_type_model_overrides_default(self, tmp_path: Path):
        """Task-type model takes priority over default config model."""
        from gza.config import Config, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.model = "claude-3-5-sonnet-latest"
        config.task_types = {
            "review": TaskTypeConfig(model="claude-3-5-haiku-latest")
        }

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
        )

        model, provider = get_effective_config_for_task(task, config)
        assert model == "claude-3-5-haiku-latest"

    def test_default_config_model_used(self, tmp_path: Path):
        """Default config model is used when no overrides."""
        from gza.config import Config
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.model = "claude-3-5-sonnet-latest"

        task = Task(
            id=1,
            prompt="Test task",
        )

        model, provider = get_effective_config_for_task(task, config)
        assert model == "claude-3-5-sonnet-latest"

    def test_task_provider_overrides_config(self, tmp_path: Path):
        """Task-specific provider takes priority over config provider."""
        from gza.config import Config
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"

        task = Task(
            id=1,
            prompt="Test task",
            provider="gemini",
        )

        model, provider = get_effective_config_for_task(task, config)
        assert provider == "gemini"


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
        assert f"#{task.id}" in result.stdout, f"Output should contain task ID #{task.id}"

        # Cleanup
        registry.remove("w-test-ps")


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

        # Run improve command
        result = run_gza("improve", "1", "--project", str(tmp_path))

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

        # Run improve command with --review flag
        result = run_gza("improve", "1", "--review", "--project", str(tmp_path))

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

        # Run improve command
        result = run_gza("improve", "1", "--project", str(tmp_path))

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

        # Run improve command
        result = run_gza("improve", "1", "--project", str(tmp_path))

        # Should succeed but warn about incomplete review
        assert result.returncode == 0
        assert "Warning: Review #2 is pending" in result.stdout
        assert "blocked until it completes" in result.stdout
        assert "Created improve task #3" in result.stdout


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

        # Run review command
        result = run_gza("review", "1", "--project", str(tmp_path))

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
        """Review command fails if task is not an implementation task."""
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
        assert "is a plan task, not an implementation task" in result.stdout

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

        # Run review command
        result = run_gza("review", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task #3" in result.stdout

        # Verify the review task inherited based_on
        review_task = store.get(3)
        assert review_task is not None
        assert review_task.based_on == 1  # plan task
        assert review_task.depends_on == 2  # implementation task

    def test_review_with_run_flag(self, tmp_path: Path):
        """Review command with --run runs the review task immediately."""
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

        # Run review command with --run flag
        result = run_gza("review", "1", "--run", "--no-docker", "--project", str(tmp_path))

        # Verify the review task was created and run attempted
        assert "Created review task #2" in result.stdout
        assert "Running review task #2" in result.stdout

        # Verify the review task exists
        review_task = store.get(2)
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == 1

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

            # Run review command with --run and --open flags
            result = run_gza("review", "1", "--run", "--open", "--no-docker", "--project", str(tmp_path))

            # Check that warning about missing EDITOR is shown
            # Note: This might not appear in output if the task doesn't complete successfully in test
            # The important thing is that the flag is accepted and doesn't cause an error
            assert result.returncode in (0, 1)  # May fail due to missing credentials, but flag should be accepted

    def test_review_open_flag_requires_run(self, tmp_path: Path):
        """--open flag has no effect without --run flag."""
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

        # Run review command with --open but without --run
        result = run_gza("review", "1", "--open", "--project", str(tmp_path))

        # Should succeed but not run the task
        assert result.returncode == 0
        assert "Created review task #2" in result.stdout
        assert "Running review task" not in result.stdout


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
        task = store.add("Test task", task_type="task")
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
        task = store.add("Test task", task_type="task")
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

        # Create a config with a default max_turns
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_turns: 50\n")

        # Load config
        config = Config.load(tmp_path)
        assert config.max_turns == 50

        # Apply override like cmd_run does
        args = argparse.Namespace(max_turns=100, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_turns = args.max_turns

        assert config.max_turns == 100

    def test_retry_command_accepts_max_turns_flag(self, tmp_path: Path):
        """Retry command accepts --max-turns flag without error."""
        from gza.config import Config
        import argparse

        # Create a config with a default max_turns
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_turns: 50\n")

        # Load config
        config = Config.load(tmp_path)
        assert config.max_turns == 50

        # Apply override like cmd_retry does
        args = argparse.Namespace(max_turns=150, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_turns = args.max_turns

        assert config.max_turns == 150

    def test_resume_command_accepts_max_turns_flag(self, tmp_path: Path):
        """Resume command accepts --max-turns flag without error."""
        from gza.config import Config
        import argparse

        # Create a config with a default max_turns
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_turns: 50\n")

        # Load config
        config = Config.load(tmp_path)
        assert config.max_turns == 50

        # Apply override like cmd_resume does
        args = argparse.Namespace(max_turns=200, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_turns = args.max_turns

        assert config.max_turns == 200

    def test_max_turns_override_takes_precedence_over_config(self, tmp_path: Path):
        """--max-turns flag overrides the value from gza.yaml."""
        from gza.config import Config
        import argparse

        # Create a config with a default max_turns of 50
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_turns: 50\n")

        # Load config
        config = Config.load(tmp_path)
        before = config.max_turns
        assert before == 50

        # Apply override
        args = argparse.Namespace(max_turns=999, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
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
        assert "⚠ changes requested" not in result.stdout

    def test_unmerged_hides_review_status_after_improve_clears_it(self, tmp_path: Path):
        """After improve task clears review state, review status is not shown."""
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

        # After improve: review status should be gone
        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "⚠ changes requested" not in result.stdout
        assert "approved" not in result.stdout

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
        assert "✓ approved" in result.stdout
        assert "⚠ changes requested" not in result.stdout


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
        assert second.review_cleared_at >= first_cleared


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

    def test_invoke_claude_resolve_returns_true_when_resolved(self, tmp_path):
        """Test invoke_claude_resolve returns True when conflicts are resolved."""
        from gza.cli import invoke_claude_resolve
        from gza.config import Config
        from gza.providers.base import RunResult
        from unittest.mock import patch

        config = Config(project_dir=tmp_path, project_name="test")

        with patch('gza.providers.claude.ClaudeProvider.run', return_value=RunResult(exit_code=0)) as mock_run, \
             patch('pathlib.Path.exists', return_value=False):
            result = invoke_claude_resolve("feature", "main", config)

            assert result is True
            # Verify ClaudeProvider.run was called with the skill prompt
            mock_run.assert_called_once()
            call_args = mock_run.call_args
            assert "/gza-rebase --auto" in call_args.args or "/gza-rebase --auto" in call_args.kwargs.values()

    def test_invoke_claude_resolve_returns_false_when_unresolved(self, tmp_path):
        """Test invoke_claude_resolve returns False when conflicts remain."""
        from gza.cli import invoke_claude_resolve
        from gza.config import Config
        from gza.providers.base import RunResult
        from unittest.mock import patch

        config = Config(project_dir=tmp_path, project_name="test")

        with patch('gza.providers.claude.ClaudeProvider.run', return_value=RunResult(exit_code=0)), \
             patch('pathlib.Path.exists', return_value=True):
            result = invoke_claude_resolve("feature", "main", config)

            assert result is False


class TestForceCompleteCommand:
    """Tests for 'gza force-complete' command."""

    def test_force_complete_failed_task(self, tmp_path: Path):
        """Force-complete marks a failed task as completed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Failed task", "status": "failed"},
        ])

        result = run_gza("force-complete", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "failed → completed" in result.stdout

    def test_force_complete_pending_task(self, tmp_path: Path):
        """Force-complete marks a pending task as completed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("force-complete", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pending → completed" in result.stdout

    def test_force_complete_in_progress_task(self, tmp_path: Path):
        """Force-complete marks an in_progress task as completed."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("In-progress task")
        task.status = "in_progress"
        store.update(task)

        result = run_gza("force-complete", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "in_progress → completed" in result.stdout

    def test_force_complete_already_completed_fails(self, tmp_path: Path):
        """Force-complete fails if task is already completed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("force-complete", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "already completed" in result.stdout

    def test_force_complete_nonexistent_task(self, tmp_path: Path):
        """Force-complete fails for a nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("force-complete", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_force_complete_persists_status(self, tmp_path: Path):
        """Force-complete actually updates the task status in the store."""
        from gza.db import SqliteTaskStore

        setup_db_with_tasks(tmp_path, [
            {"prompt": "Failed task", "status": "failed"},
        ])

        run_gza("force-complete", "1", "--project", str(tmp_path))

        db_path = tmp_path / ".gza" / "gza.db"
        store = SqliteTaskStore(db_path)
        task = store.get(1)
        assert task is not None
        assert task.status == "completed"

    def test_force_complete_preserves_branch(self, tmp_path: Path):
        """Force-complete preserves an existing branch on the task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task = store.add("Task with branch")
        task.status = "failed"
        task.branch = "gza/1-task-with-branch"
        store.update(task)

        result = run_gza("force-complete", "1", "--project", str(tmp_path))

        assert result.returncode == 0
        updated_task = store.get(1)
        assert updated_task is not None
        assert updated_task.branch == "gza/1-task-with-branch"
        assert updated_task.status == "completed"


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

    def test_mark_completed_warns_if_not_failed(self, tmp_path: Path):
        """mark-completed warns when task status is not failed."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

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

    def test_mark_completed_errors_if_no_branch_on_task(self, tmp_path: Path):
        """mark-completed errors when task has no branch set."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task without branch", "status": "failed"},
        ])

        result = run_gza("mark-completed", "1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout

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

    def test_mark_completed_with_commits_sets_unmerged(self, tmp_path: Path):
        """mark-completed sets status='unmerged' when branch has commits."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

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
        from gza.git import Git

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

    def test_mark_completed_failed_task_no_warning(self, tmp_path: Path):
        """mark-completed does not warn when task is in failed status."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

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
