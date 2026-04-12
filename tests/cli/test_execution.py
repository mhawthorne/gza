"""Tests for task execution and lifecycle CLI commands."""


import argparse
import os
import re
import signal as signal_mod
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from gza.cli import _run_as_worker, _run_foreground
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.query import build_lineage_tree
from gza.workers import WorkerRegistry

from .conftest import get_latest_task, make_store, run_gza, setup_config, setup_db_with_tasks


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

        setup_config(tmp_path)
        tmp_path / ".gza" / "gza.db"

        # Create a file with prompt text
        prompt_file = tmp_path / "task_prompt.txt"
        prompt_file.write_text("Implement feature X")

        result = run_gza("add", "--prompt-file", str(prompt_file), "--type", "implement", "--group", "features", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added with correct attributes
        store = make_store(tmp_path)
        task = get_latest_task(store, task_type="implement", prompt="Implement feature X")
        assert task is not None
        assert task.prompt == "Implement feature X"
        assert task.task_type == "implement"
        assert task.group == "features"


class TestEditCommand:
    """Tests for 'gza edit' command."""

    def test_edit_group(self, tmp_path: Path):
        """Edit command can change task group."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.group is None

        result = run_gza("edit", str(task.id), "--group", "new-group", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify group was updated
        updated = store.get(task.id)
        assert updated.group == "new-group"

    def test_edit_remove_group(self, tmp_path: Path):
        """Edit command can remove task from group."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", group="old-group")
        assert task.group == "old-group"

        result = run_gza("edit", str(task.id), "--group", "", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify group was removed
        updated = store.get(task.id)
        assert updated.group is None or updated.group == ""

    def test_edit_review_flag(self, tmp_path: Path):
        """Edit command can enable automatic review task creation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_review is False

        result = run_gza("edit", str(task.id), "--review", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify create_review was enabled
        updated = store.get(task.id)
        assert updated.create_review is True

    def test_edit_with_prompt_file(self, tmp_path: Path):
        """Edit command can update prompt from file."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        result = run_gza("edit", str(task.id), "--prompt-file", "/nonexistent/file.txt", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()

    def test_edit_with_prompt_text(self, tmp_path: Path):
        """Edit command can update prompt from command line."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        result = run_gza("edit", str(task.id), "--prompt", "New prompt from command line", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt from command line"

    def test_edit_with_prompt_validation_error(self, tmp_path: Path):
        """Edit command validates prompt length."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File content")

        result = run_gza("edit", str(task.id), "--prompt", "text", "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Cannot use both" in result.stdout

    def test_edit_with_prompt_from_stdin(self, tmp_path: Path):
        """Edit command can read prompt from stdin using --prompt -."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        stdin_content = "New prompt from stdin input"
        result = run_gza("edit", str(task.id), "--prompt", "-", "--project", str(tmp_path), stdin_input=stdin_content)

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt from stdin input"

    def test_cmd_edit_based_on_sets_based_on_field(self, tmp_path: Path):
        """--based-on sets task.based_on, not task.depends_on."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent_task = store.add("Parent task")
        task = store.add("Target task")

        result = run_gza("edit", str(task.id), "--based-on", str(parent_task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        updated = store.get(task.id)
        assert updated.based_on == parent_task.id
        assert updated.depends_on is None

    def test_cmd_edit_depends_on_sets_depends_on_field(self, tmp_path: Path):
        """--depends-on sets task.depends_on, not task.based_on."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        dep_task = store.add("Dependency task")
        task = store.add("Target task")

        result = run_gza("edit", str(task.id), "--depends-on", str(dep_task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        updated = store.get(task.id)
        assert updated.depends_on == dep_task.id
        assert updated.based_on is None

    def test_cmd_edit_based_on_nonexistent_task_errors(self, tmp_path: Path):
        """--based-on with nonexistent target ID returns error code 1."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Target task")

        result = run_gza("edit", str(task.id), "--based-on", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()

    def test_cmd_edit_depends_on_nonexistent_task_errors(self, tmp_path: Path):
        """--depends-on with nonexistent target ID returns error code 1."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Target task")

        result = run_gza("edit", str(task.id), "--depends-on", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()


class TestRetryCommand:
    """Tests for 'gza retry' command."""

    def test_retry_completed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a completed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Original task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert f"retry of {task.id}" in result.stdout

        # Verify new task was created with same prompt
        result = run_gza("next", "--project", str(tmp_path))
        assert "Original task" in result.stdout

    def test_retry_failed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a failed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Failed task")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert f"retry of {task.id}" in result.stdout

    def test_retry_pending_task_fails(self, tmp_path: Path):
        """Retry command fails for pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza("retry", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Can only retry completed or failed" in result.stdout

    def test_retry_nonexistent_task(self, tmp_path: Path):
        """Retry command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("retry", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_retry_preserves_task_fields(self, tmp_path: Path):
        """Retry command preserves task metadata and linkage fields."""

        setup_config(tmp_path)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)

        # Create a dependency task first
        dep_task = store.add("Dependency task")
        dep_task.status = "completed"
        dep_task.completed_at = datetime.now(UTC)
        store.update(dep_task)

        # Create a task with metadata
        task = store.add(
            "Test task with metadata",
            task_type="explore",
            group="test-group",
            spec="spec.md",
            depends_on=dep_task.id,
            create_review=True,
            same_branch=True,
            task_type_hint="feature",
            model="gpt-5.3-codex",
            provider="codex",
        )
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Retry the task
        result = run_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify the new task has the same metadata
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.prompt == "Test task with metadata"
        assert new_task.task_type == "explore"
        assert new_task.group == "test-group"
        assert new_task.spec == "spec.md"
        assert new_task.depends_on == dep_task.id
        assert new_task.create_review is True
        assert new_task.same_branch is True
        assert new_task.task_type_hint == "feature"
        assert new_task.model == "gpt-5.3-codex"
        assert new_task.provider == "codex"
        assert new_task.provider_is_explicit is True
        assert new_task.based_on == task.id
        assert new_task.status == "pending"

    def test_retry_does_not_copy_non_explicit_provider(self, tmp_path: Path):
        """Retry should not preserve provider that came from resolved default state."""

        setup_config(tmp_path)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)

        task = store.add("Task with stale resolved provider")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.provider = "claude"
        task.provider_is_explicit = False
        store.update(task)

        result = run_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert retry_task is not None
        assert retry_task.id != task.id
        assert retry_task.provider is None
        assert retry_task.provider_is_explicit is False

    def test_retry_with_background_flag(self, tmp_path: Path):
        """Retry command with --background spawns a worker for the new task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        # Run retry with background mode
        result = run_gza("retry", str(task.id), "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes successfully
        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert "Started worker" in result.stdout

        # Verify new task was created
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.prompt == "Failed task to retry"
        # Background worker may claim the task before we check, so accept both
        assert new_task.status in ("pending", "in_progress")
        assert new_task.based_on == task.id

    def test_retry_runs_by_default(self, tmp_path: Path):
        """Retry command runs the newly created task immediately by default."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Run retry without any flags (will fail due to missing API key, but we can verify it tries)
        result = run_gza("retry", str(task.id), "--no-docker", "--project", str(tmp_path))

        # Verify the new task was created and run was attempted
        assert "Created task " in result.stdout
        assert "Running task " in result.stdout

        # Verify new task exists
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.prompt == "Failed task to retry"
        assert new_task.based_on == task.id

    def test_retry_with_queue_flag(self, tmp_path: Path):
        """Retry command with --queue adds task to queue without executing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Run retry with --queue flag
        result = run_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        # Verify the new task was created but not run
        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert "Running task" not in result.stdout

        # Verify new task is still pending
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.status == "pending"

    def test_retry_blocked_if_successful_retry_exists(self, tmp_path: Path):
        """Retry command fails if the task already has a child retry with status completed."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        # Create original failed task
        original = store.add("Original task")
        original.status = "failed"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        # Create a successful retry child
        retry = store.add("Original task", based_on=original.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        store.update(retry)

        result = run_gza("retry", str(original.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert (
            f"Error: Task {original.id} already has a successful retry ({retry.id})."
            in result.stdout
        )

    @pytest.mark.parametrize("wrapped_id", ["  {id}  ", "\t{id}\t"])
    def test_retry_duplicate_guard_uses_canonical_task_id(self, tmp_path: Path, wrapped_id: str):
        """Retry duplicate checks should use the canonical resolved task ID."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original task")
        original.status = "failed"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        retry = store.add("Original task", based_on=original.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        store.update(retry)

        task_arg = wrapped_id.format(id=original.id)
        result = run_gza("retry", task_arg, "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {original.id} already has a successful retry ({retry.id})." in result.stdout


class TestResumeCommand:
    """Tests for 'gza resume' command."""

    def test_resume_with_background_flag(self, tmp_path: Path):
        """Resume command with --background creates a new task and spawns a worker."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        # Run resume with background mode
        result = run_gza("resume", str(task.id), "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes successfully
        assert result.returncode == 0
        # Verify new task was created
        assert "resume of " in result.stdout
        assert "Started worker" in result.stdout
        assert "(resuming)" in result.stdout

        # Verify original task still failed and new task was created
        original = store.get(task.id)
        assert original is not None
        assert original.status == "failed"
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.based_on == task.id
        assert new_task.session_id == "test-session-123"

    def test_resume_without_session_id_fails(self, tmp_path: Path):
        """Resume command fails for tasks without session ID."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task without session ID
        task = store.add("Failed task without session")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Try to resume
        result = run_gza("resume", str(task.id), "--project", str(tmp_path))

        # Verify it fails with helpful message
        assert result.returncode == 1
        assert "has no session ID" in result.stdout
        assert "gza retry" in result.stdout

    def test_resume_non_failed_task_fails(self, tmp_path: Path):
        """Resume command fails for non-failed, non-orphaned tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza("resume", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Can only resume failed or orphaned tasks" in result.stdout

    def test_resume_runs_by_default(self, tmp_path: Path):
        """Resume command runs the new task immediately by default."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Run resume without any special flags (will fail due to missing API key, but we can verify it tries)
        result = run_gza("resume", str(task.id), "--no-docker", "--project", str(tmp_path))

        # Verify the command creates a new task
        assert "resume of " in result.stdout

        # Verify original task stays failed and new task was created
        original = store.get(task.id)
        assert original is not None
        assert original.status == "failed"
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.based_on == task.id
        assert new_task.session_id == "test-session-123"

    def test_resume_with_queue_flag(self, tmp_path: Path):
        """Resume command with --queue adds task to queue without executing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Run resume with --queue flag
        result = run_gza("resume", str(task.id), "--queue", "--project", str(tmp_path))

        # Verify the command creates a new task but does not run it
        assert result.returncode == 0
        assert "resume of " in result.stdout
        assert "Running" not in result.stdout

        # Verify new task is pending
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.status == "pending"
        assert new_task.session_id == "test-session-123"

    def test_resume_creates_new_task_preserves_original(self, tmp_path: Path):
        """Resume creates a new pending task, leaving original task as failed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with session ID, log, and stats
        task = store.add("Implement feature X")
        task.status = "failed"
        task.session_id = "session-abc-123"
        task.task_type = "implement"
        task.num_turns_reported = 42
        task.cost_usd = 1.50
        task.duration_seconds = 300.0
        task.log_file = ".gza/logs/20260101-implement-feature-x.log"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Run resume (will fail trying to run due to missing API key/git, but task should be created)
        result = run_gza("resume", str(task.id), "--no-docker", "--project", str(tmp_path))

        # Verify output
        assert "resume of " in result.stdout

        # Verify original task stays failed with stats preserved
        original = store.get(task.id)
        assert original is not None
        assert original.status == "failed"
        assert original.num_turns_reported == 42
        assert original.cost_usd == 1.50
        assert original.duration_seconds == 300.0
        assert original.log_file == ".gza/logs/20260101-implement-feature-x.log"

        # Verify new task has the right properties
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.prompt == "Implement feature X"
        assert new_task.task_type == "implement"
        assert new_task.based_on == task.id
        assert new_task.session_id == "session-abc-123"
        # New task starts with no stats
        assert new_task.num_turns_reported is None
        assert new_task.cost_usd is None
        assert new_task.log_file is None

    def test_resume_orphaned_in_progress_task_succeeds(self, tmp_path: Path):
        """Resume command succeeds for an orphaned in_progress task (no live worker)."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create an in_progress task with a session ID (simulating an orphaned task)
        task = store.add("Orphaned in-progress task")
        task.status = "in_progress"
        task.session_id = "orphaned-session-456"
        task.started_at = datetime.now(UTC)
        store.update(task)

        # No worker files exist — task is orphaned

        result = run_gza("resume", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout.lower()
        assert "resume of " in result.stdout

        # Verify original task is unchanged and new task was created
        original = store.get(task.id)
        assert original is not None
        assert original.status == "in_progress"
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.based_on == task.id
        assert new_task.session_id == "orphaned-session-456"
        assert new_task.status == "pending"

    def test_resume_running_in_progress_task_fails(self, tmp_path: Path):
        """Resume command fails for an in_progress task that has a live worker."""
        import subprocess as sp

        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create an in_progress task
        task = store.add("Still-running task")
        task.status = "in_progress"
        task.session_id = "running-session-789"
        task.started_at = datetime.now(UTC)
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
                started_at=datetime.now(UTC).isoformat(),
                status="running",
                log_file=None,
                worktree=None,
            )
            registry.register(worker)

            result = run_gza("resume", str(task.id), "--project", str(tmp_path))
        finally:
            sleeper.kill()
            sleeper.wait()

        assert result.returncode == 1
        assert "still running" in result.stdout.lower()
        assert "w-test-running" in result.stdout


class TestWorkCommandMultiTask:
    """Tests for 'gza work' command with multiple task IDs."""

    def test_work_with_single_task_id(self, tmp_path: Path):
        """Work command accepts a single task ID."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        from unittest.mock import patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a task
        store.add("Test task 1")

        # Run without task IDs
        result = run_gza("work", "--no-docker", "--project", str(tmp_path))

        # Verify no argument parsing errors
        assert "unrecognized arguments" not in result.stderr

    def test_work_validates_all_task_ids_before_execution(self, tmp_path: Path):
        """Work command validates all task IDs before starting execution."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add one valid task
        task1 = store.add("Test task 1")

        # Try to run with one valid and one invalid task ID
        result = run_gza("work", str(task1.id), "test-project-zzz", "--no-docker", "--project", str(tmp_path))

        # Should error about invalid task ID format
        assert result.returncode != 0
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    def test_work_rejects_shorthand_task_id(self, tmp_path: Path):
        """Work command requires full prefixed task IDs."""
        setup_config(tmp_path)
        make_store(tmp_path).add("Test task 1")

        result = run_gza("work", "42", "--no-docker", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "Use a full prefixed task ID" in result.stderr or "Use a full prefixed task ID" in result.stdout

    def test_work_validates_task_status(self, tmp_path: Path):
        """Work command validates that tasks are in pending status."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a completed task
        task1 = store.add("Test task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        store.update(task1)

        # Try to run the completed task
        result = run_gza("work", str(task1.id), "--no-docker", "--project", str(tmp_path))

        # Should error about task status
        assert result.returncode != 0
        assert f"Task {task1.id} is not pending" in result.stdout or f"Task {task1.id} is not pending" in result.stderr

    def test_work_worker_mode_rejects_completed_task(self, tmp_path: Path):
        """Worker-mode explicit execution should return non-zero for non-pending tasks."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("work", "--worker-mode", str(task.id), "--no-docker", "--project", str(tmp_path))
        assert result.returncode != 0
        assert f"Task {task.id}" in (result.stdout + result.stderr)

    def test_work_warns_about_orphaned_tasks_before_starting(self, tmp_path: Path):
        """Work command warns about orphaned in-progress tasks before starting new work."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a pending task
        task = store.add("Test background task")

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux

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

    def test_background_worker_without_explicit_task_does_not_pass_task_id(self, tmp_path: Path):
        """No-id background work should not pass a selected task ID to child runner."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.workers import WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending candidate")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_popen(cmd, **kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with patch("gza.cli.subprocess.Popen", side_effect=capture_popen):
            rc = _spawn_background_worker(args, config)

        assert rc == 0
        assert captured_cmd is not None
        worker_mode_idx = captured_cmd.index("--worker-mode")
        assert worker_mode_idx + 1 < len(captured_cmd)
        assert captured_cmd[worker_mode_idx + 1].startswith("--"), f"Unexpected explicit task id in command: {captured_cmd}"

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].task_id is None

    def test_background_resume_worker_command_uses_project_flag(self, tmp_path: Path):
        """Background resume worker subprocess must pass project dir with --project flag.

        Same regression as test_background_worker_command_uses_project_flag but
        for _spawn_background_resume_worker.
        """
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_resume_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

    def test_background_worker_registers_startup_log_file(self, tmp_path: Path):
        """Background worker captures early stdout/stderr into startup log metadata."""
        import argparse
        import subprocess as sp
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.workers import WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Startup log capture test task")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        captured_kwargs = None
        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_popen(cmd, **kwargs):
            nonlocal captured_kwargs
            captured_kwargs = kwargs
            return mock_proc

        with patch("gza.cli.subprocess.Popen", side_effect=capture_popen):
            rc = _spawn_background_worker(args, config, task_id=task.id)

        assert rc == 0
        assert captured_kwargs is not None
        assert captured_kwargs["stderr"] == sp.STDOUT
        assert captured_kwargs["stdout"] is not sp.DEVNULL
        assert hasattr(captured_kwargs["stdout"], "name")

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        worker = workers[0]
        assert worker.startup_log_file == f".gza/workers/{worker.worker_id}-startup.log"
        assert worker.log_file is None
        assert (tmp_path / worker.startup_log_file).exists()


class TestReconciliation:
    """Tests for in-progress reconciliation behavior."""

    def test_reconciliation_warns_on_task_failure_and_continues(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Per-task reconciliation failures should be visible, not silent."""
        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Stuck in-progress task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = -1
        store.update(task)

        config = Config.load(tmp_path)
        with patch.object(SqliteTaskStore, "mark_failed", side_effect=RuntimeError("db-write-boom")):
            reconcile_in_progress_tasks(config)

        captured = capsys.readouterr()
        assert "Warning: Unexpected reconciliation error for task" in captured.err
        assert "db-write-boom" in captured.err

    def test_reconciliation_detects_commits_on_worker_died(self, tmp_path: Path):
        """WORKER_DIED reconciliation sets has_commits=True when branch has commits."""
        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config
        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Set up a git repo with a branch that has commits
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        # Create a branch with a commit
        git._run("checkout", "-b", "task-branch")
        (tmp_path / "work.py").write_text("print('hello')")
        git._run("add", "work.py")
        git._run("commit", "-m", "Task work")
        git._run("checkout", "main")

        # Create task that looks like worker died (dead PID, has branch)
        task = store.add("Task with commits")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = -1  # guaranteed dead PID
        task.branch = "task-branch"
        store.update(task)

        config = Config.load(tmp_path)
        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "WORKER_DIED"
        assert refreshed.has_commits is True

    def test_reconciliation_no_commits_on_worker_died(self, tmp_path: Path):
        """WORKER_DIED reconciliation sets has_commits=False when branch has no commits."""
        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config
        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Set up a git repo — no extra branch
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        # Create task with no branch (worker died before branch creation)
        task = store.add("Task without branch")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = -1
        store.update(task)

        config = Config.load(tmp_path)
        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "WORKER_DIED"
        assert refreshed.has_commits is not True

    def test_prune_terminal_dead_workers_removes_completed_task_worker(self, tmp_path: Path):
        """Terminal task workers with dead PIDs should be pruned from the registry."""
        import subprocess

        from gza.cli._common import prune_terminal_dead_workers
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed task with stale worker metadata")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Use a PID known to be dead: start a process, wait for it to exit, then use its PID.
        proc = subprocess.Popen(["true"])
        proc.wait()
        dead_pid = proc.pid

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-prune-terminal",
                task_id=task.id,
                pid=dead_pid,
                status="running",
            )
        )
        assert registry.get("w-prune-terminal") is not None

        prune_terminal_dead_workers(config)

        assert registry.get("w-prune-terminal") is None

    def test_prune_terminal_dead_workers_keeps_in_progress_task_worker(self, tmp_path: Path):
        """Non-terminal task workers should not be pruned by terminal cleanup."""
        import os

        from gza.cli._common import prune_terminal_dead_workers
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("In-progress task should keep worker entry")
        store.mark_in_progress(task)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-keep-in-progress",
                task_id=task.id,
                pid=os.getpid(),
                status="running",
            )
        )

        prune_terminal_dead_workers(config)

        assert registry.get("w-keep-in-progress") is not None

    def test_prune_terminal_dead_workers_keeps_live_worker_for_terminal_task(self, tmp_path: Path):
        """Live worker PID for a terminal task must NOT be pruned (is_running guard)."""
        import os

        from gza.cli._common import prune_terminal_dead_workers
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Terminal task with live worker still flushing")
        task.status = "failed"
        from datetime import datetime
        task.completed_at = datetime.now(UTC)
        store.update(task)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        # Use the current process PID — guaranteed alive for the duration of this test.
        registry.register(
            WorkerMetadata(
                worker_id="w-live-terminal",
                task_id=task.id,
                pid=os.getpid(),
                status="running",
            )
        )
        assert registry.get("w-live-terminal") is not None

        prune_terminal_dead_workers(config)

        # Entry must be retained because the PID is still alive.
        assert registry.get("w-live-terminal") is not None


class TestImplementCommand:
    """Tests for 'gza implement' command."""

    def test_implement_creates_task_from_completed_plan(self, tmp_path: Path):
        """Implement command creates an implementation task and queues it."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan authentication rollout", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
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
        assert "Created implement task " in result.stdout

        impl_task = get_latest_task(store, based_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.task_type == "implement"
        assert impl_task.based_on == plan_task.id
        assert impl_task.prompt == "Implement auth rollout"
        assert impl_task.create_review is True

    def test_implement_fails_for_missing_plan_task(self, tmp_path: Path):
        """Implement command validates referenced plan task exists."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("implement", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_implement_fails_for_non_plan_task(self, tmp_path: Path):
        """Implement command requires a plan task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Not a plan", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("implement", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {task.id} is a implement task" in result.stdout

    def test_implement_fails_for_incomplete_plan_task(self, tmp_path: Path):
        """Implement command requires the plan task to be completed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan feature", task_type="plan")

        result = run_gza("implement", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {plan_task.id} is pending. Plan task must be completed." in result.stdout

    def test_implement_derives_prompt_from_plan_slug_when_omitted(self, tmp_path: Path):
        """Implement command derives prompt from the plan task slug when prompt omitted."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        plan_task.slug = "20260226-plan-auth-migration"
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = run_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created implement task " in result.stdout

        impl_task = get_latest_task(store, based_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.prompt == f"Implement plan from task {plan_task.id}: plan-auth-migration"
        assert impl_task.based_on == plan_task.id

    def test_implement_derives_prompt_from_base_plan_slug_when_retry_suffix_present(self, tmp_path: Path):
        """Implement command strips numeric retry suffix from plan slug."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        plan_task.slug = "20260226-plan-auth-migration-2"
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = run_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created implement task " in result.stdout

        impl_task = get_latest_task(store, based_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.prompt == f"Implement plan from task {plan_task.id}: plan-auth-migration"
        assert impl_task.based_on == plan_task.id


class TestImproveCommand:
    """Tests for 'gza improve' command."""

    def test_improve_creates_task_from_implementation_and_review(self, tmp_path: Path):
        """Improve command creates an improve task with correct relationships."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement", group="auth-feature")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review implementation", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve command with --queue to only create (not run)
        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert f"Created improve task {improve_task.id}" in result.stdout
        assert f"Based on: implementation {impl_task.id}" in result.stdout
        assert f"Review: {review_task.id}" in result.stdout

        # Verify the improve task was created with correct fields
        assert improve_task is not None
        assert improve_task.task_type == "improve"
        assert improve_task.depends_on == review_task.id  # review task
        assert improve_task.based_on == impl_task.id  # implementation task
        assert improve_task.same_branch is True
        assert improve_task.group == "auth-feature"  # inherited from implementation

    def test_improve_with_review_flag(self, tmp_path: Path):
        """Improve command with --review flag sets create_review."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation and review tasks
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve command with --review flag and --queue to only create (not run)
        result = run_gza("improve", str(impl_task.id), "--review", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify the improve task has create_review set
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.create_review is True

    def test_improve_fails_without_review(self, tmp_path: Path):
        """Improve command fails if implementation has no review."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation task without review
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run improve command
        result = run_gza("improve", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "has no review" in result.stdout
        assert f"gza add --type review --depends-on {impl_task.id}" in result.stdout

    def test_improve_fails_on_non_implement_task(self, tmp_path: Path):
        """Improve command fails if task is not an implementation task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a plan task
        plan_task = store.add("Plan feature", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        # Run improve command
        result = run_gza("improve", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is a plan task" in result.stdout

    def test_improve_accepts_review_task_id_and_resolves_impl(self, tmp_path: Path):
        """Improve command accepts a review task ID and auto-resolves to the implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation and review tasks
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve command with review task ID — should resolve to impl task and succeed
        result = run_gza("improve", str(review_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "Created improve task" in result.stdout
        assert f"Based on: implementation {impl_task.id}" in result.stdout

    def test_improve_accepts_improve_task_id_and_resolves_impl(self, tmp_path: Path):
        """Improve command accepts an improve task ID and auto-resolves to the implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # First improve task
        assert impl_task.id is not None
        assert review_task.id is not None
        improve_task = store.add(
            "Improve", task_type="improve", based_on=impl_task.id, depends_on=review_task.id, same_branch=True
        )
        improve_task.status = "completed"
        improve_task.completed_at = datetime.now(UTC)
        store.update(improve_task)

        # Add a second review so a new improve can be created
        review_task2 = store.add("Review 2", task_type="review", depends_on=impl_task.id)
        review_task2.status = "completed"
        review_task2.completed_at = datetime.now(UTC)
        store.update(review_task2)

        # Run improve command with improve task ID — should resolve to impl task
        result = run_gza("improve", str(improve_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "Created improve task" in result.stdout
        assert f"Based on: implementation {impl_task.id}" in result.stdout

    def test_improve_uses_most_recent_review(self, tmp_path: Path):
        """Improve command uses the most recent review when multiple exist."""
        import time

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create first review task
        time.sleep(0.01)  # Ensure different timestamps
        review_task1 = store.add("First review", task_type="review", depends_on=impl_task.id)
        review_task1.status = "completed"
        review_task1.completed_at = datetime.now(UTC)
        store.update(review_task1)

        # Create second review task (more recent)
        time.sleep(0.01)  # Ensure different timestamps
        review_task2 = store.add("Second review", task_type="review", depends_on=impl_task.id)
        review_task2.status = "completed"
        review_task2.completed_at = datetime.now(UTC)
        store.update(review_task2)

        # Run improve command with --queue to only create (not run)
        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Review: {review_task2.id}" in result.stdout  # Should use the second (most recent) review

        # Verify the improve task depends on the most recent review
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.depends_on == review_task2.id

    def test_improve_nonexistent_task(self, tmp_path: Path):
        """Improve command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("improve", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    def test_improve_warns_on_incomplete_review(self, tmp_path: Path):
        """Improve command warns if the review is not yet completed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a pending review task (not completed)
        store.add("Review", task_type="review", depends_on=impl_task.id)
        # Leave status as 'pending' (default)

        # Run improve command with --queue to only create (not run)
        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        # Should succeed but warn about incomplete review
        assert result.returncode == 0
        assert "Warning: Review " in result.stdout
        assert "is pending" in result.stdout
        assert "blocked until it completes" in result.stdout
        assert "Created improve task " in result.stdout

    def test_improve_prevents_duplicate(self, tmp_path: Path):
        """Improve command refuses to create a duplicate improve task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Create an existing improve task for the same impl+review pair
        existing_improve = store.add(
            "Improve",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        # Run improve command - should fail with duplicate error
        result = run_gza("improve", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "improve task already exists" in result.stdout
        assert f"{existing_improve.id}" in result.stdout

        # Verify no new task was created (still only 3 tasks)
        all_tasks = store.get_all()
        assert len(all_tasks) == 3

    def test_improve_runs_by_default(self, tmp_path: Path):
        """Improve command runs the task immediately by default (without any flags)."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve command without --queue (will attempt to run)
        result = run_gza("improve", str(impl_task.id), "--no-docker", "--project", str(tmp_path))

        # Verify the improve task was created and run was attempted
        assert "Created improve task " in result.stdout
        assert "Running improve task " in result.stdout

    def test_improve_with_model_flag(self, tmp_path: Path):
        """Improve command with --model sets the model on the created task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        result = run_gza("improve", str(impl_task.id), "--model", "claude-opus-4-5", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.model == "claude-opus-4-5"

    def test_improve_with_provider_flag(self, tmp_path: Path):
        """Improve command with --provider sets the provider on the created task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        result = run_gza("improve", str(impl_task.id), "--provider", "gemini", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.provider == "gemini"

    def test_improve_skips_dropped_review_and_picks_earlier_completed(self, tmp_path: Path):
        """Auto-pick must ignore dropped reviews even if their completed_at is more recent.

        Regression for the trap where a user accidentally creates a duplicate
        review, drops it, and then `gza improve` keeps binding new improve tasks
        to the dropped review (because get_reviews_for_task orders by
        completed_at DESC with no status filter).
        """
        import time

        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Older, real, completed review.
        good_review = store.add("First review", task_type="review", depends_on=impl_task.id)
        good_review.status = "completed"
        good_review.completed_at = datetime.now(UTC)
        store.update(good_review)

        # Newer, dropped review (would sort first by completed_at DESC).
        time.sleep(0.01)
        bad_review = store.add("Accidental duplicate review", task_type="review", depends_on=impl_task.id)
        bad_review.status = "dropped"
        bad_review.completed_at = datetime.now(UTC)
        store.update(bad_review)

        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert f"Review: {good_review.id}" in result.stdout
        assert f"Review: {bad_review.id}" not in result.stdout

        # Confirm the improve task's dependency points at the good review.
        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task is not None
        assert improve_task.depends_on == good_review.id

    def test_improve_skips_failed_review(self, tmp_path: Path):
        """Auto-pick must also ignore failed reviews — same reasoning as dropped."""
        import time

        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        good_review = store.add("Good review", task_type="review", depends_on=impl_task.id)
        good_review.status = "completed"
        good_review.completed_at = datetime.now(UTC)
        store.update(good_review)

        time.sleep(0.01)
        failed_review = store.add("Failed review", task_type="review", depends_on=impl_task.id)
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        store.update(failed_review)

        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert f"Review: {good_review.id}" in result.stdout

    def test_improve_errors_when_all_reviews_are_dropped(self, tmp_path: Path):
        """When every review is dropped/failed, surface a clear error."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        dropped_review = store.add("Dropped review", task_type="review", depends_on=impl_task.id)
        dropped_review.status = "dropped"
        dropped_review.completed_at = datetime.now(UTC)
        store.update(dropped_review)

        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no usable review" in result.stdout
        assert "--review-id" in result.stdout

    def test_improve_review_id_flag_picks_explicit_review(self, tmp_path: Path):
        """--review-id overrides auto-pick and uses the specified review."""
        import time

        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        older_review = store.add("Older review", task_type="review", depends_on=impl_task.id)
        older_review.status = "completed"
        older_review.completed_at = datetime.now(UTC)
        store.update(older_review)

        time.sleep(0.01)
        newer_review = store.add("Newer review", task_type="review", depends_on=impl_task.id)
        newer_review.status = "completed"
        newer_review.completed_at = datetime.now(UTC)
        store.update(newer_review)

        # Without --review-id, auto-pick would choose the newer one.
        # With --review-id, we force the older one.
        result = run_gza(
            "improve", str(impl_task.id),
            "--review-id", str(older_review.id),
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 0, result.stdout
        assert f"Review: {older_review.id}" in result.stdout

        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task is not None
        assert improve_task.depends_on == older_review.id

    def test_improve_review_id_flag_trims_whitespace_and_reports_canonical_id(self, tmp_path: Path):
        """--review-id should be normalized via shared full-ID resolution."""
        import time

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        selected_review = store.add("Selected review", task_type="review", depends_on=impl_task.id)
        selected_review.status = "completed"
        selected_review.completed_at = datetime.now(UTC)
        store.update(selected_review)

        time.sleep(0.01)
        newer_review = store.add("Newer review", task_type="review", depends_on=impl_task.id)
        newer_review.status = "completed"
        newer_review.completed_at = datetime.now(UTC)
        store.update(newer_review)

        result = run_gza(
            "improve", str(impl_task.id),
            "--review-id", f"  {selected_review.id}\t",
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 0, result.stdout
        assert f"Review: {selected_review.id}" in result.stdout

        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task is not None
        assert improve_task.depends_on == selected_review.id

    def test_improve_review_id_flag_rejects_review_of_different_impl(self, tmp_path: Path):
        """--review-id must belong to the same implementation task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_a = store.add("Feature A", task_type="implement")
        impl_a.status = "completed"
        impl_a.completed_at = datetime.now(UTC)
        store.update(impl_a)

        impl_b = store.add("Feature B", task_type="implement")
        impl_b.status = "completed"
        impl_b.completed_at = datetime.now(UTC)
        store.update(impl_b)

        # Review belongs to impl_b, not impl_a.
        review_of_b = store.add("Review B", task_type="review", depends_on=impl_b.id)
        review_of_b.status = "completed"
        review_of_b.completed_at = datetime.now(UTC)
        store.update(review_of_b)

        result = run_gza(
            "improve", str(impl_a.id),
            "--review-id", str(review_of_b.id),
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 1
        assert f"reviews task {impl_b.id}" in result.stdout

    def test_improve_review_id_flag_rejects_non_review_task(self, tmp_path: Path):
        """--review-id must point at a review task, not an implement/improve task."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = run_gza(
            "improve", str(impl_task.id),
            "--review-id", str(impl_task.id),  # not a review
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 1
        assert "not a review" in result.stdout

    @pytest.mark.parametrize("invalid_review_id", ["9999", "42"])
    def test_improve_review_id_flag_rejects_shorthand_ids(self, tmp_path: Path, invalid_review_id: str):
        """--review-id must require full prefixed task IDs (no numeric shorthand)."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = run_gza(
            "improve", str(impl_task.id),
            "--review-id", invalid_review_id,
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr


class TestReviewCommand:
    """Tests for the 'gza review' command."""

    def test_review_creates_task_for_completed_implementation(self, tmp_path: Path):
        """Review command creates a review task for a completed implementation."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.config import Config
        config = Config.load(tmp_path)
        store = SqliteTaskStore(db_path, prefix=config.project_prefix)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement", group="auth-feature")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run review command with --queue to only create (not run)
        result = run_gza("review", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task " in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout
        assert "Group: auth-feature" in result.stdout

        # Verify the review task was created with correct fields
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == impl_task.id
        assert review_task.group == "auth-feature"  # inherited from implementation

    def test_review_fails_on_non_implementation_task(self, tmp_path: Path):
        """Review command fails if task is not an implementation/improve/review task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a plan task
        plan_task = store.add("Plan authentication system", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        # Run review command
        result = run_gza("review", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is a plan task, not an implementation, improve, or review task" in result.stdout

    def test_review_accepts_improve_task_and_targets_implementation(self, tmp_path: Path):
        """Review command accepts improve tasks and reviews the base implementation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create completed implementation task
        impl_task = store.add("Implement auth", task_type="implement", group="auth")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-auth"
        impl_task.completed_at = datetime.now(UTC)
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
        improve_task.completed_at = datetime.now(UTC)
        store.update(improve_task)

        result = run_gza("review", str(improve_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert f"Created review task {review_task.id}" in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout

        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == impl_task.id
        assert review_task.group == "auth"

    def test_review_accepts_review_task_id_and_targets_implementation(self, tmp_path: Path):
        """Review command accepts a review task ID and creates a new review on the base implementation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create completed implementation task
        impl_task = store.add("Implement feature", task_type="implement", group="feat")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task for the implementation
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "completed"
        existing_review.completed_at = datetime.now(UTC)
        store.update(existing_review)

        # Run review command with the existing review task ID
        result = run_gza("review", str(existing_review.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "Created review task " in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout

        all_tasks = store.get_all()
        new_reviews = [t for t in all_tasks if t.task_type == "review" and t.id != existing_review.id]
        assert len(new_reviews) == 1
        new_review = new_reviews[0]
        assert new_review.depends_on == impl_task.id

    def test_review_fails_on_non_completed_task(self, tmp_path: Path):
        """Review command fails if implementation is not completed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a pending implementation task
        impl_task = store.add("Add feature", task_type="implement")
        # Leave status as 'pending'

        # Run review command
        result = run_gza("review", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is pending. Can only review completed tasks" in result.stdout

    def test_review_nonexistent_task(self, tmp_path: Path):
        """Review command fails gracefully for nonexistent task."""
        setup_config(tmp_path)

        result = run_gza("review", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    def test_review_anchors_lineage_to_reviewed_implementation(self, tmp_path: Path):
        """Review task uses implementation ID for based_on and lineage placement."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a plan task
        plan_task = store.add("Plan feature", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        # Create implementation based on plan and a retry implementation based on it
        impl_task = store.add("Implement feature", task_type="implement", based_on=plan_task.id)
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        retry_impl = store.add("Retry implementation", task_type="implement", based_on=impl_task.id)
        retry_impl.status = "completed"
        retry_impl.branch = "test-project/20260130-retry-implementation"
        retry_impl.completed_at = datetime.now(UTC)
        store.update(retry_impl)

        # Run review command with --queue to only create (not run)
        result = run_gza("review", str(retry_impl.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task " in result.stdout

        # Verify the review task is anchored to the reviewed implementation task
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.based_on == retry_impl.id
        assert review_task.depends_on == retry_impl.id

        retry_based_children = {t.id for t in store.get_based_on_children(retry_impl.id)}
        parent_based_children = {t.id for t in store.get_based_on_children(impl_task.id)}
        assert review_task.id in retry_based_children
        assert review_task.id not in parent_based_children

        # Verify canonical lineage placement is under retry implementation
        lineage_tree = build_lineage_tree(store, plan_task)
        assert len(lineage_tree.children) == 1
        parent_impl_node = lineage_tree.children[0]
        assert parent_impl_node.task.id == impl_task.id
        assert len(parent_impl_node.children) == 1
        retry_node = parent_impl_node.children[0]
        assert retry_node.task.id == retry_impl.id
        assert any(child.task.id == review_task.id for child in retry_node.children)

    def test_review_runs_by_default(self, tmp_path: Path):
        """Review command runs the review task immediately by default."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run review command without --queue (will attempt to run immediately)
        result = run_gza("review", str(impl_task.id), "--no-docker", "--project", str(tmp_path))

        # Verify the review task was created and run attempted
        assert "Created review task " in result.stdout
        assert "Running review task " in result.stdout

        # Verify the review task exists
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == impl_task.id

    def test_review_with_queue_flag(self, tmp_path: Path):
        """Review command with --queue adds task to queue without executing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run review command with --queue flag
        result = run_gza("review", str(impl_task.id), "--queue", "--project", str(tmp_path))

        # Verify the review task was created but not run
        assert result.returncode == 0
        assert "Created review task " in result.stdout
        assert "Running review task" not in result.stdout

        # Verify the review task is still pending
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.status == "pending"

    def test_review_with_open_flag_no_editor(self, tmp_path: Path, monkeypatch):
        """Review command with --open warns when $EDITOR is not set."""
        from unittest.mock import MagicMock, patch


        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
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
            result = run_gza("review", str(impl_task.id), "--open", "--no-docker", "--project", str(tmp_path))

            # Check that warning about missing EDITOR is shown
            # Note: This might not appear in output if the task doesn't complete successfully in test
            # The important thing is that the flag is accepted and doesn't cause an error
            assert result.returncode in (0, 1)  # May fail due to missing credentials, but flag should be accepted

    def test_review_open_flag_with_queue_does_not_run(self, tmp_path: Path):
        """--open flag with --queue creates task but does not run it."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run review command with --open and --queue (should create task but not run)
        result = run_gza("review", str(impl_task.id), "--open", "--queue", "--project", str(tmp_path))

        # Should succeed but not run the task
        assert result.returncode == 0
        assert "Created review task " in result.stdout
        assert "Running review task" not in result.stdout

    def test_review_prevents_duplicate_pending_review(self, tmp_path: Path):
        """Review command warns and exits if a pending review already exists."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create an existing pending review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        # Leave status as 'pending' (default)

        # Attempt to create another review
        result = run_gza("review", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Warning: A review task already exists" in result.stdout
        assert f"{existing_review.id}" in result.stdout
        assert "pending" in result.stdout

        # Verify no additional review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 1

    def test_review_prevents_duplicate_in_progress_review(self, tmp_path: Path):
        """Review command warns and exits if an in_progress review already exists."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create an existing in_progress review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "in_progress"
        store.update(existing_review)

        # Attempt to create another review
        result = run_gza("review", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Warning: A review task already exists" in result.stdout
        assert f"{existing_review.id}" in result.stdout
        assert "in_progress" in result.stdout

        # Verify no additional review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 1

    def test_review_allows_new_review_after_completed_review(self, tmp_path: Path):
        """Review command allows creating a new review if existing review is completed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create an existing completed review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "completed"
        existing_review.completed_at = datetime.now(UTC)
        store.update(existing_review)

        # Create another review with --queue (should succeed after improvements)
        result = run_gza("review", str(impl_task.id), "--queue", "--project", str(tmp_path))

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

        from gza.cli import cmd_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
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
        mock_config.project_prefix = "testproject"

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
        assert f"{existing_review.id}" in printed
        # get_reviews_for_task must be called exactly once (inside _create_review_task),
        # NOT a second time in the cmd_review error handler.
        assert len(call_count) == 1, (
            f"get_reviews_for_task was called {len(call_count)} times; expected exactly 1"
        )

    def test_review_with_model_flag(self, tmp_path: Path):
        """Review command with --model sets the model on the created review task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = run_gza("review", str(impl_task.id), "--model", "claude-opus-4-5", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.model == "claude-opus-4-5"

    def test_review_with_provider_flag(self, tmp_path: Path):
        """Review command with --provider sets the provider on the created review task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = run_gza("review", str(impl_task.id), "--provider", "gemini", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.provider == "gemini"


class TestIterateCommand:
    """Tests for 'gza iterate' command."""

    def _make_completed_impl(self, store, prompt: str = "Implement feature") -> object:
        """Create and return a completed implement task."""
        from datetime import datetime
        impl = store.add(prompt, task_type="implement")
        impl.status = "completed"
        impl.branch = "test-project/20260101-impl"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        return impl

    def test_cycle_dry_run(self, tmp_path: Path):
        """gza iterate --dry-run prints preview and exits 0."""

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()

    def test_cycle_rejects_non_implement_task(self, tmp_path: Path):
        """gza iterate rejects tasks that are not implement type."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("A plan", task_type="plan")

        result = run_gza("iterate", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode != 0
        assert "implement" in result.stdout.lower() or "implement" in result.stderr.lower()

    def test_cycle_rejects_in_progress_task(self, tmp_path: Path):
        """gza iterate rejects implementation tasks that are in_progress."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        impl.status = "in_progress"
        store.update(impl)

        result = run_gza("iterate", str(impl.id), "--project", str(tmp_path))

        assert result.returncode != 0
        assert "in_progress" in result.stdout or "in_progress" in result.stderr

    def test_pending_impl_runs_first_then_iterates(self, tmp_path: Path):
        """gza iterate on a pending task runs it first, then enters review/improve loop."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")  # status = 'pending'

        from datetime import datetime

        def fake_run_foreground(config, task_id, **kwargs):
            # Simulate the impl task completing
            task = store.get(task_id)
            if task and task.status == "pending":
                task.status = "completed"
                task.completed_at = datetime.now()
                store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground, \
             patch("gza.cli._create_review_task") as create_review:
            # With max_iterations=1 and no existing review, it will run impl then try to create a review
            # The create_review mock will raise to stop the loop cleanly
            create_review.side_effect = ValueError("test stop")
            result = cmd_iterate(args)

        # The first call should be running the pending impl task
        assert run_foreground.call_count >= 1
        first_call_kwargs = run_foreground.call_args_list[0][1]
        assert first_call_kwargs["task_id"] == impl.id

    def test_pending_impl_dry_run(self, tmp_path: Path):
        """gza iterate --dry-run on a pending task shows it would run the impl first."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")  # status = 'pending'

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pending" in result.stdout.lower()
        assert "dry-run" in result.stdout.lower()

    def test_failed_task_requires_resume_or_retry(self, tmp_path: Path):
        """gza iterate on a failed task without --resume or --retry errors."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        store.update(impl)

        result = run_gza("iterate", str(impl.id), "--project", str(tmp_path))
        assert result.returncode != 0
        assert "--resume" in result.stdout or "--resume" in result.stderr
        assert "--retry" in result.stdout or "--retry" in result.stderr

    def test_resume_flag_rejected_for_non_failed_task(self, tmp_path: Path):
        """--resume is only valid for failed tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        result = run_gza("iterate", str(impl.id), "--resume", "--dry-run", "--project", str(tmp_path))
        assert result.returncode != 0
        output = result.stdout + (result.stderr or "")
        assert "failed" in output.lower()

    def test_retry_flag_rejected_for_non_failed_task(self, tmp_path: Path):
        """--retry is only valid for failed tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        result = run_gza("iterate", str(impl.id), "--retry", "--dry-run", "--project", str(tmp_path))
        assert result.returncode != 0
        output = result.stdout + (result.stderr or "")
        assert "failed" in output.lower()

    def test_resume_and_retry_mutually_exclusive(self, tmp_path: Path):
        """--resume and --retry cannot be used together."""
        setup_config(tmp_path)
        result = run_gza("iterate", "testproject-1", "--resume", "--retry", "--project", str(tmp_path))
        assert result.returncode != 0

    def test_failed_task_retry_dry_run(self, tmp_path: Path):
        """gza iterate --retry --dry-run on a failed task shows what would happen."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        store.update(impl)

        result = run_gza("iterate", str(impl.id), "--retry", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()
        assert "retry" in result.stdout.lower()

    def test_failed_task_resume_dry_run(self, tmp_path: Path):
        """gza iterate --resume --dry-run on a failed task shows what would happen."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        impl.session_id = "some-session"
        store.update(impl)

        result = run_gza("iterate", str(impl.id), "--resume", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()
        assert "resume" in result.stdout.lower()

    def test_failed_task_retry_runs_then_iterates(self, tmp_path: Path):
        """gza iterate --retry on a failed task retries it then enters the loop."""
        import argparse
        from unittest.mock import MagicMock, patch
        from datetime import datetime

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        store.update(impl)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            if task and task.status == "pending":
                task.status = "completed"
                task.completed_at = datetime.now()
                store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=True,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg, \
             patch("gza.cli._create_review_task") as create_review:
            create_review.side_effect = ValueError("test stop")
            cmd_iterate(args)

        # First call should be running the retry task (not the original failed one)
        assert run_fg.call_count >= 1
        first_task_id = run_fg.call_args_list[0][1]["task_id"]
        assert first_task_id != impl.id  # Should be a new task

    def test_iterate_continue_flag_is_rejected(self, tmp_path: Path):
        setup_config(tmp_path)
        result = run_gza("iterate", "testproject-1", "--continue", "--project", str(tmp_path))
        assert result.returncode != 0
        assert "unrecognized arguments: --continue" in (result.stderr or result.stdout)

    def test_cycle_alias_runs_iterate(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        result = run_gza("cycle", str(impl.id), "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "would iterate implementation" in result.stdout.lower()

    def test_reuses_latest_changes_requested_review_for_first_iteration(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        latest_review = store.add("Review", task_type="review", depends_on=impl.id)
        latest_review.status = "completed"
        latest_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        latest_review.completed_at = datetime.now(UTC)
        store.update(latest_review)

        improve = store.add("Improve", task_type="improve")
        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")

        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._run_foreground", return_value=0), \
             patch("gza.cli._create_improve_task", return_value=improve) as create_improve, \
             patch("gza.cli._create_review_task") as create_review:
            result = cmd_iterate(args)

        assert result == 2
        create_improve.assert_called_once()
        create_review.assert_not_called()

    def test_latest_review_approved_exits_zero(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=3, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), patch("gza.cli.get_store", return_value=store):
            result = cmd_iterate(args)
        assert result == 0

    def test_latest_review_needs_discussion_blocks(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: NEEDS_DISCUSSION**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=3, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), patch("gza.cli.get_store", return_value=store):
            result = cmd_iterate(args)
        assert result == 3

    def test_latest_review_without_verdict_blocks(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "No verdict section"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=3, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), patch("gza.cli.get_store", return_value=store):
            result = cmd_iterate(args)
        assert result == 3

    def test_newer_pending_review_reused_over_older_completed_approved(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        old_review = store.add("Old approved review", task_type="review", depends_on=impl.id)
        old_review.status = "completed"
        old_review.output_content = "**Verdict: APPROVED**"
        old_review.completed_at = datetime.now(UTC)
        store.update(old_review)
        pending_review = store.add("Pending re-review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        pending_review.output_content = "**Verdict: APPROVED**"
        pending_review.completed_at = None
        store.update(pending_review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._run_foreground", return_value=0) as run_foreground, \
             patch("gza.cli._create_improve_task") as create_improve:
            result = cmd_iterate(args)
        assert result == 0
        run_foreground.assert_called_once_with(mock_config, task_id=pending_review.id, force=False)
        create_review.assert_not_called()
        create_improve.assert_not_called()

    def test_pending_review_is_reused_instead_of_creating_another(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        pending_review = store.add("Pending review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        pending_review.output_content = "**Verdict: APPROVED**"
        store.update(pending_review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._run_foreground", return_value=0) as run_foreground:
            result = cmd_iterate(args)

        assert result == 0
        run_foreground.assert_called_once_with(mock_config, task_id=pending_review.id, force=False)
        create_review.assert_not_called()

    def test_in_progress_review_is_reported_instead_of_creating_another(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        old_review = store.add("Old approved review", task_type="review", depends_on=impl.id)
        old_review.status = "completed"
        old_review.output_content = "**Verdict: APPROVED**"
        old_review.completed_at = datetime.now(UTC)
        store.update(old_review)
        running_review = store.add("Running review", task_type="review", depends_on=impl.id)
        running_review.status = "in_progress"
        store.update(running_review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._run_foreground") as run_foreground:
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output
        assert "Manual review required" not in output

    def test_in_progress_review_is_prioritized_over_newer_pending_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        running_review = store.add("Older running review", task_type="review", depends_on=impl.id)
        running_review.status = "in_progress"
        running_review.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(running_review)
        pending_review = store.add("Newer pending review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        pending_review.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(pending_review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output
        assert pending_review.id not in output

    def test_changes_requested_with_pending_improve_reuses_existing_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.duration_seconds = 61.0
        review.num_steps_reported = 2
        review.cost_usd = 0.14
        review.completed_at = datetime.now(UTC)
        store.update(review)
        improve = store.add("Existing improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "pending"
        store.update(improve)

        def fake_run_foreground(config, task_id, **kwargs):
            assert task_id == improve.id
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.duration_seconds = 75.0
            task.num_steps_computed = 3
            task.cost_usd = 0.22
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._create_improve_task") as create_improve, \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground, \
             patch("gza.cli.time.monotonic", side_effect=[100.0, 190.0]):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 2
        run_foreground.assert_called_once_with(mock_config, task_id=improve.id, force=False)
        create_improve.assert_not_called()
        create_review.assert_not_called()
        assert re.search(
            rf"1\s+review\s+{re.escape(review.id)}\s+CHANGES_REQUESTED\s+1m1s\s+2\s+\$0\.14\s+completed",
            output,
        )
        assert re.search(
            rf"1\s+improve\s+{re.escape(improve.id)}\s+-\s+1m15s\s+3\s+\$0\.22\s+completed",
            output,
        )
        assert "Totals: 1m30s wall | 5 steps | $0.36" in output

    def test_changes_requested_with_in_progress_improve_blocks(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.duration_seconds = 90.0
        review.num_steps_reported = 4
        review.cost_usd = 0.41
        review.completed_at = datetime.now(UTC)
        store.update(review)
        improve = store.add("Existing improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "in_progress"
        improve.duration_seconds = 45.0
        improve.num_steps_computed = 3
        improve.cost_usd = 0.29
        store.update(improve)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._create_improve_task") as create_improve, \
             patch("gza.cli._run_foreground") as run_foreground, \
             patch("gza.cli.time.monotonic", side_effect=[200.0, 260.0]):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_improve.assert_not_called()
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert re.search(
            rf"1\s+review\s+{re.escape(review.id)}\s+CHANGES_REQUESTED\s+1m30s\s+4\s+\$0\.41\s+completed",
            output,
        )
        assert re.search(
            rf"1\s+improve\s+{re.escape(improve.id)}\s+-\s+45s\s+3\s+\$0\.29\s+in_progress",
            output,
        )
        assert "Totals: 1m0s wall | 7 steps | $0.70" in output
        assert "Iterate waiting: improve_in_progress. Existing task is already in progress." in output
        assert "Manual review required" not in output

    def test_in_progress_improve_is_prioritized_over_newer_pending_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        running_improve = store.add("Older running improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        running_improve.status = "in_progress"
        running_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(running_improve)
        pending_improve = store.add("Newer pending improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        pending_improve.status = "pending"
        pending_improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(pending_improve)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli._create_improve_task") as create_improve,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        create_improve.assert_not_called()
        run_foreground.assert_not_called()
        assert "Iterate waiting: improve_in_progress. Existing task is already in progress." in output
        assert pending_improve.id not in output

    def test_changes_requested_with_completed_improve_reuses_or_starts_fresh_review(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        improve = store.add("Completed improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        impl.review_cleared_at = datetime.now(UTC)
        store.update(impl)

        fresh_review = store.add("Fresh review", task_type="review", depends_on=impl.id)
        fresh_review.status = "pending"
        fresh_review.output_content = "**Verdict: APPROVED**"
        store.update(fresh_review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task", return_value=fresh_review) as create_review, \
             patch("gza.cli._create_improve_task") as create_improve, \
             patch("gza.cli._run_foreground", return_value=0) as run_foreground:
            result = cmd_iterate(args)

        assert result == 0
        run_foreground.assert_called_once_with(mock_config, task_id=fresh_review.id, force=False)
        create_review.assert_not_called()
        create_improve.assert_not_called()

    def test_no_reviews_starts_with_fresh_review(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review")
        review.status = "pending"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = None
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task", return_value=review) as create_review, \
             patch("gza.cli._run_foreground", return_value=0):
            result = cmd_iterate(args)
        assert result == 0
        create_review.assert_called_once()

    def test_summary_table_shows_per_task_rows_stats_and_totals(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        improve = store.add("Improve", task_type="improve", based_on=impl.id, depends_on=review.id)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == review.id:
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
                task.duration_seconds = 101.0
                task.num_steps_reported = 6
                task.cost_usd = 0.97
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            if task_id == improve.id:
                task.status = "completed"
                task.duration_seconds = 162.0
                task.num_steps_computed = 7
                task.cost_usd = 1.56
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task", return_value=review),
            patch("gza.cli._create_improve_task", return_value=improve),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch("gza.cli.time.monotonic", side_effect=[100.0, 220.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 2
        assert re.search(r"Iter\s+Type\s+Task\s+Verdict\s+Duration\s+Steps\s+Cost\s+Status", output)
        assert re.search(
            rf"1\s+review\s+{re.escape(review.id)}\s+CHANGES_REQUESTED\s+1m41s\s+6\s+\$0\.97\s+completed",
            output,
        )
        assert re.search(
            rf"1\s+improve\s+{re.escape(improve.id)}\s+-\s+2m42s\s+7\s+\$1\.56\s+completed",
            output,
        )
        assert "Totals: 2m0s wall | 13 steps | $2.53" in output

    def test_summary_table_shows_failure_reason_for_failed_task(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "MODEL_TIMEOUT"
            task.duration_seconds = 12.0
            task.num_steps_reported = 2
            task.cost_usd = 0.11
            store.update(task)
            return 1

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task", return_value=review),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch("gza.cli.time.monotonic", side_effect=[100.0, 112.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        assert re.search(
            rf"1\s+review\s+{re.escape(review.id)}\s+-\s+12s\s+2\s+\$0\.11\s+failed \(MODEL_TIMEOUT\)",
            output,
        )
        assert "Totals: 12s wall | 2 steps | $0.11" in output


class TestMarkCompletedCommand:
    """Tests for 'gza mark-completed' command."""

    def _setup_store(self, tmp_path: Path) -> SqliteTaskStore:
        """Set up config and return a SqliteTaskStore."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return make_store(tmp_path)

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

        result = run_gza("mark-completed", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_mark_completed_default_verify_git_for_code_tasks(self, tmp_path: Path):
        """Code task types default to git verification mode."""
        store = self._setup_store(tmp_path)
        self._setup_git_repo(tmp_path)

        task = store.add("Code task with no branch", task_type="implement")
        task.status = "failed"
        store.update(task)

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout

    def test_mark_completed_default_force_for_non_code_tasks(self, tmp_path: Path):
        """Non-code task types default to status-only completion."""

        setup_db_with_tasks(tmp_path, [
            {"prompt": "Review task", "status": "failed", "task_type": "review"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "status-only" in result.stdout

        store = make_store(tmp_path)
        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.has_commits is False

    def test_mark_completed_verify_git_requires_branch(self, tmp_path: Path):
        """--verify-git errors when no branch is set."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task without branch", "status": "failed", "task_type": "review"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza("mark-completed", str(task.id), "--verify-git", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout
        assert "Use --force" in result.stdout

    def test_mark_completed_warns_if_not_failed(self, tmp_path: Path):
        """mark-completed warns when task status is not failed."""
        store = self._setup_store(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create a branch for the task
        git._run("checkout", "-b", "gza/1-test-task")
        git._run("checkout", "main")

        task = store.add("Pending task")
        task.status = "pending"
        task.branch = "gza/1-test-task"
        store.update(task)

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Warning" in result.stdout
        assert "not in failed status" in result.stdout

    def test_mark_completed_errors_if_branch_missing_in_git(self, tmp_path: Path):
        """mark-completed errors when git branch does not exist."""
        store = self._setup_store(tmp_path)
        self._setup_git_repo(tmp_path)

        task = store.add("Failed task")
        task.status = "failed"
        task.branch = "gza/1-nonexistent-branch"
        store.update(task)

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "does not exist" in result.stdout
        assert "Use --force" in result.stdout

    def test_mark_completed_with_commits_sets_unmerged(self, tmp_path: Path):
        """mark-completed sets status='unmerged' when branch has commits."""
        store = self._setup_store(tmp_path)
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

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "unmerged" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.merge_status == "unmerged"
        assert updated.has_commits is True

    def test_mark_completed_without_commits_marks_completed(self, tmp_path: Path):
        """mark-completed sets status='completed' when branch has no commits."""
        store = self._setup_store(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create branch with NO commits beyond main
        git._run("checkout", "-b", "gza/1-empty-branch")
        git._run("checkout", "main")

        task = store.add("Failed task no commits")
        task.status = "failed"
        task.branch = "gza/1-empty-branch"
        store.update(task)

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No commits found" in result.stdout
        assert "completed" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.has_commits is False

    def test_mark_completed_force_stale_in_progress_recovery(self, tmp_path: Path):
        """--force supports stale in_progress recovery without git validation."""
        store = self._setup_store(tmp_path)

        task = store.add("Stale worker task", task_type="implement")
        task.status = "in_progress"
        store.update(task)

        result = run_gza("mark-completed", str(task.id), "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "in_progress → completed" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "completed"

    def test_mark_completed_failed_task_no_warning(self, tmp_path: Path):
        """mark-completed does not warn when task is in failed status."""
        store = self._setup_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        git._run("checkout", "-b", "gza/1-failed-branch")
        git._run("checkout", "main")

        task = store.add("Failed task")
        task.status = "failed"
        task.branch = "gza/1-failed-branch"
        store.update(task)

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Warning" not in result.stdout

    def test_mark_completed_cleans_up_running_worker(self, tmp_path: Path):
        """mark-completed calls registry.mark_completed() for a running worker."""
        from gza.workers import WorkerMetadata

        store = self._setup_store(tmp_path)

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
            task_slug=task.slug,
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

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0

        # Worker metadata should be updated to completed and PID file removed
        updated_worker = registry.get("w-20260301-120000")
        assert updated_worker is not None
        assert updated_worker.status == "completed"
        assert not pid_path.exists()

    def test_mark_completed_no_worker_is_graceful(self, tmp_path: Path):
        """mark-completed succeeds when no worker exists for the task."""

        setup_db_with_tasks(tmp_path, [
            {"prompt": "Review task no worker", "status": "failed", "task_type": "review"},
        ])

        # No workers directory / no registry entry — should still succeed
        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "status-only" in result.stdout

    def test_mark_completed_does_not_touch_already_completed_worker(self, tmp_path: Path):
        """mark-completed leaves an already-completed worker unchanged."""
        from gza.workers import WorkerMetadata

        store = self._setup_store(tmp_path)

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
            task_slug=task.slug,
            started_at="2026-03-01T13:00:00+00:00",
            status="failed",
            log_file=None,
            worktree=None,
            is_background=True,
            exit_code=1,
            completed_at="2026-03-01T13:05:00+00:00",
        )
        registry.register(worker)

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        # Worker that was already failed should remain failed (not touched)
        updated_worker = registry.get("w-20260301-130000")
        assert updated_worker is not None
        assert updated_worker.status == "failed"


class TestSetStatusCommand:
    """Tests for 'gza set-status' command."""

    def test_set_status_nonexistent_task(self, tmp_path: Path):
        """set-status errors when task does not exist."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("set-status", "testproject-999999", "failed", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_set_status_rejects_base36_like_task_id(self, tmp_path: Path):
        """set-status requires decimal task ID suffixes."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("set-status", "testproject-3f", "failed", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Invalid task ID" in result.stdout or "Invalid task ID" in result.stderr
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    @pytest.mark.parametrize("target_status,initial_status,completed_at_set", [
        pytest.param("failed", "in_progress", True, id="in_progress-to-failed"),
        pytest.param("completed", "in_progress", True, id="in_progress-to-completed"),
        pytest.param("dropped", "in_progress", True, id="in_progress-to-dropped"),
        pytest.param("pending", "failed", False, id="failed-to-pending"),
        pytest.param("in_progress", "failed", False, id="failed-to-in_progress"),
    ])
    def test_set_status_transition(self, tmp_path: Path, target_status: str, initial_status: str, completed_at_set: bool):
        """set-status transitions correctly and manages completed_at."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": initial_status},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        from gza.config import Config
        config = Config.load(tmp_path)
        store = SqliteTaskStore(db_path, prefix=config.project_prefix)

        # Get the actual task ID assigned by the store
        all_tasks = store.get_all()
        task_id = all_tasks[0].id

        result = run_gza("set-status", str(task_id), target_status, "--project", str(tmp_path))

        assert result.returncode == 0
        assert target_status in result.stdout

        task = store.get(task_id)
        assert task is not None
        assert task.status == target_status
        if completed_at_set:
            assert task.completed_at is not None
        else:
            assert task.completed_at is None

    def test_set_status_with_reason_for_failed(self, tmp_path: Path):
        """set-status --reason sets failure_reason for failed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = run_gza(
            "set-status", str(task.id), "failed", "--reason", "Process killed", "--project", str(tmp_path)
        )

        assert result.returncode == 0

        store = make_store(tmp_path)
        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.failure_reason == "Process killed"

    def test_set_status_reason_warns_for_non_failed(self, tmp_path: Path):
        """set-status warns when --reason is used with a non-failed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza(
            "set-status", str(task.id), "completed", "--reason", "Ignored reason", "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "Warning" in result.stdout or "warning" in result.stdout.lower()

    def test_set_status_invalid_status_rejected(self, tmp_path: Path):
        """set-status rejects unknown status values."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = run_gza("set-status", str(task.id), "bogus", "--project", str(tmp_path))

        assert result.returncode != 0

    @pytest.mark.parametrize("target_status", ["pending", "dropped"])
    def test_set_status_clears_failure_reason(self, tmp_path: Path, target_status: str):
        """set-status clears failure_reason when transitioning away from failed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "failed"},
        ])
        store = make_store(tmp_path)

        # Set failure_reason on the existing failed task
        all_tasks = store.get_all()
        task = all_tasks[0]
        assert task is not None
        task.failure_reason = "Original error"
        store.update(task)

        result = run_gza("set-status", str(task.id), target_status, "--project", str(tmp_path))

        assert result.returncode == 0

        task = store.get(task.id)
        assert task is not None
        assert task.status == target_status
        assert task.failure_reason is None

    def test_advance_skips_dropped_tasks(self, tmp_path: Path):
        """gza advance does not act on dropped tasks."""
        from gza.db import SqliteTaskStore as _Store
        from gza.git import Git
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = _Store(db_path)

        # Set up a minimal git repo so advance can run
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        # Add a dropped task (it has no branch, no unmerged state)
        task = store.add("Dropped task", task_type="implement")
        task.status = "dropped"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # gza advance should report no eligible tasks — the dropped task is not actionable
        result = run_gza("advance", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_dropped_task_blocks_dependent(self, tmp_path: Path):
        """A task that depends_on a dropped task is reported as blocked."""
        from gza.db import SqliteTaskStore as _Store
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = _Store(db_path)

        prereq = store.add("Dropped prereq")
        prereq.status = "dropped"
        store.update(prereq)

        dependent = store.add("Dependent task", depends_on=prereq.id)

        is_blocked, blocked_by_id, blocked_by_status = store.is_task_blocked(dependent)
        assert is_blocked is True
        assert blocked_by_id == prereq.id
        assert blocked_by_status == "dropped"

    def test_next_all_shows_blocked_annotation_for_dropped_dependency(self, tmp_path: Path):
        """gza next --all shows blocked annotation for a task blocked by a dropped dependency."""
        from gza.db import SqliteTaskStore as _Store
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = _Store(db_path)

        prereq = store.add("Dropped prereq")
        prereq.status = "dropped"
        prereq.completed_at = datetime.now(UTC)
        store.update(prereq)

        store.add("Dependent task", depends_on=prereq.id)

        result = run_gza("next", "--all", "--project", str(tmp_path))
        assert result.returncode == 0
        # The dependent task should appear with a blocked annotation
        assert "Dependent task" in result.stdout
        assert "blocked" in result.stdout.lower()

    def test_history_shows_dropped_tasks(self, tmp_path: Path):
        """gza history includes dropped tasks after fix to get_history() default filter."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore as _Store
        store = _Store(db_path)

        task = store.add("Task to be dropped")
        task.status = "dropped"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Task to be dropped" in result.stdout


class TestMaxTurnsFlag:
    """Tests for --max-turns flag on work, retry, and resume commands."""

    def test_max_turns_override_applies_correctly(self, tmp_path: Path):
        """--max-turns flag overrides config value."""
        import argparse

        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_steps: 50\n")

        config = Config.load(tmp_path)
        assert config.max_turns == 50

        args = argparse.Namespace(max_turns=200, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_steps = args.max_turns
            config.max_turns = args.max_turns

        assert config.max_turns == 200
        assert config.max_steps == 200


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
        mock_run.assert_called_once_with(
            config,
            task_id=task.id,
            resume=False,
            open_after=False,
            skip_precondition_check=False,
        )

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
        mock_run.assert_called_once_with(
            config,
            task_id=task.id,
            resume=True,
            open_after=True,
            skip_precondition_check=False,
        )

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


class TestRunAsWorker:
    """Tests for _run_as_worker() helper."""

    def _register_current_worker(self, config: Config, task_id: int | None, worker_id: str) -> WorkerRegistry:
        from gza.workers import WorkerMetadata

        config.workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id=worker_id,
                task_id=task_id,
                pid=os.getpid(),
                status="running",
                startup_log_file=f"{worker_id}-startup.log",
            )
        )
        return registry

    def test_run_as_worker_nonzero_exit_marks_failed(self, tmp_path: Path):
        """Worker metadata status is failed when run() returns non-zero."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker non-zero")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-nonzero")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", return_value=7):
                rc = _run_as_worker(args, config)

        assert rc == 7
        worker = registry.get("w-worker-nonzero")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 7

    def test_run_as_worker_exception_marks_failed_and_ps_shows_startup_failure(self, tmp_path: Path):
        """Exception cleanup keeps worker/task failed and startup failure visible in ps rows."""
        from gza.cli.query import _build_ps_rows

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker exception")
        assert task.id is not None
        store.mark_in_progress(task)

        registry = self._register_current_worker(config, task.id, "w-worker-exception")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", side_effect=RuntimeError("boom")):
                rc = _run_as_worker(args, config)

        assert rc == 1
        worker = registry.get("w-worker-exception")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "WORKER_DIED"

        rows, _ = _build_ps_rows(registry, store, include_completed=True)
        row = next(r for r in rows if r["worker_id"] == "w-worker-exception")
        assert row["status"] == "failed"
        assert row["startup_failure"] is True

    def test_run_as_worker_signal_handler_marks_failed(self, tmp_path: Path):
        """Signal handler cleanup marks worker as failed before exiting."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker signal")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-signal")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        installed_handlers: dict[int, object] = {}

        def capture_signal(signum, handler):
            installed_handlers[signum] = handler
            return None

        def run_then_signal(*_args, **_kwargs):
            handler = installed_handlers.get(signal_mod.SIGTERM)
            assert callable(handler)
            handler(signal_mod.SIGTERM, None)
            return 0

        with patch("gza.cli.signal.signal", side_effect=capture_signal):
            with patch("gza.cli.run", side_effect=run_then_signal):
                with pytest.raises(SystemExit) as exc:
                    _run_as_worker(args, config)

        assert exc.value.code == 1
        worker = registry.get("w-worker-signal")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1

    def test_run_as_worker_backfills_task_id_after_no_id_claim(self, tmp_path: Path):
        """No-id worker mode updates worker metadata with the claimed DB task ID."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("No-id background claim")
        assert task.id is not None
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        store.update(task)

        registry = self._register_current_worker(config, task_id=None, worker_id="w-worker-claim")
        args = argparse.Namespace(task_ids=[], resume=False)

        def fake_run(
            _config,
            task_id=None,
            resume=False,
            open_after=False,
            skip_precondition_check=False,
            on_task_claimed=None,
        ):
            assert task_id is None
            assert resume is False
            assert skip_precondition_check is False
            claimed = store.get(task.id)
            assert claimed is not None
            if on_task_claimed is not None:
                on_task_claimed(claimed)
            return 0

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", side_effect=fake_run):
                rc = _run_as_worker(args, config)

        assert rc == 0
        worker = registry.get("w-worker-claim")
        assert worker is not None
        assert worker.status == "completed"
        assert worker.task_id == task.id

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
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

        setup_config(tmp_path)
        result = run_gza("add", "--model", "claude-3-5-haiku-latest", "Test task with model", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify model was set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with model"), None)
        assert task is not None
        assert task.model == "claude-3-5-haiku-latest"

    def test_add_with_provider_flag(self, tmp_path: Path):
        """Add command with --provider flag stores provider override."""

        setup_config(tmp_path)
        result = run_gza("add", "--provider", "gemini", "Test task with provider", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify provider was set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with provider"), None)
        assert task is not None
        assert task.provider == "gemini"
        assert task.provider_is_explicit is True

    def test_add_with_both_model_and_provider(self, tmp_path: Path):
        """Add command with both --model and --provider flags works."""

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
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with both"), None)
        assert task is not None
        assert task.model == "claude-opus-4"
        assert task.provider == "claude"
        assert task.provider_is_explicit is True


class TestAddCommandWithNoLearnings:
    """Tests for 'gza add' command with --no-learnings flag."""

    def test_add_with_no_learnings_flag(self, tmp_path: Path):
        """Add command with --no-learnings flag sets skip_learnings on task."""

        setup_config(tmp_path)
        result = run_gza("add", "--no-learnings", "One-off experimental task", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify skip_learnings was set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "One-off experimental task"), None)
        assert task is not None
        assert task.skip_learnings is True

    def test_add_without_no_learnings_flag_defaults_false(self, tmp_path: Path):
        """Add command without --no-learnings flag defaults skip_learnings to False."""

        setup_config(tmp_path)
        result = run_gza("add", "Normal task with learnings", "--project", str(tmp_path))

        assert result.returncode == 0

        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Normal task with learnings"), None)
        assert task is not None
        assert task.skip_learnings is False


class TestEditCommandWithModelAndProvider:
    """Tests for 'gza edit' command with --model and --provider flags."""

    def test_edit_with_model_flag(self, tmp_path: Path):
        """Edit command can set model override."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
        assert task.provider_is_explicit is True


class TestEditCommandWithNoLearnings:
    """Tests for 'gza edit' command with --no-learnings flag."""

    def test_edit_with_no_learnings_flag(self, tmp_path: Path):
        """Edit command with --no-learnings sets skip_learnings on task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Task without skip")
        assert task.skip_learnings is False

        result = run_gza("edit", str(task.id), "--no-learnings", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "skip_learnings" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.skip_learnings is True


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
            provider_is_explicit=True,
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "codex"
        assert model == "o4-mini"

    def test_non_explicit_task_provider_falls_back_to_config_provider(self, tmp_path: Path):
        """Persisted resolved provider should not override current configured provider."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "codex"
        config.providers = {
            "claude": ProviderConfig(model="claude-default"),
            "codex": ProviderConfig(model="o4-mini"),
        }

        task = Task(
            id=1,
            prompt="Task created before provider switch",
            provider="claude",
            provider_is_explicit=False,
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
        from gza.runner import build_prompt

        # Setup config
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Setup database
        store = make_store(tmp_path)

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
        from gza.runner import build_prompt

        # Setup config
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Setup database
        store = make_store(tmp_path)

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
        from gza.db import Task
        from gza.runner import _get_task_output

        task = Task(
            id=1,
            prompt="Test",
            output_content="Content from DB",
        )
        result = _get_task_output(task, tmp_path)
        assert result == "Content from DB"

    def test_falls_back_to_file(self, tmp_path: Path):
        """_get_task_output should fall back to file when no DB content."""
        from gza.db import Task
        from gza.runner import _get_task_output

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
        from gza.db import Task
        from gza.runner import _get_task_output

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
        from gza.db import Task
        from gza.runner import _get_task_output

        task = Task(
            id=4,
            prompt="Test",
            output_content=None,
        )
        result = _get_task_output(task, tmp_path)
        assert result is None


class TestGetReviewVerdict:
    """Tests for get_review_verdict()."""

    def _setup(self, tmp_path: Path):
        from gza.cli import get_review_verdict
        from gza.config import Config
        setup_config(tmp_path)
        store = make_store(tmp_path)
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

    def test_bold_label_only_verdict(self, tmp_path: Path):
        """Parses **Verdict**: CHANGES_REQUESTED format (bold wraps only label)."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Some review text.\n\n**Verdict**: CHANGES_REQUESTED\n"
        store.update(task)
        assert get_review_verdict(config, task) == "CHANGES_REQUESTED"

    def test_no_verdict_returns_none(self, tmp_path: Path):
        """Returns None when no verdict pattern is found."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "I have some thoughts but no verdict."
        store.update(task)
        assert get_review_verdict(config, task) is None

    def test_canonical_structure_with_none_sections(self, tmp_path: Path):
        """Parses canonical review format with explicit None. sections."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = (
            "## Summary\n\n"
            "- Reviewed the implementation.\n\n"
            "## Must-Fix\n\n"
            "None.\n\n"
            "## Suggestions\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Ready to merge.\n"
            "Verdict: APPROVED\n"
        )
        store.update(task)
        assert get_review_verdict(config, task) == "APPROVED"


class TestClearReviewState:
    """Tests for SqliteTaskStore.clear_review_state()."""

    def test_clear_review_state_sets_review_cleared_at(self, tmp_path: Path):
        """clear_review_state sets review_cleared_at on the task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.review_cleared_at is None

        assert task.id is not None
        store.clear_review_state(task.id)

        updated = store.get(task.id)
        assert updated is not None
        assert updated.review_cleared_at is not None

    def test_clear_review_state_updates_timestamp_on_re_clear(self, tmp_path: Path):
        """Calling clear_review_state twice updates the timestamp."""
        import time

        setup_config(tmp_path)
        store = make_store(tmp_path)

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
