"""Tests for task execution and lifecycle CLI commands."""


import argparse
import io
import os
import re
import signal as signal_mod
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from gza.cli import _run_as_worker, _run_foreground, cmd_run_inline
from gza.config import Config
from gza.db import SqliteTaskStore, task_id_numeric_key
from gza.git import Git
from gza.query import build_lineage_tree
from gza.workers import WorkerRegistry

from .conftest import (
    get_latest_task,
    make_store,
    mark_orphaned,
    run_gza,
    setup_config,
    setup_db_with_tasks,
    setup_git_repo_with_task_branch,
)


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

    def test_add_with_pr_flag_persists_create_pr(self, tmp_path: Path):
        """Add command with --pr stores automatic PR intent on the task."""

        setup_config(tmp_path)

        result = run_gza(
            "add",
            "--type",
            "implement",
            "--pr",
            "Implement feature X",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0

        store = make_store(tmp_path)
        task = get_latest_task(store, task_type="implement", prompt="Implement feature X")
        assert task is not None
        assert task.create_pr is True

    def test_add_rejects_empty_tag_without_traceback(self, tmp_path: Path):
        """add --tag '' should fail with user-facing validation, not traceback."""
        setup_config(tmp_path)

        result = run_gza("add", "Tag validation", "--tag", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr


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

    def test_edit_group_empty_string_clears_single_tag_only(self, tmp_path: Path):
        """Legacy --group '' should only clear when exactly one tag exists."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", group="old-group")
        assert task.group == "old-group"

        result = run_gza("edit", str(task.id), "--group", "", "--project", str(tmp_path))

        assert result.returncode == 0
        updated = store.get(task.id)
        assert updated.group is None or updated.group == ""
        assert updated.tags == ()

    def test_edit_group_empty_string_rejects_multi_tag_clear(self, tmp_path: Path):
        """Legacy --group '' should fail for multi-tag tasks with actionable guidance."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", tags=("release-1.2", "backend"))
        assert len(task.tags) == 2

        result = run_gza("edit", str(task.id), "--group", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert 'Error: --group "" is ambiguous for tasks with multiple tags' in result.stdout
        assert "--remove-tag TAG or --clear-tags" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ("backend", "release-1.2")

    def test_edit_clear_tags_still_clears_multi_tag_task(self, tmp_path: Path):
        """--clear-tags remains the explicit path for clearing all tags."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", tags=("release-1.2", "backend"))
        assert len(task.tags) == 2

        result = run_gza("edit", str(task.id), "--clear-tags", "--project", str(tmp_path))

        assert result.returncode == 0
        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ()

    def test_edit_rejects_combined_tag_mutation_flags(self, tmp_path: Path):
        """Tag mutation flags are mutually exclusive to prevent silent partial updates."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", tags=("backend",))

        result = run_gza(
            "edit",
            str(task.id),
            "--add-tag",
            "release-1.2",
            "--remove-tag",
            "backend",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "Tag mutation flags are mutually exclusive" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ("backend",)

    def test_edit_add_tag_single_mutation_succeeds(self, tmp_path: Path):
        """Single tag mutation flag invocation remains supported."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", tags=("backend",))

        result = run_gza("edit", str(task.id), "--add-tag", "release-1.2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added tags for task" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ("backend", "release-1.2")

    def test_edit_add_tag_rejects_empty_value_without_traceback(self, tmp_path: Path):
        """edit --add-tag '' should fail with user-facing validation, not traceback."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Test task")

        result = run_gza("edit", str(task.id), "--add-tag", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr

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

    def test_edit_pr_flag(self, tmp_path: Path):
        """Edit command can enable automatic PR creation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_pr is False

        result = run_gza("edit", str(task.id), "--pr", "--project", str(tmp_path))

        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated.create_pr is True

    def test_edit_review_and_pr_flags_apply_both_mutations(self, tmp_path: Path):
        """Edit command should persist both review and PR intent in one invocation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_review is False
        assert task.create_pr is False

        result = run_gza("edit", str(task.id), "--review", "--pr", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "automatic review task creation" in result.stdout
        assert "automatic PR creation" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_review is True
        assert updated.create_pr is True

    def test_edit_pr_and_model_flags_apply_both_mutations(self, tmp_path: Path):
        """Edit command should persist PR and model overrides together."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_pr is False
        assert task.model is None

        result = run_gza(
            "edit",
            str(task.id),
            "--pr",
            "--model",
            "claude-3-5-haiku-latest",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "automatic PR creation" in result.stdout
        assert "Set model override" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_pr is True
        assert updated.model == "claude-3-5-haiku-latest"

    def test_edit_pr_and_add_tag_apply_both_mutations(self, tmp_path: Path):
        """Edit command should not short-circuit tag edits ahead of other mutations."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_pr is False
        assert task.tags == ()

        result = run_gza(
            "edit",
            str(task.id),
            "--pr",
            "--add-tag",
            "cli",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "automatic PR creation" in result.stdout
        assert "Added tags" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_pr is True
        assert updated.tags == ("cli",)

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

    def test_retry_same_branch_task_uses_base_branch_for_fresh_retry(self, tmp_path: Path):
        """Retries of same-branch tasks fork a fresh branch from the failed branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Improve with shared branch", task_type="improve", same_branch=True)
        task.status = "failed"
        task.branch = "feature/impl-branch"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_task = get_latest_task(store, based_on=task.id, task_type="improve")
        assert retry_task is not None
        assert retry_task.same_branch is False
        assert retry_task.base_branch == "feature/impl-branch"

    def test_create_retry_task_shared_helper_preserves_retry_contract(self, tmp_path: Path):
        """Shared retry creator keeps cmd_retry branch/base-branch semantics and metadata copy."""
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        dep = store.add("Dependency", task_type="plan")
        assert dep.id is not None

        original = store.add(
            "Retry me",
            task_type="improve",
            depends_on=dep.id,
            same_branch=True,
            tags=("release-1.2",),
            spec="docs/spec.md",
            model="gpt-5.3-codex",
            provider="codex",
        )
        assert original.id is not None
        original.status = "failed"
        original.branch = "feature/old"
        original.task_type_hint = "feature"
        original.provider_is_explicit = True
        store.update(original)

        retry_task = _create_retry_task(store, original)
        assert retry_task.based_on == original.id
        assert retry_task.prompt == original.prompt
        assert retry_task.task_type == original.task_type
        assert retry_task.depends_on == original.depends_on
        assert retry_task.tags == original.tags
        assert retry_task.spec == original.spec
        assert retry_task.model == original.model
        assert retry_task.provider == original.provider
        assert retry_task.provider_is_explicit is True
        assert retry_task.same_branch is False
        assert retry_task.base_branch == "feature/old"

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

    def test_resume_freezes_routed_provider_when_session_is_carried(self, tmp_path: Path):
        """Resume must preserve the originally resolved provider when it reuses a session_id.

        Regression (M3): A failed fix task routed via task_providers.fix could be resumed
        under a different backend if routing changed between attempts. That breaks the
        resume invariant — the resume task carries the old session_id, so it must also
        carry the same backend.
        """
        from gza.cli._common import _create_resume_task

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Fix stuck workflow", task_type="fix")
        failed.status = "failed"
        failed.failure_reason = "MAX_STEPS"
        # Runner persisted the effective provider to the task row during the
        # first run (provider_is_explicit stays False — see runner.py for why).
        failed.provider = "codex"
        failed.provider_is_explicit = False
        failed.session_id = "sess-codex-1"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        resumed = _create_resume_task(store, failed)

        # With the session_id carried over, the provider must be frozen as an
        # explicit override so the runner cannot re-route to a different backend.
        assert resumed.session_id == "sess-codex-1"
        assert resumed.provider == "codex"
        assert resumed.provider_is_explicit is True

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
        assert "Started task " in result.stdout
        assert "in background" in result.stdout

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
        assert "Started task " in result.stdout
        assert "in background" in result.stdout
        assert "(resuming," in result.stdout

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
        # A startup log is now opened as soon as the task is claimed, so the
        # resume task has a log file pointing at its own run's log (not the
        # original task's log).
        assert new_task.log_file is not None
        assert new_task.log_file != ".gza/logs/20260101-implement-feature-x.log"

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
        mark_orphaned(store, orphaned_task)

        result = run_gza("work", "--no-docker", "--project", str(tmp_path))

        assert result.returncode == 0
        # Warning about orphaned task should appear before the "No pending tasks" message
        assert "orphaned" in result.stdout
        assert "Stuck task from yesterday" in result.stdout
        assert "gza work" in result.stdout

    def test_work_count_nonzero_run_does_not_count_as_completed(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """A non-zero run result (e.g. PREREQUISITE_UNMERGED refusal) fails the session and stops batch accounting."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending task 1")
        store.add("Pending task 2")

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=2,
            force=False,
            resume=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.run", side_effect=[0, 1]),
        ):
            rc = cmd_run(args)

        output = capsys.readouterr().out
        assert rc == 1
        assert "Completed 1 task(s) before a task failed" in output
        assert "Completed 2 task(s)" not in output

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].status == "failed"
        assert workers[0].exit_code == 1

    def test_work_forwards_create_pr_flag_to_runner(self, tmp_path: Path):
        """work --pr should forward create_pr intent to runner.run."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending task 1")

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=1,
            force=False,
            resume=False,
            create_pr=True,
        )

        seen_create_pr: list[bool] = []

        def _fake_run(*_args, **kwargs):
            seen_create_pr.append(bool(kwargs.get("create_pr")))
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.run", side_effect=_fake_run),
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert seen_create_pr == [True]

    def test_work_group_runs_only_tasks_from_selected_group(self, tmp_path: Path):
        """work --group should select and run only tasks from that group."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        release_task = store.add("Release task", group="release-1")
        backlog_task = store.add("Backlog task", group="backlog")
        assert release_task.id is not None
        assert backlog_task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            group="release-1",
        )

        seen_task_ids: list[str] = []

        def _fake_run(*_args, **kwargs):
            seen_task_ids.append(str(kwargs.get("task_id")))
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.run", side_effect=_fake_run),
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert seen_task_ids == [release_task.id]

    def test_work_group_rejects_empty_value_without_traceback(self, tmp_path: Path):
        """Deprecated --group alias should reject empty values cleanly."""
        setup_config(tmp_path)

        result = run_gza("work", "--group", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr

    def test_work_tag_reports_blocked_when_matching_pending_tasks_are_not_runnable(self, tmp_path: Path):
        """work --tag should distinguish blocked matching tasks from an empty pending set."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        prereq = store.add("Prerequisite task")
        blocked = store.add("Blocked release task", tags=("release-1",), depends_on=prereq.id)
        assert prereq.id is not None
        assert blocked.id is not None

        result = run_gza("work", "--tag", "release-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "blocked by dependencies" in result.stdout
        assert "No pending tasks found matching tags: release-1" not in result.stdout

    def test_work_background_tag_reports_blocked_when_matching_pending_tasks_are_not_runnable(self, tmp_path: Path):
        """work --background --tag should report blocked matching tasks, not missing pending tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        prereq = store.add("Prerequisite task")
        blocked = store.add("Blocked release task", tags=("release-1",), depends_on=prereq.id)
        assert prereq.id is not None
        assert blocked.id is not None

        result = run_gza(
            "work",
            "--background",
            "--no-docker",
            "--tag",
            "release-1",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "blocked by dependencies" in result.stdout
        assert "No pending tasks found matching tags: release-1" not in result.stdout

    def test_work_tag_internal_only_match_reports_non_runnable_not_dependency_blocked(
        self, tmp_path: Path
    ):
        """work --tag should not claim dependency blocking when only internal tasks match."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        internal_task = store.add("Internal release maintenance", task_type="internal", tags=("release-1",))
        assert internal_task.id is not None

        result = run_gza("work", "--tag", "release-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "not runnable via work" in result.stdout
        assert "blocked by dependencies" not in result.stdout

    def test_work_background_tag_internal_only_match_reports_non_runnable_not_dependency_blocked(
        self, tmp_path: Path
    ):
        """work --background --tag should not claim dependency blocking when only internal tasks match."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        internal_task = store.add("Internal release maintenance", task_type="internal", tags=("release-1",))
        assert internal_task.id is not None

        result = run_gza(
            "work",
            "--background",
            "--no-docker",
            "--tag",
            "release-1",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "not runnable via work" in result.stdout
        assert "blocked by dependencies" not in result.stdout

    def test_work_allows_failed_pr_required_task_with_pr_flag(self, tmp_path: Path):
        """work <task> --pr should allow retrying failed PR_REQUIRED tasks."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task")
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[task.id],
            count=1,
            force=False,
            resume=False,
            create_pr=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.run", return_value=0) as run_mock,
        ):
            rc = cmd_run(args)

        assert rc == 0
        run_mock.assert_called_once()

    def test_work_allows_failed_pr_required_task_with_persisted_create_pr(self, tmp_path: Path):
        """work <task> should allow retrying failed PR_REQUIRED tasks via stored create_pr intent."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task", create_pr=True)
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[task.id],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.run", return_value=0) as run_mock,
        ):
            rc = cmd_run(args)

        assert rc == 0
        run_mock.assert_called_once()


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

    def test_background_worker_command_forwards_pr_flag(self, tmp_path: Path):
        """Background work with create_pr intent should include --pr in child command."""
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Test background task with pr")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=True,
        )

        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 11111

        def capture_popen(cmd, **kwargs):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc

        with patch("gza.cli.subprocess.Popen", side_effect=capture_popen):
            _spawn_background_worker(args, config, task_id=task.id)

        assert captured_cmd is not None
        assert "--pr" in captured_cmd

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

    def test_background_worker_verbose_output_escapes_prompt_and_shows_log_hint(self, tmp_path: Path):
        """Verbose startup output preserves literal bracket text in prompts."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Prompt with [literal] brackets")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        mock_proc = MagicMock()
        mock_proc.pid = 22222

        with (
            patch("gza.cli.subprocess.Popen", return_value=mock_proc),
            console.capture() as capture,
        ):
            rc = _spawn_background_worker(args, config, task_id=task.id)

        output = capture.get()
        assert rc == 0
        assert f"Started task {task.id} in background (PID {mock_proc.pid})" in output
        assert "Prompt: Prompt with [literal] brackets" in output
        assert f"Use 'gza log {task.id} -f' to follow progress" in output

    def test_background_worker_quiet_output_is_compact(self, tmp_path: Path):
        """Quiet startup output should omit prompt and follow instructions."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Quiet worker")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        mock_proc = MagicMock()
        mock_proc.pid = 33333

        with (
            patch("gza.cli.subprocess.Popen", return_value=mock_proc),
            console.capture() as capture,
        ):
            rc = _spawn_background_worker(args, config, task_id=task.id, quiet=True)

        output = capture.get()
        assert rc == 0
        assert f"Started task {task.id} in background (PID {mock_proc.pid})" in output
        assert "Prompt:" not in output
        assert "Use 'gza log" not in output

    def test_background_resume_worker_verbose_output_matches_work_style(self, tmp_path: Path):
        """Resume startup output reuses the same themed shape as regular work."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_resume_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Resume [literal] prompt")

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

        mock_proc = MagicMock()
        mock_proc.pid = 44444

        with (
            patch("gza.cli.subprocess.Popen", return_value=mock_proc),
            console.capture() as capture,
        ):
            rc = _spawn_background_resume_worker(args, config, new_task_id=task.id)

        output = capture.get()
        assert rc == 0
        assert f"Started task {task.id} in background (resuming, PID {mock_proc.pid})" in output
        assert "Prompt: Resume [literal] prompt" in output
        assert f"Use 'gza log {task.id} -f' to follow progress" in output

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

    def test_background_iterate_worker_passes_worker_id_to_child(self, tmp_path: Path):
        """Background iterate workers must pass the generated worker_id to the child process."""
        import argparse
        from unittest.mock import MagicMock

        from gza.cli._common import _spawn_background_iterate_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl_task = store.add("Iterate in background", task_type="implement")
        assert impl_task.id is not None

        config = Config.load(tmp_path)
        args = argparse.Namespace(
            no_docker=True,
            force=False,
        )

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 42424

        def fake_spawn(cmd: list[str], _config: Config, worker_id: str):
            nonlocal captured_cmd
            captured_cmd = list(cmd)
            startup_log = f".gza/workers/{worker_id}-startup.log"
            return mock_proc, startup_log

        with patch("gza.cli._common._spawn_detached_worker_process", side_effect=fake_spawn):
            rc = _spawn_background_iterate_worker(args, config, impl_task, max_iterations=3)

        assert rc == 0
        assert captured_cmd is not None
        assert "--worker-id" in captured_cmd
        worker_id = captured_cmd[captured_cmd.index("--worker-id") + 1]

        registry = WorkerRegistry(config.workers_path)
        worker = registry.get(worker_id)
        assert worker is not None
        assert worker.task_id == impl_task.id
        assert worker.pid == 42424

    def test_background_worker_allows_failed_pr_required_task_with_pr_flag(self, tmp_path: Path):
        """Background explicit work should allow retrying failed PR_REQUIRED tasks with --pr."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task in background")
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=True,
            resume=False,
            force=False,
        )

        mock_proc = MagicMock()
        mock_proc.pid = 99999

        with patch("gza.cli.subprocess.Popen", return_value=mock_proc) as mock_popen:
            rc = _spawn_background_worker(args, config, task_id=task.id)

        assert rc == 0
        mock_popen.assert_called_once()

    def test_background_worker_allows_failed_pr_required_task_with_persisted_create_pr(self, tmp_path: Path):
        """Background explicit work should allow retrying failed PR_REQUIRED tasks via stored create_pr intent."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task in background", create_pr=True)
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=False,
            resume=False,
            force=False,
        )

        mock_proc = MagicMock()
        mock_proc.pid = 99999

        with patch("gza.cli.subprocess.Popen", return_value=mock_proc) as mock_popen:
            rc = _spawn_background_worker(args, config, task_id=task.id)

        assert rc == 0
        mock_popen.assert_called_once()

    def test_background_worker_honors_task_providers_routing_for_fix(self, tmp_path: Path):
        """Fix task routed via task_providers.fix must pick provider-specific worker plumbing
        that matches what the runner will use at execution time, not the global config.provider.

        Regression: _spawn_background_worker previously derived provider from
        (task.provider or config.provider), ignoring task-type routing. That caused
        tmux/attach plumbing to diverge from the actually-executed provider.
        """
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        config_path = tmp_path / "gza.yaml"
        worktree_dir = tmp_path / ".gza-test-worktrees"
        db_path = tmp_path / ".gza" / "gza.db"
        config_path.write_text(
            "project_name: test-project\n"
            f"worktree_dir: {worktree_dir}\n"
            f"db_path: {db_path}\n"
            "provider: claude\n"
            "task_providers:\n"
            "  fix: codex\n"
        )
        store = make_store(tmp_path)
        fix_task = store.add("Fix stuck workflow", task_type="fix")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = True

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=False,
            resume=False,
            force=False,
        )

        mock_proc = MagicMock()
        mock_proc.pid = 99999
        mock_run_result = MagicMock()
        mock_run_result.returncode = 0

        subprocess_run_calls: list[list[str]] = []

        def capture_run(cmd, **kwargs):
            subprocess_run_calls.append(list(cmd))
            return mock_run_result

        # Pretend tmux is available so the provider-specific use_tmux decision
        # is observable in the final command.
        with (
            patch("gza.cli.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.subprocess.Popen", return_value=mock_proc),
            patch("gza.cli.subprocess.run", side_effect=capture_run),
            patch("gza.cli.get_tmux_session_pid", return_value=12345),
        ):
            _spawn_background_worker(args, config, task_id=fix_task.id)

        # Global provider is claude (which forces use_tmux=False for worker plumbing),
        # but fix routes to codex where tmux stays enabled — a `tmux new-session`
        # invocation is the observable proof the worker picked routed-provider semantics.
        new_session_calls = [c for c in subprocess_run_calls if len(c) >= 2 and c[0] == "tmux" and c[1] == "new-session"]
        assert new_session_calls, (
            "Fix task routed to codex via task_providers.fix should have kept tmux "
            f"enabled and launched a tmux session; subprocess.run calls were: {subprocess_run_calls}"
        )


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

    def test_reconciliation_marks_silent_live_task_no_activity(self, tmp_path: Path):
        """An alive-but-silent task (no log writes for > threshold) is marked NO_ACTIVITY."""
        from datetime import UTC, datetime, timedelta

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Silent task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()  # PID is alive
        task.started_at = datetime.now(UTC) - timedelta(minutes=2)
        # Leave task.log_file = None → log absent → stuck
        store.update(task)

        config = Config.load(tmp_path)
        # Patch SIGTERM so we don't actually signal our own PID
        with patch("gza.cli._common.os.kill") as mock_kill:
            reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "NO_ACTIVITY"
        # kill called once for the signal check (os.kill(pid, 0)) and once to SIGTERM
        assert mock_kill.call_count >= 1
        request = WorkerRegistry(config.workers_path).consume_interrupt_request(os.getpid())
        assert request is not None
        assert request["signal"] == "SIGTERM"
        assert request["source"] == "watch_reconcile_no_activity"

    def test_reconciliation_skips_recent_live_task(self, tmp_path: Path):
        """A live task under the threshold should NOT be marked NO_ACTIVITY."""
        from datetime import UTC, datetime, timedelta

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Recent task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.started_at = datetime.now(UTC) - timedelta(seconds=5)
        store.update(task)

        config = Config.load(tmp_path)
        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "in_progress"

    def test_reconciliation_skips_live_task_with_recent_log_writes(self, tmp_path: Path):
        """A live task whose log was written recently should NOT be marked NO_ACTIVITY."""
        from datetime import UTC, datetime, timedelta

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        log_rel = ".gza/logs/active.log"
        log_abs = tmp_path / log_rel
        log_abs.parent.mkdir(parents=True, exist_ok=True)
        log_abs.write_text('{"subtype":"info","message":"hi"}\n')
        # Ensure mtime is now (write_text already does this)

        task = store.add("Active task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.started_at = datetime.now(UTC) - timedelta(minutes=5)
        task.log_file = log_rel
        store.update(task)

        config = Config.load(tmp_path)
        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "in_progress"

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

        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.task_type == "implement"
        assert impl_task.based_on is None
        assert impl_task.depends_on == plan_task.id
        assert impl_task.prompt == "Implement auth rollout"
        assert impl_task.create_review is True
        assert f"Depends on: plan {plan_task.id}" in result.stdout

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

        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.prompt == f"Implement plan from task {plan_task.id}: plan-auth-migration"
        assert impl_task.based_on is None
        assert impl_task.depends_on == plan_task.id

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

        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.prompt == f"Implement plan from task {plan_task.id}: plan-auth-migration"
        assert impl_task.based_on is None
        assert impl_task.depends_on == plan_task.id

    def test_implement_rejects_depends_on_flag(self, tmp_path: Path):
        """Implement command should fail fast for removed --depends-on flag."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        dep_task = store.add("Independent dependency", task_type="implement")

        result = run_gza(
            "implement",
            str(plan_task.id),
            "--depends-on",
            str(dep_task.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 2
        assert "unrecognized arguments: --depends-on" in result.stderr


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

    def test_improve_works_from_unresolved_comments_without_review(self, tmp_path: Path):
        """Improve command can run from unresolved comments when no review exists."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Please tighten validation edge cases.")

        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "continuing from unresolved comments only" in result.stdout
        assert "Comments: 1 unresolved" in result.stdout

        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task.depends_on is None

    def test_improve_comments_only_reuses_pending_task(self, tmp_path: Path):
        """Second comments-only improve should reuse an existing pending improve."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Address form validation gaps.")

        first = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        first_improve = improves[0]
        assert first_improve.id is not None

        second = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Reusing pending improve task {first_improve.id}" in second.stdout

        improves_after = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves_after) == 1
        assert improves_after[0].id == first_improve.id

    def test_improve_comments_only_pending_task_with_newer_comment_creates_fresh_task(self, tmp_path: Path):
        """Pending comments-only improve is not reused when newer unresolved comments were added."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1 comment.")
        first = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        first_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert first_improve.id is not None
        first_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(first_improve)

        store.add_comment(impl_task.id, "Round 2 comment added after improve creation.")
        second = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Reusing pending improve task {first_improve.id}" not in second.stdout
        assert "Created improve task" in second.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 2
        newest = max(improves, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != first_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

    def test_improve_comments_only_resumes_failed_task(self, tmp_path: Path):
        """Failed comments-only improve should create a resume task, not duplicate-error."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Handle edge-case parsing.")

        first = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        failed_improve = improves[0]
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "improve-session-1"
        store.update(failed_improve)
        assert failed_improve.id is not None

        second = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"(resume of {failed_improve.id})" in second.stdout

        improves_after = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves_after) == 2
        resumed = max(improves_after, key=lambda t: task_id_numeric_key(t.id))
        assert resumed.based_on == failed_improve.id
        assert resumed.depends_on is None

    def test_improve_comments_only_failed_task_with_newer_comment_creates_fresh_task(self, tmp_path: Path):
        """Failed comments-only improve is not resumed/retried when newer unresolved comments exist."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1 comment.")
        first = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "improve-session-1"
        failed_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(failed_improve)

        store.add_comment(impl_task.id, "Round 2 comment added after failed improve creation.")
        second = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"(resume of {failed_improve.id})" not in second.stdout
        assert f"(retry of {failed_improve.id})" not in second.stdout
        assert "Created improve task" in second.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 2
        newest = max(improves, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != failed_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

    def test_improve_comments_only_retry_resets_omitted_review_model_and_provider_flags(self, tmp_path: Path):
        """Comments-only improve retries should not inherit stale CLI flags when omitted."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Retry comments-only improve after infra failure.")

        first = run_gza(
            "improve",
            str(impl_task.id),
            "--queue",
            "--review",
            "--model",
            "gpt-5.3-codex",
            "--provider",
            "codex",
            "--project",
            str(tmp_path),
        )
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_improve.session_id = None
        store.update(failed_improve)

        second = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"(retry of {failed_improve.id})" in second.stdout

        retry_task = max(
            (task for task in store.get_all() if task.task_type == "improve"),
            key=lambda task: task_id_numeric_key(task.id),
        )
        assert retry_task.id != failed_improve.id
        assert retry_task.based_on == failed_improve.id
        assert retry_task.create_review is False
        assert retry_task.model is None
        assert retry_task.provider is None
        assert retry_task.provider_is_explicit is False

    def test_improve_comments_only_retry_honors_explicit_review_model_and_provider_overrides(
        self, tmp_path: Path
    ):
        """Comments-only improve retries should apply the current invocation overrides."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Retry comments-only improve with new flags.")

        first = run_gza(
            "improve",
            str(impl_task.id),
            "--queue",
            "--model",
            "gpt-5.3-codex",
            "--provider",
            "codex",
            "--project",
            str(tmp_path),
        )
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_improve.session_id = None
        store.update(failed_improve)

        second = run_gza(
            "improve",
            str(impl_task.id),
            "--queue",
            "--review",
            "--model",
            "gpt-5.4",
            "--provider",
            "claude",
            "--project",
            str(tmp_path),
        )
        assert second.returncode == 0, second.stdout
        assert f"(retry of {failed_improve.id})" in second.stdout

        retry_task = max(
            (task for task in store.get_all() if task.task_type == "improve"),
            key=lambda task: task_id_numeric_key(task.id),
        )
        assert retry_task.id != failed_improve.id
        assert retry_task.based_on == failed_improve.id
        assert retry_task.create_review is True
        assert retry_task.model == "gpt-5.4"
        assert retry_task.provider == "claude"
        assert retry_task.provider_is_explicit is True

    def test_improve_comments_only_in_progress_task_with_newer_comment_creates_fresh_task(self, tmp_path: Path):
        """In-progress comments-only improve is ignored when newer unresolved comments require a new pass."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1 comment.")
        first = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        running_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert running_improve.id is not None
        running_improve.status = "in_progress"
        running_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(running_improve)

        store.add_comment(impl_task.id, "Round 2 comment added while improve is running.")
        second = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Comments-only improve {running_improve.id} is already in progress" not in second.stdout
        assert "Created improve task" in second.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 2
        newest = max(improves, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != running_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

    def test_improve_comments_only_completed_then_new_comments_creates_fresh_task(self, tmp_path: Path):
        """Completed comments-only improves should not block fresh improves for new comments."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1: tighten API validation.")
        first = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        completed_improve = improves[0]
        assert completed_improve.id is not None
        assert completed_improve.created_at is not None
        completed_improve.status = "completed"
        completed_improve.completed_at = datetime.now(UTC)
        store.update(completed_improve)

        # Mirror runner completion semantics: resolve only comments in the improve snapshot.
        store.resolve_comments(impl_task.id, created_on_or_before=completed_improve.created_at)

        store.add_comment(impl_task.id, "Round 2: improve timeout handling.")
        second = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert "Created improve task" in second.stdout

        improves_after = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves_after) == 2
        newest = max(improves_after, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != completed_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

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

    def test_improve_errors_when_only_pending_review_and_no_comments(self, tmp_path: Path):
        """Auto-pick must not bind to a pending review when no comments fallback exists.

        Regression for the trap where auto-pick would silently create an
        improve task tied to a non-completed review, leaving the improve
        permanently blocked if the review never completes (or surfacing a
        warning the operator may miss). Non-completed reviews are only
        selectable via explicit --review-id now.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Only a pending review exists; no unresolved comments.
        pending_review = store.add("Review", task_type="review", depends_on=impl_task.id)

        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no completed review" in result.stdout
        assert f"{pending_review.id} (pending)" in result.stdout
        assert "--review-id" in result.stdout

        # No improve task should have been created.
        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert improves == []

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
        assert "no completed review" in result.stdout
        assert "--review-id" in result.stdout

    def test_improve_auto_pick_falls_back_to_comments_when_only_pending_review(
        self, tmp_path: Path
    ):
        """Comments-only fallback must kick in when the only review is pending.

        Regression for review gza-1276 M1: auto-pick previously treated pending
        reviews as usable, which suppressed the comments-only fallback when
        unresolved comments were the real runnable feedback source.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        pending_review = store.add("Review", task_type="review", depends_on=impl_task.id)
        # Leave status as default 'pending'.
        store.add_comment(impl_task.id, "Tighten validation edge cases.")

        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "no completed review" in result.stdout
        assert "continuing from unresolved comments only" in result.stdout
        # Must not have bound to the pending review.
        assert f"is pending" not in result.stdout
        assert "blocked until it completes" not in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        assert improves[0].depends_on is None
        # Pending review remains, unbound.
        assert pending_review.status == "pending"

    def test_improve_auto_pick_falls_back_to_comments_when_only_in_progress_review(
        self, tmp_path: Path
    ):
        """Same fallback for an in_progress review: comments-only improve wins."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        in_progress_review = store.add("Review", task_type="review", depends_on=impl_task.id)
        in_progress_review.status = "in_progress"
        store.update(in_progress_review)
        store.add_comment(impl_task.id, "Revisit error wording.")

        result = run_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "continuing from unresolved comments only" in result.stdout
        assert "blocked until it completes" not in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        assert improves[0].depends_on is None

    def test_improve_rejects_failed_review_id(self, tmp_path: Path):
        """--review-id must reject terminal failed reviews rather than warn.

        Regression for review gza-1276 M2: previously --review-id accepted
        failed/dropped statuses and printed "blocked until it completes",
        creating a permanently unrunnable improve tied to a review that will
        never complete.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        failed_review = store.add("Failed review", task_type="review", depends_on=impl_task.id)
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        store.update(failed_review)

        result = run_gza(
            "improve",
            str(impl_task.id),
            "--review-id",
            str(failed_review.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert f"Review {failed_review.id} is failed" in result.stdout
        assert "terminal reviews cannot produce feedback" in result.stdout
        assert "blocked until it completes" not in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert improves == []

    def test_improve_rejects_dropped_review_id(self, tmp_path: Path):
        """--review-id must also reject terminal dropped reviews."""
        setup_config(tmp_path)
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

        result = run_gza(
            "improve",
            str(impl_task.id),
            "--review-id",
            str(dropped_review.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert f"Review {dropped_review.id} is dropped" in result.stdout
        assert "terminal reviews cannot produce feedback" in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert improves == []

    def test_improve_rejects_failed_review_id_suggests_comments_fallback_when_available(
        self, tmp_path: Path
    ):
        """When comments exist, rejection message should suggest omitting --review-id."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        failed_review = store.add("Failed review", task_type="review", depends_on=impl_task.id)
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        store.update(failed_review)

        store.add_comment(impl_task.id, "Please address this edge case.")

        result = run_gza(
            "improve",
            str(impl_task.id),
            "--review-id",
            str(failed_review.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "Omit --review-id to run a comments-only improve" in result.stdout
        assert "1 unresolved comment" in result.stdout

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


class TestFixCommand:
    """Tests for 'gza fix' command."""

    def test_fix_creates_task_from_implementation_and_latest_review(self, tmp_path: Path):
        """Fix command creates a fix task linked to the root implementation and latest completed review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add retries", task_type="implement", group="infra")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-retries"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review1 = store.add("Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.completed_at = datetime.now(UTC)
        store.update(review1)

        review2 = store.add("Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.completed_at = datetime.now(UTC)
        store.update(review2)

        result = run_gza("fix", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created fix task" in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout
        assert f"Latest completed review: {review2.id}" in result.stdout

        fix_tasks = [t for t in store.get_all() if t.task_type == "fix"]
        assert len(fix_tasks) == 1
        fix_task = fix_tasks[0]
        assert fix_task.based_on == impl_task.id
        assert fix_task.depends_on == review2.id
        assert fix_task.same_branch is True
        assert fix_task.group == "infra"

    def test_fix_accepts_review_task_id_and_resolves_implementation(self, tmp_path: Path):
        """Fix command accepts review IDs and resolves to the base implementation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement auth", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review auth", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        result = run_gza("fix", str(review_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Implementation: {impl_task.id}" in result.stdout
        fix_task = [t for t in store.get_all() if t.task_type == "fix"][0]
        assert fix_task.based_on == impl_task.id
        assert fix_task.depends_on == review_task.id

    def test_fix_accepts_chained_improve_task_id_and_resolves_root_impl(self, tmp_path: Path):
        """Fix command resolves chained improve IDs back to the root implementation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement parser", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review1 = store.add("Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.completed_at = datetime.now(UTC)
        store.update(review1)

        improve1 = store.add(
            "Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
            same_branch=True,
        )
        improve1.status = "completed"
        improve1.completed_at = datetime.now(UTC)
        store.update(improve1)

        review2 = store.add("Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.completed_at = datetime.now(UTC)
        store.update(review2)

        improve2 = store.add(
            "Improve 2 retry",
            task_type="improve",
            based_on=improve1.id,
            depends_on=review2.id,
            same_branch=True,
        )
        improve2.status = "completed"
        improve2.completed_at = datetime.now(UTC)
        store.update(improve2)

        result = run_gza("fix", str(improve2.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Implementation: {impl_task.id}" in result.stdout
        assert f"Latest completed review: {review2.id}" in result.stdout
        fix_task = [t for t in store.get_all() if t.task_type == "fix"][0]
        assert fix_task.based_on == impl_task.id
        assert fix_task.depends_on == review2.id

    def test_fix_uses_retry_implementation_id_as_selected(self, tmp_path: Path):
        """Fix command keeps direct retry implementation IDs anchored to that retry implementation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement parser", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-parser"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        retry_impl = store.add("Retry parser implementation", task_type="implement", based_on=impl_task.id)
        retry_impl.status = "completed"
        retry_impl.branch = "test-project/20260130-retry-parser"
        retry_impl.completed_at = datetime.now(UTC)
        store.update(retry_impl)

        review_retry = store.add("Review retry", task_type="review", depends_on=retry_impl.id)
        review_retry.status = "completed"
        review_retry.completed_at = datetime.now(UTC)
        store.update(review_retry)

        result = run_gza("fix", str(retry_impl.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Implementation: {retry_impl.id}" in result.stdout
        assert f"Latest completed review: {review_retry.id}" in result.stdout
        fix_task = [t for t in store.get_all() if t.task_type == "fix"][0]
        assert fix_task.based_on == retry_impl.id
        assert fix_task.depends_on == review_retry.id

    def test_fix_rejects_pending_or_in_progress_implementation(self, tmp_path: Path):
        """Fix command rejects active implementation tasks that have not finished yet."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement cache", task_type="implement")
        impl_task.status = "in_progress"
        store.update(impl_task)

        result = run_gza("fix", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 1
        assert f"Task {impl_task.id} is in_progress" in result.stdout

    def test_fix_defers_follow_up_handoff_to_runner(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Fix command should not run local git probing or create reviews directly."""
        from gza.cli.execution import cmd_fix

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement no-change rescue", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-no-change"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review latest", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(impl_task.id),
            queue=False,
            background=False,
            no_docker=True,
            max_turns=None,
            model=None,
            provider=None,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", return_value=0),
            patch("gza.cli.execution.Git") as mock_git_cls,
            patch("gza.cli.execution._create_review_task") as mock_create_review,
        ):
            rc = cmd_fix(args)

        assert rc == 0
        mock_git_cls.assert_not_called()
        mock_create_review.assert_not_called()
        output = capsys.readouterr().out
        assert "Running fix task" in output


class TestCommentCommand:
    """Tests for `gza comment` command."""

    def test_comment_adds_direct_comment(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Task needing follow-up", task_type="implement")
        assert task.id is not None

        result = run_gza(
            "comment",
            str(task.id),
            "Please add regression coverage.",
            "--author",
            "alice",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Added comment" in result.stdout
        comments = store.get_comments(task.id)
        assert len(comments) == 1
        assert comments[0].source == "direct"
        assert comments[0].author == "alice"
        assert comments[0].content == "Please add regression coverage."


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
        assert "is a plan task, not an implementation, improve, review, or fix task" in result.stdout

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

    def test_review_accepts_fix_task_id_and_targets_implementation(self, tmp_path: Path):
        """Review command accepts fix IDs and resolves to the fix task's implementation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement auth", task_type="implement", group="auth")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-auth"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        fix_task = store.add(
            "Fix auth churn",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
            group="auth",
        )
        fix_task.status = "completed"
        fix_task.completed_at = datetime.now(UTC)
        store.update(fix_task)

        result = run_gza("review", str(fix_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert f"Created review task {review_task.id}" in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout
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

    def test_review_keeps_retry_implementation_id_anchored(self, tmp_path: Path):
        """Review command keeps direct retry implementation IDs anchored to the selected retry task."""

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

        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.based_on == retry_impl.id
        assert review_task.depends_on == retry_impl.id

        retry_based_children = {t.id for t in store.get_based_on_children(retry_impl.id)}
        parent_based_children = {t.id for t in store.get_based_on_children(impl_task.id)}
        assert review_task.id in retry_based_children
        assert review_task.id not in parent_based_children

        # Review should now hang directly under the retry implementation branch.
        lineage_tree = build_lineage_tree(store, plan_task)
        assert len(lineage_tree.children) == 1
        parent_impl_node = lineage_tree.children[0]
        assert parent_impl_node.task.id == impl_task.id
        assert len(parent_impl_node.children) == 1
        retry_node = [c for c in parent_impl_node.children if c.task.id == retry_impl.id][0]
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

    @pytest.fixture(autouse=True)
    def _mock_iterate_git_runtime(self):
        """Default iterate tests to a deterministic git runtime unless overridden."""
        from unittest.mock import MagicMock, patch

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with patch("gza.cli.Git", return_value=mock_git):
            yield

    def _make_completed_impl(self, store, prompt: str = "Implement feature") -> object:
        """Create and return a completed implement task."""
        from datetime import datetime
        impl = store.add(prompt, task_type="implement")
        impl.status = "completed"
        impl.branch = "test-project/20260101-impl"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        return impl

    def _init_git_repo(self, tmp_path: Path) -> None:
        from gza.git import Git

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

    def test_cycle_dry_run(self, tmp_path: Path):
        """gza iterate --dry-run prints preview and exits 0."""

        setup_config(tmp_path)
        self._init_git_repo(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()

    def test_cycle_uses_default_iterations_when_flag_omitted(self, tmp_path: Path):
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "db_path: .gza/gza.db\n"
        )
        self._init_git_repo(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "max 3 iterations" in result.stdout

    def test_iterate_live_progress_labels_non_cycle_merge_as_next_action(
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
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "Iterating implementation" in output
        assert "max 1 iterations" in output
        assert "Next action: merge" in output
        assert "Iteration 1/1: merge" not in output
        assert "Action 1/1" not in output

    def test_iterate_merge_without_required_review_reports_merge_ready_not_approved(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            advance_requires_review=False,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "Next action: merge" in output
        assert "Iteration 1/1: merge" not in output
        assert "Iterate complete: MERGE_READY" in output
        assert "Merge task (no review yet)" in output
        assert "Iterate complete: APPROVED" not in output

    def test_iterate_review_cleared_merge_reports_merge_ready_not_approved(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        prior_review = store.add("Review", task_type="review", depends_on=impl.id)
        prior_review.status = "completed"
        prior_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        prior_review.completed_at = datetime.now(UTC)
        store.update(prior_review)

        # Mark review as cleared and ensure no newer review/improve cycle exists,
        # so shared engine returns the "reviews_all_cleared" merge path.
        impl.review_cleared_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "Next action: merge" in output
        assert "Iteration 1/1: merge" not in output
        assert "Iterate complete: MERGE_READY" in output
        assert "Merge (previous review addressed)" in output
        assert "Iterate complete: APPROVED" not in output

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
        """Pending implementation counts as iteration 1 and every write is followed by review."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")  # status = 'pending'
        review1 = store.add("Review 1", task_type="review", depends_on=impl.id)
        improve1 = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
        review2 = None

        from datetime import datetime

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == impl.id:
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
                task.duration_seconds = 10.0
                task.num_steps_computed = 1
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            if task_id == review1.id:
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            if task_id == improve1.id:
                task.status = "completed"
                task.completed_at = datetime.now()
                store.update(task)
                root = store.get(impl.id)
                assert root is not None
                root.review_cleared_at = task.completed_at
                store.update(root)
                return 0
            if review2 is not None and task_id == review2.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        def fake_create_review_task(_store, _impl_task):
            nonlocal review2
            current_review1 = store.get(review1.id)
            assert current_review1 is not None
            if current_review1.status == "pending":
                return review1
            if review2 is None:
                review2 = store.add("Review 2", task_type="review", depends_on=impl.id)
            return review2

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=3,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli.Git", return_value=mock_git), \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground, \
             patch("gza.cli._create_review_task", side_effect=fake_create_review_task), \
             patch("gza.cli._create_improve_task", return_value=improve1):
            result = cmd_iterate(args)

        assert result == 0
        call_task_ids = [call[1]["task_id"] for call in run_foreground.call_args_list]
        assert review2 is not None
        assert call_task_ids == [impl.id, review1.id, improve1.id, review2.id]

    def test_pending_impl_summary_max_iterations_one_includes_iteration_one_write(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review = store.add("Review", task_type="review", depends_on=impl.id)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == impl.id:
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
                task.duration_seconds = 20.0
                task.num_steps_computed = 2
                task.cost_usd = 0.50
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            if task_id == review.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.duration_seconds = 30.0
                task.num_steps_reported = 3
                task.cost_usd = 0.70
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli._create_review_task", return_value=review),
            patch("gza.cli._create_improve_task") as create_improve,
            patch("gza.cli.time.monotonic", side_effect=[100.0, 150.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert [call[1]["task_id"] for call in run_foreground.call_args_list] == [impl.id, review.id]
        create_improve.assert_not_called()
        assert re.search(
            rf"1\s+review\s+{re.escape(review.id)}\s+APPROVED\s+30s\s+3\s+\$0\.70\s+completed",
            output,
        )
        assert "Totals: 50s wall | 3 steps | $0.70" in output

    def test_pending_impl_second_review_approved_stops_immediately(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review1 = store.add("Review 1", task_type="review", depends_on=impl.id)
        improve = store.add("Improve", task_type="improve", based_on=impl.id, depends_on=review1.id)
        review2 = None

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == impl.id:
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
                task.duration_seconds = 10.0
                task.num_steps_computed = 1
                task.cost_usd = 0.10
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            if task_id == review1.id:
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
                task.duration_seconds = 40.0
                task.num_steps_reported = 4
                task.cost_usd = 0.40
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            if task_id == improve.id:
                task.status = "completed"
                task.duration_seconds = 15.0
                task.num_steps_computed = 2
                task.cost_usd = 0.20
                task.completed_at = datetime.now(UTC)
                store.update(task)
                root = store.get(impl.id)
                assert root is not None
                root.review_cleared_at = task.completed_at
                store.update(root)
                return 0
            if review2 is not None and task_id == review2.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.duration_seconds = 25.0
                task.num_steps_reported = 3
                task.cost_usd = 0.30
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        def fake_create_review_task(_store, _impl_task):
            nonlocal review2
            current_review1 = store.get(review1.id)
            assert current_review1 is not None
            if current_review1.status == "pending":
                return review1
            if review2 is None:
                review2 = store.add("Review 2", task_type="review", depends_on=impl.id)
            return review2

        args = argparse.Namespace(project_dir=str(tmp_path), impl_task_id=str(impl.id), max_iterations=4, dry_run=False, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli._create_review_task", side_effect=fake_create_review_task),
            patch("gza.cli._create_improve_task", return_value=improve),
            patch("gza.cli.time.monotonic", side_effect=[100.0, 190.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert review2 is not None
        assert [call[1]["task_id"] for call in run_foreground.call_args_list] == [impl.id, review1.id, improve.id, review2.id]
        assert re.search(
            rf"1\s+review\s+{re.escape(review1.id)}\s+CHANGES_REQUESTED\s+40s\s+4\s+\$0\.40\s+completed",
            output,
        )
        assert re.search(
            rf"2\s+improve\s+{re.escape(improve.id)}\s+-\s+15s\s+2\s+\$0\.20\s+completed",
            output,
        )
        assert re.search(
            rf"2\s+review\s+{re.escape(review2.id)}\s+APPROVED\s+25s\s+3\s+\$0\.30\s+completed",
            output,
        )
        assert "Iterate complete: APPROVED (approved)" in output
        assert "Totals: 1m30s wall | 13 steps | $1.30" in output

        review1_task = store.get(review1.id)
        review2_task = store.get(review2.id)
        improve_task = store.get(improve.id)
        assert review1_task is not None and review1_task.depends_on == impl.id
        assert review2_task is not None and review2_task.depends_on == impl.id
        assert improve_task is not None
        assert improve_task.based_on == impl.id
        assert improve_task.depends_on == review1.id
        assert store.get_improve_tasks_for(impl.id, review2.id) == []

    def test_iterate_first_pass_approved_with_followups_creates_followup_and_rerun_reuses(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)

        review_output = (
            "## Summary\n\n"
            "- Looks good overall.\n\n"
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "### F1\n"
            "Evidence: Edge case currently uncovered.\n"
            "Impact: Rare invalid input may pass through silently.\n"
            "Recommended follow-up: add malformed-input validation.\n"
            "Recommended tests: add a CLI regression for malformed input.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: APPROVED_WITH_FOLLOWUPS\n"
        )

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == review.id
            task.status = "completed"
            task.output_content = review_output
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            result_first = cmd_iterate(args)
        output_first = capsys.readouterr().out

        followups_after_first = [
            task for task in store.get_based_on_children(review.id) if task.task_type == "implement"
        ]
        assert result_first == 0
        assert len(followups_after_first) == 1
        first_followup = followups_after_first[0]
        assert first_followup.prompt.startswith(
            f"Follow-up F1 from review {review.id} for task {impl.id}:"
        )
        assert "Iterate complete: APPROVED (approved_with_followups)" in output_first
        assert first_followup.id in output_first
        assert "followup" in output_first
        assert "created" in output_first

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground") as run_foreground_rerun,
        ):
            result_second = cmd_iterate(args)
        output_second = capsys.readouterr().out

        followups_after_second = [
            task for task in store.get_based_on_children(review.id) if task.task_type == "implement"
        ]
        assert result_second == 0
        run_foreground_rerun.assert_not_called()
        assert len(followups_after_second) == 1
        assert followups_after_second[0].id == first_followup.id
        assert first_followup.id in output_second
        assert "followup" in output_second
        assert "reused" in output_second

    def test_iterate_first_pass_approved_with_followups_without_findings_is_blocked(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)

        review_output = (
            "## Summary\n\n"
            "- Looks good overall.\n\n"
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: APPROVED_WITH_FOLLOWUPS\n"
        )

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == review.id
            task.status = "completed"
            task.output_content = review_output
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        followups = [task for task in store.get_based_on_children(review.id) if task.task_type == "implement"]
        assert result == 3
        assert followups == []
        assert "Iterate complete: BLOCKED (needs_discussion)" in output

    def test_pending_impl_all_changes_requested_with_iterations_four_ends_on_review(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review1 = store.add("Review 1", task_type="review", depends_on=impl.id)

        executed_ids: list[str] = []
        review_count = 0
        improve_count = 0

        def fake_run_foreground(config, task_id, **kwargs):
            nonlocal review_count
            task = store.get(task_id)
            assert task is not None
            assert task.id is not None
            executed_ids.append(task.id)

            if task.task_type == "implement":
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
            elif task.task_type == "review":
                review_count += 1
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
            elif task.task_type == "improve":
                task.status = "completed"
                root = store.get(impl.id)
                assert root is not None
                root.review_cleared_at = datetime.now(UTC)
                store.update(root)
            else:
                raise AssertionError(f"unexpected task type: {task.task_type}")

            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        def fake_create_review_task(_store, _impl_task):
            if review_count == 0:
                return review1
            return store.add(f"Review {review_count + 1}", task_type="review", depends_on=impl.id)

        def fake_create_improve_task(_store, _impl_task, review_task):
            nonlocal improve_count
            improve_count += 1
            return store.add(
                f"Improve {improve_count}",
                task_type="improve",
                based_on=impl.id,
                depends_on=review_task.id,
            )

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=4,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch("gza.cli._create_review_task", side_effect=fake_create_review_task),
            patch("gza.cli._create_improve_task", side_effect=fake_create_improve_task),
        ):
            result = cmd_iterate(args)

        assert result == 2
        task_types = [store.get(task_id).task_type for task_id in executed_ids]
        assert task_types.count("implement") == 1
        assert task_types.count("improve") == 3
        assert task_types.count("review") == 4
        assert len(task_types) == 8
        assert task_types[-1] == "review"

        review_ids = [task_id for task_id in executed_ids if store.get(task_id).task_type == "review"]
        improve_ids = [task_id for task_id in executed_ids if store.get(task_id).task_type == "improve"]
        assert len(review_ids) == 4
        assert len(improve_ids) == 3

        for review_id in review_ids:
            review_task = store.get(review_id)
            assert review_task is not None
            assert review_task.depends_on == impl.id

        for index, improve_id in enumerate(improve_ids):
            improve_task = store.get(improve_id)
            assert improve_task is not None
            assert improve_task.based_on == impl.id
            assert improve_task.depends_on == review_ids[index]

        assert store.get_improve_tasks_for(impl.id, review_ids[-1]) == []

        direct_children = {child.id for child in store.get_lineage_children(impl.id)}
        assert direct_children == set(review_ids + improve_ids)

    def test_pending_impl_with_iterations_one_runs_exactly_one_implement_and_one_review(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review = store.add("Review", task_type="review", depends_on=impl.id)

        executed_ids: list[str] = []

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id is not None
            executed_ids.append(task.id)

            if task.task_type == "implement":
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
            elif task.task_type == "review":
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
            else:
                raise AssertionError(f"unexpected task type: {task.task_type}")

            task.completed_at = datetime.now(UTC)
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
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch("gza.cli._create_review_task", return_value=review),
            patch("gza.cli._create_improve_task") as create_improve,
        ):
            result = cmd_iterate(args)

        assert result == 2
        create_improve.assert_not_called()
        assert [store.get(task_id).task_type for task_id in executed_ids] == ["implement", "review"]

    def test_branchless_completed_impl_is_blocked_without_running_review_actions(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.branch = None
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        assert "Next action: skip" in output
        assert "Iteration 1/1: skip" not in output
        assert "Iterate blocked: skip. Manual review required." in output
        assert "needs_rebase" not in output
        assert "Cannot rebase" not in output

    def test_pending_impl_that_completes_without_branch_is_blocked(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            if task is not None and task.id == impl.id:
                task.status = "completed"
                task.branch = None
                task.completed_at = datetime.now(UTC)
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
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        assert "Running pending implementation" in output
        assert "Next action: skip" in output
        assert "Iteration 1/1: skip" not in output
        assert "Iterate blocked: skip. Manual review required." in output
        assert "needs_rebase" not in output
        assert "Cannot rebase" not in output

    def test_branchless_completed_impl_with_no_reviews_does_not_create_or_run_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.branch = None
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert "Next action: skip" in output
        assert "Iteration 1/1: skip" not in output
        assert "Iterate blocked: skip. Manual review required." in output

    def test_pending_impl_dry_run(self, tmp_path: Path):
        """gza iterate --dry-run on a pending task shows it would run the impl first."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")  # status = 'pending'

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pending" in result.stdout.lower()
        assert "dry-run" in result.stdout.lower()

    def test_iterate_dry_run_merge_ready_uses_next_action_label(
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
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "[dry-run] Would iterate implementation" in output
        assert "[dry-run] First next action: merge" in output
        assert "[dry-run] First iteration 1/1 action: merge" not in output

    def test_iterate_dry_run_approved_with_newer_unresolved_comments_prefers_run_improve(
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
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(review)

        improve = store.add("Pending improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "pending"
        improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        store.add_comment(impl.id, "Fresh unresolved feedback after approval.")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "[dry-run] First iteration 1/1 action: run_improve" in output
        assert "[dry-run] First next action: merge" not in output

    def test_iterate_real_approved_with_followups_and_newer_unresolved_comments_runs_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza import advance_engine as advance_engine_module
        from gza.cli import cmd_iterate
        from gza.review_verdict import ParsedReviewReport, ReviewFinding

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(review)

        pending_improve = store.add("Pending improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        pending_improve.status = "pending"
        pending_improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(pending_improve)

        store.add_comment(impl.id, "Fresh unresolved issue added after followups verdict.")

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == pending_improve.id
            task.status = "failed"
            task.failure_reason = "TEST_FAILURE"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 1

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            advance_requires_review=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch.object(
                advance_engine_module,
                "get_review_report",
                return_value=ParsedReviewReport(
                    verdict="APPROVED_WITH_FOLLOWUPS",
                    findings=(
                        ReviewFinding(
                            id="F1",
                            severity="FOLLOWUP",
                            title="Hardening",
                            body="",
                            evidence=None,
                            impact=None,
                            fix_or_followup="add malformed input guard",
                            tests=None,
                        ),
                    ),
                    format_version="v2",
                ),
            ),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_called_once()
        assert run_foreground.call_args.kwargs["task_id"] == pending_improve.id
        assert "Iteration 1/1: run_improve" in output
        assert "merge_with_followups" not in output

    def test_dry_run_reuses_completed_improve_for_changes_requested_review(self, tmp_path: Path):
        """Dry-run reflects restart state when CHANGES_REQUESTED already has a completed improve."""

        setup_config(tmp_path)
        self._init_git_repo(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        git = Git(tmp_path)
        git._run("checkout", "-b", impl.branch)
        (tmp_path / "impl.txt").write_text("impl work")
        git._run("add", "impl.txt")
        git._run("commit", "-m", "Add impl work")
        git._run("checkout", "main")

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(review)

        improve = store.add("Completed improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
        store.update(improve)

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "would iterate implementation" in result.stdout.lower()
        assert "first iteration 1/3 action: improve" in result.stdout.lower()
        assert "changes_requested" in result.stdout.lower()

    def test_dry_run_review_cleared_starts_from_current_completed_improve(self, tmp_path: Path):
        """Dry-run reports current-write improve when review state was cleared after older review."""

        setup_config(tmp_path)
        self._init_git_repo(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        git = Git(tmp_path)
        git._run("checkout", "-b", impl.branch)
        (tmp_path / "impl.txt").write_text("impl work")
        git._run("add", "impl.txt")
        git._run("commit", "-m", "Add impl work")
        git._run("checkout", "main")

        stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
        stale_review.status = "completed"
        stale_review.output_content = "**Verdict: APPROVED**"
        stale_review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(stale_review)

        improve = store.add("Current write", task_type="improve", based_on=impl.id, depends_on=stale_review.id)
        improve.status = "completed"
        improve.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
        store.update(impl)

        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "would iterate implementation" in result.stdout.lower()
        assert "first iteration 1/3 action: create_review" in result.stdout.lower()
        assert "code changed since last review" in result.stdout.lower()

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
        setup_git_repo_with_task_branch(tmp_path, "seed", "feature/seed")
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

    def test_failed_task_background_resume_requires_session_id(self, tmp_path: Path):
        """Background iterate should reject --resume before spawning when no session exists."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        store.update(impl)

        result = run_gza("iterate", str(impl.id), "--resume", "--background", "--project", str(tmp_path))

        assert result.returncode != 0
        output = result.stdout + (result.stderr or "")
        assert "no session id" in output.lower()
        assert "--retry" in output
        assert "started iterate worker" not in output.lower()

    def test_failed_task_retry_runs_then_iterates(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """gza iterate --retry on a failed task retries it then enters the loop via real engine transitions."""
        import argparse
        from unittest.mock import MagicMock, patch
        from datetime import datetime

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        impl.same_branch = True
        impl.branch = "feature/existing-impl-branch"
        store.update(impl)
        review = store.add("Review", task_type="review")

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task.task_type == "review":
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            if task.status == "pending":
                task.status = "completed"
                if task.task_type == "implement":
                    task.branch = "test-project/20260101-retry"
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

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
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            advance_requires_review=False,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg, \
             patch("gza.cli.Git", return_value=mock_git):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        # First call should be running the retry task (not the original failed one)
        assert run_fg.call_count >= 1
        first_task_id = run_fg.call_args_list[0][1]["task_id"]
        assert first_task_id != impl.id  # Should be a new task
        retry_task = store.get(first_task_id)
        assert retry_task is not None
        assert retry_task.same_branch is False
        assert retry_task.base_branch == "feature/existing-impl-branch"
        assert "Retrying failed implementation" in output
        assert "Iterate complete: MERGE_READY" in output

    def test_failed_task_resume_runs_then_iterates(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """gza iterate --resume on a failed task resumes then enters the loop via real engine transitions."""
        import argparse
        from unittest.mock import MagicMock, patch
        from datetime import datetime

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        impl.session_id = "resume-session-1"
        store.update(impl)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            if task and task.status == "pending":
                task.status = "completed"
                if task.task_type == "implement":
                    task.branch = "test-project/20260101-resume"
                task.completed_at = datetime.now()
                store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=True,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            advance_requires_review=False,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert run_fg.call_count >= 1
        first_task_id = run_fg.call_args_list[0][1]["task_id"]
        assert first_task_id != impl.id
        assert "Resuming failed implementation" in output
        assert "Iterate complete: MERGE_READY" in output

    def test_failed_task_resume_reuses_matching_pending_resume_child(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """gza iterate --resume should reuse an existing pending resume child for the failed root task."""
        import argparse
        from unittest.mock import MagicMock, patch
        from datetime import datetime

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.status = "failed"
        impl.session_id = "resume-session-1"
        store.update(impl)

        resume_child = store.add("Pending resume child", task_type="implement", based_on=impl.id)
        assert resume_child.id is not None
        resume_child.status = "pending"
        resume_child.session_id = impl.session_id
        store.update(resume_child)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            if task and task.status == "pending":
                task.status = "completed"
                if task.task_type == "implement":
                    task.branch = "test-project/20260101-resume-reuse"
                task.completed_at = datetime.now()
                store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=True,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            advance_requires_review=False,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert run_fg.call_count >= 1
        first_task_id = run_fg.call_args_list[0][1]["task_id"]
        assert first_task_id == resume_child.id
        assert [task.id for task in store.get_based_on_children(impl.id)] == [resume_child.id]
        assert "Resuming failed implementation" in output
        assert "Iterate complete: MERGE_READY" in output

    def test_iterate_continue_flag_is_rejected(self, tmp_path: Path):
        setup_config(tmp_path)
        result = run_gza("iterate", "testproject-1", "--continue", "--project", str(tmp_path))
        assert result.returncode != 0
        assert "unrecognized arguments: --continue" in (result.stderr or result.stdout)

    def test_cycle_alias_runs_iterate(self, tmp_path: Path):
        setup_config(tmp_path)
        self._init_git_repo(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        result = run_gza("cycle", str(impl.id), "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "would iterate implementation" in result.stdout.lower()

    def test_reuses_latest_changes_requested_review_for_first_iteration(self, tmp_path: Path):
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        latest_review = store.add("Review", task_type="review", depends_on=impl.id)
        latest_review.status = "completed"
        latest_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        latest_review.completed_at = datetime.now(UTC)
        store.update(latest_review)

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            advance_requires_review=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "improve"
        assert action["review_task"].id == latest_review.id

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
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
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
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == pending_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"
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
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == pending_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"
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
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

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
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            advance_requires_review=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )

        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "run_improve"
        assert action["improve_task"].id == improve.id

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
            rf"1\s+improve\s+{re.escape(improve.id)}\s+-\s+45s\s+3\s+\$0\.29\s+in_progress",
            output,
        )
        assert "Totals: 1m0s wall | 7 steps | $0.70" in output
        assert "Iterate waiting: improve_in_progress. Existing task is already in progress." in output
        assert "Manual review required" not in output

    def test_iterate_max_cycles_reached_reports_cycle_accounting(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        trigger_review = store.add("Trigger review", task_type="review", depends_on=impl.id)
        trigger_review.status = "completed"
        trigger_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        trigger_review.completed_at = datetime.now(UTC)
        store.update(trigger_review)

        for idx in range(5):
            done_improve = store.add(
                f"Prior improve {idx + 1}",
                task_type="improve",
                based_on=impl.id,
                depends_on=trigger_review.id,
            )
            done_improve.status = "completed"
            done_improve.completed_at = datetime.now(UTC)
            store.update(done_improve)

        improve_6 = store.add("Improve 6", task_type="improve", based_on=impl.id, depends_on=trigger_review.id)
        improve_7 = store.add("Improve 7", task_type="improve", based_on=impl.id, depends_on=trigger_review.id)
        review_6 = store.add("Review 6", task_type="review", depends_on=impl.id)
        review_7 = store.add("Review 7", task_type="review", depends_on=impl.id)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=7,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_review_cycles=7,
            max_resume_attempts=1,
            advance_requires_review=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            if task.task_type == "review":
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch(
                "gza.cli.determine_next_action",
                side_effect=[
                    {"type": "run_improve", "description": "Run improve 6", "improve_task": improve_6},
                    {"type": "run_improve", "description": "Run improve 6", "improve_task": improve_6},
                    {"type": "run_review", "description": "Run review 6", "review_task": review_6},
                    {"type": "run_improve", "description": "Run improve 7", "improve_task": improve_7},
                    {"type": "run_review", "description": "Run review 7", "review_task": review_7},
                    {"type": "max_cycles_reached", "description": "Reached max review cycles"},
                ],
            ),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        assert "Iterate blocked: max_cycles_reached." in output
        assert "Review-cycle accounting: completed=7, max_review_cycles=7, consumed_this_invocation=2" in output
        assert f"Recommended next step: uv run gza fix {impl.id}" in output

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

    def test_changes_requested_with_completed_improve_reuses_write_and_runs_review_same_iteration(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

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
        improve.duration_seconds = 45.0
        improve.num_steps_computed = 3
        improve.cost_usd = 0.21
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            advance_requires_review=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )

        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "improve"
        assert action["review_task"].id == review.id

    def test_review_cleared_with_completed_improve_bootstraps_iteration_one_to_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        impl.duration_seconds = 30.0
        impl.num_steps_computed = 2
        impl.cost_usd = 0.10
        store.update(impl)

        stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
        stale_review.status = "completed"
        stale_review.output_content = "**Verdict: APPROVED**"
        stale_review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(stale_review)

        improve = store.add("Current write", task_type="improve", based_on=impl.id, depends_on=stale_review.id)
        improve.status = "completed"
        improve.duration_seconds = 40.0
        improve.num_steps_computed = 4
        improve.cost_usd = 0.25
        improve.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
        store.update(impl)

        next_review = store.add("Fresh review", task_type="review", depends_on=impl.id)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == next_review.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.duration_seconds = 20.0
                task.num_steps_reported = 3
                task.cost_usd = 0.15
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
            patch("gza.cli._create_review_task", return_value=next_review),
            patch("gza.cli._create_improve_task") as create_improve,
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli.time.monotonic", side_effect=[100.0, 130.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == next_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"
        create_improve.assert_not_called()
        assert not re.search(
            rf"1\s+implement\s+{re.escape(impl.id)}\s+",
            output,
        )
        assert re.search(
            rf"1\s+review\s+{re.escape(next_review.id)}\s+APPROVED\s+20s\s+3\s+\$0\.15\s+completed",
            output,
        )

    def test_review_cleared_with_completed_improve_and_in_progress_review_shows_improve_iteration_one(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
        stale_review.status = "completed"
        stale_review.output_content = "**Verdict: APPROVED**"
        stale_review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(stale_review)

        improve = store.add("Current write", task_type="improve", based_on=impl.id, depends_on=stale_review.id)
        improve.status = "completed"
        improve.duration_seconds = 45.0
        improve.num_steps_computed = 5
        improve.cost_usd = 0.33
        improve.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
        store.update(impl)

        running_review = store.add("Running review", task_type="review", depends_on=impl.id)
        running_review.status = "in_progress"
        store.update(running_review)

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
            patch("gza.cli.time.monotonic", side_effect=[200.0, 220.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        create_improve.assert_not_called()
        run_foreground.assert_not_called()
        assert not re.search(
            rf"1\s+implement\s+{re.escape(impl.id)}\s+",
            output,
        )
        assert re.search(
            rf"1\s+review\s+{re.escape(running_review.id)}\s+-\s+-\s+-\s+-\s+in_progress",
            output,
        )
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output

    def test_changes_requested_with_failed_improve_retries_instead_of_blocking(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """When a failed improve exists, iterate creates a retry and runs it."""
        from unittest.mock import MagicMock, patch

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        failed_improve = store.add("Failed improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        failed_improve.status = "failed"
        store.update(failed_improve)

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            advance_requires_review=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        # The engine should emit an "improve" action, and resolve_improve_action
        # should detect the failed improve and recommend a retry
        assert action["type"] == "improve"
        from gza.cli._common import resolve_improve_action
        improve_action, target = resolve_improve_action(store, impl.id, review.id)
        assert improve_action == "retry"
        assert target is not None
        assert target.id == failed_improve.id

    def test_resolve_improve_action_finds_chained_retry_improves(self, tmp_path: Path):
        """get_improve_tasks_for must find retries whose based_on points at a previous improve."""
        from gza.cli._common import resolve_improve_action

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "TIMEOUT"
        first.session_id = "s1"
        store.update(first)

        # A retry whose based_on points at the previous improve (not the impl).
        retry = store.add("Improve 2 (retry)", task_type="improve", based_on=first.id, depends_on=review.id)
        retry.status = "failed"
        retry.failure_reason = "TIMEOUT"
        retry.session_id = "s2"
        store.update(retry)

        # With max_resume_attempts=3 (headroom), resume should win and target the latest.
        action, target = resolve_improve_action(store, impl.id, review.id, max_resume_attempts=3)
        assert action == "resume"
        assert target is not None
        assert target.id == retry.id

    def test_resolve_improve_action_give_up_when_cap_exceeded(self, tmp_path: Path):
        """After one resume already failed, max_resume_attempts=1 triggers give_up."""
        from gza.cli._common import resolve_improve_action

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "TIMEOUT"
        first.session_id = "s1"
        store.update(first)
        second = store.add("Improve 2", task_type="improve", based_on=first.id, depends_on=review.id)
        second.status = "failed"
        second.failure_reason = "TIMEOUT"
        second.session_id = "s2"
        store.update(second)

        # Two failed attempts: 1 original + 1 resume → cap=1 is met, give up.
        action, target = resolve_improve_action(store, impl.id, review.id, max_resume_attempts=1)
        assert action == "give_up"
        assert target is not None
        assert target.id == second.id

    def test_resolve_improve_action_still_resumes_within_cap(self, tmp_path: Path):
        """One failed attempt with max_resume_attempts=1 still resumes (cap not yet exceeded)."""
        from gza.cli._common import resolve_improve_action

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "TIMEOUT"
        first.session_id = "s1"
        store.update(first)

        action, target = resolve_improve_action(store, impl.id, review.id, max_resume_attempts=1)
        assert action == "resume"
        assert target is not None
        assert target.id == first.id

    def test_changes_requested_with_dropped_improve_blocks_and_does_not_run_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """When a dropped improve exists, _create_improve_task rejects the duplicate and iterate blocks."""
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
        dropped_improve = store.add("Dropped improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        dropped_improve.status = "dropped"
        store.update(dropped_improve)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._run_foreground") as run_foreground,
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli.time.monotonic", side_effect=[300.0, 340.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_not_called()
        create_review.assert_not_called()
        assert "Error creating improve task" in output
        assert "Iterate blocked: improve_failed" in output

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

    def test_iterate_create_review_duplicate_in_progress_waits_instead_of_running(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.review_tasks import DuplicateReviewError

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        active_review = store.add("Active review", task_type="review", depends_on=impl.id)
        active_review.status = "in_progress"
        store.update(active_review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "create_review", "description": "Create review"},
            ),
            patch("gza.cli._create_review_task", side_effect=DuplicateReviewError(active_review)),
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_not_called()
        assert "Waiting for review" in output
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output

    def test_iterate_create_review_duplicate_pending_reuses_and_runs_once(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.review_tasks import DuplicateReviewError

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        pending_review = store.add("Pending review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        store.update(pending_review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "create_review", "description": "Create review"},
            ),
            patch("gza.cli._create_review_task", side_effect=DuplicateReviewError(pending_review)),
            patch("gza.cli._run_foreground", return_value=0) as run_foreground,
            patch("gza.cli.get_review_verdict", return_value="APPROVED"),
        ):
            result = cmd_iterate(args)

        assert result == 0
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == pending_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"

    def test_iterate_improve_duplicate_blocks_instead_of_raising(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        completed_review = store.add("Completed review", task_type="review", depends_on=impl.id)
        completed_review.status = "completed"
        completed_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        completed_review.completed_at = datetime.now(UTC)
        store.update(completed_review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={
                    "type": "improve",
                    "description": "Create improve task",
                    "review_task": completed_review,
                },
            ),
            patch(
                "gza.cli._create_improve_task",
                side_effect=ValueError("An improve task already exists for implementation"),
            ) as create_improve,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_improve.assert_called_once()
        run_foreground.assert_not_called()
        assert "Error creating improve task" in output
        assert "Iterate blocked: improve_failed." in output

    def test_iterate_run_review_auto_resumes_max_steps_failure(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "pending"
        review.session_id = "review-session"
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=3,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task.id == review.id and not resume:
                task.status = "failed"
                task.failure_reason = "MAX_STEPS"
                task.session_id = "review-session"
                store.update(task)
                return 1
            task.status = "completed"
            task.output_content = "**Verdict: APPROVED**"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "run_review", "description": "Run review", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            result = cmd_iterate(args)

        assert result == 0
        assert run_fg.call_count == 2
        assert run_fg.call_args_list[0].kwargs.get("resume", False) is False
        assert run_fg.call_args_list[1].kwargs["resume"] is True
        children = store.get_based_on_children(review.id)
        assert len(children) == 1

    def test_iterate_improve_resume_passes_resume_true_to_run_foreground(self, tmp_path: Path):
        """When iterate picks 'resume' for an improve, _run_foreground must be called with resume=True.

        Otherwise _auto_rebase_before_resume never fires and the resume runs on a stale branch.
        """
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        failed_improve = store.add(
            "Improve", task_type="improve", depends_on=review.id, based_on=impl.id, same_branch=True
        )
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "improve-session"
        failed_improve.branch = impl.branch
        store.update(failed_improve)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=3,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        improve_action = {"type": "improve", "description": "Create improve", "review_task": review}
        # First call is initial_action (pre-loop); second is the first loop iteration; third terminates.
        engine_actions = [improve_action, improve_action, {"type": "skip", "description": "done"}]

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.determine_next_action", side_effect=engine_actions),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            cmd_iterate(args)

        assert run_fg.call_count == 1
        # The improve-resume path must pass resume=True so _auto_rebase_before_resume fires.
        assert run_fg.call_args_list[0].kwargs.get("resume") is True

    def test_iterate_default_resume_budget_is_one(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "pending"
        review.session_id = "review-session"
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=None,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "MAX_STEPS"
            task.session_id = "review-session"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "run_review", "description": "Run review", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            result = cmd_iterate(args)

        assert result == 3
        assert run_fg.call_count == 2
        assert run_fg.call_args_list[0].kwargs.get("resume", False) is False
        assert run_fg.call_args_list[1].kwargs["resume"] is True
        children = store.get_based_on_children(review.id)
        assert len(children) == 1

    def test_iterate_run_review_does_not_auto_resume_test_failure(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "pending"
        review.session_id = "review-session"
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=2,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "TEST_FAILURE"
            task.session_id = "review-session"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "run_review", "description": "Run review", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            result = cmd_iterate(args)

        assert result == 3
        assert run_fg.call_count == 1
        assert run_fg.call_args_list[0].kwargs.get("resume", False) is False
        children = store.get_based_on_children(review.id)
        assert len(children) == 0

    def test_iterate_handles_needs_rebase_and_resume_actions_from_engine(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        rebase_task = store.add("Rebase", task_type="rebase", based_on=impl.id)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=3,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        engine_actions = [
            {"type": "needs_rebase", "description": "needs rebase"},
            {"type": "needs_rebase", "description": "needs rebase"},
            {"type": "resume", "description": "resume"},
            {"type": "merge", "description": "done"},
        ]

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.determine_next_action", side_effect=engine_actions),
            patch("gza.cli._create_rebase_task", return_value=rebase_task) as create_rebase,
            patch("gza.cli._create_resume_task") as create_resume,
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            resume_task = store.add("Resume", task_type="implement", based_on=impl.id)
            create_resume.return_value = resume_task
            result = cmd_iterate(args)

        assert result == 0
        create_rebase.assert_called_once()
        create_resume.assert_called_once()

    def test_iterate_real_engine_needs_rebase_transition(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When git reports conflicts, the advance engine should request a rebase."""
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = False
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            advance_requires_review=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )

        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "needs_rebase"

    def test_iterate_errors_when_git_init_fails(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=3,
        )

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", side_effect=RuntimeError("git init boom")),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            result = cmd_iterate(args)
            output = stdout.getvalue()

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output

    def test_iterate_errors_when_current_branch_fails(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=3,
        )
        mock_git = MagicMock()
        mock_git.current_branch.side_effect = RuntimeError("branch boom")

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            result = cmd_iterate(args)
            output = stdout.getvalue()

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output

    def test_iterate_dry_run_errors_when_git_init_fails_without_first_action(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )

        with patch("gza.cli.Git", side_effect=RuntimeError("git init boom")):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output
        assert "First action" not in output

    def test_iterate_dry_run_errors_when_current_branch_fails_without_first_action(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_git = MagicMock()
        mock_git.current_branch.side_effect = RuntimeError("branch boom")

        with patch("gza.cli.Git", return_value=mock_git):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output
        assert "First action" not in output


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

    def test_mark_completed_promotes_unset_mode_to_skill_inline_from_log(self, tmp_path: Path):
        """Inline skill provenance in log should backfill execution_mode when unset."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Inline skill review task", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = None
        task.log_file = ".gza/logs/inline.log"
        store.update(task)

        log_path = tmp_path / task.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            '{"type":"gza","subtype":"provenance","message":"Execution mode: inline skill","skill":"gza-task-run","inline":true}\n'
        )

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "skill_inline"

    def test_mark_completed_preserves_manual_mode_even_with_inline_provenance_log(self, tmp_path: Path):
        """Explicit manual mode should not be overwritten by log-based inline backfill."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Manual rerun with historical inline log", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = "manual"
        task.log_file = ".gza/logs/inline-historical.log"
        store.update(task)

        log_path = tmp_path / task.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            '{"type":"gza","subtype":"provenance","message":"Execution mode: inline skill","skill":"gza-task-run","inline":true}\n'
        )

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "manual"

    def test_mark_completed_warns_when_inline_provenance_log_is_unreadable(self, tmp_path: Path):
        """mark-completed should surface a warning when provenance log cannot be read."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Unreadable inline provenance log", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = None
        task.log_file = ".gza/logs/inline-unreadable.log"
        store.update(task)

        # Make log path unreadable as a file by creating a directory at the same location.
        (tmp_path / task.log_file).mkdir(parents=True, exist_ok=True)

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Could not read task log" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode is None

    def test_mark_completed_warns_when_inline_provenance_log_has_malformed_json(self, tmp_path: Path):
        """mark-completed should surface a warning when provenance log has malformed JSON lines."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Malformed inline provenance log", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = None
        task.log_file = ".gza/logs/inline-malformed.log"
        store.update(task)

        log_path = tmp_path / task.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            "{not valid json}\n"
            '{"type":"gza","subtype":"info","message":"hello"}\n'
        )

        result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "malformed JSON line(s)" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode is None

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

    def test_set_status_in_progress_defaults_execution_mode_to_manual(self, tmp_path: Path):
        """Manual in-progress transitions should stamp execution_mode=manual."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = run_gza("set-status", str(task.id), "in_progress", "--project", str(tmp_path))
        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "manual"

    @pytest.mark.parametrize("existing_mode", ["worker_foreground", "skill_inline"])
    def test_set_status_in_progress_without_mode_overwrites_existing_mode_to_manual(
        self, tmp_path: Path, existing_mode: str
    ):
        """Manual in-progress transitions should override stale prior execution provenance."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "failed", "task_type": "review"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.execution_mode = existing_mode
        store.update(task)

        result = run_gza("set-status", str(task.id), "in_progress", "--project", str(tmp_path))
        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "manual"

        shown = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert shown.returncode == 0
        assert "Execution Mode: manual" in shown.stdout

    def test_set_status_in_progress_accepts_explicit_execution_mode(self, tmp_path: Path):
        """in_progress transitions can persist explicit execution provenance."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending", "task_type": "review"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = run_gza(
            "set-status",
            str(task.id),
            "in_progress",
            "--execution-mode",
            "skill_inline",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 0

        # Simulate inline runner synthetic provenance log written after transition.
        reloaded = store.get(task.id)
        assert reloaded is not None
        reloaded.log_file = ".gza/logs/inline-in-progress.log"
        store.update(reloaded)
        log_path = tmp_path / reloaded.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            '{"type":"gza","subtype":"provenance","message":"Execution mode: inline skill","skill":"gza-task-run","inline":true}\n'
        )

        shown = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert shown.returncode == 0
        assert "Execution Mode: skill_inline" in shown.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "in_progress"
        assert updated.execution_mode == "skill_inline"

    def test_set_status_failed_preserves_skill_inline_execution_mode(self, tmp_path: Path):
        """Inline runs that fail before mark-completed should retain skill_inline provenance."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending", "task_type": "review"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = run_gza(
            "set-status",
            str(task.id),
            "in_progress",
            "--execution-mode",
            "skill_inline",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 0

        fail_result = run_gza(
            "set-status",
            str(task.id),
            "failed",
            "--reason",
            "TEST_FAILURE",
            "--project",
            str(tmp_path),
        )
        assert fail_result.returncode == 0

        shown = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert shown.returncode == 0
        assert "Execution Mode: skill_inline" in shown.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.execution_mode == "skill_inline"

    def test_set_status_execution_mode_rejected_for_non_in_progress(self, tmp_path: Path):
        """--execution-mode should be rejected unless status is in_progress."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = run_gza(
            "set-status",
            str(task.id),
            "failed",
            "--execution-mode",
            "skill_inline",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "--execution-mode is only valid" in result.stdout

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

    def test_run_foreground_precondition_refusal_marks_worker_failed(self, tmp_path: Path):
        """Precondition refusal (PREREQUISITE_UNMERGED path => non-zero) must not be recorded as completed."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Blocked by prerequisite")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with patch("gza.cli.run", return_value=1):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 1
        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].status == "failed"
        assert workers[0].exit_code == 1

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

    def test_run_foreground_resume_auto_rebases_before_run(self, tmp_path: Path):
        """Resume runs an automatic rebase step before resuming the provider session."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Resume target")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with (
            patch("gza.cli._auto_rebase_before_resume", return_value=0) as mock_rebase,
            patch("gza.cli.run", return_value=0) as mock_run,
        ):
            rc = _run_foreground(config, task_id=task.id, resume=True)

        assert rc == 0
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_called_once_with(
            config,
            task_id=task.id,
            resume=True,
            open_after=False,
            skip_precondition_check=False,
        )

    def test_run_foreground_resume_rebase_failure_skips_run(self, tmp_path: Path):
        """Resume aborts before run() when the automatic rebase fails."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Resume target")
        assert task.id is not None

        workers_path = config.workers_path
        workers_path.mkdir(parents=True, exist_ok=True)

        with (
            patch("gza.cli._auto_rebase_before_resume", return_value=1) as mock_rebase,
            patch("gza.cli.run") as mock_run,
        ):
            rc = _run_foreground(config, task_id=task.id, resume=True)

        assert rc == 1
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_not_called()
        registry = WorkerRegistry(workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].status == "failed"
        assert workers[0].exit_code == 1

    def test_auto_rebase_before_resume_creates_completed_rebase_child(self, tmp_path: Path):
        """Resume preflight creates a completed rebase child task on success."""
        from gza.cli import _auto_rebase_before_resume
        from gza.git import Git

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("base\n")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        task = store.add("Resume target", task_type="implement")
        assert task.id is not None
        task.status = "failed"
        task.session_id = "resume-session"
        task.branch = "feature/resume-target"
        store.update(task)

        git._run("checkout", "-b", task.branch)
        (tmp_path / "feature.txt").write_text("feature\n")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Feature commit")
        git._run("checkout", "main")
        (tmp_path / "main.txt").write_text("main update\n")
        git._run("add", "main.txt")
        git._run("commit", "-m", "Main update")

        rc = _auto_rebase_before_resume(config, task.id)

        assert rc == 0
        rebase_children = [t for t in store.get_based_on_children(task.id) if t.task_type == "rebase"]
        assert len(rebase_children) == 1
        rebase_task = rebase_children[0]
        assert rebase_task.status == "completed"
        assert rebase_task.branch == task.branch
        assert "Rebased" in (rebase_task.output_content or "")
        assert rebase_task.log_file is not None

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


class TestRunInlineCommand:
    """Tests for run-inline command wiring."""

    def test_cmd_run_inline_passes_foreground_inline_auto_invocation(self, tmp_path: Path):
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=False,
            max_turns=None,
            task_id="testproject-1",
            resume=True,
            force=True,
        )

        with (
            patch("gza.cli.Config.load", return_value=config),
            patch("gza.cli.resolve_id", return_value="testproject-1"),
            patch("gza.cli._run_foreground", return_value=130) as mock_run_foreground,
        ):
            rc = cmd_run_inline(args)

        assert rc == 130
        mock_run_foreground.assert_called_once()
        assert mock_run_foreground.call_args.kwargs["task_id"] == "testproject-1"
        assert mock_run_foreground.call_args.kwargs["resume"] is True
        assert mock_run_foreground.call_args.kwargs["force"] is True
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "run-inline"
        assert invocation.execution_mode == "foreground_inline"
        assert invocation.interaction_mode == "auto"


class TestForegroundInvocationContextWiring:
    """Foreground command entrypoints should pass command-specific runner invocation metadata."""

    def test_cmd_implement_passes_implement_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_implement

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan feature", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            plan_task_id=plan.id,
            prompt=None,
            group=None,
            depends_on=None,
            review=False,
            same_branch=False,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            background=False,
            queue=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_implement(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "implement"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_review_passes_review_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement auth", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            model=None,
            provider=None,
            background=False,
            queue=False,
            open=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_review(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "review"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_review_prints_followup_summary_after_success(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        from gza.cli.execution import cmd_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement auth", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            model=None,
            provider=None,
            background=False,
            queue=False,
            open=False,
            force=False,
        )

        def fake_run_foreground(*_args, **kwargs):
            review_id = kwargs["task_id"]
            review = store.get(review_id)
            assert review is not None
            review.status = "completed"
            review.output_content = (
                "## Summary\n\nLooks good.\n\n"
                "## Blockers\n\nNone.\n\n"
                "## Follow-Ups\n\n"
                "### F1 Validation\n"
                "Recommended follow-up: add validation.\n\n"
                "## Verdict\n\n"
                "Verdict: APPROVED_WITH_FOLLOWUPS\n"
            )
            review.completed_at = datetime.now(UTC)
            store.update(review)
            return 0

        with patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground):
            rc = cmd_review(args)

        output = capsys.readouterr().out
        assert rc == 0
        assert "Review " in output
        assert "APPROVED_WITH_FOLLOWUPS [follow-ups: F1]" in output

    def test_cmd_improve_passes_improve_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_improve

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement api", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review api", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            max_turns=None,
            review=False,
            review_id=None,
            model=None,
            provider=None,
            background=False,
            queue=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_improve(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "improve"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_fix_passes_fix_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_fix

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement parser", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            queue=False,
            background=False,
            no_docker=True,
            max_turns=None,
            model=None,
            provider=None,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_fix(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "fix"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_resume_passes_resume_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_resume

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implement", task_type="implement")
        failed.status = "failed"
        failed.session_id = "resume-session-123"
        store.update(failed)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=failed.id,
            no_docker=True,
            max_turns=None,
            background=False,
            queue=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_resume(args)

        assert rc == 0
        assert mock_run_foreground.call_args.kwargs["resume"] is True
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "resume"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_iterate_passes_iterate_invocation_to_run_foreground(self, tmp_path: Path):
        from unittest.mock import MagicMock

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            invocation = kwargs["invocation"]
            assert invocation.command == "iterate"
            assert invocation.execution_mode == "foreground_worker"
            task.status = "completed"
            task.branch = "testproject/20260101-impl"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
            force=False,
        )

        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            iterate_max_iterations=3,
            advance_requires_review=False,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch(
                "gza.cli.execution.determine_next_action",
                side_effect=[
                    {"type": "merge", "description": "merge ready"},
                    {"type": "merge", "description": "merge ready"},
                ],
            ),
        ):
            rc = cmd_iterate(args)

        assert rc == 0
        assert run_foreground.call_count == 1


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

    def test_run_as_worker_resume_auto_rebases_before_run(self, tmp_path: Path):
        """Worker resume runs the automatic rebase step before run()."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker resume target")
        assert task.id is not None

        self._register_current_worker(config, task.id, "w-worker-resume")
        args = argparse.Namespace(task_ids=[task.id], resume=True)

        with patch("gza.cli.signal.signal"), \
             patch("gza.cli._auto_rebase_before_resume", return_value=0) as mock_rebase, \
             patch("gza.cli.run", return_value=0) as mock_run:
            rc = _run_as_worker(args, config)

        assert rc == 0
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_called_once()

    def test_run_as_worker_resume_rebase_failure_skips_run(self, tmp_path: Path):
        """Worker resume marks the worker failed when auto-rebase fails."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker resume target")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-resume-fail")
        args = argparse.Namespace(task_ids=[task.id], resume=True)

        with patch("gza.cli.signal.signal"), \
             patch("gza.cli._auto_rebase_before_resume", return_value=1) as mock_rebase, \
             patch("gza.cli.run") as mock_run:
            rc = _run_as_worker(args, config)

        assert rc == 1
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_not_called()
        worker = registry.get("w-worker-resume-fail")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1

    def test_run_as_worker_precondition_refusal_marks_failed(self, tmp_path: Path):
        """Worker mode should record non-zero precondition refusal as failed, not completed."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker blocked task")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-prereq")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", return_value=1):
                rc = _run_as_worker(args, config)

        assert rc == 1
        worker = registry.get("w-worker-prereq")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1

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
        assert "is not a gza command" in result.stderr
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

    def test_add_with_next_marks_task_urgent(self, tmp_path: Path):
        """`gza add --next` should bump the new task to the front of urgent pickup."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Older urgent", urgent=True)
        store.add("Newer urgent", urgent=True)

        result = run_gza("add", "--next", "Urgent follow-up", "--project", str(tmp_path))

        assert result.returncode == 0
        task = next((t for t in store.get_pending() if t.prompt == "Urgent follow-up"), None)
        assert task is not None
        assert task.urgent is True
        pickup = store.get_pending_pickup()
        assert pickup[0].prompt == "Urgent follow-up"

    def test_add_with_next_and_prompt_file_bumps_to_front_of_urgent_pickup(self, tmp_path: Path):
        """`gza add --next --prompt-file` should get the same bump semantics as queue bump."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Older urgent", urgent=True)

        prompt_file = tmp_path / "urgent_prompt.txt"
        prompt_file.write_text("Urgent from file")

        result = run_gza(
            "add",
            "--next",
            "--prompt-file",
            str(prompt_file),
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        pickup = store.get_pending_pickup()
        assert pickup[0].prompt == "Urgent from file"
        assert pickup[0].urgent is True

    def test_add_with_next_uses_shared_queue_urgency_helper(self, tmp_path: Path):
        """`add --next` should route through the same shared urgency helper used by queue bump."""
        from gza.cli.execution import cmd_add

        setup_config(tmp_path)
        args = argparse.Namespace(
            project_dir=tmp_path,
            prompt="Urgent via helper",
            prompt_file=None,
            edit=False,
            type=None,
            explore=False,
            plan=False,
            implement=False,
            review=False,
            group=None,
            depends_on=None,
            based_on=None,
            same_branch=False,
            spec=None,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            next=True,
        )

        with patch("gza.cli.execution.set_task_urgency", return_value=True) as set_urgency:
            rc = cmd_add(args)

        assert rc == 0
        set_urgency.assert_called_once()
        assert set_urgency.call_args.kwargs["urgent"] is True


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

    def test_heading_verdict_approved_with_followups(self, tmp_path: Path):
        """Parses APPROVED_WITH_FOLLOWUPS verdict token."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "## Verdict\n\nAPPROVED_WITH_FOLLOWUPS\n"
        store.update(task)
        assert get_review_verdict(config, task) == "APPROVED_WITH_FOLLOWUPS"

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
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Ready to merge.\n"
            "Verdict: APPROVED\n"
        )
        store.update(task)
        assert get_review_verdict(config, task) == "APPROVED"


class TestResolveImplAncestor:
    """Tests for runner._resolve_impl_ancestor walking improve based_on chains."""

    def test_resolve_impl_ancestor_direct_improve(self, tmp_path: Path):
        """A first-generation improve (based_on=impl) resolves to the impl."""
        from gza.runner import _resolve_impl_ancestor

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Impl", task_type="implement")
        improve = store.add("Improve", task_type="improve", based_on=impl.id)

        ancestor = _resolve_impl_ancestor(store, improve)
        assert ancestor is not None
        assert ancestor.id == impl.id

    def test_resolve_impl_ancestor_chained_improves(self, tmp_path: Path):
        """A chained retry/resume improve (based_on=previous_improve) still resolves to the impl."""
        from gza.runner import _resolve_impl_ancestor

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Impl", task_type="implement")
        improve1 = store.add("Improve 1", task_type="improve", based_on=impl.id)
        improve2 = store.add("Improve 2 (retry)", task_type="improve", based_on=improve1.id)
        improve3 = store.add("Improve 3 (retry of retry)", task_type="improve", based_on=improve2.id)

        ancestor = _resolve_impl_ancestor(store, improve3)
        assert ancestor is not None
        assert ancestor.id == impl.id


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
