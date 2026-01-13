"""Tests for the CLI commands."""

import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest


def run_gza(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Run gza command and return result."""
    return subprocess.run(
        ["uv", "run", "gza", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
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

        result = run_gza("history", str(tmp_path))

        assert result.returncode == 0
        assert "Test task 1" in result.stdout
        assert "Test task 2" in result.stdout
        assert "Test task 3" not in result.stdout  # pending tasks not shown

    def test_history_with_no_tasks(self, tmp_path: Path):
        """History command handles missing database gracefully."""
        setup_config(tmp_path)
        result = run_gza("history", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout

    def test_history_with_empty_tasks(self, tmp_path: Path):
        """History command handles empty tasks list."""
        # Create empty database
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("history", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout


class TestNextCommand:
    """Tests for 'gza next' command."""

    def test_next_shows_pending_tasks(self, tmp_path: Path):
        """Next command shows pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "First pending task", "status": "pending"},
            {"prompt": "Second pending task", "status": "pending"},
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("next", str(tmp_path))

        assert result.returncode == 0
        assert "First pending task" in result.stdout
        assert "Second pending task" in result.stdout
        assert "Completed task" not in result.stdout

    def test_next_with_no_pending_tasks(self, tmp_path: Path):
        """Next command handles no pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_gza("next", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" in result.stdout


class TestAddCommand:
    """Tests for 'gza add' command."""

    def test_add_with_inline_prompt(self, tmp_path: Path):
        """Add command with inline prompt creates a task."""
        setup_config(tmp_path)
        result = run_gza("add", "Test inline task", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added
        result = run_gza("next", str(tmp_path))
        assert "Test inline task" in result.stdout

    def test_add_explore_task(self, tmp_path: Path):
        """Add command with --explore flag creates explore task."""
        setup_config(tmp_path)
        result = run_gza("add", "--explore", "Explore the codebase", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task type is shown
        result = run_gza("next", str(tmp_path))
        assert "[explore]" in result.stdout


class TestShowCommand:
    """Tests for 'gza show' command."""

    def test_show_existing_task(self, tmp_path: Path):
        """Show command displays task details."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A detailed task prompt", "status": "pending"},
        ])

        result = run_gza("show", "1", str(tmp_path))

        assert result.returncode == 0
        assert "Task #1" in result.stdout
        assert "A detailed task prompt" in result.stdout
        assert "Status: pending" in result.stdout

    def test_show_nonexistent_task(self, tmp_path: Path):
        """Show command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("show", "999", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout


class TestDeleteCommand:
    """Tests for 'gza delete' command."""

    def test_delete_with_force(self, tmp_path: Path):
        """Delete command with --force removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_gza("delete", "1", "--force", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_gza("next", str(tmp_path))
        assert "No pending tasks" in result.stdout

    def test_delete_nonexistent_task(self, tmp_path: Path):
        """Delete command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("delete", "999", "--force", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout


class TestRetryCommand:
    """Tests for 'gza retry' command."""

    def test_retry_completed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a completed task."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Original task", "status": "completed", "task_type": "implement"},
        ])

        result = run_gza("retry", "1", str(tmp_path))

        assert result.returncode == 0
        assert "Created task #2" in result.stdout
        assert "retry of #1" in result.stdout

        # Verify new task was created with same prompt
        result = run_gza("next", str(tmp_path))
        assert "Original task" in result.stdout

    def test_retry_failed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a failed task."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Failed task", "status": "failed"},
        ])

        result = run_gza("retry", "1", str(tmp_path))

        assert result.returncode == 0
        assert "Created task #2" in result.stdout
        assert "retry of #1" in result.stdout

    def test_retry_pending_task_fails(self, tmp_path: Path):
        """Retry command fails for pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("retry", "1", str(tmp_path))

        assert result.returncode == 1
        assert "Can only retry completed or failed" in result.stdout

    def test_retry_nonexistent_task(self, tmp_path: Path):
        """Retry command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_gza("retry", "999", str(tmp_path))

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
        result = run_gza("retry", "1", str(tmp_path))

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


class TestConfigRequirements:
    """Tests for gza.yaml configuration requirements."""

    def test_missing_config_file(self, tmp_path: Path):
        """Commands fail when gza.yaml is missing."""
        result = run_gza("next", str(tmp_path))

        assert result.returncode == 1
        assert "Configuration file not found" in result.stderr
        assert "gza init" in result.stderr

    def test_missing_project_name(self, tmp_path: Path):
        """Commands fail when project_name is missing from config."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("timeout_minutes: 5\n")

        result = run_gza("next", str(tmp_path))

        assert result.returncode == 1
        assert "project_name" in result.stderr
        assert "required" in result.stderr

    def test_unknown_keys_warning(self, tmp_path: Path):
        """Unknown keys in config produce warnings but don't fail."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nunknown_key: value\n")

        result = run_gza("next", str(tmp_path))

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
        result = run_gza("validate", str(tmp_path))

        assert result.returncode == 0
        assert "valid" in result.stdout.lower()

    def test_validate_missing_config(self, tmp_path: Path):
        """Validate command fails with missing config."""
        result = run_gza("validate", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_validate_missing_project_name(self, tmp_path: Path):
        """Validate command fails when project_name is missing."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("timeout_minutes: 5\n")

        result = run_gza("validate", str(tmp_path))

        assert result.returncode == 1
        assert "project_name" in result.stdout
        assert "required" in result.stdout

    def test_validate_unknown_keys_warning(self, tmp_path: Path):
        """Validate command shows warnings for unknown keys."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nunknown_field: value\n")

        result = run_gza("validate", str(tmp_path))

        assert result.returncode == 0  # Unknown keys don't fail validation
        assert "unknown_field" in result.stdout
        assert "Warning" in result.stdout


class TestInitCommand:
    """Tests for 'gza init' command."""

    def test_init_creates_config(self, tmp_path: Path):
        """Init command creates config in project root."""
        result = run_gza("init", str(tmp_path))

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

        result = run_gza("init", str(tmp_path))

        assert result.returncode == 1
        assert "already exists" in result.stdout

        # Verify original content is preserved
        config_path = tmp_path / "gza.yaml"
        assert "original" in config_path.read_text()

    def test_init_force_overwrites(self, tmp_path: Path):
        """Init command overwrites existing config with --force."""
        setup_config(tmp_path, project_name="original")

        result = run_gza("init", "--force", str(tmp_path))

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

        result = run_gza("import", str(tmp_path))

        assert result.returncode == 0
        assert "Imported 2 tasks" in result.stdout

        # Verify tasks were imported
        result = run_gza("next", str(tmp_path))
        assert "Task from YAML" in result.stdout

    def test_import_no_yaml(self, tmp_path: Path):
        """Import command handles missing tasks.yaml."""
        setup_config(tmp_path)
        result = run_gza("import", str(tmp_path))

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

        result = run_gza("log", "--task", "1", str(tmp_path))

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

        result = run_gza("log", "--task", "1", str(tmp_path))

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

        result = run_gza("log", "--task", "1", str(tmp_path))

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

        result = run_gza("log", "--task", "1", str(tmp_path))

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

        result = run_gza("log", "--task", "1", str(tmp_path))

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

        result = run_gza("log", "--task", "999", str(tmp_path))

        assert result.returncode == 1
        assert "Task 999 not found" in result.stdout

    def test_log_by_task_id_invalid_id(self, tmp_path: Path):
        """Log command with --task rejects non-numeric ID."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "--task", "not-a-number", str(tmp_path))

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

        result = run_gza("log", "--slug", "20260108-test-slug", str(tmp_path))

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

        result = run_gza("log", "--slug", "partial-slug", str(tmp_path))

        assert result.returncode == 0
        assert "Partial match works!" in result.stdout

    def test_log_by_slug_not_found(self, tmp_path: Path):
        """Log command with --slug handles nonexistent slug."""
        setup_config(tmp_path)

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_gza("log", "--slug", "nonexistent-slug", str(tmp_path))

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

        result = run_gza("log", "--worker", worker_id, str(tmp_path))

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

        result = run_gza("log", "--worker", "w-nonexistent", str(tmp_path))

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

        result = run_gza("log", "--task", "1", str(tmp_path))

        # Should detect startup failure and display the error
        assert result.returncode == 1
        assert "Task failed during startup (no Claude session):" in result.stdout
        assert "exec /usr/local/bin/docker-entrypoint.sh: argument list too long" in result.stdout
        # The error should be indented
        assert "  exec /usr/local/bin/docker-entrypoint.sh" in result.stdout

    def test_log_requires_lookup_type(self, tmp_path: Path):
        """Log command requires --task, --slug, or --worker flag."""
        setup_config(tmp_path)

        result = run_gza("log", "123", str(tmp_path))

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

        result = run_gza("pr", "999", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_pr_task_not_completed(self, tmp_path: Path):
        """PR command rejects pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("pr", "1", str(tmp_path))

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

        result = run_gza("pr", "1", str(tmp_path))

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

        result = run_gza("pr", "1", str(tmp_path))

        assert result.returncode == 1
        assert "no commits" in result.stdout


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

        result = run_gza("groups", str(tmp_path))

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

        result = run_gza("groups", str(tmp_path))

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

        result = run_gza("status", "test-group", str(tmp_path))

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

        result = run_gza("edit", str(task.id), "--group", "new-group", str(tmp_path))

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

        result = run_gza("edit", str(task.id), "--group", "", str(tmp_path))

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

        result = run_gza("edit", str(task.id), "--review", str(tmp_path))

        assert result.returncode == 0

        # Verify create_review was enabled
        updated = store.get(task.id)
        assert updated.create_review is True


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

        result = run_gza("next", str(tmp_path))

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

        result = run_gza("next", "--all", str(tmp_path))

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

        result = run_gza("next", str(tmp_path))

        assert result.returncode == 0
        # Should mention 2 blocked tasks
        assert "2" in result.stdout and "blocked" in result.stdout.lower()


class TestAddCommandWithChaining:
    """Tests for 'gza add' command with chaining features."""

    def test_add_with_type_plan(self, tmp_path: Path):
        """Add command can create plan tasks."""
        setup_config(tmp_path)
        result = run_gza("add", "--type", "plan", "Create a plan", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

    def test_add_with_type_implement(self, tmp_path: Path):
        """Add command can create implement tasks."""
        setup_config(tmp_path)
        result = run_gza("add", "--type", "implement", "Implement feature", str(tmp_path))

        assert result.returncode == 0

    def test_add_with_type_review(self, tmp_path: Path):
        """Add command can create review tasks."""
        setup_config(tmp_path)
        result = run_gza("add", "--type", "review", "Review implementation", str(tmp_path))

        assert result.returncode == 0

    def test_add_with_based_on(self, tmp_path: Path):
        """Add command can create tasks with based_on reference."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)

        task1 = store.add("First task")

        result = run_gza("add", "--based-on", str(task1.id), "Follow-up task", str(tmp_path))

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

        result = run_gza("add", "--spec", "specs/feature.md", "Implement feature", str(tmp_path))

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

        result = run_gza("add", "--spec", "nonexistent.md", "Implement feature", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Spec file not found: nonexistent.md" in result.stdout


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
