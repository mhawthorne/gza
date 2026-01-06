"""Tests for the CLI commands."""

import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest


def run_theo(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Run theo command and return result."""
    return subprocess.run(
        ["uv", "run", "theo", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def setup_config(tmp_path: Path, project_name: str = "test-project") -> None:
    """Set up a minimal theo config file."""
    config_path = tmp_path / "theo.yaml"
    config_path.write_text(f"project_name: {project_name}\n")


def setup_db_with_tasks(tmp_path: Path, tasks: list[dict]) -> None:
    """Set up a SQLite database with the given tasks (also creates config)."""
    from theo.db import SqliteTaskStore

    # Ensure config exists
    setup_config(tmp_path)

    db_path = tmp_path / ".theo" / "theo.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path)

    for task_data in tasks:
        task = store.add(task_data["prompt"], task_type=task_data.get("task_type", "task"))
        task.status = task_data.get("status", "pending")
        if task.status in ("completed", "failed"):
            task.completed_at = datetime.now(timezone.utc)
        store.update(task)


class TestHistoryCommand:
    """Tests for 'theo history' command."""

    def test_history_with_tasks(self, tmp_path: Path):
        """History command works with SQLite tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Test task 1", "status": "completed"},
            {"prompt": "Test task 2", "status": "failed"},
            {"prompt": "Test task 3", "status": "pending"},
        ])

        result = run_theo("history", str(tmp_path))

        assert result.returncode == 0
        assert "Test task 1" in result.stdout
        assert "Test task 2" in result.stdout
        assert "Test task 3" not in result.stdout  # pending tasks not shown

    def test_history_with_no_tasks(self, tmp_path: Path):
        """History command handles missing database gracefully."""
        setup_config(tmp_path)
        result = run_theo("history", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout

    def test_history_with_empty_tasks(self, tmp_path: Path):
        """History command handles empty tasks list."""
        # Create empty database
        setup_db_with_tasks(tmp_path, [])

        result = run_theo("history", str(tmp_path))

        assert result.returncode == 0
        assert "No completed or failed tasks" in result.stdout


class TestNextCommand:
    """Tests for 'theo next' command."""

    def test_next_shows_pending_tasks(self, tmp_path: Path):
        """Next command shows pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "First pending task", "status": "pending"},
            {"prompt": "Second pending task", "status": "pending"},
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_theo("next", str(tmp_path))

        assert result.returncode == 0
        assert "First pending task" in result.stdout
        assert "Second pending task" in result.stdout
        assert "Completed task" not in result.stdout

    def test_next_with_no_pending_tasks(self, tmp_path: Path):
        """Next command handles no pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Completed task", "status": "completed"},
        ])

        result = run_theo("next", str(tmp_path))

        assert result.returncode == 0
        assert "No pending tasks" in result.stdout


class TestAddCommand:
    """Tests for 'theo add' command."""

    def test_add_with_inline_prompt(self, tmp_path: Path):
        """Add command with inline prompt creates a task."""
        setup_config(tmp_path)
        result = run_theo("add", "Test inline task", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added
        result = run_theo("next", str(tmp_path))
        assert "Test inline task" in result.stdout

    def test_add_explore_task(self, tmp_path: Path):
        """Add command with --explore flag creates explore task."""
        setup_config(tmp_path)
        result = run_theo("add", "--explore", "Explore the codebase", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task type is shown
        result = run_theo("next", str(tmp_path))
        assert "[explore]" in result.stdout


class TestShowCommand:
    """Tests for 'theo show' command."""

    def test_show_existing_task(self, tmp_path: Path):
        """Show command displays task details."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A detailed task prompt", "status": "pending"},
        ])

        result = run_theo("show", "1", str(tmp_path))

        assert result.returncode == 0
        assert "Task #1" in result.stdout
        assert "A detailed task prompt" in result.stdout
        assert "Status: pending" in result.stdout

    def test_show_nonexistent_task(self, tmp_path: Path):
        """Show command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_theo("show", "999", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout


class TestDeleteCommand:
    """Tests for 'theo delete' command."""

    def test_delete_with_force(self, tmp_path: Path):
        """Delete command with --force removes task without confirmation."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task to delete", "status": "pending"},
        ])

        result = run_theo("delete", "1", "--force", str(tmp_path))

        assert result.returncode == 0
        assert "Deleted task" in result.stdout

        # Verify task was deleted
        result = run_theo("next", str(tmp_path))
        assert "No pending tasks" in result.stdout

    def test_delete_nonexistent_task(self, tmp_path: Path):
        """Delete command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = run_theo("delete", "999", "--force", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout


class TestConfigRequirements:
    """Tests for theo.yaml configuration requirements."""

    def test_missing_config_file(self, tmp_path: Path):
        """Commands fail when theo.yaml is missing."""
        result = run_theo("next", str(tmp_path))

        assert result.returncode == 1
        assert "Configuration file not found" in result.stderr
        assert "theo init" in result.stderr

    def test_missing_project_name(self, tmp_path: Path):
        """Commands fail when project_name is missing from config."""
        config_path = tmp_path / "theo.yaml"
        config_path.write_text("timeout_minutes: 5\n")

        result = run_theo("next", str(tmp_path))

        assert result.returncode == 1
        assert "project_name" in result.stderr
        assert "required" in result.stderr

    def test_unknown_keys_warning(self, tmp_path: Path):
        """Unknown keys in config produce warnings but don't fail."""
        config_path = tmp_path / "theo.yaml"
        config_path.write_text("project_name: test\nunknown_key: value\n")

        result = run_theo("next", str(tmp_path))

        # Should succeed
        assert result.returncode == 0
        # Warning should be printed to stderr
        assert "unknown_key" in result.stderr
        assert "Warning" in result.stderr or "warning" in result.stderr.lower()


class TestValidateCommand:
    """Tests for 'theo validate' command."""

    def test_validate_valid_config(self, tmp_path: Path):
        """Validate command succeeds with valid config."""
        setup_config(tmp_path)
        result = run_theo("validate", str(tmp_path))

        assert result.returncode == 0
        assert "valid" in result.stdout.lower()

    def test_validate_missing_config(self, tmp_path: Path):
        """Validate command fails with missing config."""
        result = run_theo("validate", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_validate_missing_project_name(self, tmp_path: Path):
        """Validate command fails when project_name is missing."""
        config_path = tmp_path / "theo.yaml"
        config_path.write_text("timeout_minutes: 5\n")

        result = run_theo("validate", str(tmp_path))

        assert result.returncode == 1
        assert "project_name" in result.stdout
        assert "required" in result.stdout

    def test_validate_unknown_keys_warning(self, tmp_path: Path):
        """Validate command shows warnings for unknown keys."""
        config_path = tmp_path / "theo.yaml"
        config_path.write_text("project_name: test\nunknown_field: value\n")

        result = run_theo("validate", str(tmp_path))

        assert result.returncode == 0  # Unknown keys don't fail validation
        assert "unknown_field" in result.stdout
        assert "Warning" in result.stdout


class TestInitCommand:
    """Tests for 'theo init' command."""

    def test_init_creates_config(self, tmp_path: Path):
        """Init command creates config in project root."""
        result = run_theo("init", str(tmp_path))

        assert result.returncode == 0
        config_path = tmp_path / "theo.yaml"
        assert config_path.exists()

        # Verify project_name is set (derived from directory name)
        content = config_path.read_text()
        assert "project_name:" in content
        assert tmp_path.name in content

    def test_init_does_not_overwrite(self, tmp_path: Path):
        """Init command does not overwrite existing config without --force."""
        setup_config(tmp_path, project_name="original")

        result = run_theo("init", str(tmp_path))

        assert result.returncode == 1
        assert "already exists" in result.stdout

        # Verify original content is preserved
        config_path = tmp_path / "theo.yaml"
        assert "original" in config_path.read_text()

    def test_init_force_overwrites(self, tmp_path: Path):
        """Init command overwrites existing config with --force."""
        setup_config(tmp_path, project_name="original")

        result = run_theo("init", "--force", str(tmp_path))

        assert result.returncode == 0

        # Verify config was overwritten (has directory name, not "original")
        config_path = tmp_path / "theo.yaml"
        content = config_path.read_text()
        assert tmp_path.name in content


class TestImportCommand:
    """Tests for 'theo import' command."""

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

        result = run_theo("import", str(tmp_path))

        assert result.returncode == 0
        assert "Imported 2 tasks" in result.stdout

        # Verify tasks were imported
        result = run_theo("next", str(tmp_path))
        assert "Task from YAML" in result.stdout

    def test_import_no_yaml(self, tmp_path: Path):
        """Import command handles missing tasks.yaml."""
        setup_config(tmp_path)
        result = run_theo("import", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout


class TestLogCommand:
    """Tests for 'theo log' command."""

    def test_log_single_json_format_success(self, tmp_path: Path):
        """Log command parses single JSON format with successful result."""
        import json
        from theo.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".theo" / "theo.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for log")
        task.status = "completed"
        task.log_file = ".theo/logs/test.log"
        store.update(task)

        # Create a single JSON log file (old format)
        log_dir = tmp_path / ".theo" / "logs"
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

        result = run_theo("log", "1", str(tmp_path))

        assert result.returncode == 0
        assert "Task completed successfully!" in result.stdout
        assert "Duration:" in result.stdout
        assert "Turns: 10" in result.stdout
        assert "Cost: $0.5000" in result.stdout

    def test_log_jsonl_format_success(self, tmp_path: Path):
        """Log command parses JSONL format with successful result."""
        import json
        from theo.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".theo" / "theo.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task for JSONL log")
        task.status = "completed"
        task.log_file = ".theo/logs/test.log"
        store.update(task)

        # Create a JSONL log file (new format)
        log_dir = tmp_path / ".theo" / "logs"
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

        result = run_theo("log", "1", str(tmp_path))

        assert result.returncode == 0
        assert "This was parsed from JSONL!" in result.stdout
        assert "Duration:" in result.stdout
        assert "Turns: 5" in result.stdout
        assert "Cost: $0.2500" in result.stdout

    def test_log_jsonl_format_error_max_turns(self, tmp_path: Path):
        """Log command handles JSONL format with error_max_turns result."""
        import json
        from theo.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".theo" / "theo.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task that hit max turns")
        task.status = "completed"
        task.log_file = ".theo/logs/test.log"
        store.update(task)

        # Create a JSONL log file with error_max_turns (no result field)
        log_dir = tmp_path / ".theo" / "logs"
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

        result = run_theo("log", "1", str(tmp_path))

        assert result.returncode == 0
        assert "error_max_turns" in result.stdout
        assert "Turns: 60" in result.stdout
        assert "Cost: $1.5000" in result.stdout

    def test_log_missing_log_file(self, tmp_path: Path):
        """Log command handles missing log file."""
        from theo.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file path that doesn't exist
        db_path = tmp_path / ".theo" / "theo.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with missing log")
        task.status = "completed"
        task.log_file = ".theo/logs/nonexistent.log"
        store.update(task)

        result = run_theo("log", "1", str(tmp_path))

        assert result.returncode == 1
        assert "Log file not found" in result.stdout

    def test_log_no_result_entry(self, tmp_path: Path):
        """Log command handles JSONL with no result entry."""
        import json
        from theo.db import SqliteTaskStore

        setup_config(tmp_path)

        # Create a task with a log file
        db_path = tmp_path / ".theo" / "theo.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add("Test task with incomplete log")
        task.status = "completed"
        task.log_file = ".theo/logs/test.log"
        store.update(task)

        # Create a JSONL log file with no result entry
        log_dir = tmp_path / ".theo" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        lines = [
            {"type": "system", "subtype": "init", "session_id": "abc123"},
            {"type": "assistant", "message": {"role": "assistant", "content": "Working..."}},
        ]
        log_file.write_text("\n".join(json.dumps(line) for line in lines))

        result = run_theo("log", "1", str(tmp_path))

        assert result.returncode == 1
        assert "No result entry found" in result.stdout

    def test_log_task_not_found(self, tmp_path: Path):
        """Log command handles nonexistent task."""
        setup_config(tmp_path)

        # Create empty database
        db_path = tmp_path / ".theo" / "theo.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from theo.db import SqliteTaskStore
        SqliteTaskStore(db_path)

        result = run_theo("log", "999", str(tmp_path))

        assert result.returncode == 1
        assert "No task found" in result.stdout
