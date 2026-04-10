"""Tests for configuration, setup, and admin CLI commands."""


import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from .conftest import make_store, run_gza, setup_config


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


class TestProjectPrefixValidation:
    """Tests for project_prefix config field validation."""

    def test_project_prefix_valid_accepted(self, tmp_path: Path):
        """Valid project_prefix is accepted without error."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: myproject\nproject_prefix: myproj\n")
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 0

    def test_project_prefix_defaults_to_project_name(self, tmp_path: Path):
        """When project_prefix is absent, it defaults to project_name."""
        from gza.config import Config

        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: myproject\n")
        config = Config.load(tmp_path)
        assert config.project_prefix == "myproject"

    def test_project_prefix_too_long_rejected(self, tmp_path: Path):
        """project_prefix longer than 12 characters raises a config error."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: myproject\nproject_prefix: toolongprefix\n")
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "project_prefix" in result.stdout

    def test_project_prefix_invalid_chars_rejected(self, tmp_path: Path):
        """project_prefix with uppercase letters raises a config error."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: myproject\nproject_prefix: MyProj\n")
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "project_prefix" in result.stdout

    def test_project_prefix_hyphen_start_rejected(self, tmp_path: Path):
        """project_prefix starting with a hyphen raises a config error."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: myproject\nproject_prefix: -myproj\n")
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "project_prefix" in result.stdout

    def test_project_prefix_non_string_rejected(self, tmp_path: Path):
        """project_prefix that is not a string raises a config error."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: myproject\nproject_prefix: 123\n")
        result = run_gza("validate", "--project", str(tmp_path))
        # YAML parses 123 as an integer, triggering type validation
        assert result.returncode == 1
        assert "project_prefix" in result.stdout

    def test_project_prefix_trailing_hyphen_rejected(self, tmp_path: Path):
        """project_prefix with a trailing hyphen is rejected (M3)."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: myproject\nproject_prefix: myproj-\n")
        result = run_gza("validate", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "project_prefix" in result.stdout

    def test_project_prefix_default_sanitized_from_invalid_project_name(self, tmp_path: Path):
        """When project_name is not a valid prefix, defaulted project_prefix is sanitized (M2)."""
        from gza.config import Config

        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: MyLargeProjectName\n")
        config = Config.load(tmp_path)
        # Sanitized prefix must be lowercase, alphanumeric+hyphens, no leading/trailing hyphens,
        # max 12 chars, and non-empty
        prefix = config.project_prefix
        assert prefix, "project_prefix must not be empty after sanitization"
        assert prefix == prefix.lower(), f"project_prefix must be lowercase, got: {prefix!r}"
        assert len(prefix) <= 12, f"project_prefix must be at most 12 chars, got: {prefix!r}"
        assert not prefix.startswith("-"), f"project_prefix must not start with hyphen, got: {prefix!r}"
        assert not prefix.endswith("-"), f"project_prefix must not end with hyphen, got: {prefix!r}"
        import re as _re
        assert _re.match(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?$', prefix), (
            f"project_prefix has invalid characters: {prefix!r}"
        )


class TestConfigEnvVars:
    """Tests for environment variable overrides in config."""

    def test_docker_volumes_tilde_expansion(self, tmp_path: Path):
        """Docker volumes should expand tilde in source paths."""
        from pathlib import Path as PathLib

        from gza.config import Config

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

    def test_local_override_applies_to_loaded_config(self, tmp_path: Path):
        """Local overrides should be reflected in the loaded config."""
        (tmp_path / "gza.yaml").write_text("project_name: test\nuse_docker: true\n")
        (tmp_path / "gza.local.yaml").write_text("use_docker: false\n")

        from gza.config import Config

        cfg = Config.load(tmp_path)
        assert cfg.use_docker is False

    def test_config_command_shows_effective_values_with_sources(self, tmp_path: Path):
        """gza config --json should include effective values and source attribution."""

        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "timeout_minutes: 10\n"
            "use_docker: true\n"
        )
        (tmp_path / "gza.local.yaml").write_text(
            "use_docker: false\n"
        )

        result = subprocess.run(
            ["uv", "run", "gza", "config", "--json", "--project", str(tmp_path)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["effective"]["timeout_minutes"] == 10
        assert payload["effective"]["use_docker"] is False
        assert payload["sources"]["timeout_minutes"] == "base"
        assert payload["sources"]["use_docker"] == "local"
        assert payload["local_overrides_active"] is True
        assert payload["local_override_file"] == "gza.local.yaml"

    def test_config_command_projects_source_for_branch_strategy_preset(self, tmp_path: Path):
        """gza config should attribute normalized branch_strategy fields to configured source."""

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


class TestCleanCommand:
    """Tests for 'gza clean' command (default mode)."""

    def test_clean_dry_run(self, tmp_path: Path):
        """Clean command dry run works."""
        from gza.config import Config
        from gza.git import Git
        from gza.workers import WorkerRegistry

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
            "started_at": datetime.now(UTC).isoformat(),
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
        os.utime(old_log, (old_time, old_time))

        result = run_gza("clean", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Dry run" in result.stdout
        # The old log should still exist after dry run
        assert old_log.exists()

    def test_clean_logs_only(self, tmp_path: Path):
        """Clean command with --logs flag works."""
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
        old_time = time.time() - (60 * 24 * 60 * 60)
        os.utime(old_log, (old_time, old_time))

        result = run_gza("clean", "--logs", "--days", "30", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Logs cleaned" in result.stdout
        assert not old_log.exists()
        assert new_log.exists()

    def test_clean_workers(self, tmp_path: Path):
        """Clean command cleans stale worker metadata and startup logs."""
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

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
            started_at=datetime.now(UTC).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
            startup_log_file=f".gza/workers/{worker_id}-startup.log",
            is_background=True,
        )
        registry.register(worker_meta)
        startup_log_file = config.workers_path / f"{worker_id}-startup.log"
        startup_log_file.write_text("startup output")

        # Verify worker file exists
        worker_file = config.workers_path / f"{worker_id}.json"
        assert worker_file.exists()
        assert startup_log_file.exists()

        result = run_gza("clean", "--workers", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "worker files cleaned" in result.stdout.lower()
        # Worker metadata should be cleaned up
        assert not worker_file.exists()
        assert not startup_log_file.exists()

    def test_clean_keep_unmerged_logs(self, tmp_path: Path):
        """Clean command with --keep-unmerged keeps logs for unmerged tasks."""
        import time

        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.git import Git

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
        unmerged_task.slug = "20200101-unmerged"
        unmerged_task.branch = "feature/unmerged"
        unmerged_task.has_commits = True
        unmerged_task.completed_at = datetime.now(UTC)
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

        result = run_gza("clean", "--logs", "--days", "30", "--keep-unmerged", "--project", str(tmp_path))

        assert result.returncode == 0
        # Unmerged task log should be kept
        assert unmerged_log.exists()
        # Merged task log should be removed
        assert not merged_log.exists()

    def test_clean_lineage_aware_preserves_recent(self, tmp_path: Path):
        """Worktrees with recent lineage activity are preserved."""
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.git import Git

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (tmp_path / "README.md").write_text("# Test")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        wt_base = tmp_path / "worktrees"
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(f"project_name: test-project\nworktree_dir: {wt_base}\n")
        config = Config.load(tmp_path)

        # Create a task with recent activity
        store = SqliteTaskStore(config.db_path)
        task = store.add("Recent feature", task_type="implement")
        task.slug = "20260301-recent-feature"
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Create a worktree directory tracked by git
        worktree_dir = config.worktree_path
        worktree_dir.mkdir(parents=True, exist_ok=True)
        wt_path = worktree_dir / "20260301-recent-feature"
        git._run("worktree", "add", str(wt_path), "-b", "wt-recent")

        result = run_gza("clean", "--worktrees", "--force", "--days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        # Worktree should be preserved — lineage is recent
        assert wt_path.exists()

    def test_clean_lineage_aware_removes_old(self, tmp_path: Path):
        """Worktrees with old lineage activity are removed."""
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.git import Git

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (tmp_path / "README.md").write_text("# Test")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        wt_base = tmp_path / "worktrees"
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(f"project_name: test-project\nworktree_dir: {wt_base}\n")
        config = Config.load(tmp_path)

        # Create a task with old activity
        store = SqliteTaskStore(config.db_path)
        task = store.add("Old feature", task_type="implement")
        task.slug = "20250101-old-feature"
        task.status = "completed"
        task.completed_at = datetime(2025, 1, 1, tzinfo=UTC)
        store.update(task)

        # Create a worktree directory tracked by git
        worktree_dir = config.worktree_path
        worktree_dir.mkdir(parents=True, exist_ok=True)
        wt_path = worktree_dir / "20250101-old-feature"
        git._run("worktree", "add", str(wt_path), "-b", "wt-old")

        result = run_gza("clean", "--worktrees", "--force", "--days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        # Worktree should be removed — lineage is old
        assert not wt_path.exists()
        assert "lineage inactive" in result.stdout

    def test_clean_force_skips_prompt(self, tmp_path: Path):
        """--force flag skips the confirmation prompt."""
        from gza.config import Config
        from gza.git import Git

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (tmp_path / "README.md").write_text("# Test")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        wt_base = tmp_path / "worktrees"
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(f"project_name: test-project\nworktree_dir: {wt_base}\n")
        config = Config.load(tmp_path)

        # Create an orphaned worktree directory (not in git worktree list)
        worktree_dir = config.worktree_path
        worktree_dir.mkdir(parents=True, exist_ok=True)
        orphan = worktree_dir / "orphaned-dir"
        orphan.mkdir()
        (orphan / "dummy.txt").write_text("dummy")

        # With --force, no stdin needed — should succeed without hanging
        result = run_gza("clean", "--worktrees", "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert not orphan.exists()
        assert "orphaned" in result.stdout

    def test_clean_no_force_denies_removal(self, tmp_path: Path):
        """Without --force, answering 'n' skips worktree removal."""
        from gza.config import Config
        from gza.git import Git

        # Initialize git repo
        git = Git(tmp_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (tmp_path / "README.md").write_text("# Test")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")

        wt_base = tmp_path / "worktrees"
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(f"project_name: test-project\nworktree_dir: {wt_base}\n")
        config = Config.load(tmp_path)

        # Create an orphaned worktree directory
        worktree_dir = config.worktree_path
        worktree_dir.mkdir(parents=True, exist_ok=True)
        orphan = worktree_dir / "orphaned-dir"
        orphan.mkdir()
        (orphan / "dummy.txt").write_text("dummy")

        # Provide 'n' via stdin
        result = run_gza("clean", "--worktrees", "--project", str(tmp_path), stdin_input="n\n")

        assert result.returncode == 0
        # Orphan should still exist — user said no
        assert orphan.exists()
        assert "Skipped worktree removal" in result.stdout

    def test_clean_uses_config_cleanup_days(self, tmp_path: Path):
        """Clean uses cleanup_days from config when --days not specified."""
        from gza.config import Config

        # Create config with custom cleanup_days
        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test-project\ncleanup_days: 7\n")

        config = Config.load(tmp_path)
        assert config.cleanup_days == 7


class TestCleanArchiveCommand:
    """Tests for 'gza clean --archive' command."""

    def test_clean_archive_default_behavior(self, tmp_path: Path):
        """Clean --archive archives files older than 30 days by default."""
        from datetime import datetime

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
        old_time = (datetime.now(UTC) - timedelta(days=35)).timestamp()
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

        recent_time = (datetime.now(UTC) - timedelta(days=10)).timestamp()
        recent_log.touch()
        recent_worker.touch()
        os.utime(recent_log, (recent_time, recent_time))
        os.utime(recent_worker, (recent_time, recent_time))

        # Run clean --archive command
        result = run_gza("clean", "--archive", "--project", str(tmp_path))

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
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create file 8 days old
        log_file = logs_dir / "log.txt"
        log_file.write_text("content")

        old_time = (datetime.now(UTC) - timedelta(days=8)).timestamp()
        os.utime(log_file, (old_time, old_time))

        # Run with --archive --days 7 (should archive 8-day-old file)
        result = run_gza("clean", "--archive", "--days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Archived files older than 7 days" in result.stdout
        assert not log_file.exists()
        # Verify file was archived
        archives_dir = tmp_path / ".gza" / "archives"
        assert (archives_dir / "logs" / "log.txt").exists()

    def test_clean_dry_run_mode(self, tmp_path: Path):
        """Clean command with --dry-run shows what would be archived without archiving."""
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old file
        old_log = logs_dir / "old_log.txt"
        old_log.write_text("old content")

        old_time = (datetime.now(UTC) - timedelta(days=40)).timestamp()
        os.utime(old_log, (old_time, old_time))

        # Run with --archive --dry-run
        result = run_gza("clean", "--archive", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Dry run: would archive files older than 30 days" in result.stdout
        assert "old_log.txt" in result.stdout

        # Verify file was NOT archived
        assert old_log.exists()
        archives_dir = tmp_path / ".gza" / "archives"
        assert not (archives_dir / "logs" / "old_log.txt").exists()

    def test_clean_archive_empty_directories(self, tmp_path: Path):
        """Clean --archive handles empty directories without errors."""
        setup_config(tmp_path)

        # Create empty directories
        logs_dir = tmp_path / ".gza" / "logs"
        workers_dir = tmp_path / ".gza" / "workers"
        logs_dir.mkdir(parents=True, exist_ok=True)
        workers_dir.mkdir(parents=True, exist_ok=True)

        result = run_gza("clean", "--archive", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Logs: 0 files" in result.stdout
        assert "Workers: 0 files" in result.stdout

    def test_clean_archive_nonexistent_directories(self, tmp_path: Path):
        """Clean --archive handles nonexistent directories without errors."""
        setup_config(tmp_path)

        # Don't create .gza directories
        result = run_gza("clean", "--archive", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Logs: 0 files" in result.stdout
        assert "Workers: 0 files" in result.stdout

    def test_clean_mixed_old_and_new_files(self, tmp_path: Path):
        """Clean command correctly handles mixed old and new files."""
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create multiple old and new files
        for i in range(3):
            old_file = logs_dir / f"old_{i}.txt"
            old_file.write_text(f"old content {i}")
            old_time = (datetime.now(UTC) - timedelta(days=35 + i)).timestamp()
            os.utime(old_file, (old_time, old_time))

            new_file = logs_dir / f"new_{i}.txt"
            new_file.write_text(f"new content {i}")
            new_time = (datetime.now(UTC) - timedelta(days=5 + i)).timestamp()
            os.utime(new_file, (new_time, new_time))

        result = run_gza("clean", "--archive", "--project", str(tmp_path))

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
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create an old subdirectory
        old_subdir = logs_dir / "old_subdir"
        old_subdir.mkdir()

        # Set directory mtime to old
        old_time = (datetime.now(UTC) - timedelta(days=40)).timestamp()
        os.utime(old_subdir, (old_time, old_time))

        result = run_gza("clean", "--archive", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify subdirectory was NOT archived
        assert old_subdir.exists()

    def test_clean_second_run_is_noop(self, tmp_path: Path):
        """Second run of clean should be a no-op (only checks source dirs)."""
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        logs_dir = tmp_path / ".gza" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old file
        old_log = logs_dir / "old_log.txt"
        old_log.write_text("old content")
        old_time = (datetime.now(UTC) - timedelta(days=40)).timestamp()
        os.utime(old_log, (old_time, old_time))

        # First run - archives the file
        result1 = run_gza("clean", "--archive", "--project", str(tmp_path))
        assert result1.returncode == 0
        assert "Logs: 1 files" in result1.stdout

        # Second run - should find nothing to archive
        result2 = run_gza("clean", "--archive", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "Logs: 0 files" in result2.stdout

    def test_clean_purge_mode(self, tmp_path: Path):
        """Clean with --purge deletes archived files older than N days."""
        from datetime import datetime, timedelta

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

        very_old_time = (datetime.now(UTC) - timedelta(days=400)).timestamp()
        os.utime(old_archived_log, (very_old_time, very_old_time))
        os.utime(old_archived_worker, (very_old_time, very_old_time))

        # Create recent archived files (100 days old)
        recent_archived_log = archives_logs_dir / "recent_archived.txt"
        recent_archived_log.write_text("recent archived content")
        recent_time = (datetime.now(UTC) - timedelta(days=100)).timestamp()
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
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        # Create archives directory
        archives_logs_dir = tmp_path / ".gza" / "archives" / "logs"
        archives_logs_dir.mkdir(parents=True, exist_ok=True)

        # Create archived file 200 days old
        archived_log = archives_logs_dir / "archived.txt"
        archived_log.write_text("archived content")
        old_time = (datetime.now(UTC) - timedelta(days=200)).timestamp()
        os.utime(archived_log, (old_time, old_time))

        # Run purge with --days 180 (should delete 200-day-old file)
        result = run_gza("clean", "--purge", "--days", "180", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Purged archived files older than 180 days" in result.stdout
        assert not archived_log.exists()

    def test_clean_purge_dry_run(self, tmp_path: Path):
        """Clean --purge --dry-run shows what would be deleted without deleting."""
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        # Create archives directory
        archives_logs_dir = tmp_path / ".gza" / "archives" / "logs"
        archives_logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old archived file
        old_archived = archives_logs_dir / "old_archived.txt"
        old_archived.write_text("old archived content")
        old_time = (datetime.now(UTC) - timedelta(days=400)).timestamp()
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
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        # Create archives directory
        archives_logs_dir = tmp_path / ".gza" / "archives" / "logs"
        archives_logs_dir.mkdir(parents=True, exist_ok=True)

        # Create old archived file
        old_archived = archives_logs_dir / "old_archived.txt"
        old_archived.write_text("old archived content")
        old_time = (datetime.now(UTC) - timedelta(days=400)).timestamp()
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
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        backups_dir = tmp_path / ".gza" / "backups"
        backups_dir.mkdir(parents=True, exist_ok=True)

        # Create an old backup file (35 days old)
        old_backup = backups_dir / "gza-2026011400.db"
        old_backup.write_bytes(b"old backup data")
        old_time = (datetime.now(UTC) - timedelta(days=35)).timestamp()
        os.utime(old_backup, (old_time, old_time))

        # Create a recent backup file (1 day old)
        recent_backup = backups_dir / "gza-2026021900.db"
        recent_backup.write_bytes(b"recent backup data")
        recent_time = (datetime.now(UTC) - timedelta(days=1)).timestamp()
        os.utime(recent_backup, (recent_time, recent_time))

        result = run_gza("clean", "--archive", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Backups deleted: 1 files" in result.stdout

        # Old backup should be deleted
        assert not old_backup.exists()
        # Recent backup should be kept
        assert recent_backup.exists()

    def test_clean_dry_run_shows_backups(self, tmp_path: Path):
        """Clean --dry-run shows old backup files that would be deleted."""
        from datetime import datetime, timedelta

        setup_config(tmp_path)

        backups_dir = tmp_path / ".gza" / "backups"
        backups_dir.mkdir(parents=True, exist_ok=True)

        old_backup = backups_dir / "gza-2026010100.db"
        old_backup.write_bytes(b"old backup")
        old_time = (datetime.now(UTC) - timedelta(days=40)).timestamp()
        os.utime(old_backup, (old_time, old_time))

        result = run_gza("clean", "--archive", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "gza-2026010100.db" in result.stdout
        # File should NOT have been deleted (dry run)
        assert old_backup.exists()


class TestStatsReviewsCommand:
    """Tests for 'gza stats reviews' command."""

    def test_stats_reviews_no_tasks(self, tmp_path: Path):
        """gza stats reviews with no tasks shows zero counts."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        result = run_gza("stats", "reviews", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement tasks: 0" in result.stdout
        assert "Total reviews:   0" in result.stdout

    def test_stats_reviews_shows_table_header(self, tmp_path: Path):
        """gza stats reviews output includes the weekly table header."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        result = run_gza("stats", "reviews", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Week" in result.stdout
        assert "Impls" in result.stdout
        assert "Rvws" in result.stdout

    def test_stats_reviews_with_reviewed_impl(self, tmp_path: Path):
        """gza stats reviews shows cycle stats for a reviewed implementation task."""
        from gza.db import TaskStats

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        store.mark_completed(impl, has_commits=False, stats=TaskStats(cost_usd=0.10))

        review = store.add("Review feature", task_type="review", depends_on=impl.id)
        assert review.id is not None
        store.mark_completed(review, has_commits=False, stats=TaskStats(cost_usd=0.02))

        result = run_gza("stats", "reviews", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement tasks: 1" in result.stdout
        assert "Total reviews:   1" in result.stdout
        assert "Reviewed:        1/1" in result.stdout

    def test_stats_reviews_unreviewed_impl(self, tmp_path: Path):
        """gza stats reviews shows impl count but no cycle stats for unreviewed impls."""
        from gza.db import TaskStats

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement no review", task_type="implement")
        assert impl.id is not None
        store.mark_completed(impl, has_commits=False, stats=TaskStats(cost_usd=0.10))

        result = run_gza("stats", "reviews", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement tasks: 1" in result.stdout
        assert "Total reviews:   0" in result.stdout
        assert "Reviewed:        0/1" in result.stdout

    def test_stats_reviews_cycle_distribution(self, tmp_path: Path):
        """gza stats reviews shows cycle distribution for reviewed impls."""
        from gza.db import TaskStats

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        store.mark_completed(impl, has_commits=False, stats=TaskStats(cost_usd=0.10))

        for i in range(2):
            review = store.add(f"Review {i}", task_type="review", depends_on=impl.id)
            assert review.id is not None
            store.mark_completed(review, has_commits=False, stats=TaskStats(cost_usd=0.02))

        result = run_gza("stats", "reviews", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Cycle distribution" in result.stdout

    def test_stats_reviews_days_filter(self, tmp_path: Path):
        """gza stats reviews --days 7 restricts to last 7 days."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        result = run_gza("stats", "reviews", "--days", "7", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Implement tasks: 0" in result.stdout

    def test_stats_reviews_default_14_day_range(self, tmp_path: Path):
        """gza stats reviews with no date flags uses a 14-day range ending today."""
        from datetime import date

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        result = run_gza("stats", "reviews", "--project", str(tmp_path))

        assert result.returncode == 0
        today = date.today()
        start = today - timedelta(days=14)
        assert str(start) in result.stdout
        assert str(today) in result.stdout


class TestStatsCommand:
    """Tests for the 'gza stats' parent command."""

    def test_stats_no_subcommand_prints_help(self, tmp_path: Path):
        """gza stats with no subcommand exits 0 and prints help containing 'reviews'."""
        setup_config(tmp_path)

        result = run_gza("stats", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "reviews" in result.stdout

    def test_stats_cycles_subcommand_not_found(self, tmp_path: Path):
        """gza stats cycles exits non-zero since the subcommand was removed."""
        setup_config(tmp_path)

        result = run_gza("stats", "cycles", "--project", str(tmp_path))

        assert result.returncode != 0


class TestImportCommand:
    """Tests for 'gza import' command."""

    def test_import_no_file_specified(self, tmp_path: Path):
        """Import command requires a file argument."""
        setup_config(tmp_path)
        result = run_gza("import", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "No file specified" in result.stdout


class TestSyncReportCommand:
    """Tests for 'gza sync-report' command."""

    def test_sync_report_updates_db_from_disk_for_plan(self, tmp_path: Path):
        """sync-report copies disk content into DB output_content for plan tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Plan something", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Review feature", task_type="review")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Explore codebase", task_type="explore")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Plan task", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
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

        setup_config(tmp_path)
        store = make_store(tmp_path)

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

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement testing flow", task_type="implement")
        store.mark_completed(task, output_content="- Use dedicated fixtures for tests\n", has_commits=False)

        result = run_gza("learnings", "update", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Updated learnings" in result.stdout
        assert "Delta:" in result.stdout
        learnings_path = tmp_path / ".gza" / "learnings.md"
        assert learnings_path.exists()
        assert "Use dedicated fixtures for tests" in learnings_path.read_text()


class TestTmuxConfigValidation:
    """Tests for tmux config section parsing and validation."""

    def _write_config(self, tmp_path: Path, extra: str) -> None:
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(f"project_name: test\n{extra}")

    def test_config_tmux_invalid_auto_accept_timeout_raises(self, tmp_path: Path):
        """Config.load raises ConfigError for a non-numeric auto_accept_timeout."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  auto_accept_timeout: bad\n")
        with pytest.raises(ConfigError, match="auto_accept_timeout"):
            Config.load(tmp_path)

    def test_config_tmux_invalid_max_idle_timeout_raises(self, tmp_path: Path):
        """Config.load raises ConfigError for a non-numeric max_idle_timeout."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  max_idle_timeout: bad\n")
        with pytest.raises(ConfigError, match="max_idle_timeout"):
            Config.load(tmp_path)

    def test_config_tmux_invalid_detach_grace_raises(self, tmp_path: Path):
        """Config.load raises ConfigError for a non-numeric detach_grace."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  detach_grace: bad\n")
        with pytest.raises(ConfigError, match="detach_grace"):
            Config.load(tmp_path)

    def test_config_tmux_negative_auto_accept_timeout_raises(self, tmp_path: Path):
        """Config.load raises ConfigError for negative auto_accept_timeout."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  auto_accept_timeout: -1\n")
        with pytest.raises(ConfigError, match="auto_accept_timeout"):
            Config.load(tmp_path)

    def test_config_tmux_negative_max_idle_timeout_raises(self, tmp_path: Path):
        """Config.load raises ConfigError for negative max_idle_timeout."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  max_idle_timeout: -1\n")
        with pytest.raises(ConfigError, match="max_idle_timeout"):
            Config.load(tmp_path)

    def test_config_tmux_zero_timeout_raises(self, tmp_path: Path):
        """Config.load raises ConfigError for zero-valued timeout fields."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  auto_accept_timeout: 0\n")
        with pytest.raises(ConfigError, match="auto_accept_timeout"):
            Config.load(tmp_path)

    def test_config_tmux_invalid_terminal_size_string_raises(self, tmp_path: Path):
        """Config.load raises ConfigError when terminal_size is a string."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  terminal_size: '200x50'\n")
        with pytest.raises(ConfigError, match="terminal_size"):
            Config.load(tmp_path)

    def test_config_tmux_invalid_terminal_size_one_element_raises(self, tmp_path: Path):
        """Config.load raises ConfigError when terminal_size has only one element."""
        from gza.config import Config, ConfigError

        self._write_config(tmp_path, "tmux:\n  terminal_size: [200]\n")
        with pytest.raises(ConfigError, match="terminal_size"):
            Config.load(tmp_path)

    def test_config_tmux_valid_defaults_load(self, tmp_path: Path):
        """Config.load succeeds and returns TmuxConfig defaults when no tmux key present."""
        from gza.config import Config

        self._write_config(tmp_path, "")
        config = Config.load(tmp_path)
        assert config.tmux.enabled is False
        assert config.tmux.auto_accept_timeout == 10.0
        assert config.tmux.max_idle_timeout == 300.0
        assert config.tmux.detach_grace == 5.0
        assert config.tmux.terminal_size == [200, 50]

    def test_config_tmux_custom_values_load(self, tmp_path: Path):
        """Config.load stores custom tmux values correctly."""
        from gza.config import Config

        self._write_config(
            tmp_path,
            "tmux:\n  enabled: false\n  auto_accept_timeout: 20\n  max_idle_timeout: 600\n  detach_grace: 10\n  terminal_size: [160, 40]\n",
        )
        config = Config.load(tmp_path)
        assert config.tmux.enabled is False
        assert config.tmux.auto_accept_timeout == 20.0
        assert config.tmux.max_idle_timeout == 600.0
        assert config.tmux.detach_grace == 10.0
        assert config.tmux.terminal_size == [160, 40]
