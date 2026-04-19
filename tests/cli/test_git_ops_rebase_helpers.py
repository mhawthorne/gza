"""Tests for git operations CLI commands."""


import argparse
import io
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.cli import _determine_advance_action, cmd_advance
from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import (
    make_store,
    run_gza,
    setup_config,
    setup_db_with_tasks,
    setup_git_repo_with_task_branch,
)


class TestRebaseHelpers:
    """Tests for rebase helper functions."""

    def test_invoke_provider_resolve_uses_effective_codex_provider(self, tmp_path):
        """Auto-resolve uses effective provider selection (codex override)."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        task = SimpleNamespace(task_type="implement", provider="codex", provider_is_explicit=True, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "codex"
            assert resolve_config.use_docker is config.use_docker
            mock_provider.run.assert_called_once()
            assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto --continue"

    def test_invoke_provider_resolve_uses_effective_gemini_provider(self, tmp_path):
        """Auto-resolve supports gemini provider selection from effective config."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="gemini")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "gemini"
            mock_provider.run.assert_called_once()
            assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto --continue"

    def test_invoke_provider_resolve_fails_fast_when_skill_missing(self, tmp_path, capsys, monkeypatch):
        """Auto-resolve fails before provider run when runtime skill is missing and auto-install fails."""
        from types import SimpleNamespace
        from unittest.mock import patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config

        codex_home = tmp_path / "codex-home"
        monkeypatch.setenv("CODEX_HOME", str(codex_home))
        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=False), \
             patch("gza.providers.get_provider") as mock_get_provider:
            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is False
            assert mock_get_provider.call_count == 0

        out = capsys.readouterr().out
        assert "Missing required 'gza-rebase' skill for provider 'codex'" in out
        assert "uv run gza skills-install --target codex gza-rebase --project" in out

    def test_invoke_provider_resolve_uses_current_config_after_provider_switch(self, tmp_path):
        """Auto-resolve uses current config provider when task provider is non-explicit stale state."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        task = SimpleNamespace(task_type="implement", provider="claude", provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "codex"

    def test_invoke_provider_resolve_honors_use_docker_override(self, tmp_path):
        """Auto-resolve should respect config.use_docker when launching provider."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex", use_docker=False)
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is True
        resolve_config = mock_get_provider.call_args.args[0]
        assert resolve_config.use_docker is False

    def test_invoke_provider_resolve_creates_internal_task_record(self, tmp_path):
        """Auto-resolve should persist an internal task so history/auditing can find it."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=42,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is True
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "completed"
        assert internal_task.skip_learnings is True
        assert internal_task.output_content is not None
        assert "/gza-rebase --auto --continue" in internal_task.output_content

    def test_invoke_provider_resolve_marks_internal_task_failed_on_exception(self, tmp_path):
        """Provider exceptions should mark the tracking internal task as failed."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=43,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.side_effect = RuntimeError("provider failure")
            mock_get_provider.return_value = mock_provider

            with pytest.raises(RuntimeError, match="provider failure"):
                invoke_provider_resolve(task, "feature", "main", config)

        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "failed"
        assert internal_task.failure_reason == "UNKNOWN"

    def test_invoke_provider_resolve_marks_internal_task_failed_if_rebase_still_in_progress(self, tmp_path):
        """Auto-resolve should fail when git still reports active rebase markers."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=44,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops.load_dotenv") as mock_load_dotenv, \
             patch("pathlib.Path.exists", side_effect=[True, False]):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is False
        mock_load_dotenv.assert_called_once_with(tmp_path)
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "failed"
        assert internal_task.failure_reason == "UNKNOWN"

    def test_invoke_provider_resolve_marks_internal_task_failed_on_nonzero_exit(self, tmp_path):
        """Non-zero provider exits should fail resolve even without an exception."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=45,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops.load_dotenv"), \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=1)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is False
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "failed"
        assert internal_task.failure_reason == "UNKNOWN"
        assert internal_task.output_content is None

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
        from unittest.mock import patch

        from gza.cli import ensure_skill

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
        from unittest.mock import patch

        from gza.cli import ensure_skill

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

    # --- worktree-path variant tests ---

    def test_invoke_provider_resolve_worktree_uses_auto_without_continue(self, tmp_path):
        """When worktree_path is provided the provider is called with /gza-rebase --auto (no --continue)."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config, worktree_path=tmp_path)

        assert result is True
        mock_provider.run.assert_called_once()
        assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto"

    def test_invoke_provider_resolve_worktree_runs_provider_in_worktree(self, tmp_path):
        """When worktree_path is provided the provider runs in that directory."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        worktree = tmp_path / "wt"
        worktree.mkdir()
        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            invoke_provider_resolve(task, "feature", "main", config, worktree_path=worktree)

        # Provider must be run with the worktree as the working directory
        assert mock_provider.run.call_args.args[3] == worktree

    def test_invoke_provider_resolve_worktree_fails_if_rebase_still_in_progress(self, tmp_path):
        """Worktree path: returns False when _is_rebase_in_progress reports True."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=99,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops.load_dotenv"), \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=True), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config, worktree_path=tmp_path)

        assert result is False
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        assert internal_tasks[0].status == "failed"

    def test_invoke_provider_resolve_worktree_records_auto_command_in_output(self, tmp_path):
        """Worktree path: internal task output_content contains /gza-rebase --auto."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=100,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config, worktree_path=tmp_path)

        assert result is True
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "completed"
        assert "/gza-rebase --auto" in internal_task.output_content
        assert "--continue" not in internal_task.output_content
