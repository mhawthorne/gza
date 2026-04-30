"""Integration tests for runner snapshot behavior across real process boundaries."""

import sqlite3
import stat
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.providers import RunResult
from gza.runner import run

pytestmark = pytest.mark.integration


class TestWorktreeDbSnapshotIntegration:
    """Runner-path regressions for worktree DB snapshot behavior."""

    def _make_code_config(self, tmp_path: Path, db_path: Path) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.project_name = "test"
        config.project_prefix = "test"
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.workers_path = tmp_path / ".gza" / "workers"
        config.workers_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        config.learnings_interval = 0
        config.learnings_window = 25
        return config

    def test_run_code_task_uses_readonly_frozen_snapshot(self, tmp_path: Path) -> None:
        """Code-task path should expose readonly worktree snapshot and keep it frozen."""
        uv_project = Path(__file__).resolve().parents[1]
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement snapshot feature", task_type="implement")
        task.slug = "20260414-implement-snapshot-feature"
        store.update(task)

        host_conn = sqlite3.connect(str(db_path))
        host_conn.execute("CREATE TABLE snapshot_probe (value TEXT)")
        host_conn.execute("INSERT INTO snapshot_probe (value) VALUES ('before')")
        host_conn.commit()
        host_conn.close()
        (tmp_path / "gza.yaml").write_text(
            "project_name: gza\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n"
        )

        config = self._make_code_config(tmp_path, db_path)
        observed: dict[str, str | int | None] = {
            "snapshot_mode": None,
            "task_prompt": None,
            "snapshot_probe_before": None,
            "snapshot_probe_after_host_mutation": None,
            "write_error": None,
            "show_worktree_rc": None,
            "show_worktree_stdout": None,
            "show_worktree_stderr": None,
            "show_host_rc": None,
            "show_host_stdout": None,
            "show_host_stderr": None,
            "add_worktree_rc": None,
            "add_worktree_stdout": None,
            "add_worktree_stderr": None,
        }
        task_count_before = len(store.get_all())

        def provider_run(_cfg, _prompt, _log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            del _cfg, _prompt, _log_file, resume_session_id, on_session_id, on_step_count
            snapshot_path = work_dir / ".gza" / "gza.db"
            assert snapshot_path.exists()
            observed["snapshot_mode"] = stat.S_IMODE(snapshot_path.stat().st_mode)

            snapshot_conn = sqlite3.connect(str(snapshot_path))
            task_row = snapshot_conn.execute("SELECT prompt FROM tasks WHERE id = ?", (task.id,)).fetchone()
            assert task_row is not None
            observed["task_prompt"] = task_row[0]
            probe_row = snapshot_conn.execute("SELECT value FROM snapshot_probe").fetchone()
            assert probe_row is not None
            observed["snapshot_probe_before"] = probe_row[0]

            host_mutation_conn = sqlite3.connect(str(db_path))
            host_mutation_conn.execute("UPDATE snapshot_probe SET value = 'after'")
            host_mutation_conn.commit()
            host_mutation_conn.close()

            frozen_probe_row = snapshot_conn.execute("SELECT value FROM snapshot_probe").fetchone()
            assert frozen_probe_row is not None
            observed["snapshot_probe_after_host_mutation"] = frozen_probe_row[0]

            try:
                snapshot_conn.execute("CREATE TABLE sandbox_write_attempt (id INTEGER)")
            except sqlite3.OperationalError as exc:
                observed["write_error"] = str(exc).lower()
            assert observed["write_error"] is not None
            snapshot_conn.close()
            (work_dir / "gza.yaml").write_text(
                "project_name: gza\n"
                "project_id: default\n"
                "db_path: .gza/gza.db\n"
            )

            show_worktree = subprocess.run(
                ["uv", "run", "--project", str(uv_project), "gza", "show", task.id],
                capture_output=True,
                text=True,
                cwd=work_dir,
            )
            observed["show_worktree_rc"] = show_worktree.returncode
            observed["show_worktree_stdout"] = show_worktree.stdout
            observed["show_worktree_stderr"] = show_worktree.stderr

            show_host = subprocess.run(
                ["uv", "run", "--project", str(uv_project), "gza", "show", task.id],
                capture_output=True,
                text=True,
                cwd=tmp_path,
            )
            observed["show_host_rc"] = show_host.returncode
            observed["show_host_stdout"] = show_host.stdout
            observed["show_host_stderr"] = show_host.stderr

            add_worktree = subprocess.run(
                ["uv", "run", "--project", str(uv_project), "gza", "add", "write attempt from worktree snapshot"],
                capture_output=True,
                text=True,
                cwd=work_dir,
            )
            observed["add_worktree_rc"] = add_worktree.returncode
            observed["add_worktree_stdout"] = add_worktree.stdout
            observed["add_worktree_stderr"] = add_worktree.stderr

            summary_dir = work_dir / ".gza" / "summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            (summary_dir / f"{task.slug}.md").write_text("# Summary\n\n- Completed snapshot checks.")

            return RunResult(
                exit_code=0,
                duration_seconds=1.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="snapshot-session",
                error_type=None,
            )

        with (
            patch("gza.runner.get_provider") as mock_get_provider,
            patch("gza.runner.Git") as mock_git_class,
            patch("gza.runner.load_dotenv"),
        ):
            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.count_commits_ahead.return_value = 0
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            mock_worktree_git.status_porcelain.side_effect = [
                set(),
                {("M", "changed.py")},
            ]
            mock_worktree_git.default_branch.return_value = "main"
            mock_worktree_git.get_diff_numstat.return_value = "1\t0\tchanged.py\n"
            mock_worktree_git._run.return_value = Mock(stdout="")
            mock_worktree_git.count_commits_ahead.return_value = 1

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            result = run(config, task_id=task.id)

        assert result == 0
        assert observed["snapshot_mode"] == 0o444
        assert observed["task_prompt"] == "Implement snapshot feature"
        assert observed["snapshot_probe_before"] == "before"
        assert observed["snapshot_probe_after_host_mutation"] == "before"
        assert observed["write_error"] is not None and "readonly" in str(observed["write_error"])
        assert observed["show_worktree_rc"] == 0, (
            f"worktree show failed\nstdout:\n{observed['show_worktree_stdout']}\n"
            f"stderr:\n{observed['show_worktree_stderr']}"
        )
        assert observed["show_host_rc"] == 0, (
            f"host show failed\nstdout:\n{observed['show_host_stdout']}\n"
            f"stderr:\n{observed['show_host_stderr']}"
        )
        assert observed["show_worktree_stdout"] is not None
        assert observed["show_host_stdout"] is not None
        assert f"Task {task.id}" in str(observed["show_worktree_stdout"])
        assert "Implement snapshot feature" in str(observed["show_worktree_stdout"])
        assert f"Task {task.id}" in str(observed["show_host_stdout"])
        assert "Implement snapshot feature" in str(observed["show_host_stdout"])
        assert observed["add_worktree_rc"] is not None
        assert int(observed["add_worktree_rc"]) != 0
        add_output = (
            f"{observed['add_worktree_stdout'] or ''}\n{observed['add_worktree_stderr'] or ''}"
        ).lower()
        assert "readonly" in add_output or "read-only" in add_output

        host_check_conn = sqlite3.connect(str(db_path))
        host_value = host_check_conn.execute("SELECT value FROM snapshot_probe").fetchone()
        sandbox_table = host_check_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sandbox_write_attempt'"
        ).fetchone()
        task_count_after = host_check_conn.execute("SELECT COUNT(*) FROM tasks").fetchone()
        host_check_conn.close()
        assert host_value is not None
        assert host_value[0] == "after"
        assert sandbox_table is None
        assert task_count_after is not None
        assert task_count_after[0] == task_count_before
