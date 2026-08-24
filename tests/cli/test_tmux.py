"""Tests for tmux-related CLI functionality: attach command and tmux spawn logic."""

import argparse
import os
import signal
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from .conftest import make_store, setup_config


def _make_args(project_dir: Path, **kwargs) -> argparse.Namespace:
    """Create a minimal argparse.Namespace for tests."""
    defaults = {"project_dir": project_dir, "no_docker": False, "max_turns": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdAttach:
    """Tests for cmd_attach CLI command."""

    def _setup_running_worker(
        self,
        tmp_path: Path,
        task_id: int = 1,
        tmux_session: str | None = None,
        provider: str | None = None,
        session_id: str | None = None,
    ) -> None:
        """Create a running worker JSON file in the workers directory."""
        import json

        setup_config(tmp_path)

        # Create DB with the task first so we know the actual task ID
        store = make_store(tmp_path)
        task = store.add("test task")
        if provider:
            task.provider = provider
        if session_id:
            task.session_id = session_id
        task.status = "in_progress"
        task.running_pid = 12345
        task.log_file = f".gza/logs/{task.id}.log"
        store.update(task)

        actual_task_id = task.id

        workers_dir = tmp_path / ".gza" / "workers"
        workers_dir.mkdir(parents=True, exist_ok=True)

        worker_data = {
            "worker_id": "w-20260301-1",
            "task_id": actual_task_id,
            "pid": 12345,
            "status": "running",
            "is_background": True,
            "tmux_session": tmux_session or f"gza-{actual_task_id}",
        }
        (workers_dir / "w-20260301-1.json").write_text(json.dumps(worker_data))
        (workers_dir / "w-20260301-1.pid").write_text("12345")

    def test_cmd_attach_finds_session_by_worker_id(self, tmp_path: Path, monkeypatch):
        """cmd_attach attaches to tmux session when looked up by worker ID."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")
        monkeypatch.delenv("TMUX", raising=False)

        args = _make_args(tmp_path, worker_id="w-20260301-1")

        tmux_has_session = MagicMock(returncode=0)

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", return_value=tmux_has_session), \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvpe.assert_called_once()
        call_args = mock_execvpe.call_args[0]
        assert call_args[0] == "tmux"
        assert "attach-session" in call_args[1]
        assert "gza-1" in call_args[1]

    def test_cmd_attach_finds_session_by_task_id(self, tmp_path: Path):
        """cmd_attach attaches to tmux session when looked up by full prefixed task ID."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")

        # Find actual task_id from DB
        store = make_store(tmp_path)
        task = store.get_all()[0]

        args = _make_args(tmp_path, worker_id=str(task.id))

        tmux_has_session = MagicMock(returncode=0)

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", return_value=tmux_has_session), \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvpe.assert_called_once()

    def test_cmd_attach_metadata_absent_fallback_uses_live_legacy_session_owned_by_worker(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        """Attach fallback for old metadata validates legacy tmux PID ownership."""
        import json

        from gza.config import Config
        from gza.runtime_context import runtime_tmux_session_name

        self._setup_running_worker(tmp_path, task_id=1, tmux_session="legacy-will-be-removed", provider="codex")
        worker_path = tmp_path / ".gza" / "workers" / "w-20260301-1.json"
        worker_data = json.loads(worker_path.read_text())
        worker_data.pop("tmux_session", None)
        worker_path.write_text(json.dumps(worker_data))
        store = make_store(tmp_path)
        task = store.get_all()[0]
        config = Config.load(tmp_path)
        expected_session = runtime_tmux_session_name(
            task_id=task.id,
            project_id=config.project_id,
            db_path=config.db_path,
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")
        tmux_has_session = MagicMock(returncode=0)

        def fake_tmux_pid(session_name, **kwargs):
            assert kwargs["cwd"] == tmp_path
            assert kwargs["env"]["GZA_DB_PATH"] == str(config.db_path)
            if session_name == f"gza-{task.id}":
                return 12345
            if session_name == expected_session:
                return 99999
            return None

        with patch("gza.cli.query.get_tmux_session_pid", side_effect=fake_tmux_pid) as mock_pid, \
             patch("gza.cli.query.subprocess.run", return_value=tmux_has_session) as mock_run, \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        assert [call.args[0] for call in mock_pid.call_args_list] == [
            expected_session,
            f"gza-{task.id}",
        ]
        has_session_call = mock_run.call_args_list[0][0][0]
        assert has_session_call == ["tmux", "has-session", "-t", f"gza-{task.id}"]
        exec_args = mock_execvpe.call_args[0][1]
        assert f"gza-{task.id}" in exec_args

    def test_cmd_attach_refuses_persisted_legacy_session_owned_by_other_project(
        self,
        tmp_path: Path,
        capsys,
        monkeypatch,
    ) -> None:
        """Persisted legacy tmux names must prove pane PID ownership before attach."""
        project_a = tmp_path / "a"
        project_b = tmp_path / "b"
        project_a.mkdir()
        project_b.mkdir()
        self._setup_running_worker(project_a, task_id=1, tmux_session="gza-1", provider="codex")
        self._setup_running_worker(project_b, task_id=1, tmux_session="gza-1", provider="codex")
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(project_a, worker_id="w-20260301-1")

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345 + 1), \
             patch("gza.cli.query.subprocess.run") as mock_run, \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach

            result = cmd_attach(args)

        assert result == 1
        mock_run.assert_not_called()
        mock_execvpe.assert_not_called()
        assert "expected worker" in capsys.readouterr().out

    def test_cmd_attach_accepts_persisted_legacy_session_with_matching_worker_pid(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", return_value=MagicMock(returncode=0)), \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach

            result = cmd_attach(args)

        assert result == 1
        assert "gza-1" in mock_execvpe.call_args[0][1]

    def test_cmd_attach_accepts_persisted_qualified_session_with_matching_worker_pid(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        from gza.config import Config
        from gza.runtime_context import runtime_tmux_session_name

        self._setup_running_worker(tmp_path, task_id=1, tmux_session=None, provider="codex")
        config = Config.load(tmp_path)
        task = make_store(tmp_path).get_all()[0]
        qualified = runtime_tmux_session_name(
            task_id=task.id,
            project_id=config.project_id,
            db_path=config.db_path,
        )
        worker_path = tmp_path / ".gza" / "workers" / "w-20260301-1.json"
        import json

        worker_data = json.loads(worker_path.read_text())
        worker_data["tmux_session"] = qualified
        worker_path.write_text(json.dumps(worker_data))
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", return_value=MagicMock(returncode=0)), \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach

            result = cmd_attach(args)

        assert result == 1
        assert qualified in mock_execvpe.call_args[0][1]

    def test_cmd_attach_no_running_worker_returns_1(self, tmp_path: Path):
        """cmd_attach returns 1 when no running worker is found."""
        setup_config(tmp_path)
        (tmp_path / ".gza" / "workers").mkdir(parents=True, exist_ok=True)

        args = _make_args(tmp_path, worker_id="w-nonexistent")

        from gza.cli.query import cmd_attach
        result = cmd_attach(args)
        assert result == 1

    def test_cmd_attach_no_tmux_session_returns_1(self, tmp_path: Path):
        """cmd_attach returns 1 when tmux session does not exist."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1")

        args = _make_args(tmp_path, worker_id="w-20260301-1")

        with patch("gza.cli.query.get_tmux_session_pid", return_value=None), \
             patch("gza.cli.query.subprocess.run") as mock_run:
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 1
        mock_run.assert_not_called()

    def test_cmd_attach_prints_warning_for_observe_only_provider(self, tmp_path: Path, capsys, monkeypatch):
        """cmd_attach attaches read-only and prints notice for codex/gemini providers."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")
        monkeypatch.delenv("TMUX", raising=False)

        args = _make_args(tmp_path, worker_id="w-20260301-1")
        tmux_has_session = MagicMock(returncode=0)

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", return_value=tmux_has_session), \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        # Should attach with -r (read-only) flag
        mock_execvpe.assert_called_once()
        call_args = mock_execvpe.call_args[0]
        assert "-r" in call_args[1], "Observe-only providers should attach read-only (-r)"

        captured = capsys.readouterr()
        assert "headless" in captured.out.lower() or "observe" in captured.out.lower()

    def test_cmd_attach_tmux_calls_use_project_runtime_environment(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        """Attach/control tmux calls and final exec use the owning project's env."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")
        (tmp_path / ".env").write_text(
            "TMUX_TMPDIR=/tmp/project-tmux\nPATH=/tmp/project-bin:/usr/bin\n",
            encoding="utf-8",
        )
        monkeypatch.delenv("TMUX", raising=False)
        monkeypatch.setenv("TMUX_TMPDIR", "/tmp/supervisor-tmux")
        monkeypatch.setenv("PATH", "/tmp/supervisor-bin:/usr/bin")
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        def fake_tmux_run(_cmd, **kwargs):
            assert kwargs["cwd"] == tmp_path
            assert kwargs["env"]["TMUX_TMPDIR"] == "/tmp/project-tmux"
            assert kwargs["env"]["PATH"] == "/tmp/project-bin:/usr/bin"
            return MagicMock(returncode=0)

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run), \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        exec_env = mock_execvpe.call_args[0][2]
        assert exec_env["TMUX_TMPDIR"] == "/tmp/project-tmux"
        assert exec_env["PATH"] == "/tmp/project-bin:/usr/bin"
        assert os.environ["TMUX_TMPDIR"] == "/tmp/supervisor-tmux"

    def test_cmd_attach_uses_switch_client_inside_tmux(self, tmp_path: Path):
        """cmd_attach uses switch-client instead of attach-session when already in tmux."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")

        args = _make_args(tmp_path, worker_id="w-20260301-1")
        tmux_has_session = MagicMock(returncode=0)

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", return_value=tmux_has_session) as mock_run, \
             patch("gza.cli.query.os.execvpe") as mock_execvpe, \
             patch.dict("os.environ", {"TMUX": "/tmp/tmux-501/default,12345,0"}):
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvpe.assert_called_once()
        call_args = mock_execvpe.call_args[0]
        assert call_args[0] == "tmux"
        assert "switch-client" in call_args[1]
        assert "gza-1" in call_args[1]

        # Verify detach-on-destroy is set on the task session (not globally)
        set_option_calls = [
            c for c in mock_run.call_args_list
            if "set-option" in c[0][0] and "detach-on-destroy" in c[0][0]
        ]
        assert len(set_option_calls) == 1, "detach-on-destroy must be set when inside tmux"
        set_args = set_option_calls[0][0][0]
        assert "-t" in set_args, "detach-on-destroy must be session-scoped (-t), not global (-g)"
        assert "gza-1" in set_args

    def test_cmd_attach_observe_only_uses_switch_client_inside_tmux(self, tmp_path: Path):
        """cmd_attach uses switch-client -r for observe-only providers when inside tmux."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")

        args = _make_args(tmp_path, worker_id="w-20260301-1")
        tmux_has_session = MagicMock(returncode=0)

        with patch("gza.cli.query.get_tmux_session_pid", return_value=12345), \
             patch("gza.cli.query.subprocess.run", return_value=tmux_has_session) as mock_run, \
             patch("gza.cli.query.os.execvpe") as mock_execvpe, \
             patch.dict("os.environ", {"TMUX": "/tmp/tmux-501/default,12345,0"}):
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvpe.assert_called_once()
        call_args = mock_execvpe.call_args[0]
        assert "switch-client" in call_args[1]
        assert "-r" in call_args[1]

        # Verify detach-on-destroy is set on the task session (not globally)
        set_option_calls = [
            c for c in mock_run.call_args_list
            if "set-option" in c[0][0] and "detach-on-destroy" in c[0][0]
        ]
        assert len(set_option_calls) == 1
        assert "-t" in set_option_calls[0][0][0]

    def test_cmd_attach_claude_stops_worker_and_starts_interactive_session(self, tmp_path: Path, monkeypatch):
        """Claude attach should stop worker and launch a fresh interactive tmux resume session."""
        from gza.workers import WorkerRegistry

        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with patch("gza.cli.query.os.kill", side_effect=fake_kill) as mock_kill, \
             patch("gza.cli.query.subprocess.run", return_value=MagicMock(returncode=0)), \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.execvpe") as mock_execvpe:
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 0
        assert mock_kill.called, "interactive attach must stop the running worker"
        mock_execvpe.assert_called_once()
        tmux_cmd = mock_execvpe.call_args[0][1]
        assert "attach-session" in tmux_cmd
        assert any(part.startswith("gza-attach-") for part in tmux_cmd)

        registry = WorkerRegistry(tmp_path / ".gza" / "workers")
        worker = registry.get("w-20260301-1")
        assert worker is not None
        assert worker.status == "completed"
        assert worker.completion_reason == "stopped_for_attach"

    def test_cmd_attach_interactive_session_names_are_project_qualified(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        """Same task IDs in different projects must not kill each other's attach session."""
        from gza.config import Config
        from gza.runtime_context import runtime_tmux_session_name

        project_a = tmp_path / "a"
        project_b = tmp_path / "b"
        project_a.mkdir()
        project_b.mkdir()
        self._setup_running_worker(project_a, task_id=1, provider="claude", session_id="ses_a")
        self._setup_running_worker(project_b, task_id=1, provider="claude", session_id="ses_b")
        config_a = Config.load(project_a)
        config_b = Config.load(project_b)
        task_a = make_store(project_a).get_all()[0]
        task_b = make_store(project_b).get_all()[0]
        session_a = runtime_tmux_session_name(
            task_id=str(task_a.id),
            project_id=config_a.project_id,
            db_path=config_a.db_path,
            session_kind="attach",
        )
        session_b = runtime_tmux_session_name(
            task_id=str(task_b.id),
            project_id=config_b.project_id,
            db_path=config_b.db_path,
            session_kind="attach",
        )
        assert session_a != session_b
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(project_b, worker_id="w-20260301-1")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with (
            patch("gza.cli.query.os.kill", side_effect=fake_kill),
            patch("gza.cli.query.subprocess.run", return_value=MagicMock(returncode=0)) as mock_run,
            patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.query.os.execvpe") as mock_execvpe,
        ):
            from gza.cli.query import cmd_attach

            result = cmd_attach(args)

        assert result == 0
        tmux_commands = [call.args[0] for call in mock_run.call_args_list if call.args and call.args[0][0] == "tmux"]
        assert any(cmd[:3] == ["tmux", "kill-session", "-t"] and cmd[3] == session_b for cmd in tmux_commands)
        assert all(session_a not in cmd for cmd in tmux_commands)
        assert session_b in mock_execvpe.call_args[0][1]

    def test_cmd_attach_claude_prints_normal_exit_auto_resume_message(self, tmp_path: Path, monkeypatch, capsys):
        """Claude attach should communicate that normal exit auto-resumes in background."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with patch("gza.cli.query.os.kill", side_effect=fake_kill), \
             patch("gza.cli.query.subprocess.run", return_value=MagicMock(returncode=0)), \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.execvpe"):
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Detach with Ctrl-B D or exit Claude normally to auto-resume in background." in captured.out

    def test_cmd_attach_claude_passes_force_override_to_attach_wrapper(self, tmp_path: Path, monkeypatch):
        """Attach handoff should preserve --force when rebuilding wrapper command."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        call_state = {"new_session_calls": 0, "wrapper_has_force": False}

        def fake_tmux_run(cmd, **kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                call_state["new_session_calls"] += 1
                if call_state["new_session_calls"] == 2:
                    call_state["wrapper_has_force"] = "--force" in cmd
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run), \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.kill", side_effect=fake_kill), \
             patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer, \
             patch("gza.cli.query.os.execvpe"):
            from gza.cli.query import ResumeOverrideInference
            mock_infer.return_value = ResumeOverrideInference(force=True)
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 0
        assert call_state["new_session_calls"] == 2
        assert call_state["wrapper_has_force"] is True

    def test_cmd_attach_claude_sets_pipe_pane_to_task_log(self, tmp_path: Path, monkeypatch):
        """Claude attach should set tmux pipe-pane so interactive output is captured in the task log."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with patch("gza.cli.query.os.kill", side_effect=fake_kill), \
             patch("gza.cli.query.subprocess.run", return_value=MagicMock(returncode=0)) as mock_run, \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.execvpe"):
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 0
        pipe_calls = [c[0][0] for c in mock_run.call_args_list if c[0][0][:2] == ["tmux", "pipe-pane"]]
        assert pipe_calls, "Expected tmux pipe-pane call during interactive attach setup"
        pipe_cmd = pipe_calls[0]
        assert "gza-attach-" in pipe_cmd[3]
        assert "cat >>" in pipe_cmd[4]
        assert ".gza/logs/" in pipe_cmd[4]

    def test_cmd_attach_claude_preflight_failure_does_not_stop_worker(self, tmp_path: Path, monkeypatch):
        """If tmux preflight fails, cmd_attach must not stop the running worker or mutate task state."""
        from gza.workers import WorkerRegistry

        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.status = "in_progress"
        task.running_pid = 12345
        store.update(task)

        call_state = {"new_session_calls": 0}

        def fake_tmux_run(cmd, **kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                call_state["new_session_calls"] += 1
                return MagicMock(returncode=1, stderr="preflight create failed")
            return MagicMock(returncode=0, stderr="")

        with patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run), \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.kill") as mock_kill, \
             patch("gza.cli.query._spawn_background_worker") as mock_spawn_bg:
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 1
        assert call_state["new_session_calls"] == 1, "Only preflight new-session should run"
        mock_kill.assert_not_called()
        mock_spawn_bg.assert_not_called()

        registry = WorkerRegistry(tmp_path / ".gza" / "workers")
        worker = registry.get("w-20260301-1")
        assert worker is not None
        assert worker.status == "running"

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "in_progress"
        assert refreshed.running_pid == 12345

    def test_cmd_attach_tmux_creation_failure_sanitizes_runtime_secrets_and_keeps_env_handoff(
        self,
        tmp_path: Path,
        monkeypatch,
        capsys,
    ):
        """Attach tmux failures must not disclose runtime env values in output or argv."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        (tmp_path / ".env").write_text(
            "\n".join(
                [
                    "PATH=/owned/bin",
                    "ANTHROPIC_API_KEY=anthropic-poison",
                    "CODEX_API_KEY=codex-poison",
                    "GEMINI_API_KEY=gemini-poison",
                    "PROJECT_ONLY_TOKEN=arbitrary-poison",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")
        call_state = {"new_session_calls": 0}
        new_session_calls: list[tuple[list[str], dict]] = []

        def fake_tmux_run(cmd, **kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                call_state["new_session_calls"] += 1
                new_session_calls.append((cmd, kwargs))
                if call_state["new_session_calls"] == 1:
                    return MagicMock(returncode=0, stderr="")
                return MagicMock(
                    returncode=1,
                    stderr="create failed: anthropic-poison codex-poison gemini-poison arbitrary-poison",
                )
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with (
            patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run),
            patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.query.os.kill", side_effect=fake_kill),
            patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer,
            patch("gza.cli.query._spawn_background_worker", return_value=0),
        ):
            from gza.cli.query import ResumeOverrideInference, cmd_attach

            mock_infer.return_value = ResumeOverrideInference(no_docker=True)
            result = cmd_attach(args)

        assert result == 1
        captured = capsys.readouterr()
        output_text = captured.out + captured.err
        for secret in ("anthropic-poison", "codex-poison", "gemini-poison", "arbitrary-poison"):
            assert secret not in output_text
        assert "[redacted]" in output_text
        assert len(new_session_calls) == 2
        create_cmd, create_kwargs = new_session_calls[1]
        assert "-e" not in create_cmd
        argv_text = "\0".join(create_cmd)
        for secret in ("anthropic-poison", "codex-poison", "gemini-poison", "arbitrary-poison"):
            assert secret not in argv_text
        create_env = create_kwargs["env"]
        assert create_env["PATH"] == "/owned/bin"
        assert create_env["PWD"] == str(tmp_path.resolve())
        assert create_env["GZA_DB_PATH"] == str((tmp_path / ".gza" / "gza.db").resolve())
        assert create_env["ANTHROPIC_API_KEY"] == "anthropic-poison"
        assert create_env["CODEX_API_KEY"] == "codex-poison"
        assert create_env["GEMINI_API_KEY"] == "gemini-poison"
        assert create_env["PROJECT_ONLY_TOKEN"] == "arbitrary-poison"

    @pytest.mark.parametrize(
        "error",
        [
            "timed out while inspecting worker w-20260301-1 command line",
            "'ps' command was not found in the runtime PATH",
            "ps failed while inspecting worker w-20260301-1: denied",
            "ps returned no command line for worker w-20260301-1",
            "worker w-20260301-1 has malformed --max-turns value",
        ],
    )
    def test_cmd_attach_claude_refuses_when_launch_parity_cannot_be_proven(
        self,
        tmp_path: Path,
        monkeypatch,
        capsys,
        error: str,
    ):
        """Attach must fail closed when worker CLI override introspection is inconclusive."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        with (
            patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer,
            patch("gza.cli.query._stop_worker_for_attach") as mock_stop,
            patch("gza.cli.query.subprocess.run") as mock_run,
            patch("gza.cli.query.os.execvpe") as mock_execvpe,
        ):
            from gza.cli.query import ResumeOverrideInference, cmd_attach

            mock_infer.return_value = ResumeOverrideInference(error=error)
            result = cmd_attach(args)

        assert result == 1
        assert "cannot prove attach handoff launch parity" in capsys.readouterr().out
        mock_stop.assert_not_called()
        mock_run.assert_not_called()
        mock_execvpe.assert_not_called()

    def test_cmd_attach_claude_ps_uses_project_runtime_context_with_poisoned_supervisor(
        self,
        tmp_path: Path,
        monkeypatch,
    ):
        """Interactive attach must inspect the worker from the owning project runtime."""
        project = tmp_path / "project"
        project.mkdir()
        self._setup_running_worker(
            project,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        (project / ".env").write_text("PATH=/project/bin\nPWD=/dotenv-pwd\n", encoding="utf-8")
        supervisor_pwd = tmp_path / "supervisor"
        supervisor_pwd.mkdir()
        monkeypatch.chdir(supervisor_pwd)
        monkeypatch.setenv("PATH", "/supervisor/bin")
        monkeypatch.setenv("PWD", str(supervisor_pwd))
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(project, worker_id="w-20260301-1")

        observed_ps: dict[str, object] = {}

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["ps", "-p"]:
                observed_ps["cwd"] = kwargs.get("cwd")
                observed_ps["env"] = kwargs.get("env")
                return MagicMock(
                    returncode=0,
                    stdout="gza implement --no-docker --max-turns 9 --force task\n",
                    stderr="",
                )
            return MagicMock(returncode=0, stdout="", stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with (
            patch("gza.cli.query.subprocess.run", side_effect=fake_run),
            patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.query.os.kill", side_effect=fake_kill),
            patch("gza.cli.query.os.execvpe"),
        ):
            from gza.cli.query import cmd_attach

            result = cmd_attach(args)

        assert result == 0
        assert observed_ps["cwd"] == project
        ps_env = observed_ps["env"]
        assert isinstance(ps_env, dict)
        assert ps_env["PATH"] == "/project/bin"
        assert ps_env["PWD"] == str(project.resolve())

    def test_cmd_attach_claude_restarts_background_worker_if_session_create_fails(self, tmp_path: Path, monkeypatch):
        """If real attach-session creation fails after stop, cmd_attach should auto-recover by restarting worker."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        call_state = {"new_session_calls": 0}

        def fake_tmux_run(cmd, **kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                call_state["new_session_calls"] += 1
                # preflight succeeds; real create fails
                if call_state["new_session_calls"] == 1:
                    return MagicMock(returncode=0, stderr="")
                return MagicMock(returncode=1, stderr="create failed")
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run), \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.kill", side_effect=fake_kill), \
             patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer, \
             patch("gza.cli.query._spawn_background_worker", return_value=0) as mock_spawn_bg:
            from gza.cli.query import ResumeOverrideInference
            mock_infer.return_value = ResumeOverrideInference(no_docker=True, max_turns=77, force=True)
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 1
        assert call_state["new_session_calls"] == 2
        mock_spawn_bg.assert_called_once()
        recovery_args = mock_spawn_bg.call_args[0][0]
        assert recovery_args.no_docker is True
        assert recovery_args.max_turns == 77
        assert recovery_args.force is True

    def test_cmd_attach_recovers_when_second_env_file_write_raises(
        self,
        tmp_path: Path,
        monkeypatch,
        capsys,
    ):
        """Post-stop env handoff exceptions must sanitize output and restart the worker."""
        from gza.config import Config
        from gza.runtime_context import RuntimeExecutionContext

        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        (tmp_path / ".env").write_text(
            "PATH=/owned/bin\nANTHROPIC_API_KEY=anthropic-poison\n",
            encoding="utf-8",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        write_calls = {"count": 0}

        def fake_write_env_file(**_kwargs):
            write_calls["count"] += 1
            if write_calls["count"] == 1:
                path = tmp_path / ".gza" / "tmp" / "probe.sh"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("probe\n", encoding="utf-8")
                return path
            raise OSError("env write failed: anthropic-poison")

        def fake_tmux_run(cmd, **_kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                return MagicMock(returncode=0, stderr="")
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with (
            patch("gza.cli.query.write_tmux_environment_file", side_effect=fake_write_env_file),
            patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run) as mock_run,
            patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.query.os.kill", side_effect=fake_kill),
            patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer,
            patch("gza.cli.query._spawn_background_worker", return_value=0) as mock_spawn_bg,
        ):
            from gza.cli.query import ResumeOverrideInference, cmd_attach

            mock_infer.return_value = ResumeOverrideInference(no_docker=True, max_turns=77, force=True)
            result = cmd_attach(args)

        assert result == 1
        assert write_calls["count"] == 2
        output_text = capsys.readouterr().out
        assert "anthropic-poison" not in output_text
        assert "[redacted]" in output_text
        mock_spawn_bg.assert_called_once()
        recovery_args = mock_spawn_bg.call_args.args[0]
        assert recovery_args.no_docker is True
        assert recovery_args.max_turns == 77
        assert recovery_args.force is True
        runtime_context = mock_spawn_bg.call_args.kwargs["runtime_context"]
        assert isinstance(runtime_context, RuntimeExecutionContext)
        assert runtime_context.cwd == tmp_path
        assert runtime_context.db_path == Config.load(tmp_path).db_path
        kill_sessions = [
            call.args[0]
            for call in mock_run.call_args_list
            if call.args and call.args[0][:3] == ["tmux", "kill-session", "-t"]
        ]
        assert len(kill_sessions) >= 2

    def test_cmd_attach_recovers_when_post_stop_tmux_run_raises_oserror(
        self,
        tmp_path: Path,
        monkeypatch,
        capsys,
    ):
        """Post-stop tmux OSError must clean env/session state and restart with the same runtime context."""
        from gza.config import Config
        from gza.runtime_context import RuntimeExecutionContext

        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        (tmp_path / ".env").write_text("PATH=/owned/bin\nCODEX_API_KEY=codex-poison\n", encoding="utf-8")
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")
        preflight_env = tmp_path / ".gza" / "tmp" / "preflight.sh"
        create_env = tmp_path / ".gza" / "tmp" / "create.sh"
        write_paths = [preflight_env, create_env]

        def fake_write_env_file(**_kwargs):
            path = write_paths.pop(0)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("secret handoff\n", encoding="utf-8")
            return path

        new_session_calls = {"count": 0}

        def fake_tmux_run(cmd, **_kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                new_session_calls["count"] += 1
                if new_session_calls["count"] == 1:
                    return MagicMock(returncode=0, stderr="")
                raise OSError("tmux spawn failed: codex-poison")
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with (
            patch("gza.cli.query.write_tmux_environment_file", side_effect=fake_write_env_file),
            patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run) as mock_run,
            patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.query.os.kill", side_effect=fake_kill),
            patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer,
            patch("gza.cli.query._spawn_background_worker", return_value=0) as mock_spawn_bg,
        ):
            from gza.cli.query import ResumeOverrideInference, cmd_attach

            mock_infer.return_value = ResumeOverrideInference(no_docker=True, max_turns=77, force=True)
            result = cmd_attach(args)

        assert result == 1
        assert new_session_calls["count"] == 2
        assert not create_env.exists()
        output_text = capsys.readouterr().out
        assert "codex-poison" not in output_text
        assert "[redacted]" in output_text
        mock_spawn_bg.assert_called_once()
        recovery_args = mock_spawn_bg.call_args.args[0]
        assert recovery_args.no_docker is True
        assert recovery_args.max_turns == 77
        assert recovery_args.force is True
        runtime_context = mock_spawn_bg.call_args.kwargs["runtime_context"]
        assert isinstance(runtime_context, RuntimeExecutionContext)
        assert runtime_context.cwd == tmp_path
        assert runtime_context.db_path == Config.load(tmp_path).db_path
        kill_sessions = [
            call.args[0]
            for call in mock_run.call_args_list
            if call.args and call.args[0][:3] == ["tmux", "kill-session", "-t"]
        ]
        assert len(kill_sessions) >= 2

    def test_cmd_attach_recovers_with_prepared_task_when_post_stop_store_update_fails(
        self,
        tmp_path: Path,
        monkeypatch,
        capsys,
    ):
        """A stale in-progress DB row after worker stop must not defeat recovery."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        (tmp_path / ".env").write_text("PATH=/owned/bin\nGEMINI_API_KEY=gemini-poison\n", encoding="utf-8")
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        store = make_store(tmp_path)
        task = store.get_all()[0]
        task_id = task.id

        def fake_tmux_run(cmd, **_kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                return MagicMock(returncode=0, stderr="")
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with (
            patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run),
            patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.query.os.kill", side_effect=fake_kill),
            patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer,
            patch("gza.cli.query.SqliteTaskStore.update", side_effect=OSError("update failed: gemini-poison")),
            patch("gza.cli.query._spawn_background_worker", return_value=0) as mock_spawn_bg,
        ):
            from gza.cli.query import ResumeOverrideInference, cmd_attach

            mock_infer.return_value = ResumeOverrideInference(no_docker=True, max_turns=77, force=True)
            result = cmd_attach(args)

        assert result == 1
        output_text = capsys.readouterr().out
        assert "gemini-poison" not in output_text
        assert "[redacted]" in output_text
        mock_spawn_bg.assert_called_once()
        prepared_task = mock_spawn_bg.call_args.kwargs["prepared_task"]
        assert prepared_task.id == task_id
        assert prepared_task.status == "pending"
        assert prepared_task.running_pid is None
        recovery_args = mock_spawn_bg.call_args.args[0]
        assert recovery_args.no_docker is True
        assert recovery_args.max_turns == 77
        assert recovery_args.force is True

        stale_row = make_store(tmp_path).get(task_id)
        assert stale_row is not None
        assert stale_row.status == "in_progress"
        assert stale_row.running_pid == 12345

    def test_cmd_attach_exception_recovery_failure_marks_failed_and_logs_sanitized_handoff(
        self,
        tmp_path: Path,
        monkeypatch,
    ):
        """If exception recovery cannot restart the worker, the task is durably failed and logged."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        (tmp_path / ".env").write_text("PATH=/owned/bin\nCODEX_API_KEY=codex-poison\n", encoding="utf-8")
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        store = make_store(tmp_path)
        task = store.get_all()[0]
        task_id = task.id
        log_path = tmp_path / task.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        create_env = tmp_path / ".gza" / "tmp" / "create.sh"
        write_calls = {"count": 0}
        new_session_calls = {"count": 0}

        def fake_write_env_file(**_kwargs):
            write_calls["count"] += 1
            if write_calls["count"] == 2:
                path = create_env
            else:
                path = tmp_path / ".gza" / "tmp" / f"env-{write_calls['count']}.sh"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("secret handoff\n", encoding="utf-8")
            return path

        def fake_tmux_run(cmd, **_kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                new_session_calls["count"] += 1
                if new_session_calls["count"] == 1:
                    return MagicMock(returncode=0, stderr="")
                raise OSError("tmux spawn failed: codex-poison")
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with (
            patch("gza.cli.query.write_tmux_environment_file", side_effect=fake_write_env_file),
            patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run),
            patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.query.os.kill", side_effect=fake_kill),
            patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer,
            patch("gza.cli.query._spawn_background_worker", return_value=9) as mock_spawn_bg,
        ):
            from gza.cli.query import ResumeOverrideInference, cmd_attach

            mock_infer.return_value = ResumeOverrideInference(no_docker=True, max_turns=77, force=True)
            result = cmd_attach(args)

        assert result == 1
        assert not create_env.exists()
        mock_spawn_bg.assert_called_once()
        recovery_args = mock_spawn_bg.call_args.args[0]
        assert recovery_args.no_docker is True
        assert recovery_args.max_turns == 77
        assert recovery_args.force is True

        refreshed = make_store(tmp_path).get(task_id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "WORKER_DIED"

        import json as _json

        from gza.log_paths import ops_log_path_for

        events = [
            _json.loads(line)
            for line in ops_log_path_for(log_path).read_text().splitlines()
            if line.strip()
        ]
        handoff_events = [
            e for e in events
            if e.get("subtype") == "worker_lifecycle" and e.get("event") == "handoff_failed"
        ]
        assert handoff_events
        assert handoff_events[-1]["reason"] == "WORKER_DIED"
        assert handoff_events[-1]["recovery_exit_code"] == 9
        assert "codex-poison" not in handoff_events[-1]["tmux_error"]
        assert "[redacted]" in handoff_events[-1]["tmux_error"]

    def test_cmd_attach_claude_marks_failed_when_session_and_recovery_both_fail(self, tmp_path: Path, monkeypatch):
        """If post-stop session create fails AND recovery also fails, task must end in failed/WORKER_DIED with a handoff log."""
        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        store = make_store(tmp_path)
        task = store.get_all()[0]
        task_id = task.id
        log_rel = task.log_file
        log_path = tmp_path / log_rel
        log_path.parent.mkdir(parents=True, exist_ok=True)

        call_state = {"new_session_calls": 0}

        def fake_tmux_run(cmd, **kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                call_state["new_session_calls"] += 1
                if call_state["new_session_calls"] == 1:
                    return MagicMock(returncode=0, stderr="")
                return MagicMock(returncode=1, stderr="create failed")
            return MagicMock(returncode=0, stderr="")

        def fake_kill(_pid: int, sig: int):
            if sig == 0:
                raise OSError("no such process")
            return None

        with patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run), \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.kill", side_effect=fake_kill), \
             patch("gza.cli.query._infer_resume_overrides_from_worker") as mock_infer, \
             patch("gza.cli.query._spawn_background_worker", return_value=1) as mock_spawn_bg:
            from gza.cli.query import ResumeOverrideInference
            mock_infer.return_value = ResumeOverrideInference()
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 1
        assert call_state["new_session_calls"] == 2
        mock_spawn_bg.assert_called_once()

        refreshed = make_store(tmp_path).get(task_id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "WORKER_DIED"

        import json as _json

        from gza.log_paths import ops_log_path_for

        events = [
            _json.loads(line)
            for line in ops_log_path_for(log_path).read_text().splitlines()
            if line.strip()
        ]
        handoff_events = [
            e for e in events
            if e.get("subtype") == "worker_lifecycle" and e.get("event") == "handoff_failed"
        ]
        assert handoff_events, "expected handoff_failed lifecycle event"
        assert handoff_events[-1]["reason"] == "WORKER_DIED"
        assert handoff_events[-1]["recovery_exit_code"] == 1

    def test_cmd_attach_claude_aborts_if_worker_still_alive_after_escalation(self, tmp_path: Path, monkeypatch):
        """Attach must fail safely if worker remains alive after SIGTERM/SIGKILL escalation."""
        from gza.workers import WorkerRegistry

        self._setup_running_worker(
            tmp_path,
            task_id=1,
            provider="claude",
            session_id="ses_attach_123",
        )
        monkeypatch.delenv("TMUX", raising=False)
        args = _make_args(tmp_path, worker_id="w-20260301-1")

        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.status = "in_progress"
        task.running_pid = 12345
        store.update(task)

        call_state = {"new_session_calls": 0}

        def fake_tmux_run(cmd, **kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                call_state["new_session_calls"] += 1
            return MagicMock(returncode=0, stderr="")

        def fake_kill(pid: int, sig: int):
            assert pid == 12345
            if sig in (signal.SIGTERM, signal.SIGKILL, 0):
                return None
            raise AssertionError(f"unexpected signal: {sig}")

        # First call sets SIGTERM deadline. Next calls force immediate escalation and failed post-kill check.
        fake_times = [0.0, 0.0, 3.1, 3.1, 3.2, 4.3, 4.3]

        with patch("gza.cli.query.subprocess.run", side_effect=fake_tmux_run), \
             patch("gza.cli.query.shutil.which", return_value="/usr/bin/tmux"), \
             patch("gza.cli.query.os.kill", side_effect=fake_kill), \
             patch("gza.cli.query.time.time", side_effect=fake_times), \
             patch("gza.cli.query.time.sleep"), \
             patch("gza.cli.query._spawn_background_worker") as mock_spawn_bg:
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 1
        assert call_state["new_session_calls"] == 1, "Only tmux preflight should run before stop failure abort"
        mock_spawn_bg.assert_not_called()

        registry = WorkerRegistry(tmp_path / ".gza" / "workers")
        worker = registry.get("w-20260301-1")
        assert worker is not None
        assert worker.status == "running"

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "in_progress"
        assert refreshed.running_pid == 12345


class TestInferResumeOverrides:
    """Tests for _infer_resume_overrides_from_worker (cross-platform ps-based)."""

    def _runtime_context(self, tmp_path: Path):
        from gza.runtime_context import RuntimeExecutionContext

        project = tmp_path / "runtime-project"
        project.mkdir()
        return RuntimeExecutionContext(
            cwd=project,
            env={"PATH": "/runtime/bin", "PWD": "/supervisor-pwd"},
            project_id="runtime",
            db_path=project / ".gza" / "gza.db",
        )

    def test_parses_no_docker_and_max_turns(self, tmp_path: Path):
        from gza.cli.query import _infer_resume_overrides_from_worker
        from gza.workers import WorkerMetadata

        worker = MagicMock(spec=WorkerMetadata)
        worker.worker_id = "w-1"
        worker.pid = 99999
        runtime_context = self._runtime_context(tmp_path)

        fake_result = MagicMock(
            returncode=0,
            stdout="gza implement --no-docker --max-turns 42 --force some-task\n",
        )
        with patch("gza.cli.query.subprocess.run", return_value=fake_result) as mock_run:
            result = _infer_resume_overrides_from_worker(worker, runtime_context=runtime_context)

        assert result.error is None
        assert result.no_docker is True
        assert result.max_turns == 42
        assert result.force is True
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd == ["ps", "-p", "99999", "-o", "args="]
        assert mock_run.call_args.kwargs["cwd"] == runtime_context.cwd
        assert mock_run.call_args.kwargs["env"]["PATH"] == "/runtime/bin"
        assert mock_run.call_args.kwargs["env"]["PWD"] == str(runtime_context.cwd.resolve())

    @pytest.mark.parametrize(
        ("side_effect", "fake_result", "expected_error"),
        [
            (subprocess.TimeoutExpired(cmd="ps", timeout=5), None, "timed out"),
            (FileNotFoundError(), None, "not found"),
            (None, MagicMock(returncode=1, stdout="", stderr="denied"), "ps failed"),
            (None, MagicMock(returncode=0, stdout="", stderr=""), "returned no command line"),
            (None, MagicMock(returncode=0, stdout="gza implement --max-turns nope task\n", stderr=""), "malformed"),
            (None, MagicMock(returncode=0, stdout="gza implement --max-turns=NaN task\n", stderr=""), "malformed"),
        ],
    )
    def test_reports_introspection_failures_without_downgrading_overrides(
        self,
        tmp_path: Path,
        side_effect,
        fake_result,
        expected_error: str,
    ):
        from gza.cli.query import _infer_resume_overrides_from_worker
        from gza.workers import WorkerMetadata

        worker = MagicMock(spec=WorkerMetadata)
        worker.worker_id = "w-1"
        worker.pid = 99999
        runtime_context = self._runtime_context(tmp_path)

        kwargs = {"side_effect": side_effect} if side_effect is not None else {"return_value": fake_result}
        with patch("gza.cli.query.subprocess.run", **kwargs):
            result = _infer_resume_overrides_from_worker(worker, runtime_context=runtime_context)

        assert result.error is not None
        assert expected_error in result.error
        assert result.no_docker is False
        assert result.max_turns is None
        assert result.force is False

    def test_returns_defaults_when_no_overrides_in_cmdline(self, tmp_path: Path):
        from gza.cli.query import _infer_resume_overrides_from_worker
        from gza.workers import WorkerMetadata

        worker = MagicMock(spec=WorkerMetadata)
        worker.worker_id = "w-1"
        worker.pid = 99999
        runtime_context = self._runtime_context(tmp_path)

        fake_result = MagicMock(
            returncode=0,
            stdout="gza implement some-task\n",
        )
        with patch("gza.cli.query.subprocess.run", return_value=fake_result):
            result = _infer_resume_overrides_from_worker(worker, runtime_context=runtime_context)

        assert result.error is None
        assert result.no_docker is False
        assert result.max_turns is None
        assert result.force is False

    def test_parses_max_turns_equals_format(self, tmp_path: Path):
        from gza.cli.query import _infer_resume_overrides_from_worker
        from gza.workers import WorkerMetadata

        worker = MagicMock(spec=WorkerMetadata)
        worker.worker_id = "w-1"
        worker.pid = 99999
        runtime_context = self._runtime_context(tmp_path)

        fake_result = MagicMock(
            returncode=0,
            stdout="gza implement --max-turns=77 some-task\n",
        )
        with patch("gza.cli.query.subprocess.run", return_value=fake_result):
            result = _infer_resume_overrides_from_worker(worker, runtime_context=runtime_context)

        assert result.error is None
        assert result.no_docker is False
        assert result.max_turns == 77
        assert result.force is False


class TestSpawnBackgroundWorkerTmux:
    """Tests for tmux integration in _spawn_background_worker."""

    def _make_config(self, tmp_path: Path, tmux_enabled: bool = True):
        from gza.config import Config

        config_content = (
            "project_name: test\nprovider: codex\nmodel: gpt-5.5\n"
            f"tmux:\n  enabled: {'true' if tmux_enabled else 'false'}\n"
        )
        (tmp_path / "gza.yaml").write_text(config_content)
        (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)
        return Config.load(tmp_path)

    def _make_same_identity_config(self, project_dir: Path, db_path: Path):
        from gza.config import Config

        project_dir.mkdir(parents=True)
        (project_dir / ".gza").mkdir(parents=True, exist_ok=True)
        (project_dir / ".env").write_text("PROJECT_TOKEN=owned\n", encoding="utf-8")
        (project_dir / "gza.yaml").write_text(
            "\n".join(
                [
                    "project_name: same",
                    "project_id: sameproj",
                    "project_prefix: same",
                    f"db_path: {db_path}",
                    "provider: codex",
                    "model: gpt-5.5",
                    "tmux:",
                    "  enabled: true",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return Config.load(project_dir)

    def test_tmux_launch_names_are_distinct_for_same_task_id_in_different_databases(
        self,
        tmp_path: Path,
    ) -> None:
        """Tmux launch identity includes project/database identity, not just task ID."""
        from gza.cli._common import _spawn_background_worker
        from gza.db import SqliteTaskStore

        config_a = self._make_same_identity_config(tmp_path / "a", tmp_path / "a.db")
        config_b = self._make_same_identity_config(tmp_path / "b", tmp_path / "b.db")
        store_a = SqliteTaskStore.from_config(config_a)
        store_b = SqliteTaskStore.from_config(config_b)
        task_a = store_a.add("test task")
        task_b = store_b.add("test task")
        assert task_a.id == task_b.id
        for task, store in ((task_a, store_a), (task_b, store_b)):
            task.provider = "codex"
            task.provider_is_explicit = True
            store.update(task)

        run_calls: list[tuple[list[str], dict]] = []

        def fake_run(cmd, **kwargs):
            run_calls.append((cmd, kwargs))
            return MagicMock(returncode=0)

        stores_by_dir = {config_a.project_dir: store_a, config_b.project_dir: store_b}

        with (
            patch("gza.cli._common.subprocess.run", side_effect=fake_run),
            patch("gza.cli._common.get_tmux_session_pid", side_effect=[1111, 2222]),
            patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task),
            patch("gza.cli._common.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli._common.get_store", side_effect=lambda cfg: stores_by_dir[cfg.project_dir]),
        ):
            assert _spawn_background_worker(_make_args(config_a.project_dir), config_a, task_id=task_a.id) == 0
            assert _spawn_background_worker(_make_args(config_b.project_dir), config_b, task_id=task_b.id) == 0

        new_session_names = [
            cmd[cmd.index("-s") + 1]
            for cmd, _kwargs in run_calls
            if cmd[:2] == ["tmux", "new-session"]
        ]
        kill_session_names = [
            cmd[cmd.index("-t") + 1]
            for cmd, _kwargs in run_calls
            if cmd[:2] == ["tmux", "kill-session"]
        ]

        assert len(new_session_names) == 2
        assert new_session_names[0] != new_session_names[1]
        assert f"gza-{task_a.id}" not in new_session_names
        assert kill_session_names == new_session_names

    def test_tmux_failed_launch_rollback_uses_owning_session_cwd_and_env(
        self,
        tmp_path: Path,
    ) -> None:
        """Rollback after a post-session failure must kill only the owning session."""
        from gza.cli._common import _spawn_background_worker
        from gza.db import SqliteTaskStore

        config = self._make_same_identity_config(tmp_path / "project", tmp_path / "project.db")
        store = SqliteTaskStore.from_config(config)
        task = store.add("test task")
        task.provider = "codex"
        task.provider_is_explicit = True
        store.update(task)
        run_calls: list[tuple[list[str], dict]] = []

        def fake_run(cmd, **kwargs):
            run_calls.append((cmd, kwargs))
            if cmd[:2] == ["tmux", "set-option"]:
                raise RuntimeError("post-session failure")
            return MagicMock(returncode=0)

        with (
            patch("gza.cli._common.subprocess.run", side_effect=fake_run),
            patch("gza.cli._common.get_tmux_session_pid", return_value=9999),
            patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task),
            patch("gza.cli._common.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli._common.get_store", return_value=store),
        ):
            assert _spawn_background_worker(_make_args(config.project_dir), config, task_id=task.id) == 1

        new_session = next(
            cmd[cmd.index("-s") + 1]
            for cmd, _kwargs in run_calls
            if cmd[:2] == ["tmux", "new-session"]
        )
        rollback_kill_cmd, rollback_kill_kwargs = [
            (cmd, kwargs)
            for cmd, kwargs in run_calls
            if cmd[:2] == ["tmux", "kill-session"]
        ][-1]

        assert rollback_kill_cmd[rollback_kill_cmd.index("-t") + 1] == new_session
        assert rollback_kill_kwargs["cwd"] == config.project_dir
        assert rollback_kill_kwargs["env"]["PROJECT_TOKEN"] == "owned"

    def test_spawn_background_worker_uses_tmux_when_enabled(self, tmp_path: Path):
        """_spawn_background_worker calls tmux new-session when config.tmux.enabled is True."""

        config = self._make_config(tmp_path, tmux_enabled=True)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
        task.provider_is_explicit = True
        store.update(task)

        args = _make_args(tmp_path)

        tmux_run_result = MagicMock(returncode=0)
        mock_pid_result = 9999

        with patch("gza.cli._common.subprocess.run", return_value=tmux_run_result) as mock_run, \
             patch("gza.cli._common.get_tmux_session_pid", return_value=mock_pid_result), \
             patch("gza.cli._common.get_store") as mock_get_store, \
             patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task), \
             patch("gza.cli._common.shutil.which", return_value="/usr/bin/tmux"):
            mock_get_store.return_value = store
            from gza.cli._common import _spawn_background_worker
            result = _spawn_background_worker(args, config, task_id=task.id)

        assert result == 0
        # Verify tmux kill-session + new-session + set-option were called
        tmux_calls = [c for c in mock_run.call_args_list if c[0][0][0] == "tmux"]
        assert len(tmux_calls) == 3, "Expected kill-session + new-session + set-option tmux commands"
        kill_args = tmux_calls[0][0][0]
        assert "kill-session" in kill_args
        new_args = tmux_calls[1][0][0]
        assert "new-session" in new_args
        assert "-d" in new_args
        assert new_args[new_args.index("-c") + 1] == str(tmp_path.resolve())
        assert "-e" not in new_args
        assert str(config.db_path.resolve()) not in "\0".join(new_args)
        env_file = Path(new_args[new_args.index("gza-tmux-env") + 1])
        env_file_text = env_file.read_text(encoding="utf-8")
        assert f"export GZA_DB_PATH={config.db_path.resolve()}" in env_file_text
        assert f"export PWD={tmp_path.resolve()}" in env_file_text
        set_args = tmux_calls[2][0][0]
        assert "set-option" in set_args
        assert "remain-on-exit" in set_args

    def test_spawn_background_worker_tmux_failure_sanitizes_runtime_secrets_and_keeps_env_handoff(
        self,
        tmp_path: Path,
        capsys,
    ):
        """Background tmux launch failures must not disclose runtime env values."""
        config = self._make_config(tmp_path, tmux_enabled=True)
        (tmp_path / ".env").write_text(
            "\n".join(
                [
                    "PATH=/owned/bin",
                    "ANTHROPIC_API_KEY=anthropic-poison",
                    "CODEX_API_KEY=codex-poison",
                    "GEMINI_API_KEY=gemini-poison",
                    "PROJECT_ONLY_TOKEN=arbitrary-poison",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
        task.provider_is_explicit = True
        store.update(task)
        args = _make_args(tmp_path)
        new_session_calls: list[tuple[list[str], dict]] = []

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["tmux", "new-session"]:
                new_session_calls.append((cmd, kwargs))
                raise subprocess.CalledProcessError(
                    1,
                    cmd,
                    stderr="anthropic-poison codex-poison gemini-poison arbitrary-poison",
                )
            return MagicMock(returncode=0, stderr="")

        with (
            patch("gza.cli._common.subprocess.run", side_effect=fake_run),
            patch("gza.cli._common.get_tmux_session_pid", return_value=9999),
            patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task),
            patch("gza.cli._common.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli._common.get_store", return_value=store),
        ):
            from gza.cli._common import _spawn_background_worker

            result = _spawn_background_worker(args, config, task_id=task.id)

        assert result == 1
        captured = capsys.readouterr()
        output_text = captured.out + captured.err
        for secret in ("anthropic-poison", "codex-poison", "gemini-poison", "arbitrary-poison"):
            assert secret not in output_text
        assert new_session_calls
        new_cmd, new_kwargs = new_session_calls[0]
        assert "-e" not in new_cmd
        argv_text = "\0".join(new_cmd)
        for secret in ("anthropic-poison", "codex-poison", "gemini-poison", "arbitrary-poison"):
            assert secret not in argv_text
        tmux_env = new_kwargs["env"]
        assert tmux_env["PATH"] == "/owned/bin"
        assert tmux_env["PWD"] == str(tmp_path.resolve())
        assert tmux_env["GZA_DB_PATH"] == str(config.db_path.resolve())
        assert tmux_env["ANTHROPIC_API_KEY"] == "anthropic-poison"
        assert tmux_env["CODEX_API_KEY"] == "codex-poison"
        assert tmux_env["GEMINI_API_KEY"] == "gemini-poison"
        assert tmux_env["PROJECT_ONLY_TOKEN"] == "arbitrary-poison"

    def test_spawn_warns_on_remain_on_exit_failure(self, tmp_path: Path, capsys):
        """_spawn_background_worker warns when remain-on-exit set-option fails."""

        config = self._make_config(tmp_path, tmux_enabled=True)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
        task.provider_is_explicit = True
        store.update(task)

        args = _make_args(tmp_path)
        mock_pid_result = 9999

        def side_effect_fn(cmd, **kwargs):
            # Return failure for the set-option remain-on-exit call
            if cmd[0] == "tmux" and "set-option" in cmd and "remain-on-exit" in cmd:
                return MagicMock(returncode=1)
            result = MagicMock(returncode=0)
            return result

        with patch("gza.cli._common.subprocess.run", side_effect=side_effect_fn), \
             patch("gza.cli._common.get_tmux_session_pid", return_value=mock_pid_result), \
             patch("gza.cli._common.get_store") as mock_get_store, \
             patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task), \
             patch("gza.cli._common.shutil.which", return_value="/usr/bin/tmux"):
            mock_get_store.return_value = store
            from gza.cli._common import _spawn_background_worker
            result = _spawn_background_worker(args, config, task_id=task.id)

        assert result == 0, "Spawn should still succeed even if set-option fails"
        captured = capsys.readouterr()
        assert "remain-on-exit" in captured.err, "Warning about remain-on-exit failure should be printed to stderr"

    def test_spawn_background_worker_skips_tmux_when_disabled(self, tmp_path: Path):
        """_spawn_background_worker uses bare Popen when config.tmux.enabled is False."""

        config = self._make_config(tmp_path, tmux_enabled=False)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
        task.provider_is_explicit = True
        store.update(task)

        args = _make_args(tmp_path)

        mock_proc = MagicMock()
        mock_proc.pid = 1234

        with patch("gza.cli._common.subprocess.Popen", return_value=mock_proc) as mock_popen, \
             patch("gza.cli._common.subprocess.run") as mock_run, \
             patch("gza.cli._common.get_store") as mock_get_store, \
             patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task):
            mock_get_store.return_value = store
            from gza.cli._common import _spawn_background_worker
            result = _spawn_background_worker(args, config, task_id=task.id)

        assert result == 0
        # Popen should be called (bare subprocess)
        assert mock_popen.called
        # tmux new-session should NOT be called
        tmux_calls = [c for c in mock_run.call_args_list if c[0] and c[0][0] and c[0][0][0] == "tmux"]
        assert len(tmux_calls) == 0, "tmux should NOT be called when disabled"


    def test_spawn_kills_existing_tmux_session(self, tmp_path: Path):
        """_spawn_background_worker calls tmux kill-session before tmux new-session (M4)."""

        config = self._make_config(tmp_path, tmux_enabled=True)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
        task.provider_is_explicit = True
        store.update(task)

        args = _make_args(tmp_path)

        tmux_run_result = MagicMock(returncode=0)
        mock_pid_result = 9999

        with patch("gza.cli._common.subprocess.run", return_value=tmux_run_result) as mock_run, \
             patch("gza.cli._common.get_tmux_session_pid", return_value=mock_pid_result), \
             patch("gza.cli._common.get_store") as mock_get_store, \
             patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task), \
             patch("gza.cli._common.shutil.which", return_value="/usr/bin/tmux"):
            mock_get_store.return_value = store
            from gza.cli._common import _spawn_background_worker
            result = _spawn_background_worker(args, config, task_id=task.id)

        assert result == 0
        tmux_calls = [c for c in mock_run.call_args_list if c[0][0][0] == "tmux"]
        # kill-session must come before new-session
        assert len(tmux_calls) >= 2
        assert "kill-session" in tmux_calls[0][0][0]
        assert "new-session" in tmux_calls[1][0][0]

    def test_spawn_warns_when_tmux_unavailable(self, tmp_path: Path, capsys):
        """_spawn_background_worker prints a warning and falls back when tmux is not found (S2)."""

        config = self._make_config(tmp_path, tmux_enabled=True)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
        task.provider_is_explicit = True
        store.update(task)

        args = _make_args(tmp_path)

        mock_proc = MagicMock()
        mock_proc.pid = 1234

        with patch("gza.cli._common.subprocess.Popen", return_value=mock_proc), \
             patch("gza.cli._common.get_store") as mock_get_store, \
             patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task), \
             patch("gza.cli._common.shutil.which", return_value=None):  # tmux not found
            mock_get_store.return_value = store
            from gza.cli._common import _spawn_background_worker
            result = _spawn_background_worker(args, config, task_id=task.id)

        assert result == 0
        captured = capsys.readouterr()
        assert "tmux" in captured.err.lower() and "not found" in captured.err.lower()


class TestClaudeProviderTmuxMode:
    """Tests for Claude provider interactive mode in tmux sessions (M1/M2/M3)."""

    def _make_config(self, tmp_path: Path, tmux_session: str | None = None):
        from gza.config import Config

        config_content = "project_name: test\nprovider: claude\nmodel: claude-sonnet-4\n"
        (tmp_path / "gza.yaml").write_text(config_content)
        (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        if tmux_session:
            config.tmux.session_name = tmux_session
        return config

    def test_claude_provider_uses_print_mode_when_no_tmux(self, tmp_path: Path):
        """ClaudeProvider uses -p - flags (non-interactive) when no tmux session is set (M1 baseline)."""
        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session=None)
        log_file = tmp_path / ".gza" / "logs" / "test.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()
        captured_cmd: list[list[str]] = []

        def fake_run_with_output(cmd, *args, **kwargs):
            captured_cmd.append(cmd)
            from gza.providers.base import RunResult
            return RunResult(exit_code=0)

        with patch.object(provider, "_run_with_output_parsing", side_effect=fake_run_with_output):
            provider._run_direct(config, "test prompt", log_file, tmp_path)

        assert captured_cmd, "Expected _run_with_output_parsing to be called"
        cmd = captured_cmd[0]
        assert "-p" in cmd, "Non-tmux mode should use -p flag"
        assert "--output-format" in cmd, "Non-tmux mode should use --output-format"

    def test_claude_provider_interactive_foreground_non_tmux_uses_true_interactive_cli(self, tmp_path: Path):
        """Interactive foreground mode should avoid print-mode stream-json flags."""
        import io

        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session=None)
        config.use_docker = False
        log_file = tmp_path / ".gza" / "logs" / "test.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        provider = ClaudeProvider()

        with (
            patch.object(provider, "_run_with_output_parsing") as mock_stream,
            patch("gza.providers.claude.subprocess.Popen") as mock_popen,
            patch("gza.providers.claude.sys.stdout", new=io.StringIO()),
        ):
            mock_process = MagicMock()
            mock_process.stdout = iter([])
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process
            provider.run(config, "interactive task prompt", log_file, tmp_path, interactive=True)

        mock_stream.assert_not_called()
        mock_popen.assert_called_once()
        cmd = mock_popen.call_args[0][0]
        assert "-p" not in cmd
        assert "--output-format" not in cmd
        assert "--max-turns" in cmd
        assert "interactive task prompt" not in cmd

    def test_claude_provider_uses_interactive_mode_in_tmux_session(self, tmp_path: Path):
        """ClaudeProvider omits -p flag and uses interactive mode when tmux session is set (M1)."""
        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session="gza-42")
        log_file = tmp_path / ".gza" / "logs" / "test.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()

        fake_result = MagicMock()
        fake_result.returncode = 0

        with patch("gza.providers.claude.subprocess.run", return_value=fake_result) as mock_run:
            result = provider._run_direct(config, "test prompt", log_file, tmp_path)

        # Should call subprocess.run (interactive), NOT _run_with_output_parsing
        assert mock_run.called, "Interactive mode should call subprocess.run directly"
        cmd = mock_run.call_args[0][0]
        assert "-p" not in cmd, "Interactive mode must NOT use -p flag"
        assert "--output-format" not in cmd, "Interactive mode must NOT use --output-format"
        assert result.exit_code == 0

    def test_prompt_not_passed_as_positional_arg_in_tmux_mode(self, tmp_path: Path):
        """In tmux mode, prompt is NOT a positional arg — proxy delivers it via PTY (M1)."""
        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session="gza-42")
        log_file = tmp_path / ".gza" / "logs" / "42.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()
        task_prompt = "Implement a hello world function"

        fake_result = MagicMock()
        fake_result.returncode = 0

        with patch("gza.providers.claude.subprocess.run", return_value=fake_result) as mock_run:
            provider._run_direct(config, task_prompt, log_file, tmp_path)

        # subprocess.run should be called at least once (first for tmux pipe-pane, then claude)
        assert mock_run.called
        # The claude invocation is the last subprocess.run call
        claude_call = mock_run.call_args_list[-1]
        cmd = claude_call[0][0]
        assert task_prompt not in cmd, (
            "Task prompt must NOT be passed to Claude as a positional argument in tmux mode; "
            "the proxy delivers it via PTY"
        )
        assert "claude" in cmd[0], "Claude must still be invoked"

    def test_tmux_pipe_pane_captures_raw_output(self, tmp_path: Path):
        """_run_direct_tmux sets up tmux pipe-pane using the runtime cwd/env."""
        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session="gza-42")
        runtime_cwd = tmp_path / "runtime-cwd"
        runtime_cwd.mkdir()
        config.provider_cwd = runtime_cwd
        runtime_env = {"PATH": "/runtime/bin", "TMUX_TMPDIR": "/runtime-tmux", "PWD": "/poisoned"}
        log_file = tmp_path / ".gza" / "logs" / "42.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()
        fake_result = MagicMock()
        fake_result.returncode = 0

        pipe_pane_call = {}

        def fake_run(cmd, *args, **kwargs):
            if cmd[0] == "tmux" and "pipe-pane" in cmd:
                pipe_pane_call["kwargs"] = kwargs
            return fake_result

        with patch("gza.providers.claude.subprocess.run", side_effect=fake_run):
            provider._run_direct_tmux(config, "test prompt", log_file, tmp_path, env=runtime_env)

        assert pipe_pane_call, (
            "tmux pipe-pane must be called to capture raw terminal output to the main log file"
        )
        assert pipe_pane_call["kwargs"]["cwd"] == runtime_cwd
        assert pipe_pane_call["kwargs"]["env"] == {
            **runtime_env,
            "PWD": str(runtime_cwd.resolve()),
        }

    def test_proxy_events_written_to_separate_log(self, tmp_path: Path):
        """Proxy JSONL events go to a separate *-proxy.log file, not the main log (M2)."""
        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session="gza-42")
        log_file = tmp_path / ".gza" / "logs" / "42.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()
        fake_result = MagicMock()
        fake_result.returncode = 0

        with patch("gza.providers.claude.subprocess.run", return_value=fake_result):
            provider._run_direct_tmux(config, "test prompt", log_file, tmp_path)

        proxy_log = log_file.parent / "42-proxy.log"
        assert proxy_log.exists(), "Proxy events log file must be created at <stem>-proxy.log"

        proxy_content = proxy_log.read_text()
        assert "tmux_start" in proxy_content, "Proxy log must contain tmux_start event"
        assert "tmux_end" in proxy_content, "Proxy log must contain tmux_end event"

        # Main log should NOT contain proxy JSONL events (tmux pipe-pane writes raw output)
        if log_file.exists():
            main_content = log_file.read_text()
            assert "tmux_start" not in main_content, (
                "Main log must NOT contain proxy JSONL events; those belong in -proxy.log"
            )

    def test_log_parsing_handles_tmux_mode_logs(self, tmp_path: Path):
        """Main log is clean terminal output; proxy log is JSONL — compatible with parsers (M2)."""
        import json

        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session="gza-42")
        log_file = tmp_path / ".gza" / "logs" / "42.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()
        fake_result = MagicMock()
        fake_result.returncode = 0

        with patch("gza.providers.claude.subprocess.run", return_value=fake_result):
            provider._run_direct_tmux(config, "test prompt", log_file, tmp_path)

        # Proxy log should be valid JSONL
        proxy_log = log_file.parent / "42-proxy.log"
        assert proxy_log.exists()
        for line in proxy_log.read_text().splitlines():
            if line.strip():
                event = json.loads(line)  # must parse without error
                assert event.get("type") == "gza"
                assert event.get("subtype") in ("tmux_start", "tmux_end")

    def test_spawn_background_worker_disables_tmux_proxy_for_claude(self, tmp_path: Path):
        """Claude workers should default to pipe-mode background execution (no tmux proxy)."""

        config_content = "project_name: test\nprovider: claude\nmodel: claude-sonnet-4\ntmux:\n  enabled: true\n"
        (tmp_path / "gza.yaml").write_text(config_content)
        (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)
        from gza.config import Config
        config = Config.load(tmp_path)

        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("Implement hello world feature")
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path, no_docker=False, max_turns=None
        )

        tmux_run_result = MagicMock(returncode=0)

        with patch("gza.cli._common.subprocess.run", return_value=tmux_run_result) as mock_run, \
             patch("gza.cli._common.get_tmux_session_pid", return_value=9999), \
             patch("gza.cli._common.get_store") as mock_get_store, \
             patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task), \
             patch(
                 "gza.cli._common._spawn_detached_worker_process",
                 return_value=(MagicMock(pid=4242), ".gza/workers/w-test-startup.log"),
             ), \
             patch("gza.cli._common.shutil.which", return_value="/usr/bin/tmux"):
            mock_get_store.return_value = store
            from gza.cli._common import _spawn_background_worker
            result = _spawn_background_worker(args, config, task_id=task.id)

        assert result == 0
        tmux_calls = [c for c in mock_run.call_args_list if c[0][0][0] == "tmux"]
        assert tmux_calls == [], "tmux proxy path should be disabled for Claude by default"

    def test_tmux_session_set_on_config_in_worker_mode(self, tmp_path: Path):
        """_run_as_worker propagates args.tmux_session to config.tmux.session_name (M3)."""
        from gza.config import Config

        config_content = "project_name: test\nprovider: claude\nmodel: claude-sonnet-4\n"
        (tmp_path / "gza.yaml").write_text(config_content)
        (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)

        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        store.update(task)

        captured_config: list = []

        def fake_run(cfg, **kwargs):
            captured_config.append(cfg)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=False,
            max_turns=None,
            worker_mode=True,
            resume=False,
            task_ids=[task.id],
            tmux_session="gza-42",
        )

        with patch("gza.cli._common.run", side_effect=fake_run), \
             patch("gza.cli._common.get_store", return_value=store), \
             patch("gza.cli._common.WorkerRegistry") as mock_registry:
            mock_registry.return_value.list_all.return_value = []
            mock_registry.return_value.get.return_value = None
            mock_registry.return_value.mark_completed.return_value = None
            from gza.cli._common import _run_as_worker
            _run_as_worker(args, config)

        assert captured_config, "run() should have been called"
        assert captured_config[0].tmux.session_name == "gza-42", \
            "config.tmux.session_name should be set from args.tmux_session"
