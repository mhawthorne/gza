"""Tests for tmux-related CLI functionality: attach command and tmux spawn logic."""

import argparse
import signal
from pathlib import Path
from unittest.mock import MagicMock, patch

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

        with patch("gza.cli.query.subprocess.run", return_value=tmux_has_session), \
             patch("gza.cli.query.os.execvp") as mock_execvp:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvp.assert_called_once()
        call_args = mock_execvp.call_args[0]
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

        with patch("gza.cli.query.subprocess.run", return_value=tmux_has_session), \
             patch("gza.cli.query.os.execvp") as mock_execvp:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvp.assert_called_once()

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
        tmux_no_session = MagicMock(returncode=1)

        with patch("gza.cli.query.subprocess.run", return_value=tmux_no_session):
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 1

    def test_cmd_attach_prints_warning_for_observe_only_provider(self, tmp_path: Path, capsys, monkeypatch):
        """cmd_attach attaches read-only and prints notice for codex/gemini providers."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")
        monkeypatch.delenv("TMUX", raising=False)

        args = _make_args(tmp_path, worker_id="w-20260301-1")
        tmux_has_session = MagicMock(returncode=0)

        with patch("gza.cli.query.subprocess.run", return_value=tmux_has_session), \
             patch("gza.cli.query.os.execvp") as mock_execvp:
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        # Should attach with -r (read-only) flag
        mock_execvp.assert_called_once()
        call_args = mock_execvp.call_args[0]
        assert "-r" in call_args[1], "Observe-only providers should attach read-only (-r)"

        captured = capsys.readouterr()
        assert "headless" in captured.out.lower() or "observe" in captured.out.lower()

    def test_cmd_attach_uses_switch_client_inside_tmux(self, tmp_path: Path):
        """cmd_attach uses switch-client instead of attach-session when already in tmux."""
        self._setup_running_worker(tmp_path, task_id=1, tmux_session="gza-1", provider="codex")

        args = _make_args(tmp_path, worker_id="w-20260301-1")
        tmux_has_session = MagicMock(returncode=0)

        with patch("gza.cli.query.subprocess.run", return_value=tmux_has_session) as mock_run, \
             patch("gza.cli.query.os.execvp") as mock_execvp, \
             patch.dict("os.environ", {"TMUX": "/tmp/tmux-501/default,12345,0"}):
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvp.assert_called_once()
        call_args = mock_execvp.call_args[0]
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

        with patch("gza.cli.query.subprocess.run", return_value=tmux_has_session) as mock_run, \
             patch("gza.cli.query.os.execvp") as mock_execvp, \
             patch.dict("os.environ", {"TMUX": "/tmp/tmux-501/default,12345,0"}):
            from gza.cli.query import cmd_attach
            cmd_attach(args)

        mock_execvp.assert_called_once()
        call_args = mock_execvp.call_args[0]
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
             patch("gza.cli.query.os.execvp") as mock_execvp:
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 0
        assert mock_kill.called, "interactive attach must stop the running worker"
        mock_execvp.assert_called_once()
        tmux_cmd = mock_execvp.call_args[0][1]
        assert "attach-session" in tmux_cmd
        assert any(part.startswith("gza-attach-") for part in tmux_cmd)

        registry = WorkerRegistry(tmp_path / ".gza" / "workers")
        worker = registry.get("w-20260301-1")
        assert worker is not None
        assert worker.status == "completed"
        assert worker.completion_reason == "stopped_for_attach"

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
             patch("gza.cli.query.os.execvp"):
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Detach with Ctrl-B D or exit Claude normally to auto-resume in background." in captured.out

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
             patch("gza.cli.query.os.execvp"):
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
             patch("gza.cli.query._infer_resume_overrides_from_worker", return_value=(True, 77)), \
             patch("gza.cli.query._spawn_background_worker", return_value=0) as mock_spawn_bg:
            from gza.cli.query import cmd_attach
            result = cmd_attach(args)

        assert result == 1
        assert call_state["new_session_calls"] == 2
        mock_spawn_bg.assert_called_once()
        recovery_args = mock_spawn_bg.call_args[0][0]
        assert recovery_args.no_docker is True
        assert recovery_args.max_turns == 77

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


class TestSpawnBackgroundWorkerTmux:
    """Tests for tmux integration in _spawn_background_worker."""

    def _make_config(self, tmp_path: Path, tmux_enabled: bool = True):
        from gza.config import Config

        config_content = f"project_name: test\ntmux:\n  enabled: {'true' if tmux_enabled else 'false'}\n"
        (tmp_path / "gza.yaml").write_text(config_content)
        (tmp_path / ".gza").mkdir(parents=True, exist_ok=True)
        return Config.load(tmp_path)

    def test_spawn_background_worker_uses_tmux_when_enabled(self, tmp_path: Path):
        """_spawn_background_worker calls tmux new-session when config.tmux.enabled is True."""

        config = self._make_config(tmp_path, tmux_enabled=True)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
        store.update(task)

        args = _make_args(tmp_path)

        tmux_run_result = MagicMock(returncode=0)
        mock_pid_result = 9999

        with patch("gza.cli._common.subprocess.run", return_value=tmux_run_result) as mock_run, \
             patch("gza.cli._common.get_tmux_session_pid", return_value=mock_pid_result), \
             patch("gza.cli._common.get_store") as mock_get_store, \
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
        set_args = tmux_calls[2][0][0]
        assert "set-option" in set_args
        assert "remain-on-exit" in set_args

    def test_spawn_warns_on_remain_on_exit_failure(self, tmp_path: Path, capsys):
        """_spawn_background_worker warns when remain-on-exit set-option fails."""

        config = self._make_config(tmp_path, tmux_enabled=True)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        task = store.add("test task")
        task.provider = "codex"
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
        store.update(task)

        args = _make_args(tmp_path)

        mock_proc = MagicMock()
        mock_proc.pid = 1234

        with patch("gza.cli._common.subprocess.Popen", return_value=mock_proc) as mock_popen, \
             patch("gza.cli._common.subprocess.run") as mock_run, \
             patch("gza.cli._common.get_store") as mock_get_store:
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
        store.update(task)

        args = _make_args(tmp_path)

        tmux_run_result = MagicMock(returncode=0)
        mock_pid_result = 9999

        with patch("gza.cli._common.subprocess.run", return_value=tmux_run_result) as mock_run, \
             patch("gza.cli._common.get_tmux_session_pid", return_value=mock_pid_result), \
             patch("gza.cli._common.get_store") as mock_get_store, \
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
        store.update(task)

        args = _make_args(tmp_path)

        mock_proc = MagicMock()
        mock_proc.pid = 1234

        with patch("gza.cli._common.subprocess.Popen", return_value=mock_proc), \
             patch("gza.cli._common.get_store") as mock_get_store, \
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

        config_content = "project_name: test\n"
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
        """_run_direct_tmux sets up tmux pipe-pane to capture raw terminal output (M2)."""
        from gza.providers.claude import ClaudeProvider

        config = self._make_config(tmp_path, tmux_session="gza-42")
        log_file = tmp_path / ".gza" / "logs" / "42.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        provider = ClaudeProvider()
        fake_result = MagicMock()
        fake_result.returncode = 0

        pipe_pane_called = [False]

        def fake_run(cmd, *args, **kwargs):
            if cmd[0] == "tmux" and "pipe-pane" in cmd:
                pipe_pane_called[0] = True
            return fake_result

        with patch("gza.providers.claude.subprocess.run", side_effect=fake_run):
            provider._run_direct_tmux(config, "test prompt", log_file, tmp_path)

        assert pipe_pane_called[0], (
            "tmux pipe-pane must be called to capture raw terminal output to the main log file"
        )

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

        config_content = "project_name: test\ntmux:\n  enabled: true\n"
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

        config_content = "project_name: test\n"
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
