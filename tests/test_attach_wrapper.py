"""Tests for interactive attach wrapper behavior."""

import json
import signal
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from gza.attach_wrapper import main
from gza.config import Config
from gza.db import SqliteTaskStore


def _setup_task_with_log(project_dir: Path) -> tuple[str, Path]:
    (project_dir / "gza.yaml").write_text("project_name: test-project\n")
    (project_dir / ".gza" / "logs").mkdir(parents=True, exist_ok=True)
    config = Config.load(project_dir)
    store = SqliteTaskStore(project_dir / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.add("Test attach wrapper")
    task.log_file = ".gza/logs/task.log"
    store.update(task)
    assert task.id is not None
    return task.id, project_dir / task.log_file


def _read_log_events(log_path: Path) -> list[dict]:
    return [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]


def test_attach_wrapper_normal_exit_auto_resumes_when_task_incomplete(tmp_path: Path) -> None:
    """Normal interactive exit should resume in background when task is incomplete."""
    task_id, log_path = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_spawn.assert_called_once()

    events = _read_log_events(log_path)
    names = [event["event"] for event in events if event.get("subtype") == "worker_lifecycle"]
    assert "attach" in names
    detach_events = [event for event in events if event.get("event") == "detach"]
    assert detach_events
    assert detach_events[-1]["reason"] == "exited_ok"
    assert "resume" in names


def test_attach_wrapper_sigterm_detach_auto_resumes(tmp_path: Path) -> None:
    """SIGTERM detach path should auto-resume in background and emit lifecycle events."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    handlers: dict[int, object] = {}

    def fake_signal(sig, handler):
        handlers[sig] = handler
        return None

    def fake_run_interactive(_config, _session_id, *, max_turns=None):
        del max_turns
        assert signal.SIGTERM in handlers
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.signal.signal", side_effect=fake_signal),
        patch("gza.attach_wrapper._run_interactive_claude", side_effect=fake_run_interactive),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 128 + signal.SIGTERM
    mock_spawn.assert_called_once()

    events = _read_log_events(log_path)
    names = [event["event"] for event in events if event.get("subtype") == "worker_lifecycle"]
    assert "attach" in names
    assert "detach" in names
    assert "resume" in names
    detach_events = [event for event in events if event.get("event") == "detach"]
    assert detach_events[-1]["reason"] == "detached"


def test_attach_wrapper_resume_failure_marks_task_failed(tmp_path: Path) -> None:
    """Failed detach/exit handoff should not emit resume success and must mark task failed."""
    task_id, log_path = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=7),
    ):
        rc = main()

    assert rc == 0
    events = _read_log_events(log_path)
    lifecycle_events = [event for event in events if event.get("subtype") == "worker_lifecycle"]
    event_names = [event["event"] for event in lifecycle_events]
    assert "resume" not in event_names
    assert "resume_failed" in event_names
    failure_event = [event for event in lifecycle_events if event["event"] == "resume_failed"][-1]
    assert failure_event["handoff_exit_code"] == 7

    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    refreshed = store.get(task_id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "WORKER_DIED"


def test_attach_wrapper_passes_resume_overrides_to_background_worker(tmp_path: Path) -> None:
    """Wrapper should preserve no-docker/max-turns overrides when respawning."""
    task_id, _ = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
            "--no-docker",
            "--max-turns", "66",
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0) as mock_interactive,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_interactive.assert_called_once()
    assert mock_interactive.call_args.kwargs["max_turns"] == 66
    assert mock_spawn.call_count == 1
    worker_args = mock_spawn.call_args[0][0]
    assert worker_args.no_docker is True
    assert worker_args.max_turns == 66


def test_attach_wrapper_sigint_during_interactive_forwarded_to_child_without_detach_state(tmp_path: Path) -> None:
    """Ctrl-C during interactive attach should be forwarded to Claude, not treated as wrapper detach."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    handlers: dict[int, object] = {}

    def fake_signal(sig, handler):
        previous = handlers.get(sig, signal.SIG_DFL)
        handlers[sig] = handler
        return previous

    fake_proc = MagicMock()
    fake_proc.pid = 4242

    def fake_wait():
        assert signal.SIGINT in handlers
        handlers[signal.SIGINT](signal.SIGINT, None)
        return 0

    fake_proc.wait.side_effect = fake_wait

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.signal.signal", side_effect=fake_signal),
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc),
        patch("gza.attach_wrapper.os.kill") as mock_kill,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0) as mock_spawn,
    ):
        rc = main()

    assert rc == 0
    mock_kill.assert_called_once_with(4242, signal.SIGINT)
    mock_spawn.assert_called_once()

    events = _read_log_events(log_path)
    detach_events = [event for event in events if event.get("event") == "detach"]
    assert detach_events
    assert detach_events[-1]["reason"] == "exited_ok"
