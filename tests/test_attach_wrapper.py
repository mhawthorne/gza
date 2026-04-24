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
    (project_dir / "gza.yaml").write_text(
        "project_name: test-project\nuse_docker: false\n"
    )
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


def test_attach_wrapper_sets_foreground_attach_resume_execution_mode(tmp_path: Path) -> None:
    """Interactive attach/resume should stamp foreground_attach_resume provenance on the task."""
    task_id, _ = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper._run_interactive_claude", return_value=0),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    refreshed = store.get(task_id)
    assert refreshed is not None
    assert refreshed.execution_mode == "foreground_attach_resume"


def test_attach_wrapper_sigterm_detach_auto_resumes(tmp_path: Path) -> None:
    """SIGTERM detach path should auto-resume in background and emit lifecycle events."""
    task_id, log_path = _setup_task_with_log(tmp_path)
    handlers: dict[int, object] = {}

    def fake_signal(sig, handler):
        handlers[sig] = handler
        return None

    def fake_run_interactive(_config, _session_id, *, max_turns=None, task=None, no_docker=False):
        del max_turns, task, no_docker
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
    """Wrapper should preserve no-docker/max-turns/force overrides when respawning."""
    task_id, _ = _setup_task_with_log(tmp_path)

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
            "--no-docker",
            "--max-turns", "66",
            "--force",
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
    assert worker_args.force is True


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


def test_attach_wrapper_calls_load_dotenv_before_interactive_claude(tmp_path: Path) -> None:
    """Attach wrapper must load .env files so API keys are available during interactive session."""
    task_id, _ = _setup_task_with_log(tmp_path)

    call_order: list[str] = []

    def track_dotenv(project_dir):
        call_order.append("load_dotenv")

    def track_interactive(*args, **kwargs):
        call_order.append("interactive_claude")
        return 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-123",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.load_dotenv", side_effect=track_dotenv) as mock_dotenv,
        patch("gza.attach_wrapper._run_interactive_claude", side_effect=track_interactive),
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_dotenv.assert_called_once_with(tmp_path)
    assert call_order.index("load_dotenv") < call_order.index("interactive_claude"), \
        "load_dotenv must be called before _run_interactive_claude"


def _setup_docker_task(project_dir: Path) -> tuple[str, Path]:
    (project_dir / "gza.yaml").write_text(
        "project_name: test-project\nuse_docker: true\ndocker_image: test-project-gza\n"
    )
    (project_dir / ".gza" / "logs").mkdir(parents=True, exist_ok=True)
    config = Config.load(project_dir)
    assert config.use_docker is True
    store = SqliteTaskStore(project_dir / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.add("Test docker attach")
    task.log_file = ".gza/logs/task.log"
    store.update(task)
    assert task.id is not None
    return task.id, project_dir / task.log_file


def test_attach_wrapper_docker_task_launches_via_docker(tmp_path: Path) -> None:
    """Docker-backed Claude task must route the interactive resume through Docker, not host claude."""
    task_id, _ = _setup_docker_task(tmp_path)

    fake_proc = MagicMock()
    fake_proc.pid = 5555
    fake_proc.wait.return_value = 0

    docker_cmd_stub = [
        "timeout", "60m",
        "docker", "run", "--rm", "-it",
        "-v", f"{tmp_path}:/workspace",
        "-w", "/workspace",
        "test-project-gza-claude",
        "claude", "--resume", "sess-docker", "--max-turns", "200",
    ]

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-docker",
            "--project", str(tmp_path),
        ]),
        patch(
            "gza.attach_wrapper._build_docker_interactive_cmd",
            return_value=docker_cmd_stub,
        ) as mock_build,
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc) as mock_popen,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_build.assert_called_once()
    _, build_kwargs = mock_build.call_args
    assert build_kwargs["max_turns"] is None
    # First positional: config; second: task; third: session id.
    build_args = mock_build.call_args[0]
    assert build_args[2] == "sess-docker"

    mock_popen.assert_called_once()
    cmd = mock_popen.call_args[0][0]
    assert cmd is docker_cmd_stub
    assert mock_popen.call_args.kwargs.get("cwd") is None


def test_build_docker_interactive_cmd_uses_it_and_claude_resume(tmp_path: Path) -> None:
    """Unit test: _build_docker_interactive_cmd builds a -it docker run + claude --resume command."""
    from gza.attach_wrapper import _build_docker_interactive_cmd

    task_id, _ = _setup_docker_task(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.get(task_id)
    assert task is not None

    with (
        patch("gza.attach_wrapper.ensure_docker_image", return_value=True),
        patch("gza.attach_wrapper.sync_keychain_credentials", return_value=True),
        patch("gza.providers.base.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
    ):
        cmd = _build_docker_interactive_cmd(config, task, "sess-docker", max_turns=42)

    assert "docker" in cmd
    assert "run" in cmd
    assert "-it" in cmd, f"expected -it for interactive attach, got {cmd}"
    assert "-i" not in cmd
    claude_idx = cmd.index("claude")
    assert cmd[claude_idx + 1 : claude_idx + 3] == ["--resume", "sess-docker"]
    assert "--max-turns" in cmd[claude_idx:]
    max_turns_idx = cmd.index("--max-turns", claude_idx)
    assert cmd[max_turns_idx + 1] == "42"


def test_build_docker_interactive_cmd_tolerates_unwritable_provider_json_mirror(tmp_path: Path) -> None:
    """Docker command construction should fail soft when the provider JSON mirror cannot be written."""
    from gza.attach_wrapper import _build_docker_interactive_cmd

    task_id, _ = _setup_docker_task(tmp_path)
    config = Config.load(tmp_path)
    store = SqliteTaskStore(tmp_path / ".gza" / "gza.db", prefix=config.project_prefix)
    task = store.get(task_id)
    assert task is not None

    with (
        patch("gza.attach_wrapper.ensure_docker_image", return_value=True),
        patch("gza.attach_wrapper.sync_keychain_credentials", return_value=True),
        patch("gza.providers.base.shutil.copy2", side_effect=PermissionError("denied")),
        patch("gza.providers.base.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
    ):
        cmd = _build_docker_interactive_cmd(config, task, "sess-docker", max_turns=42)

    assert "docker" in cmd
    assert "run" in cmd


def test_attach_wrapper_docker_task_with_no_docker_flag_uses_host(tmp_path: Path) -> None:
    """Even when config.use_docker is True, --no-docker should preserve host execution."""
    task_id, _ = _setup_docker_task(tmp_path)

    fake_proc = MagicMock()
    fake_proc.pid = 5555
    fake_proc.wait.return_value = 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-host",
            "--project", str(tmp_path),
            "--no-docker",
        ]),
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc) as mock_popen,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_popen.assert_called_once()
    cmd = mock_popen.call_args[0][0]
    assert cmd[0] == "claude"
    assert "docker" not in cmd
    assert mock_popen.call_args.kwargs.get("cwd") == tmp_path


def test_attach_wrapper_non_docker_task_uses_host(tmp_path: Path) -> None:
    """Tasks without use_docker must continue to resume via host claude."""
    task_id, _ = _setup_task_with_log(tmp_path)

    fake_proc = MagicMock()
    fake_proc.pid = 5555
    fake_proc.wait.return_value = 0

    with (
        patch.object(sys, "argv", [
            "gza.attach_wrapper",
            "--task-id", task_id,
            "--session-id", "sess-plain",
            "--project", str(tmp_path),
        ]),
        patch("gza.attach_wrapper.subprocess.Popen", return_value=fake_proc) as mock_popen,
        patch("gza.attach_wrapper._spawn_background_worker", return_value=0),
    ):
        rc = main()

    assert rc == 0
    mock_popen.assert_called_once()
    cmd = mock_popen.call_args[0][0]
    assert cmd[0] == "claude"
    assert "docker" not in cmd
