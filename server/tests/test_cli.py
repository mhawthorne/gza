import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
import signal
import subprocess
import threading
from unittest.mock import MagicMock, Mock, call, patch
from urllib.error import URLError

import pytest

from gza_server.cli import (
    IdentityStatus,
    LifecycleError,
    ServerState,
    open_server,
    read_state,
    start_server,
    status_server,
    state_file_path,
    stop_server,
)


def _write_state(
    path: Path,
    *,
    pid: int = 1234,
    port: int = 4321,
    instance_id: str = "recorded-instance",
    process_start_id: str = "",
) -> ServerState:
    state = ServerState(
        pid=pid,
        port=port,
        started_at="2026-08-17T05:00:00+00:00",
        instance_id=instance_id,
        process_start_id=process_start_id,
    )
    path.write_text(json.dumps(state.__dict__), encoding="utf-8")
    return state


def _write_legacy_state(path: Path, *, pid: int = 1234, port: int = 4321) -> None:
    path.write_text(
        json.dumps(
            {
                "pid": pid,
                "port": port,
                "started_at": "2026-08-17T05:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )


def _health_response(instance_id: str = "recorded-instance") -> MagicMock:
    response = MagicMock(status=200)
    response.read.return_value = json.dumps({"instance_id": instance_id}).encode()
    response.__enter__.return_value = response
    return response


def test_read_state_returns_none_when_missing(tmp_path):
    assert read_state(tmp_path / "gza-server.json") is None


def test_state_file_uses_discovered_project_gza_directory(tmp_path):
    project = tmp_path / "project"
    nested = project / "nested"
    nested.mkdir(parents=True)
    (project / "gza.yaml").write_text(
        "project_name: test-project\nproject_id: testproject\n",
        encoding="utf-8",
    )

    assert state_file_path(nested) == project / ".gza" / "gza-server.json"


def test_read_state_rejects_malformed_content(tmp_path):
    path = tmp_path / "gza-server.json"
    path.write_text("not json", encoding="utf-8")

    with pytest.raises(LifecycleError, match="invalid server state file"):
        read_state(path)


def test_start_spawns_uvicorn_records_state_opens_browser_and_returns_url(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=2468)
    process.poll.return_value = None
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
        patch("gza_server.cli.subprocess.Popen", return_value=process) as popen,
        patch("gza_server.cli.health_identity_matches", return_value=True),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        url = start_server(path)

    assert url == "http://127.0.0.1:8765/"
    state = read_state(path)
    assert state is not None
    assert (state.pid, state.port) == (2468, 8765)
    assert state.instance_id
    assert state.process_start_id == "spawn-start"
    datetime.fromisoformat(state.started_at)
    command = popen.call_args.args[0]
    assert command[-5:] == ["--factory", "--host", "127.0.0.1", "--port", "8765"]
    assert "gza_server.app:create_app" in command
    assert popen.call_args.kwargs["env"]["GZA_SERVER_INSTANCE_ID"] == state.instance_id
    browser_open.assert_called_once_with(url)


def test_start_rejects_an_existing_live_server(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)

    with patch(
        "gza_server.cli.classify_server_identity",
        return_value=IdentityStatus.MATCH,
    ):
        with pytest.raises(LifecycleError, match="already running"):
            start_server(path)


def test_start_replaces_state_for_dead_pid(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, pid=111)
    process = Mock(pid=222)
    process.poll.return_value = None
    with (
        patch(
            "gza_server.cli.classify_server_identity",
            return_value=IdentityStatus.DEAD,
        ),
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch("gza_server.cli.health_identity_matches", return_value=True),
        patch("gza_server.cli.webbrowser.open"),
    ):
        start_server(path)

    state = read_state(path)
    assert state is not None
    assert state.pid == 222


def test_start_replaces_legacy_state_without_signalling_old_pid(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_legacy_state(path, pid=111)
    process = Mock(pid=222)
    process.poll.return_value = None
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.process_start_id", return_value="replacement-start"),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch("gza_server.cli.health_identity_matches", return_value=True),
        patch("gza_server.cli.os.kill") as kill,
        patch("gza_server.cli.webbrowser.open"),
    ):
        start_server(path)

    state = read_state(path)
    assert state is not None
    assert state.pid == 222
    assert state.instance_id
    kill.assert_not_called()


def test_stop_terminates_live_process_and_removes_state(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.process_start_id", return_value="stable-start"),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch(
            "gza_server.cli._wait_for_owned_process_exit",
            return_value=IdentityStatus.DEAD,
        ),
        patch("gza_server.cli.os.kill") as kill,
    ):
        result = stop_server(path)

    assert result == "stopped"
    kill.assert_called_once()
    assert not path.exists()


def test_stop_removes_stale_state_without_signalling(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch("gza_server.cli.process_is_alive", return_value=False),
        patch("gza_server.cli.urlopen") as health_request,
        patch("gza_server.cli.os.kill") as kill,
    ):
        result = stop_server(path)

    assert result == "not running (removed stale state file)"
    health_request.assert_not_called()
    kill.assert_not_called()
    assert not path.exists()


def test_stop_removes_legacy_state_without_signalling_old_pid(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_legacy_state(path)
    with (
        patch("gza_server.cli.process_is_alive") as process_alive,
        patch("gza_server.cli.urlopen") as health_request,
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert stop_server(path) == "not running (removed stale state file)"

    process_alive.assert_not_called()
    health_request.assert_not_called()
    kill.assert_not_called()
    assert not path.exists()


def test_stop_handles_process_exiting_before_sigterm(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.process_start_id", return_value="stable-start"),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch("gza_server.cli.os.kill", side_effect=ProcessLookupError),
    ):
        result = stop_server(path)

    assert result == "not running (removed stale state file)"
    assert not path.exists()


def test_stop_force_terminates_verified_process_after_endpoint_disappears(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.process_start_id", return_value="stable-start"),
        patch(
            "gza_server.cli.urlopen",
            return_value=_health_response(),
        ) as urlopen_mock,
        patch(
            "gza_server.cli._wait_for_owned_process_exit",
            side_effect=[IdentityStatus.MATCH, IdentityStatus.DEAD],
        ),
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert stop_server(path) == "stopped"

    assert kill.call_args_list == [
        call(1234, signal.SIGTERM),
        call(1234, signal.SIGKILL),
    ]
    urlopen_mock.assert_called_once()
    assert not path.exists()


def test_stop_does_not_sigterm_pid_whose_start_marker_changed(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch(
            "gza_server.cli.process_start_id",
            side_effect=["stable-start", "replacement-start"],
        ),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert stop_server(path) == "not running (removed stale state file)"

    kill.assert_not_called()
    assert not path.exists()


def test_stop_preserves_state_when_start_marker_unavailable_before_sigterm(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch(
            "gza_server.cli.process_start_id",
            side_effect=["stable-start", None],
        ),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch("gza_server.cli.os.kill") as kill,
    ):
        with pytest.raises(LifecycleError, match="cannot stop.*state was preserved"):
            stop_server(path)

    kill.assert_not_called()
    assert path.exists()


def test_stop_does_not_sigkill_pid_whose_start_marker_changed(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch(
            "gza_server.cli.process_start_id",
            side_effect=["stable-start", "stable-start", "replacement-start"],
        ),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch(
            "gza_server.cli._wait_for_owned_process_exit",
            return_value=IdentityStatus.MATCH,
        ),
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert stop_server(path) == "stopped"

    kill.assert_called_once_with(1234, signal.SIGTERM)
    assert not path.exists()


def test_stop_preserves_state_when_start_marker_unavailable_before_sigkill(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch(
            "gza_server.cli.process_start_id",
            side_effect=["stable-start", "stable-start", None],
        ),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch(
            "gza_server.cli._wait_for_owned_process_exit",
            return_value=IdentityStatus.MATCH,
        ),
        patch("gza_server.cli.os.kill") as kill,
    ):
        with pytest.raises(LifecycleError, match="cannot stop.*state was preserved"):
            stop_server(path)

    kill.assert_called_once_with(1234, signal.SIGTERM)
    assert path.exists()


def test_stop_clears_state_when_process_exits_immediately_before_sigkill(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.process_start_id", return_value="stable-start"),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch(
            "gza_server.cli._wait_for_owned_process_exit",
            return_value=IdentityStatus.MATCH,
        ),
        patch(
            "gza_server.cli.os.kill",
            side_effect=[None, ProcessLookupError],
        ) as kill,
    ):
        assert stop_server(path) == "stopped"

    assert kill.call_args_list == [
        call(1234, signal.SIGTERM),
        call(1234, signal.SIGKILL),
    ]
    assert not path.exists()


def test_status_reports_pid_port_and_uptime(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)

    with patch(
        "gza_server.cli.classify_server_identity",
        return_value=IdentityStatus.MATCH,
    ):
        result = status_server(path, now=datetime(2026, 8, 17, 5, 1, 30, tzinfo=UTC))

    assert result == "pid: 1234\nport: 4321\nuptime: 90s"


def test_status_clears_stale_state(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)

    with patch(
        "gza_server.cli.classify_server_identity",
        return_value=IdentityStatus.DEAD,
    ):
        assert status_server(path) == "not running"

    assert not path.exists()


def test_status_removes_legacy_state_and_reports_not_running(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_legacy_state(path)
    with (
        patch("gza_server.cli.process_is_alive") as process_alive,
        patch("gza_server.cli.urlopen") as health_request,
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert status_server(path) == "not running"

    process_alive.assert_not_called()
    health_request.assert_not_called()
    kill.assert_not_called()
    assert not path.exists()


def test_open_opens_live_server(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch(
            "gza_server.cli.classify_server_identity",
            return_value=IdentityStatus.MATCH,
        ),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        assert open_server(path) == "http://127.0.0.1:4321/"

    browser_open.assert_called_once_with("http://127.0.0.1:4321/")


def test_open_has_clear_error_when_not_running(tmp_path):
    with pytest.raises(LifecycleError, match="run 'gza-server start' first"):
        open_server(tmp_path / "gza-server.json")


def test_open_removes_legacy_state_and_reports_normal_not_running_error(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_legacy_state(path)
    with (
        patch("gza_server.cli.process_is_alive") as process_alive,
        patch("gza_server.cli.urlopen") as health_request,
        patch("gza_server.cli.os.kill") as kill,
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        with pytest.raises(LifecycleError, match="run 'gza-server start' first"):
            open_server(path)

    process_alive.assert_not_called()
    health_request.assert_not_called()
    kill.assert_not_called()
    browser_open.assert_not_called()
    assert not path.exists()


def test_start_preserves_live_state_when_health_is_unreachable(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.urlopen", side_effect=URLError("refused")),
        patch("gza_server.cli.subprocess.Popen") as popen,
    ):
        with pytest.raises(LifecycleError, match="cannot start.*state was preserved"):
            start_server(path)

    popen.assert_not_called()
    assert path.exists()


def test_stop_preserves_live_state_when_health_is_unreachable(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.urlopen", side_effect=URLError("refused")),
        patch("gza_server.cli.os.kill") as kill,
    ):
        with pytest.raises(LifecycleError, match="cannot stop.*state was preserved"):
            stop_server(path)

    kill.assert_not_called()
    assert path.exists()


def test_status_preserves_live_state_when_health_times_out(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.urlopen", side_effect=TimeoutError),
    ):
        with pytest.raises(
            LifecycleError,
            match="cannot check status of.*state was preserved",
        ):
            status_server(path)

    assert path.exists()


def test_open_preserves_live_state_when_health_is_unreachable(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.urlopen", side_effect=URLError("refused")),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        with pytest.raises(LifecycleError, match="cannot open.*state was preserved"):
            open_server(path)

    browser_open.assert_not_called()
    assert path.exists()


def test_positive_endpoint_mismatch_is_safe_stale_state(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.urlopen", return_value=_health_response("other-instance")),
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert stop_server(path) == "not running (removed stale state file)"

    kill.assert_not_called()
    assert not path.exists()


def test_start_early_exit_reaps_child_and_reports_startup_output(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    process.poll.return_value = 3

    def spawn(*args, **kwargs):
        kwargs["stdout"].write("address already in use\n")
        return process

    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.subprocess.Popen", side_effect=spawn),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        with pytest.raises(LifecycleError, match="status 3.*address already in use"):
            start_server(path)

    process.terminate.assert_not_called()
    process.wait.assert_called_once_with()
    browser_open.assert_not_called()
    assert not path.exists()


def test_start_timeout_terminates_and_reaps_child_without_publishing(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    process.poll.return_value = None
    process.wait.return_value = 0
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch(
            "gza_server.cli.wait_until_ready",
            side_effect=LifecycleError(
                "gza-server did not become ready; the port may already be in use"
            ),
        ),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        with pytest.raises(LifecycleError, match="did not become ready.*port"):
            start_server(path)

    process.terminate.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=5.0)
    process.kill.assert_not_called()
    browser_open.assert_not_called()
    assert not path.exists()


def test_start_keyboard_interrupt_force_cleans_child_and_reraises(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    process.poll.return_value = None
    process.wait.side_effect = [
        subprocess.TimeoutExpired(cmd="uvicorn", timeout=5.0),
        0,
    ]
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch("gza_server.cli.wait_until_ready", side_effect=KeyboardInterrupt),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        with pytest.raises(KeyboardInterrupt):
            start_server(path)

    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert process.wait.call_args_list == [
        call(timeout=5.0),
        call(),
    ]
    browser_open.assert_not_called()
    assert not path.exists()


def test_start_waits_for_delayed_health_before_publication_and_browser(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    process.poll.return_value = None
    readiness_attempts = 0
    browser_patched = Mock()

    def delayed_health(state):
        nonlocal readiness_attempts
        readiness_attempts += 1
        assert not path.exists()
        browser_patched.assert_not_called()
        return readiness_attempts == 3

    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch("gza_server.cli.health_identity_matches", side_effect=delayed_health),
        patch("gza_server.cli.time.sleep"),
        patch("gza_server.cli.webbrowser.open", browser_patched),
    ):
        url = start_server(path)

    assert readiness_attempts == 3
    assert read_state(path) is not None
    browser_patched.assert_called_once_with(url)


def test_concurrent_starts_publish_one_managed_process(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    process.poll.return_value = None
    first_in_readiness = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()

    def coordinated_readiness(spawned, state):
        first_in_readiness.set()
        assert release_first.wait(timeout=2)

    def second_start():
        second_started.set()
        return start_server(path)

    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.subprocess.Popen", return_value=process) as popen,
        patch("gza_server.cli.wait_until_ready", side_effect=coordinated_readiness),
        patch(
            "gza_server.cli.classify_server_identity",
            return_value=IdentityStatus.MATCH,
        ),
        patch("gza_server.cli.webbrowser.open"),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        winner = executor.submit(start_server, path)
        assert first_in_readiness.wait(timeout=2)
        loser = executor.submit(second_start)
        assert second_started.wait(timeout=2)
        release_first.set()
        assert winner.result(timeout=2) == "http://127.0.0.1:8765/"
        with pytest.raises(LifecycleError, match="already running"):
            loser.result(timeout=2)

    assert popen.call_count == 1
    state = read_state(path)
    assert state is not None
    assert state.pid == process.pid
    process.terminate.assert_not_called()
    process.kill.assert_not_called()
