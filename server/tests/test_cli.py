import json
import os
import signal
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch
from urllib.error import URLError

import pytest
from gza_server.cli import (
    DARWIN_PROCESS_START_ENV,
    PS_TIMEOUT_SECONDS,
    IdentityStatus,
    LifecycleError,
    ServerState,
    open_server,
    process_start_id,
    read_state,
    start_server,
    state_file_path,
    status_server,
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
    assert "--no-access-log" in command
    assert popen.call_args.kwargs["stdout"] is not subprocess.DEVNULL
    assert popen.call_args.kwargs["stderr"] is subprocess.STDOUT
    assert popen.call_args.kwargs["env"]["GZA_SERVER_INSTANCE_ID"] == state.instance_id
    browser_open.assert_called_once_with(url)


def test_process_start_id_reads_macos_lstart():
    result = Mock(returncode=0, stdout="Mon Aug 17 05:01:02 2026\n")

    with (
        patch("gza_server.cli.sys.platform", "darwin"),
        patch("gza_server.cli.subprocess.run", return_value=result) as run,
    ):
        assert process_start_id(2468) == "Mon Aug 17 05:01:02 2026"

    run.assert_called_once_with(
        ["ps", "-o", "lstart=", "-p", "2468"],
        capture_output=True,
        check=False,
        env={**os.environ, **DARWIN_PROCESS_START_ENV},
        text=True,
        timeout=PS_TIMEOUT_SECONDS,
    )


def test_darwin_process_start_id_is_stable_across_caller_locale_environment():
    def ps_lstart(*_args, **kwargs):
        env = kwargs["env"]
        assert env["LC_ALL"] == "C"
        assert env["TZ"] == "UTC"
        return Mock(
            returncode=0,
            stdout=f"{env['LC_ALL']} {env['TZ']} Mon Aug 17 05:01:02 2026\n",
        )

    with (
        patch("gza_server.cli.sys.platform", "darwin"),
        patch("gza_server.cli.subprocess.run", side_effect=ps_lstart),
        patch.dict(os.environ, {"LC_ALL": "en_US.UTF-8", "TZ": "America/Los_Angeles"}),
    ):
        first = process_start_id(2468)

    with (
        patch("gza_server.cli.sys.platform", "darwin"),
        patch("gza_server.cli.subprocess.run", side_effect=ps_lstart),
        patch.dict(os.environ, {"LC_ALL": "de_DE.UTF-8", "TZ": "Asia/Tokyo"}),
    ):
        second = process_start_id(2468)

    assert first == second == "C UTC Mon Aug 17 05:01:02 2026"


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
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
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


def test_stop_darwin_owned_pid_survives_caller_locale_environment_change(tmp_path):
    path = tmp_path / "gza-server.json"

    def ps_lstart(*_args, **kwargs):
        env = kwargs["env"]
        assert env["LC_ALL"] == "C"
        assert env["TZ"] == "UTC"
        return Mock(returncode=0, stdout="Mon Aug 17 05:01:02 2026\n")

    with (
        patch("gza_server.cli.sys.platform", "darwin"),
        patch("gza_server.cli.subprocess.run", side_effect=ps_lstart),
        patch.dict(os.environ, {"LC_ALL": "en_US.UTF-8", "TZ": "America/Los_Angeles"}),
    ):
        start_id = process_start_id(1234)

    assert start_id is not None
    _write_state(path, process_start_id=start_id)

    with (
        patch("gza_server.cli.sys.platform", "darwin"),
        patch("gza_server.cli.subprocess.run", side_effect=ps_lstart),
        patch.dict(os.environ, {"LC_ALL": "de_DE.UTF-8", "TZ": "Asia/Tokyo"}),
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch(
            "gza_server.cli._wait_for_owned_process_exit",
            return_value=IdentityStatus.DEAD,
        ),
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert stop_server(path) == "stopped"

    kill.assert_called_once_with(1234, signal.SIGTERM)
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


def test_stop_with_no_start_marker_waits_through_refused_health_after_sigterm(tmp_path):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch(
            "gza_server.cli.process_is_alive",
            side_effect=[True, True, True, False],
        ),
        patch(
            "gza_server.cli.urlopen",
            side_effect=[
                _health_response(),
                _health_response(),
                URLError("refused"),
            ],
        ),
        patch("gza_server.cli.time.sleep") as sleep,
        patch("gza_server.cli.os.kill") as kill,
    ):
        assert stop_server(path) == "stopped"

    kill.assert_called_once_with(1234, signal.SIGTERM)
    sleep.assert_called_once_with(0.05)
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
    # A matching start marker settles identity on its own, so the health
    # endpoint is never consulted -- which is what lets a wedged server be
    # stopped at all.
    urlopen_mock.assert_not_called()
    assert not path.exists()


def test_stop_with_no_start_marker_preserves_state_when_health_unverifiable_before_signal(
    tmp_path,
):
    path = tmp_path / "gza-server.json"
    _write_state(path)
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch(
            "gza_server.cli.urlopen",
            side_effect=[_health_response(), URLError("refused")],
        ),
        patch("gza_server.cli.os.kill") as kill,
    ):
        with pytest.raises(LifecycleError, match="cannot stop.*state was preserved"):
            stop_server(path)

    kill.assert_not_called()
    assert path.exists()


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


def test_stop_escalates_when_start_marker_becomes_unreadable_mid_exit(tmp_path):
    """An exiting process can be alive with no readable start marker.

    Refusing to escalate there left a server that ignored SIGTERM running
    forever. Once SIGTERM has been sent to a PID we verified, an unreadable
    marker means the process is on its way out, so the stop proceeds.
    """
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    alive = {"value": True}

    def kill_process(pid: int, sig: int) -> None:
        if sig == signal.SIGKILL:
            alive["value"] = False

    with (
        patch("gza_server.cli.process_is_alive", side_effect=lambda pid: alive["value"]),
        patch(
            "gza_server.cli.process_start_id",
            side_effect=["stable-start", "stable-start", None, None, None],
        ),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch(
            "gza_server.cli._wait_for_owned_process_exit",
            # SIGTERM is ignored; the process only goes after SIGKILL.
            side_effect=[IdentityStatus.MATCH, IdentityStatus.DEAD],
        ),
        patch("gza_server.cli.os.kill", side_effect=kill_process) as kill,
    ):
        assert stop_server(path) == "stopped"

    assert kill.call_args_list == [
        call(1234, signal.SIGTERM),
        call(1234, signal.SIGKILL),
    ]
    assert not path.exists()


def test_stop_still_refuses_an_unverifiable_process_before_signalling(tmp_path):
    """Escalation is only licensed after ownership was verified and SIGTERM sent."""
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        patch("gza_server.cli.process_start_id", return_value=None),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch("gza_server.cli.os.kill") as kill,
    ):
        with pytest.raises(LifecycleError, match="cannot stop.*state was preserved"):
            stop_server(path)

    kill.assert_not_called()
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


def test_start_early_exit_reaps_child_and_reports_bounded_startup_output(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    process.poll.return_value = 3

    def spawn(*args, **kwargs):
        kwargs["stdout"].write(
            b"discarded startup output\n"
            + (b"x" * 2100)
            + b"\naddress already in use\n"
        )
        return process

    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.subprocess.Popen", side_effect=spawn),
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        with pytest.raises(LifecycleError, match="status 3") as exc_info:
            start_server(path)

    assert "address already in use" in str(exc_info.value)
    assert "discarded startup output" not in str(exc_info.value)
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
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
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


def test_start_cleanup_failure_preserves_recovery_state_and_reports_both_errors(
    tmp_path,
):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch(
            "gza_server.cli.wait_until_ready",
            side_effect=LifecycleError("gza-server did not become ready"),
        ),
        patch("gza_server.cli._cleanup_child", side_effect=OSError("wait failed")),
        patch("gza_server.cli.webbrowser.open") as browser_open,
    ):
        with pytest.raises(LifecycleError) as exc_info:
            start_server(path)

    message = str(exc_info.value)
    assert "gza-server did not become ready" in message
    assert "cleanup failed: OSError: wait failed" in message
    assert "PID 222" in message
    assert f"recovery state was preserved at {path}" in message
    state = read_state(path)
    assert state is not None
    assert (state.pid, state.port) == (222, 8765)
    assert state.instance_id
    assert state.process_start_id == "spawn-start"
    browser_open.assert_not_called()


def test_start_cleanup_and_recovery_write_failures_report_recovery_metadata(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch(
            "gza_server.cli.wait_until_ready",
            side_effect=LifecycleError("gza-server did not become ready"),
        ),
        patch("gza_server.cli._cleanup_child", side_effect=OSError("wait failed")),
        patch("gza_server.cli.write_state", side_effect=OSError("disk full")),
    ):
        with pytest.raises(LifecycleError) as exc_info:
            start_server(path)

    message = str(exc_info.value)
    assert "gza-server did not become ready" in message
    assert "cleanup failed: OSError: wait failed" in message
    assert "PID 222" in message
    assert (
        f"recovery state could not be written to {path}: OSError: disk full" in message
    )
    assert '"pid": 222' in message
    assert '"port": 8765' in message
    assert '"process_start_id": "spawn-start"' in message
    assert '"instance_id":' in message


def test_start_interruption_cleanup_failure_preserves_state_and_interrupt(tmp_path):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch("gza_server.cli.wait_until_ready", side_effect=KeyboardInterrupt),
        patch("gza_server.cli._cleanup_child", side_effect=OSError("wait failed")),
    ):
        with pytest.raises(KeyboardInterrupt) as exc_info:
            start_server(path)

    assert isinstance(exc_info.value.__cause__, OSError)
    diagnostic = "\n".join(exc_info.value.__notes__)
    assert "startup failed for PID 222: KeyboardInterrupt" in diagnostic
    assert "cleanup failed: OSError: wait failed" in diagnostic
    assert f"recovery state was preserved at {path}" in diagnostic
    state = read_state(path)
    assert state is not None
    assert (state.pid, state.port, state.process_start_id) == (
        222,
        8765,
        "spawn-start",
    )


def test_start_system_exit_cleanup_failure_is_visible_and_preserves_state(
    tmp_path, capsys
):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch(
            "gza_server.cli.wait_until_ready",
            side_effect=SystemExit("startup aborted"),
        ),
        patch("gza_server.cli._cleanup_child", side_effect=OSError("wait failed")),
    ):
        with pytest.raises(SystemExit) as exc_info:
            start_server(path)

    assert exc_info.value.code == "startup aborted"
    assert isinstance(exc_info.value.__cause__, OSError)
    diagnostic = capsys.readouterr().err
    assert "startup failed for PID 222: SystemExit: startup aborted" in diagnostic
    assert "cleanup failed: OSError: wait failed" in diagnostic
    assert f"recovery state was preserved at {path}" in diagnostic
    state = read_state(path)
    assert state is not None
    assert (state.pid, state.port, state.process_start_id) == (
        222,
        8765,
        "spawn-start",
    )


def test_start_system_exit_state_unlink_failure_is_visible_and_preserves_exit(
    tmp_path, capsys
):
    path = tmp_path / "gza-server.json"
    process = Mock(pid=222)
    with (
        patch("gza_server.cli.find_free_port", return_value=8765),
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
        patch("gza_server.cli.subprocess.Popen", return_value=process),
        patch(
            "gza_server.cli.wait_until_ready",
            side_effect=SystemExit("startup aborted"),
        ),
        patch("gza_server.cli._cleanup_child") as cleanup_child,
        patch.object(Path, "unlink", side_effect=OSError("unlink failed")),
    ):
        with pytest.raises(SystemExit) as exc_info:
            start_server(path)

    assert exc_info.value.code == "startup aborted"
    assert isinstance(exc_info.value.__cause__, OSError)
    cleanup_child.assert_called_once_with(process)
    diagnostic = capsys.readouterr().err
    assert "startup failed for PID 222: SystemExit: startup aborted" in diagnostic
    assert "child exit was confirmed" in diagnostic
    assert f"failed to remove state at {path}: OSError: unlink failed" in diagnostic


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
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
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
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
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
        patch("gza_server.cli.process_start_id", return_value="spawn-start"),
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


def test_process_start_id_is_readable_without_proc(tmp_path):
    """macOS has no /proc; identity must not silently degrade to nothing there."""
    import os as _os

    from gza_server.cli import process_start_id

    assert process_start_id(_os.getpid())


def test_stop_escalates_to_sigkill_when_shutdown_closes_the_health_endpoint(tmp_path):
    """A server draining a slow request stops answering health checks first.

    Treating that unreachable endpoint as a lost identity used to abort the stop
    before SIGKILL, leaving the process running -- the reported symptom.
    """
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="")
    signals: list[int] = []

    with (
        patch("gza_server.cli.process_is_alive", return_value=True),
        # No start marker available at all, so identity can only come from health.
        patch("gza_server.cli.process_start_id", return_value=None),
        # Health answers once for the initial ownership check, then the socket
        # closes and every later probe fails.
        patch(
            "gza_server.cli.urlopen",
            side_effect=[
                # Answers the ownership checks, then SIGTERM closes the socket.
                _health_response(),
                _health_response(),
                *[OSError("connection refused")] * 50,
            ],
        ),
        patch("gza_server.cli.STOP_TIMEOUT_SECONDS", 0.05),
        patch("gza_server.cli.os.kill", side_effect=lambda pid, sig: signals.append(sig)),
    ):
        with pytest.raises(LifecycleError, match="did not exit after SIGKILL"):
            stop_server(path)

    assert signal.SIGTERM in signals
    assert signal.SIGKILL in signals


def test_stop_kills_a_hung_server_and_removes_state(tmp_path):
    """The full recovery path: SIGTERM ignored, SIGKILL lands, state cleared."""
    path = tmp_path / "gza-server.json"
    _write_state(path, process_start_id="stable-start")
    signals: list[int] = []
    alive = {"value": True}

    def kill(pid: int, sig: int) -> None:
        signals.append(sig)
        if sig == signal.SIGKILL:
            alive["value"] = False

    with (
        patch("gza_server.cli.process_is_alive", side_effect=lambda pid: alive["value"]),
        patch("gza_server.cli.process_start_id", return_value="stable-start"),
        patch("gza_server.cli.urlopen", return_value=_health_response()),
        patch("gza_server.cli.STOP_TIMEOUT_SECONDS", 0.05),
        patch("gza_server.cli.os.kill", side_effect=kill),
    ):
        result = stop_server(path)

    assert result == "stopped"
    assert signals == [signal.SIGTERM, signal.SIGKILL]
    assert not path.exists()
