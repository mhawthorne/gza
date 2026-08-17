"""Manage the local gza HTTP API and web UI process."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from contextlib import contextmanager
import fcntl
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
from typing import IO
from urllib.error import URLError
from urllib.request import urlopen
import webbrowser

from gza.config import Config

HOST = "127.0.0.1"
STATE_FILENAME = "gza-server.json"
LOCK_FILENAME = "gza-server.lock"
STOP_TIMEOUT_SECONDS = 5.0
STARTUP_TIMEOUT_SECONDS = 10.0
HEALTH_REQUEST_TIMEOUT_SECONDS = 0.25
STARTUP_POLL_INTERVAL_SECONDS = 0.05


class LifecycleError(RuntimeError):
    """An expected, user-facing server lifecycle error."""


class IdentityStatus(Enum):
    """How confidently recorded state identifies a running server instance."""

    DEAD = "dead"
    MATCH = "match"
    MISMATCH = "mismatch"
    UNVERIFIABLE = "unverifiable"


@dataclass(frozen=True)
class ServerState:
    pid: int
    port: int
    started_at: str
    instance_id: str
    process_start_id: str = ""

    @classmethod
    def from_dict(cls, value: object) -> "ServerState":
        if not isinstance(value, dict):
            raise ValueError("state must be a JSON object")
        state = cls(
            pid=int(value["pid"]),
            port=int(value["port"]),
            started_at=str(value["started_at"]),
            # State written before instance identities were introduced is safe to
            # read, but can never validate as a managed server.
            instance_id=str(value.get("instance_id", "")),
            process_start_id=str(value.get("process_start_id", "")),
        )
        if state.pid <= 0 or not 1 <= state.port <= 65535:
            raise ValueError("state contains an invalid pid or port")
        datetime.fromisoformat(state.started_at)
        return state


def state_file_path(project_dir: Path | None = None) -> Path:
    """Return the server state file in gza's project-local state directory."""
    config = Config.load(project_dir or Path.cwd(), discover=True)
    return config.project_dir / ".gza" / STATE_FILENAME


def read_state(path: Path) -> ServerState | None:
    if not path.exists():
        return None
    try:
        return ServerState.from_dict(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise LifecycleError(f"invalid server state file {path}: {exc}") from exc


def write_state(path: Path, state: ServerState) -> None:
    """Publish state atomically using a temporary file unique to this writer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(asdict(state), temporary, indent=2)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


@contextmanager
def server_lock(path: Path) -> Iterator[None]:
    """Serialize lifecycle transactions for one project-local state directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.parent / LOCK_FILENAME
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def process_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def process_start_id(pid: int) -> str | None:
    """Return Linux's stable start marker for a PID, when it can be read."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError, OSError):
        return None
    try:
        # Fields after the final ')' begin with field 3; starttime is field 22.
        return stat.rsplit(")", 1)[1].split()[19]
    except (IndexError, ValueError):
        return None


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((HOST, 0))
        return int(listener.getsockname()[1])


def server_url(state: ServerState) -> str:
    return f"http://{HOST}:{state.port}/"


def health_url(state: ServerState) -> str:
    return f"http://{HOST}:{state.port}/api/health"


def health_identity_matches(state: ServerState) -> bool:
    """Return whether the recorded endpoint identifies the recorded instance."""
    return _health_identity_status(state) is IdentityStatus.MATCH


def _health_identity_status(state: ServerState) -> IdentityStatus:
    """Classify an endpoint response without treating I/O failure as mismatch."""
    if not state.instance_id:
        return IdentityStatus.UNVERIFIABLE
    try:
        with urlopen(health_url(state), timeout=HEALTH_REQUEST_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                return IdentityStatus.UNVERIFIABLE
            payload = json.loads(response.read())
    except (OSError, TimeoutError, URLError, ValueError, json.JSONDecodeError):
        return IdentityStatus.UNVERIFIABLE
    if not isinstance(payload, dict) or "instance_id" not in payload:
        return IdentityStatus.UNVERIFIABLE
    if payload["instance_id"] == state.instance_id:
        return IdentityStatus.MATCH
    return IdentityStatus.MISMATCH


def classify_server_identity(state: ServerState) -> IdentityStatus:
    """Classify PID liveness, stable process identity, and endpoint identity."""
    # Legacy state cannot identify a managed server and must never authorize a
    # signal, even when its recorded PID now belongs to a live process.
    if not state.instance_id:
        return IdentityStatus.MISMATCH
    if not process_is_alive(state.pid):
        return IdentityStatus.DEAD
    if state.process_start_id:
        current_start_id = process_start_id(state.pid)
        if current_start_id is None:
            return IdentityStatus.UNVERIFIABLE
        if current_start_id != state.process_start_id:
            return IdentityStatus.MISMATCH
    return _health_identity_status(state)


def _raise_unverifiable(state: ServerState, action: str) -> None:
    raise LifecycleError(
        f"cannot {action} gza-server: recorded PID {state.pid} is alive, but its "
        "identity could not be verified; state was preserved"
    )


def _owned_process_status(
    state: ServerState,
    expected_start_id: str | None,
) -> IdentityStatus:
    """Revalidate the strongest available identity for a previously matched server."""
    if not process_is_alive(state.pid):
        return IdentityStatus.DEAD
    if expected_start_id is None:
        return _health_identity_status(state)
    current_start_id = process_start_id(state.pid)
    if current_start_id is None:
        return IdentityStatus.UNVERIFIABLE
    if current_start_id != expected_start_id:
        return IdentityStatus.MISMATCH
    return IdentityStatus.MATCH


def _signal_owned_process(
    state: ServerState,
    expected_start_id: str | None,
    sig: signal.Signals,
) -> IdentityStatus:
    """Signal only after an immediate strict ownership revalidation."""
    identity = _owned_process_status(state, expected_start_id)
    if identity is not IdentityStatus.MATCH:
        return identity
    try:
        os.kill(state.pid, sig)
    except ProcessLookupError:
        return IdentityStatus.DEAD
    return IdentityStatus.MATCH


def _wait_for_owned_process_exit(
    state: ServerState,
    expected_start_id: str | None,
) -> IdentityStatus:
    """Wait for death, tolerating lost health after a markerless SIGTERM."""
    deadline = time.monotonic() + STOP_TIMEOUT_SECONDS
    while True:
        identity = _owned_process_status(state, expected_start_id)
        if identity in (IdentityStatus.DEAD, IdentityStatus.MISMATCH):
            return identity
        if (
            identity is IdentityStatus.UNVERIFIABLE
            and expected_start_id is not None
        ):
            return identity
        if time.monotonic() >= deadline:
            return identity
        time.sleep(0.05)


def wait_until_ready(
    process: subprocess.Popen[object],
    state: ServerState,
    *,
    timeout: float = STARTUP_TIMEOUT_SECONDS,
) -> None:
    """Wait for the child to stay alive and serve its matching health identity."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise LifecycleError(
                f"gza-server exited during startup with status {return_code}"
            )
        if health_identity_matches(state):
            return_code = process.poll()
            if return_code is None:
                return
            raise LifecycleError(
                f"gza-server exited during startup with status {return_code}"
            )
        time.sleep(STARTUP_POLL_INTERVAL_SECONDS)
    raise LifecycleError(
        f"gza-server did not become ready at {health_url(state)} within {timeout:.1f}s; "
        "the port may already be in use"
    )


def _cleanup_child(process: subprocess.Popen[object]) -> None:
    """Ensure a failed startup leaves no child process or unreaped zombie."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=STOP_TIMEOUT_SECONDS)
            return
        except subprocess.TimeoutExpired:
            process.kill()
    process.wait()


def _startup_diagnostic(stream: IO[str]) -> str:
    try:
        stream.flush()
        stream.seek(0)
        output = stream.read().strip()
    except (OSError, ValueError):
        return ""
    if not output:
        return ""
    return f"; startup output: {output[-2000:]}"


def _exception_diagnostic(exc: BaseException) -> str:
    message = str(exc)
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__


def _report_preserved_exception_failure(
    exc: BaseException, diagnostic: str
) -> None:
    """Make cleanup failures visible while preserving the startup exception."""
    print(f"error: {diagnostic}", file=sys.stderr, flush=True)
    exc.add_note(diagnostic)


def start_server(path: Path) -> str:
    with server_lock(path):
        current = read_state(path)
        if current:
            identity = classify_server_identity(current)
            if identity is IdentityStatus.MATCH:
                raise LifecycleError(
                    f"gza-server is already running at {server_url(current)}"
                )
            if identity is IdentityStatus.UNVERIFIABLE:
                _raise_unverifiable(current, "start")
            path.unlink(missing_ok=True)

        port = find_free_port()
        instance_id = secrets.token_urlsafe(32)
        environment = os.environ.copy()
        environment["GZA_SERVER_INSTANCE_ID"] = instance_id
        with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as startup_output:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "gza_server.app:create_app",
                    "--factory",
                    "--host",
                    HOST,
                    "--port",
                    str(port),
                ],
                cwd=Path.cwd(),
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=startup_output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
            state = ServerState(
                pid=process.pid,
                port=port,
                started_at=datetime.now(UTC).isoformat(),
                instance_id=instance_id,
                process_start_id=process_start_id(process.pid) or "",
            )
            try:
                wait_until_ready(process, state)
                write_state(path, state)
            except BaseException as exc:
                startup_diagnostic = (
                    f"{_exception_diagnostic(exc)}"
                    f"{_startup_diagnostic(startup_output)}"
                )
                try:
                    _cleanup_child(process)
                except BaseException as cleanup_exc:
                    try:
                        write_state(path, state)
                    except BaseException as recovery_exc:
                        recovery_diagnostic = (
                            f"recovery state could not be written to {path}: "
                            f"{_exception_diagnostic(recovery_exc)}; "
                            "recovery metadata: "
                            f"{json.dumps(asdict(state), sort_keys=True)}"
                        )
                    else:
                        recovery_diagnostic = f"recovery state was preserved at {path}"
                    failure_diagnostic = (
                        f"gza-server startup failed for PID {state.pid}: "
                        f"{startup_diagnostic}; cleanup failed: "
                        f"{_exception_diagnostic(cleanup_exc)}; {recovery_diagnostic}"
                    )
                    if isinstance(exc, LifecycleError):
                        raise LifecycleError(failure_diagnostic) from cleanup_exc
                    _report_preserved_exception_failure(exc, failure_diagnostic)
                    raise exc from cleanup_exc
                try:
                    path.unlink(missing_ok=True)
                except BaseException as cleanup_exc:
                    failure_diagnostic = (
                        f"gza-server startup failed for PID {state.pid}: "
                        f"{startup_diagnostic}; child exit was confirmed, but failed "
                        f"to remove state at {path}: "
                        f"{_exception_diagnostic(cleanup_exc)}"
                    )
                    if isinstance(exc, LifecycleError):
                        raise LifecycleError(failure_diagnostic) from cleanup_exc
                    _report_preserved_exception_failure(exc, failure_diagnostic)
                    raise exc from cleanup_exc
                if isinstance(exc, LifecycleError):
                    raise LifecycleError(startup_diagnostic) from exc
                raise

        url = server_url(state)
        webbrowser.open(url)
        return url


def stop_server(path: Path) -> str:
    with server_lock(path):
        state = read_state(path)
        if state is None:
            return "not running"
        identity = classify_server_identity(state)
        if identity in (IdentityStatus.DEAD, IdentityStatus.MISMATCH):
            path.unlink(missing_ok=True)
            return "not running (removed stale state file)"
        if identity is IdentityStatus.UNVERIFIABLE:
            _raise_unverifiable(state, "stop")

        owned_start_id = state.process_start_id or None
        signal_status = _signal_owned_process(
            state,
            owned_start_id,
            signal.SIGTERM,
        )
        if signal_status in (IdentityStatus.DEAD, IdentityStatus.MISMATCH):
            path.unlink(missing_ok=True)
            return "not running (removed stale state file)"
        if signal_status is IdentityStatus.UNVERIFIABLE:
            _raise_unverifiable(state, "stop")

        exit_status = _wait_for_owned_process_exit(state, owned_start_id)
        if exit_status is IdentityStatus.UNVERIFIABLE:
            _raise_unverifiable(state, "stop")
        if exit_status is IdentityStatus.MATCH:
            signal_status = _signal_owned_process(
                state,
                owned_start_id,
                signal.SIGKILL,
            )
            if signal_status is IdentityStatus.UNVERIFIABLE:
                _raise_unverifiable(state, "stop")
            if signal_status is IdentityStatus.MATCH:
                exit_status = _wait_for_owned_process_exit(state, owned_start_id)
                if exit_status is IdentityStatus.UNVERIFIABLE:
                    _raise_unverifiable(state, "stop")
                if exit_status is IdentityStatus.MATCH:
                    raise LifecycleError(
                        f"gza-server PID {state.pid} did not exit after SIGKILL; "
                        "state was preserved"
                    )
        path.unlink(missing_ok=True)
        return "stopped"


def status_server(path: Path, *, now: datetime | None = None) -> str:
    with server_lock(path):
        state = read_state(path)
        if state is None:
            return "not running"
        identity = classify_server_identity(state)
        if identity in (IdentityStatus.DEAD, IdentityStatus.MISMATCH):
            path.unlink(missing_ok=True)
            return "not running"
        if identity is IdentityStatus.UNVERIFIABLE:
            _raise_unverifiable(state, "check status of")
        started_at = datetime.fromisoformat(state.started_at)
        current = now or datetime.now(UTC)
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        uptime = max(0, int((current - started_at).total_seconds()))
        return f"pid: {state.pid}\nport: {state.port}\nuptime: {uptime}s"


def open_server(path: Path) -> str:
    with server_lock(path):
        state = read_state(path)
        if state is None:
            raise LifecycleError(
                "gza-server is not running; run 'gza-server start' first"
            )
        identity = classify_server_identity(state)
        if identity in (IdentityStatus.DEAD, IdentityStatus.MISMATCH):
            path.unlink(missing_ok=True)
            raise LifecycleError(
                "gza-server is not running; run 'gza-server start' first"
            )
        if identity is IdentityStatus.UNVERIFIABLE:
            _raise_unverifiable(state, "open")
        url = server_url(state)
        webbrowser.open(url)
        return url


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gza-server", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "start",
        help="start the server, wait until ready, open it, and print its URL",
    )
    subparsers.add_parser("stop", help="stop the running server")
    subparsers.add_parser(
        "status",
        help="show the running server's pid, port, and uptime",
    )
    subparsers.add_parser("open", help="open the running server in a browser")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        path = state_file_path()
        if args.command == "start":
            message = start_server(path)
        elif args.command == "stop":
            message = stop_server(path)
        elif args.command == "status":
            message = status_server(path)
        else:
            message = open_server(path)
    except LifecycleError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
