"""Manage the local gza HTTP API and web UI process."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
import webbrowser
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import BinaryIO
from urllib.error import URLError
from urllib.request import urlopen

from gza.config import DEFAULT_SERVER_PORT, Config, ConfigError

HOST = "127.0.0.1"
PORT_ENV_VAR = "GZA_SERVER_PORT"
STATE_FILENAME = "gza-server.json"
LOCK_FILENAME = "gza-server.lock"
LOG_FILENAME = "gza-server.log"
# Rotate one generation so a crash loop cannot fill the disk, while the run
# before the current one stays readable.
LOG_MAX_BYTES = 5_000_000
STOP_TIMEOUT_SECONDS = 5.0
STARTUP_TIMEOUT_SECONDS = 10.0
HEALTH_REQUEST_TIMEOUT_SECONDS = 0.25
STARTUP_POLL_INTERVAL_SECONDS = 0.05
STARTUP_DIAGNOSTIC_MAX_BYTES = 2000
DARWIN_PROCESS_START_ENV = {"LC_ALL": "C", "TZ": "UTC"}
PS_TIMEOUT_SECONDS = 5.0
# Watch only this package. Uvicorn's default watches the whole working
# directory, which here is a repository holding a task database and worktrees,
# and every write to those would bounce the server.
RELOAD_DIR = Path(__file__).resolve().parent


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
    def from_dict(cls, value: object) -> ServerState:
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


def log_file_path(path: Path) -> Path:
    """Return the server log beside the state file for this project."""
    return path.parent / LOG_FILENAME


def _open_log_file(path: Path) -> BinaryIO:
    """Open the append-only server log, rotating one generation when large."""
    log_path = log_file_path(path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if log_path.stat().st_size > LOG_MAX_BYTES:
            os.replace(log_path, log_path.with_suffix(log_path.suffix + ".1"))
    except OSError:
        pass
    return log_path.open("ab")


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
    """Return the platform's stable start marker for a PID, when it can be read.

    A PID alone is not an identity: the kernel reuses it, so a signal aimed at
    a recorded PID can land on an unrelated process. Pairing the PID with the
    time it started makes that reuse detectable.
    """
    if sys.platform.startswith("linux"):
        return _linux_process_start_id(pid)
    if sys.platform == "darwin":
        return _darwin_process_start_id(pid)
    return None


def _linux_process_start_id(pid: int) -> str | None:
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


def _darwin_process_start_id(pid: int) -> str | None:
    """Read absolute process start time via ps, for platforms without /proc.

    macOS has no /proc, which previously left every process here unidentifiable
    and forced identity to rest entirely on the HTTP health endpoint.
    """
    try:
        environment = {**os.environ, **DARWIN_PROCESS_START_ENV}
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True,
            check=False,
            env=environment,
            text=True,
            timeout=PS_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    start_time = result.stdout.strip()
    return start_time or None


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((HOST, 0))
        return int(listener.getsockname()[1])


def resolve_port(explicit: int | None = None, project_dir: Path | None = None) -> int:
    """Resolve the port to listen on, most specific source first.

    A fixed port is the point: an address that changes on every restart cannot
    be bookmarked or kept open in a tab. 0 stays available at every layer as an
    explicit request for a throwaway ephemeral port.
    """
    if explicit is not None:
        return explicit

    from_env = os.environ.get(PORT_ENV_VAR)
    if from_env:
        try:
            return int(from_env)
        except ValueError as exc:
            raise LifecycleError(
                f"{PORT_ENV_VAR} must be an integer, got {from_env!r}"
            ) from exc

    try:
        return int(Config.load(project_dir or Path.cwd(), discover=True).server_port)
    except (ConfigError, OSError, ValueError):
        # Running outside a configured project is normal for a local tool.
        return DEFAULT_SERVER_PORT


def claim_port(port: int) -> int:
    """Return a bindable port, refusing to silently move to a different one.

    Falling back to a free port would defeat the purpose of configuring one,
    and would do it silently -- the server would come up somewhere the operator
    is not looking.
    """
    if port == 0:
        return find_free_port()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            # Match how uvicorn binds. Without this the probe is stricter than
            # the server it speaks for: a socket left in TIME_WAIT by the
            # previous instance reads as busy, so an immediate restart on a
            # fixed port would fail even though the port is usable.
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((HOST, port))
    except OSError as exc:
        raise LifecycleError(
            f"port {port} is not available ({exc.strerror or exc}). "
            f"Free it, pass --port, or set {PORT_ENV_VAR} or server_port in gza.yaml."
        ) from exc
    return port


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
        # A matching start marker already proves this is the process we
        # launched. Requiring a health response on top of it made a server too
        # busy to answer within the health timeout unstoppable, which is the
        # state a server most needs stopping from.
        return IdentityStatus.MATCH
    return _health_identity_status(state)


def _raise_unverifiable(state: ServerState, action: str) -> None:
    raise LifecycleError(
        f"cannot {action} gza-server: recorded PID {state.pid} is alive, but its "
        "identity could not be verified; state was preserved"
    )


def _owned_process_status(
    state: ServerState,
    expected_start_id: str | None,
    *,
    terminating: bool = False,
) -> IdentityStatus:
    """Revalidate the strongest available identity for a previously matched server.

    ``terminating`` marks the phase after we have already verified ownership and
    signalled the process. A server that is shutting down closes its listening
    socket before it exits, so its health endpoint goes away while the process is
    still alive. Treating that as a lost identity used to abort the stop before
    it could escalate, leaving the process running -- so during termination an
    unreachable endpoint means "still exiting", not "no longer ours".
    """
    if not process_is_alive(state.pid):
        return IdentityStatus.DEAD
    if expected_start_id is None:
        status = _health_identity_status(state)
        if terminating and status is IdentityStatus.UNVERIFIABLE:
            return IdentityStatus.MATCH
        return status
    current_start_id = process_start_id(state.pid)
    if current_start_id is None:
        # A process caught mid-exit can be alive with no readable start marker.
        # While terminating that means "still going", not "no longer ours";
        # the next poll sees it gone.
        return IdentityStatus.MATCH if terminating else IdentityStatus.UNVERIFIABLE
    if current_start_id != expected_start_id:
        return IdentityStatus.MISMATCH
    return IdentityStatus.MATCH


def _signaled_process_exit_status(
    state: ServerState,
    expected_start_id: str | None,
) -> IdentityStatus:
    """Observe a previously signaled owned process during its shutdown window.

    This whole phase is post-signal, so it is terminating by definition: a
    process that is alive but can no longer be identified is one on its way
    out, not one that stopped being ours. A start marker that reads back
    *different* is still a mismatch, so a recycled PID is caught.
    """
    return _owned_process_status(state, expected_start_id, terminating=True)


def _signal_owned_process(
    state: ServerState,
    expected_start_id: str | None,
    sig: signal.Signals,
    *,
    terminating: bool = False,
) -> IdentityStatus:
    """Signal only after an immediate strict ownership revalidation."""
    identity = _owned_process_status(state, expected_start_id, terminating=terminating)
    if identity is not IdentityStatus.MATCH:
        return identity
    try:
        _kill_owned_group(state.pid, sig)
    except ProcessLookupError:
        return IdentityStatus.DEAD
    return IdentityStatus.MATCH


def _kill_owned_group(pid: int, sig: signal.Signals) -> None:
    """Signal the managed process and anything it spawned.

    Under --reload uvicorn runs a supervisor that forks a worker, and the state
    file only records the supervisor. Signalling the pid alone would leave the
    worker holding the port after a SIGKILL escalation. The process is spawned
    with start_new_session, so it leads its own group and the group is exactly
    the server and its children.
    """
    try:
        os.killpg(os.getpgid(pid), sig)
    except (OSError, AttributeError):
        # A process mid-exit can lose its group before its pid; fall back rather
        # than reporting a live process as already dead.
        os.kill(pid, sig)


def _wait_for_owned_process_exit(
    state: ServerState,
    expected_start_id: str | None,
) -> IdentityStatus:
    """Wait until the managed process dies or ownership is no longer verified."""
    deadline = time.monotonic() + STOP_TIMEOUT_SECONDS
    while True:
        identity = _signaled_process_exit_status(state, expected_start_id)
        if identity is not IdentityStatus.MATCH:
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


@contextmanager
def _log_reader(stream: BinaryIO) -> Iterator[BinaryIO]:
    """Read a stream opened for append, which cannot itself be read."""
    with open(stream.name, "rb") as reader:  # type: ignore[arg-type]
        yield reader


def _startup_diagnostic(stream: BinaryIO, start_offset: int = 0) -> str:
    """Read this run's output, ignoring any earlier run's tail in the log."""
    try:
        stream.flush()
        with _log_reader(stream) as reader:
            reader.seek(0, os.SEEK_END)
            size = reader.tell()
            reader.seek(max(start_offset, size - STARTUP_DIAGNOSTIC_MAX_BYTES))
            output = reader.read().decode("utf-8", errors="replace").strip()
    except (OSError, ValueError):
        return ""
    if not output:
        return ""
    return f"; startup output: {output}"


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


def start_server(path: Path, *, reload: bool = True, port: int | None = None) -> str:
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

        port = claim_port(resolve_port(port))
        instance_id = secrets.token_urlsafe(32)
        environment = os.environ.copy()
        environment["GZA_SERVER_INSTANCE_ID"] = instance_id
        environment[PORT_ENV_VAR] = str(port)
        with _open_log_file(path) as startup_output:
            start_offset = startup_output.tell()
            command = [
                sys.executable,
                "-m",
                "uvicorn",
                "gza_server.app:create_app",
                "--no-access-log",
                "--factory",
                "--host",
                HOST,
                "--port",
                str(port),
            ]
            if reload:
                command += ["--reload", "--reload-dir", str(RELOAD_DIR)]
            # The guard owns the child, so a server that stops answering is
            # noticed instead of leaving the port held with nothing behind it.
            command = [sys.executable, "-m", "gza_server.supervisor", *command]
            process = subprocess.Popen(
                command,
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
                    f"{_startup_diagnostic(startup_output, start_offset)}"
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

        # Pin the strongest identity available right now. State written on a
        # platform that could not read a start marker still gets one here, so a
        # PID recycled mid-stop is detected rather than signalled blindly.
        owned_start_id = state.process_start_id or process_start_id(state.pid)
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
                terminating=True,
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
        return (
            f"pid: {state.pid}\nport: {state.port}\nuptime: {uptime}s\n"
            f"log: {log_file_path(path)}"
        )


def logs_server(path: Path, *, lines: int = 200) -> str:
    """Return the tail of the server log, which outlives the server process."""
    log_path = log_file_path(path)
    if not log_path.exists():
        return f"no server log yet at {log_path}"
    with log_path.open("rb") as reader:
        text = reader.read().decode("utf-8", errors="replace")
    tail = text.splitlines()[-lines:]
    return "\n".join([f"==> {log_path} <==", *tail])


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
    start = subparsers.add_parser(
        "start",
        help=(
            "start the server without routine access logging, wait until ready, "
            "open it, and print its URL"
        ),
    )
    start.add_argument(
        "--port",
        type=int,
        default=None,
        help=(
            "port to listen on; 0 picks a free one. "
            f"Defaults to ${PORT_ENV_VAR}, then server_port in gza.yaml, then {DEFAULT_SERVER_PORT}."
        ),
    )
    start.add_argument(
        "--no-reload",
        dest="reload",
        action="store_false",
        help="do not restart the server when its source changes",
    )
    subparsers.add_parser("stop", help="stop the running server")
    subparsers.add_parser(
        "status",
        help="show the running server's pid, port, and uptime",
    )
    subparsers.add_parser("open", help="open the running server in a browser")
    logs = subparsers.add_parser(
        "logs", help="print the tail of the server log, including past runs"
    )
    logs.add_argument(
        "-n",
        "--lines",
        type=int,
        default=200,
        help="how many trailing lines to print",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        path = state_file_path()
        if args.command == "start":
            message = start_server(path, reload=args.reload, port=args.port)
        elif args.command == "stop":
            message = stop_server(path)
        elif args.command == "status":
            message = status_server(path)
        elif args.command == "logs":
            message = logs_server(path, lines=args.lines)
        else:
            message = open_server(path)
    except LifecycleError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
