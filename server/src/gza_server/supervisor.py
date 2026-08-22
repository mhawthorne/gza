"""Keep a uvicorn server serving, or exit rather than hold the port silently.

Uvicorn's reload supervisor only respawns its worker in response to a file
change. A worker that dies on its own leaves the supervisor looping while it
still owns the listening socket, so connections are accepted and then never
answered -- the server looks alive to the kernel and hangs for every client.
This process watches the health endpoint instead of the child's PID, which is
the only signal that distinguishes serving from merely running.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from types import FrameType
from urllib.error import URLError
from urllib.request import urlopen

HEALTH_TIMEOUT_SECONDS = 2.0
HEALTH_POLL_INTERVAL_SECONDS = 5.0
# A restarting child needs to import the app and bind before it can answer, and
# under --reload that is slower than a steady-state poll.
STARTUP_GRACE_SECONDS = 30.0
UNHEALTHY_POLLS_BEFORE_RESTART = 3
# The CLI's `stop` gives this guard 5s before it SIGKILLs it. Escalating to
# SIGKILL well inside that budget is what keeps a stop from leaving an orphaned
# child still holding the port.
CHILD_STOP_TIMEOUT_SECONDS = 2.0
RESTART_BACKOFF_SECONDS = 2.0
# Restarting forever would hide a crash that reproduces on every boot. Past this
# many restarts in the window the guard exits, freeing the port and letting
# `gza-server status` report the truth.
MAX_RESTARTS = 5
RESTART_WINDOW_SECONDS = 600.0


class _Shutdown:
    """Records an explicit stop so the guard never fights a manual stop."""

    def __init__(self) -> None:
        self.requested = False

    def request(self, signum: int, frame: FrameType | None) -> None:
        self.requested = True


def _log(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(f"{timestamp} [gza-server guard] {message}", flush=True)


def _is_healthy(url: str, instance_id: str) -> bool:
    try:
        with urlopen(url, timeout=HEALTH_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                return False
            payload = json.loads(response.read())
    except (OSError, TimeoutError, URLError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    # Without an identity to match, any listener on the port would read as
    # healthy -- including one that replaced this server.
    if instance_id:
        return payload.get("instance_id") == instance_id
    return True


def _spawn(command: list[str]) -> subprocess.Popen[bytes]:
    _log(f"starting: {' '.join(command)}")
    # Its own process group, so stopping it also reaps the reload worker it
    # spawns. Killing only the direct child orphans that worker, which keeps
    # the port bound and makes every restart fail to bind.
    return subprocess.Popen(command, stdin=subprocess.DEVNULL, start_new_session=True)


def _signal_tree(child: subprocess.Popen[bytes], sig: signal.Signals) -> None:
    """Signal the child's whole process group, falling back to the child.

    `_spawn` makes the child a group leader, so its group id is its pid. Using
    that directly rather than looking it up keeps the group reachable after the
    child itself has been reaped, which is exactly when an orphaned reload
    worker still holds the port.
    """
    try:
        os.killpg(child.pid, sig)
        if sig == signal.SIGTERM:
            # A stopped process cannot act on SIGTERM. Waking it is the
            # difference between a clean exit and an orphan holding the port.
            os.killpg(child.pid, signal.SIGCONT)
        return
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        if sig == signal.SIGKILL:
            child.kill()
        else:
            child.terminate()
    except ProcessLookupError:
        pass


def _stop_child(child: subprocess.Popen[bytes]) -> None:
    if child.poll() is not None:
        child.wait()
        return
    _signal_tree(child, signal.SIGTERM)
    try:
        child.wait(timeout=CHILD_STOP_TIMEOUT_SECONDS)
        return
    except subprocess.TimeoutExpired:
        _log("child did not exit on SIGTERM; killing")
    _signal_tree(child, signal.SIGKILL)
    child.wait()


def _reap_orphans(child: subprocess.Popen[bytes]) -> None:
    """Clear anything left in the exited child's process group."""
    child.wait()
    _signal_tree(child, signal.SIGKILL)


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        print("usage: gza-server-guard <uvicorn command...>", file=sys.stderr)
        return 2

    port = os.environ.get("GZA_SERVER_PORT", "")
    instance_id = os.environ.get("GZA_SERVER_INSTANCE_ID", "")
    health = f"http://127.0.0.1:{port}/api/health"

    shutdown = _Shutdown()
    signal.signal(signal.SIGTERM, shutdown.request)
    signal.signal(signal.SIGINT, shutdown.request)

    restart_times: list[float] = []
    child = _spawn(arguments)
    spawned_at = time.monotonic()
    consecutive_failures = 0

    try:
        while not shutdown.requested:
            time.sleep(HEALTH_POLL_INTERVAL_SECONDS)
            if shutdown.requested:
                break

            exited = child.poll()
            within_grace = time.monotonic() - spawned_at < STARTUP_GRACE_SECONDS
            if exited is None:
                if within_grace:
                    continue
                if _is_healthy(health, instance_id):
                    consecutive_failures = 0
                    continue
                consecutive_failures += 1
                if consecutive_failures < UNHEALTHY_POLLS_BEFORE_RESTART:
                    continue
                _log(
                    f"unhealthy for {consecutive_failures} consecutive checks "
                    f"at {health}; recycling the server"
                )
                _stop_child(child)
            else:
                _log(f"server exited with status {exited}")
                # The child's reload worker outlives it and keeps the port
                # bound, so the next start would fail to bind.
                _reap_orphans(child)

            now = time.monotonic()
            restart_times = [
                stamp
                for stamp in restart_times
                if now - stamp < RESTART_WINDOW_SECONDS
            ]
            if len(restart_times) >= MAX_RESTARTS:
                _log(
                    f"{len(restart_times)} restarts within "
                    f"{RESTART_WINDOW_SECONDS:.0f}s; giving up so the port is "
                    "released and `gza-server status` reports the failure"
                )
                return 1
            restart_times.append(now)
            time.sleep(RESTART_BACKOFF_SECONDS)
            if shutdown.requested:
                break
            child = _spawn(arguments)
            spawned_at = time.monotonic()
            consecutive_failures = 0
    finally:
        if shutdown.requested:
            _log("stop requested; shutting the server down")
        _stop_child(child)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
