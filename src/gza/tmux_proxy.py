"""Tmux proxy for running tasks in interactive Claude Code sessions.

The proxy sits between the tmux PTY and the inner command (Claude Code).
When no human is attached, it auto-accepts tool prompts by sending Enter
after a quiescence timeout. When a human attaches, all I/O passes through
to the terminal.
"""

import os
import pty
import select
import signal
import subprocess
import sys
import time


class TmuxProxy:
    """Proxy that mediates between a tmux PTY and Claude Code.

    Detached (autonomous) mode:
      - Claude runs interactively and prompts for tool approvals.
      - Proxy detects no tmux clients are attached.
      - After ``auto_accept_timeout`` seconds of quiescence, proxy sends
        Enter to accept the default choice.
      - After ``max_idle_timeout`` seconds of quiescence, proxy assumes the
        session is stuck, sends Ctrl-C + EOF, and exits.

    Attached (supervised) mode:
      - Human has attached via ``gza attach``.
      - Proxy simply forwards all I/O between the PTY and stdin/stdout.
      - On detach, auto-accept resumes after ``detach_grace`` seconds.
    """

    def __init__(
        self,
        session_name: str,
        auto_accept_timeout: float = 10.0,
        max_idle_timeout: float = 300.0,
        detach_grace: float = 5.0,
    ) -> None:
        self.session_name = session_name
        self.auto_accept_timeout = auto_accept_timeout
        self.max_idle_timeout = max_idle_timeout
        self.detach_grace = detach_grace
        self.last_output_time = time.monotonic()
        self._detach_time: float | None = None

    def run(self, cmd: list[str]) -> int:
        """Launch *cmd* in a PTY, mediating I/O based on attach state.

        Returns the exit code of the child process.
        """
        # Fork a PTY for the inner command
        pid, fd = pty.fork()
        if pid == 0:
            # Child process: exec the command
            try:
                os.execvp(cmd[0], cmd)
            except Exception:
                pass
            sys.exit(127)

        # Parent: proxy loop
        try:
            return self._proxy_loop(pid, fd)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    def _proxy_loop(self, child_pid: int, pty_fd: int) -> int:
        """Forward I/O between PTY and terminal; auto-accept when detached."""
        # Forward SIGWINCH (terminal resize) to the child PTY
        def _forward_sigwinch(signum: int, frame: object) -> None:
            try:
                import fcntl
                import termios
                winsize = fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, b"\x00" * 8)
                fcntl.ioctl(pty_fd, termios.TIOCSWINSZ, winsize)
            except OSError:
                pass

        def _forward_sigterm(signum: int, frame: object) -> None:
            try:
                os.kill(child_pid, signal.SIGTERM)
            except OSError:
                pass

        original_sigwinch = signal.signal(signal.SIGWINCH, _forward_sigwinch)
        original_sigterm = signal.signal(signal.SIGTERM, _forward_sigterm)

        try:
            return self._io_loop(child_pid, pty_fd)
        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGWINCH, original_sigwinch)
            signal.signal(signal.SIGTERM, original_sigterm)

    def _io_loop(self, child_pid: int, pty_fd: int) -> int:
        """Core select-based I/O loop."""
        stdin_fd = sys.stdin.fileno() if sys.stdin.isatty() else -1

        while True:
            # Build the read-set; only include stdin when it's a real tty
            read_fds = [pty_fd]
            if stdin_fd >= 0:
                read_fds.append(stdin_fd)

            try:
                readable, _, _ = select.select(read_fds, [], [], 0.2)
            except (OSError, select.error, ValueError):
                break

            for fd in readable:
                if fd == pty_fd:
                    try:
                        data = os.read(pty_fd, 4096)
                        if not data:
                            # EOF from child PTY → child has exited
                            return self._reap(child_pid)
                        try:
                            sys.stdout.buffer.write(data)
                            sys.stdout.buffer.flush()
                        except (OSError, BrokenPipeError):
                            pass
                        self.last_output_time = time.monotonic()
                    except OSError:
                        # PTY closed
                        return self._reap(child_pid)

                elif fd == stdin_fd:
                    try:
                        data = os.read(stdin_fd, 4096)
                        if data:
                            try:
                                os.write(pty_fd, data)
                            except OSError:
                                pass
                    except OSError:
                        pass

            # Non-blocking check for child exit
            try:
                wpid, wstatus = os.waitpid(child_pid, os.WNOHANG)
                if wpid != 0:
                    return os.waitstatus_to_exitcode(wstatus)
            except ChildProcessError:
                return 0

            # Auto-accept logic when no human is present
            if not self._has_human():
                idle = time.monotonic() - self.last_output_time

                if idle > self.max_idle_timeout:
                    # Session appears stuck — send Ctrl-C, then EOF
                    try:
                        os.write(pty_fd, b"\x03")  # Ctrl-C
                        time.sleep(1)
                        os.write(pty_fd, b"\x04")  # Ctrl-D / EOF
                    except OSError:
                        pass
                    break

                if idle > self.auto_accept_timeout:
                    # Send Enter to accept the default prompt choice
                    try:
                        os.write(pty_fd, b"\n")
                        self.last_output_time = time.monotonic()
                    except OSError:
                        break

        return self._reap(child_pid)

    def _reap(self, child_pid: int) -> int:
        """Wait for child process and return its exit code."""
        try:
            _, status = os.waitpid(child_pid, 0)
            return os.waitstatus_to_exitcode(status)
        except ChildProcessError:
            return 0

    def _has_human(self) -> bool:
        """Return True if any tmux client is currently attached to the session."""
        result = subprocess.run(
            ["tmux", "list-clients", "-t", self.session_name, "-F", "#{client_name}"],
            capture_output=True,
            text=True,
        )
        return bool(result.stdout.strip())


def check_tmux_available() -> None:
    """Raise RuntimeError if tmux is not installed."""
    result = subprocess.run(["tmux", "-V"], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "tmux is required for gza tmux support. "
            "Install with: brew install tmux  (macOS) or  apt install tmux  (Debian/Ubuntu)"
        )


def get_tmux_session_pid(session_name: str) -> int | None:
    """Return the PID of the foreground process in the given tmux session pane."""
    result = subprocess.run(
        ["tmux", "display-message", "-p", "-t", session_name, "#{pane_pid}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        return int(result.stdout.strip())
    except ValueError:
        return None


def main() -> int:
    """Entry point for ``python -m gza.tmux_proxy``."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Gza tmux proxy — mediates I/O between a tmux session and Claude Code."
    )
    parser.add_argument("--session", required=True, help="Tmux session name")
    parser.add_argument(
        "--auto-accept-timeout",
        type=float,
        default=10.0,
        metavar="SECS",
        help="Seconds of quiescence before auto-accepting a prompt (default: 10)",
    )
    parser.add_argument(
        "--max-idle-timeout",
        type=float,
        default=300.0,
        metavar="SECS",
        help="Seconds before assuming the session is stuck (default: 300)",
    )
    parser.add_argument(
        "--detach-grace",
        type=float,
        default=5.0,
        metavar="SECS",
        help="Grace period after human detaches before auto-accept resumes (default: 5)",
    )
    parser.add_argument(
        "cmd",
        nargs=argparse.REMAINDER,
        help="Command to run inside the proxy",
    )

    args = parser.parse_args()

    # Strip leading '--' separator that separates proxy args from command
    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd:
        print("Error: No command specified", file=sys.stderr)
        return 1

    proxy = TmuxProxy(
        session_name=args.session,
        auto_accept_timeout=args.auto_accept_timeout,
        max_idle_timeout=args.max_idle_timeout,
        detach_grace=args.detach_grace,
    )
    return proxy.run(cmd)


if __name__ == "__main__":
    sys.exit(main())
