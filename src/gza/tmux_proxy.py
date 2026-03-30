"""Tmux proxy for running tasks in interactive Claude Code sessions.

The proxy sits between the tmux PTY and the inner command (Claude Code).
When no human is attached, it auto-accepts tool prompts by sending Enter
after a quiescence timeout. When a human attaches, all I/O passes through
to the terminal.
"""

import logging
import os
import pty
import select
import signal
import subprocess
import sys
import time

logger = logging.getLogger(__name__)

# Maximum bytes written to the PTY in a single chunk for prompt delivery.
# The PTY line-discipline input buffer is typically 4 KiB; writing in smaller
# chunks and pausing between them prevents overflow for long prompts.
_PROMPT_CHUNK_SIZE = 2000


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
        prompt_file: str | None = None,
    ) -> None:
        self.session_name = session_name
        self.auto_accept_timeout = auto_accept_timeout
        self.max_idle_timeout = max_idle_timeout
        self.detach_grace = detach_grace
        self.prompt_file = prompt_file
        self.last_output_time = time.monotonic()
        self._detach_time: float | None = None
        # Cache for _has_human(): (value, monotonic_timestamp)
        self._has_human_cache: tuple[bool, float] | None = None

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

        # S1: Forward SIGINT so Ctrl-C from an attached user interrupts Claude
        def _forward_sigint(signum: int, frame: object) -> None:
            try:
                os.kill(child_pid, signal.SIGINT)
            except OSError:
                pass

        original_sigwinch = signal.signal(signal.SIGWINCH, _forward_sigwinch)
        original_sigterm = signal.signal(signal.SIGTERM, _forward_sigterm)
        original_sigint = signal.signal(signal.SIGINT, _forward_sigint)

        try:
            return self._io_loop(child_pid, pty_fd)
        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGWINCH, original_sigwinch)
            signal.signal(signal.SIGTERM, original_sigterm)
            signal.signal(signal.SIGINT, original_sigint)

    def _io_loop(self, child_pid: int, pty_fd: int) -> int:
        """Core select-based I/O loop."""
        # Deliver initial prompt to child process via PTY before entering main loop
        if self.prompt_file:
            self._write_prompt_to_pty(pty_fd)

        # Track whether a human was attached on the previous iteration
        _prev_has_human = self._has_human()
        _first_output = True

        while True:
            # Check stdin dynamically on each iteration so attach/detach is handled
            # correctly: when a user attaches to a detached tmux session, isatty()
            # transitions from False to True and stdin becomes available for input.
            try:
                stdin_fd = sys.stdin.fileno() if sys.stdin.isatty() else -1
            except (AttributeError, OSError):
                stdin_fd = -1

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
                        if _first_output:
                            logger.info("First output received from child process")
                            _first_output = False
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

            # Track human attach/detach transitions for grace period
            current_has_human = self._has_human()
            if current_has_human and not _prev_has_human:
                # Human reattached — clear the detach timer
                logger.info("Human attached to session %s", self.session_name)
                self._detach_time = None
            elif not current_has_human and _prev_has_human:
                # Human just detached — record the detach time
                logger.info("Human detached from session %s; grace period %.1fs", self.session_name, self.detach_grace)
                self._detach_time = time.monotonic()
            _prev_has_human = current_has_human

            # Auto-accept logic when no human is present
            if not current_has_human:
                idle = time.monotonic() - self.last_output_time

                if idle > self.max_idle_timeout:
                    # Session appears stuck — send Ctrl-C, wait for response, then EOF
                    logger.info("Session idle for %.1fs (max %s); sending Ctrl-C + EOF", idle, self.max_idle_timeout)
                    try:
                        os.write(pty_fd, b"\x03")  # Ctrl-C
                        time.sleep(3)  # Give child time to respond to Ctrl-C
                        os.write(pty_fd, b"\x04")  # Ctrl-D / EOF
                    except OSError:
                        pass
                    break

                # Respect detach_grace: only auto-accept after grace period
                # has elapsed since the human detached. If _detach_time is None
                # the session was never attached, so no grace period is needed.
                grace_elapsed = (
                    self._detach_time is None
                    or (time.monotonic() - self._detach_time) >= self.detach_grace
                )
                if idle > self.auto_accept_timeout and grace_elapsed:
                    # Send Enter to accept the default prompt choice
                    logger.info("Detached and quiescent (%.1fs); sending auto-accept", idle)
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

    def _write_prompt_to_pty(self, pty_fd: int) -> None:
        """Deliver the initial task prompt to the child process via the PTY master fd.

        The proxy "types" the prompt into the session so Claude receives it as if
        a user had typed it at the terminal. For short prompts (≤2000 bytes) the
        bytes are written in one call; longer prompts are chunked to avoid
        overflowing the PTY line-discipline input buffer.

        The prompt file is deleted after delivery (it is a one-shot temp file).
        """
        if not self.prompt_file:
            return
        try:
            from pathlib import Path as _Path
            prompt_bytes = _Path(self.prompt_file).read_bytes()
            if not prompt_bytes.endswith(b"\n"):
                prompt_bytes += b"\n"
            if len(prompt_bytes) <= _PROMPT_CHUNK_SIZE:
                os.write(pty_fd, prompt_bytes)
            else:
                # Long prompt: write in chunks with brief pauses so the PTY
                # buffer does not fill before the child process reads it.
                for i in range(0, len(prompt_bytes), _PROMPT_CHUNK_SIZE):
                    try:
                        os.write(pty_fd, prompt_bytes[i : i + _PROMPT_CHUNK_SIZE])
                        time.sleep(0.05)
                    except OSError as e:
                        logger.warning("Failed to write prompt chunk to PTY: %s", e)
                        break
        except Exception as e:
            logger.warning("Failed to deliver prompt to PTY: %s", e)
        finally:
            # Clean up the temp file — it is only needed for delivery.
            try:
                os.unlink(self.prompt_file)
            except OSError:
                pass

    def _has_human(self) -> bool:
        """Return True if any tmux client is currently attached to the session.

        The result is cached for 1 second to avoid spawning a subprocess on
        every 0.2-second select timeout.
        """
        now = time.monotonic()
        if self._has_human_cache is not None:
            cached_val, cached_time = self._has_human_cache
            if now - cached_time < 1.0:
                return cached_val
        result = subprocess.run(
            ["tmux", "list-clients", "-t", self.session_name, "-F", "#{client_name}"],
            capture_output=True,
            text=True,
        )
        val = bool(result.stdout.strip())
        self._has_human_cache = (val, now)
        return val


def check_tmux_available() -> None:
    """Raise RuntimeError or FileNotFoundError if tmux is not installed.

    Raises:
        FileNotFoundError: If tmux is not found on PATH.
        RuntimeError: If tmux is found but returns a non-zero exit code.
    """
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
        "--prompt-file",
        metavar="PATH",
        default=None,
        help=(
            "Path to a file containing the initial task prompt. "
            "The proxy writes the file contents to the PTY after the child starts, "
            "simulating a user typing the prompt. The file is deleted after delivery."
        ),
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
        prompt_file=args.prompt_file,
    )
    return proxy.run(cmd)


if __name__ == "__main__":
    sys.exit(main())
