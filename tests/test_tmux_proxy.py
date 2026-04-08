"""Tests for the tmux proxy module."""

import errno
import os
import subprocess
import time
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from gza.tmux_proxy import TmuxProxy, check_tmux_available, get_tmux_session_pid


class TestTmuxProxyHasHuman:
    """Tests for TmuxProxy._has_human()."""

    def test_has_human_returns_true_when_clients_present(self):
        proxy = TmuxProxy(session_name="gza-99")
        mock_result = MagicMock()
        mock_result.stdout = "client-1\n"
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            assert proxy._has_human() is True

    def test_has_human_returns_false_when_no_clients(self):
        proxy = TmuxProxy(session_name="gza-99")
        mock_result = MagicMock()
        mock_result.stdout = ""
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            assert proxy._has_human() is False

    def test_has_human_returns_false_on_whitespace_only(self):
        proxy = TmuxProxy(session_name="gza-99")
        mock_result = MagicMock()
        mock_result.stdout = "   \n  "
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            assert proxy._has_human() is False

    def test_has_human_passes_correct_session_name(self):
        proxy = TmuxProxy(session_name="gza-42")
        mock_result = MagicMock()
        mock_result.stdout = ""
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result) as mock_run:
            proxy._has_human()
            call_args = mock_run.call_args[0][0]
            assert "gza-42" in call_args


class TestCheckTmuxAvailable:
    """Tests for check_tmux_available()."""

    def test_raises_when_tmux_not_found(self):
        with patch(
            "gza.tmux_proxy.subprocess.run",
            side_effect=FileNotFoundError("tmux not found"),
        ):
            with pytest.raises(FileNotFoundError):
                check_tmux_available()

    def test_raises_when_tmux_returns_nonzero(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="tmux is required"):
                check_tmux_available()

    def test_succeeds_when_tmux_available(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            # Should not raise
            check_tmux_available()


class TestGetTmuxSessionPid:
    """Tests for get_tmux_session_pid()."""

    def test_returns_pid_on_success(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "12345\n"
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            assert get_tmux_session_pid("gza-42") == 12345

    def test_returns_none_on_nonzero_returncode(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            assert get_tmux_session_pid("gza-42") is None

    def test_returns_none_on_empty_output(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            assert get_tmux_session_pid("gza-42") is None

    def test_returns_none_on_non_integer_output(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not-a-number"
        with patch("gza.tmux_proxy.subprocess.run", return_value=mock_result):
            assert get_tmux_session_pid("gza-42") is None


class TestTmuxProxyInit:
    """Tests for TmuxProxy initialization."""

    def test_default_timeouts(self):
        proxy = TmuxProxy(session_name="gza-1")
        assert proxy.auto_accept_timeout == 10.0
        assert proxy.max_idle_timeout == 300.0
        assert proxy.detach_grace == 5.0

    def test_custom_timeouts(self):
        proxy = TmuxProxy(
            session_name="gza-1",
            auto_accept_timeout=20.0,
            max_idle_timeout=600.0,
            detach_grace=3.0,
        )
        assert proxy.auto_accept_timeout == 20.0
        assert proxy.max_idle_timeout == 600.0
        assert proxy.detach_grace == 3.0

    def test_session_name_stored(self):
        proxy = TmuxProxy(session_name="gza-99")
        assert proxy.session_name == "gza-99"

    def test_detach_time_initially_none(self):
        proxy = TmuxProxy(session_name="gza-1")
        assert proxy._detach_time is None


class TestIoLoopAutoAccept:
    """Tests for TmuxProxy._io_loop auto-accept and detach-grace behavior."""

    def _make_proxy(self, auto_accept_timeout=1.0, detach_grace=5.0, max_idle_timeout=300.0):
        return TmuxProxy(
            session_name="gza-test",
            auto_accept_timeout=auto_accept_timeout,
            max_idle_timeout=max_idle_timeout,
            detach_grace=detach_grace,
        )

    def test_auto_accept_fires_after_quiescence_when_detached(self):
        """Auto-accept sends Enter after quiescence when no human is attached."""
        proxy = self._make_proxy(auto_accept_timeout=0.0, detach_grace=0.0)
        proxy.last_output_time = time.monotonic() - 1.0  # stale

        written = []

        def fake_write(fd, data):
            written.append(data)

        def fake_select(rds, wds, eds, timeout):
            return [], [], []

        def fake_waitpid(pid, flags):
            # Return done after first write
            if written:
                return (pid, 0)
            return (0, 0)

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=False), \
             patch("select.select", side_effect=fake_select), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("os.write", side_effect=fake_write), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            proxy._io_loop(child_pid=999, pty_fd=10)

        assert b"\n" in written, "Expected Enter keystroke to be sent"

    def test_no_auto_accept_when_human_attached(self):
        """Auto-accept is suppressed when a human is attached."""
        proxy = self._make_proxy(auto_accept_timeout=0.0, detach_grace=0.0)
        proxy.last_output_time = time.monotonic() - 10.0  # very stale

        written = []
        call_count = [0]

        def fake_waitpid(pid, flags):
            call_count[0] += 1
            if call_count[0] > 3:
                return (pid, 0)
            return (0, 0)

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=True), \
             patch("select.select", return_value=([], [], [])), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("os.write", side_effect=lambda fd, d: written.append(d)), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            proxy._io_loop(child_pid=999, pty_fd=10)

        assert b"\n" not in written, "Enter should NOT be sent when human is attached"

    def test_auto_accept_does_not_fire_within_detach_grace(self):
        """Auto-accept does not fire during the detach grace window."""
        # detach_grace is 60s so the grace period never expires in this test
        proxy = self._make_proxy(auto_accept_timeout=0.0, detach_grace=60.0)
        proxy.last_output_time = time.monotonic() - 10.0  # stale

        # Simulate: human was attached last iteration, now detached
        # _prev_has_human starts as the result of the first _has_human() call.
        # We need: first call returns True (was attached), subsequent calls False.
        has_human_returns = [True, False, False, False]
        call_idx = [0]
        written = []
        waitpid_calls = [0]

        def fake_has_human():
            idx = min(call_idx[0], len(has_human_returns) - 1)
            call_idx[0] += 1
            return has_human_returns[idx]

        def fake_waitpid(pid, flags):
            waitpid_calls[0] += 1
            if waitpid_calls[0] > 5:
                return (pid, 0)
            return (0, 0)

        with patch("gza.tmux_proxy.TmuxProxy._has_human", side_effect=fake_has_human), \
             patch("select.select", return_value=([], [], [])), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("os.write", side_effect=lambda fd, d: written.append(d)), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            proxy._io_loop(child_pid=999, pty_fd=10)

        assert b"\n" not in written, (
            "Auto-accept should NOT fire within the detach_grace window"
        )


class TestPromptDeliveryViaPty:
    """Tests for M1: prompt delivery via PTY master fd (not positional arg)."""

    def test_prompt_delivery_via_stdin_to_claude_in_tmux(self, tmp_path: Path):
        """Proxy writes prompt to pty_fd before main loop, not passed as positional arg."""
        prompt_file = tmp_path / "prompt.txt"
        prompt_text = "Implement a hello world function"
        prompt_file.write_text(prompt_text)

        proxy = TmuxProxy(session_name="gza-42", prompt_file=str(prompt_file))

        written_to_pty: list[bytes] = []

        def fake_write(fd: int, data: bytes) -> int:
            if fd == 10:  # pty_fd
                written_to_pty.append(data)
            return len(data)

        def fake_select(rds, wds, eds, timeout):
            return [], [], []

        call_count = [0]

        def fake_waitpid(pid, flags):
            call_count[0] += 1
            if call_count[0] > 2:
                return (pid, 0)
            return (0, 0)

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=False), \
             patch("select.select", side_effect=fake_select), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("os.write", side_effect=fake_write), \
             patch("os.unlink"), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            proxy._io_loop(child_pid=999, pty_fd=10)

        # Prompt should have been written to the PTY fd
        all_written = b"".join(written_to_pty)
        assert prompt_text.encode() in all_written, (
            "Prompt must be delivered to pty_fd, not passed as positional arg"
        )

    def test_long_prompt_delivery_uses_chunked_writes(self, tmp_path: Path):
        """Prompts >2000 bytes are delivered in chunks via the IO loop."""
        from gza.tmux_proxy import _PROMPT_CHUNK_SIZE

        prompt_text = "x" * ((_PROMPT_CHUNK_SIZE * 2) + 100)  # ~4100+ chars
        prompt_file = tmp_path / "long_prompt.txt"
        prompt_file.write_text(prompt_text)

        proxy = TmuxProxy(session_name="gza-42", prompt_file=str(prompt_file))

        written_to_pty: list[bytes] = []

        def fake_write(fd: int, data: bytes) -> int:
            if fd == 10:  # pty_fd
                written_to_pty.append(data)
            return len(data)

        call_count = [0]

        def fake_waitpid(pid, flags):
            call_count[0] += 1
            if call_count[0] > 5:  # enough iterations for chunked delivery
                return (pid, 0)
            return (0, 0)

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=False), \
             patch("select.select", return_value=([], [], [])), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("os.write", side_effect=fake_write), \
             patch("os.unlink"), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            proxy._io_loop(child_pid=999, pty_fd=10)

        # Should have written in multiple chunks
        assert len(written_to_pty) > 1, (
            "Long prompts must be written in multiple chunks"
        )
        # All content should be delivered
        all_written = b"".join(written_to_pty)
        assert prompt_text.encode() in all_written

    def test_prompt_file_deleted_after_load(self, tmp_path: Path):
        """The temp prompt file is cleaned up after loading, before IO loop."""
        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("test prompt")

        proxy = TmuxProxy(session_name="gza-42", prompt_file=str(prompt_file))

        result = proxy._load_prompt_data()

        assert result is not None
        assert b"test prompt" in result
        assert not prompt_file.exists(), "Prompt temp file must be deleted after loading"

    def test_no_prompt_data_when_prompt_file_not_set(self):
        """_load_prompt_data returns None when prompt_file is None."""
        proxy = TmuxProxy(session_name="gza-42", prompt_file=None)

        result = proxy._load_prompt_data()

        assert result is None


class TestDynamicStdinHandling:
    """Tests for M3: stdin is checked dynamically on each select iteration."""

    def test_stdin_forwarded_to_pty_after_attach(self):
        """Stdin input is forwarded to PTY once isatty() becomes True (simulating attach)."""
        proxy = TmuxProxy(session_name="gza-test")

        forwarded_to_pty: list[bytes] = []
        isatty_calls = [0]

        def dynamic_isatty():
            # First 3 calls: not a tty (detached); subsequent calls: is a tty (attached)
            isatty_calls[0] += 1
            return isatty_calls[0] > 3

        stdin_data_sent = [False]

        def fake_select(rds, wds, eds, timeout):
            # Once stdin_fd is in rds (isatty returned True), return stdin as readable
            if len(rds) > 1 and not stdin_data_sent[0]:
                stdin_data_sent[0] = True
                return [rds[-1]], [], []  # return stdin fd as readable
            return [], [], []

        waitpid_calls = [0]

        def fake_waitpid(pid, flags):
            waitpid_calls[0] += 1
            if waitpid_calls[0] > 10:
                return (pid, 0)
            return (0, 0)

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=True), \
             patch("select.select", side_effect=fake_select), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("os.read", return_value=b"hello"), \
             patch("os.write", side_effect=lambda fd, data: forwarded_to_pty.append((fd, data)) or len(data)), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.side_effect = dynamic_isatty
            mock_stdin.fileno.return_value = 5  # fake stdin fd
            proxy._io_loop(child_pid=999, pty_fd=10)

        # Verify that at some point stdin data was forwarded to the PTY
        pty_writes = [data for fd, data in forwarded_to_pty if fd == 10]
        assert any(b"hello" in d for d in pty_writes), (
            "stdin data must be forwarded to PTY after user attaches"
        )

    def test_proxy_forwards_keyboard_input_when_attached(self):
        """When isatty() is True, keyboard input read from stdin is written to pty_fd."""
        proxy = TmuxProxy(session_name="gza-test")

        keyboard_data = b"approve\n"
        written_to_pty: list[bytes] = []
        called = [False]

        def fake_select(rds, wds, eds, timeout):
            if not called[0] and len(rds) > 1:
                called[0] = True
                return [rds[-1]], [], []  # stdin fd readable
            return [], [], []

        waitpid_calls = [0]

        def fake_waitpid(pid, flags):
            waitpid_calls[0] += 1
            if waitpid_calls[0] > 5:
                return (pid, 0)
            return (0, 0)

        def fake_read(fd, n):
            if fd != 10:  # stdin fd
                return keyboard_data
            raise OSError("no data")

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=True), \
             patch("select.select", side_effect=fake_select), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("os.read", side_effect=fake_read), \
             patch("os.write", side_effect=lambda fd, data: written_to_pty.append((fd, data)) or len(data)), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = True
            mock_stdin.fileno.return_value = 5
            proxy._io_loop(child_pid=999, pty_fd=10)

        pty_writes = [data for fd, data in written_to_pty if fd == 10]
        assert any(keyboard_data in d for d in pty_writes), (
            "Keyboard input from stdin must be forwarded to PTY when attached"
        )


class TestDrainPty:
    """Tests for TmuxProxy._drain_pty()."""

    def test_drain_pty_forwards_remaining_output(self):
        """_drain_pty reads buffered data and writes it to stdout."""
        proxy = TmuxProxy(session_name="gza-test")

        remaining = [b"last line\n", b""]
        written: list[bytes] = []

        def fake_select(rds, wds, eds, timeout):
            # Return readable only while there is data left
            if remaining and remaining[0]:
                return [rds[0]], [], []
            return [], [], []

        def fake_read(fd, n):
            if remaining:
                chunk = remaining.pop(0)
                if not chunk:
                    raise OSError("eof")
                return chunk
            raise OSError("eof")

        with patch("select.select", side_effect=fake_select), \
             patch("os.read", side_effect=fake_read), \
             patch("sys.stdout") as mock_stdout:
            mock_stdout.buffer = MagicMock()
            mock_stdout.buffer.write = lambda d: written.append(d)
            mock_stdout.buffer.flush = lambda: None
            proxy._drain_pty(pty_fd=10)

        assert b"last line\n" in written, "Remaining PTY data must be forwarded to stdout"

    def test_drain_pty_stops_when_no_data_ready(self):
        """_drain_pty exits immediately when no data is readable."""
        proxy = TmuxProxy(session_name="gza-test")

        with patch("select.select", return_value=([], [], [])), \
             patch("os.read") as mock_read:
            proxy._drain_pty(pty_fd=10)

        mock_read.assert_not_called()

    def test_drain_pty_stops_on_pty_oserror(self):
        """_drain_pty handles OSError from os.read gracefully."""
        proxy = TmuxProxy(session_name="gza-test")

        with patch("select.select", return_value=([10], [], [])), \
             patch("os.read", side_effect=OSError("pty closed")), \
             patch("sys.stdout") as mock_stdout:
            mock_stdout.buffer = MagicMock()
            # Should not raise
            proxy._drain_pty(pty_fd=10)

    def test_drain_pty_stops_on_select_oserror(self):
        """_drain_pty handles OSError from select gracefully."""
        proxy = TmuxProxy(session_name="gza-test")

        with patch("select.select", side_effect=OSError("bad fd")), \
             patch("os.read") as mock_read:
            proxy._drain_pty(pty_fd=10)

        mock_read.assert_not_called()

    def test_wnohang_exit_drains_pty_before_returning(self):
        """When WNOHANG detects child exit, remaining PTY output is drained first."""
        proxy = TmuxProxy(session_name="gza-test")
        proxy.last_output_time = time.monotonic()

        # Data that arrives in PTY after WNOHANG detects exit
        final_data = [b"final output"]
        drained: list[bytes] = []
        waitpid_calls = [0]

        def fake_waitpid(pid, flags):
            waitpid_calls[0] += 1
            if flags == os.WNOHANG and waitpid_calls[0] >= 2:
                return (pid, 0)  # child exited
            return (0, 0)

        def fake_drain_pty(pty_fd):
            # Simulate draining: collect remaining data
            if final_data:
                drained.extend(final_data)
                final_data.clear()

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=False), \
             patch("select.select", return_value=([], [], [])), \
             patch("os.waitpid", side_effect=fake_waitpid), \
             patch("gza.tmux_proxy.TmuxProxy._drain_pty", side_effect=fake_drain_pty), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            proxy._io_loop(child_pid=999, pty_fd=10)

        assert drained, "_drain_pty must be called when WNOHANG detects child exit"

    def test_break_path_drains_pty_before_reap(self):
        """When the loop breaks (e.g. select error), remaining PTY output is drained."""
        proxy = TmuxProxy(session_name="gza-test")

        drained: list[bool] = []

        def fake_drain_pty(pty_fd):
            drained.append(True)

        def fake_reap(child_pid):
            return 0

        with patch("gza.tmux_proxy.TmuxProxy._has_human", return_value=False), \
             patch("select.select", side_effect=OSError("bad fd")), \
             patch("gza.tmux_proxy.TmuxProxy._drain_pty", side_effect=fake_drain_pty), \
             patch("gza.tmux_proxy.TmuxProxy._reap", side_effect=fake_reap), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            proxy._io_loop(child_pid=999, pty_fd=10)

        assert drained, "_drain_pty must be called before _reap when the loop breaks"


class TestHasHumanCaching:
    """Tests for S1: _has_human() result is cached for 1 second."""

    def test_has_human_cached_for_one_second(self):
        """_has_human() does not call subprocess within 1 second of the previous call."""
        proxy = TmuxProxy(session_name="gza-99")
        mock_result = MagicMock()
        mock_result.stdout = "client-1\n"
        call_count = [0]

        def counting_run(*args, **kwargs):
            call_count[0] += 1
            return mock_result

        with patch("gza.tmux_proxy.subprocess.run", side_effect=counting_run):
            proxy._has_human()
            proxy._has_human()
            proxy._has_human()

        assert call_count[0] == 1, "subprocess.run should only be called once within cache TTL"

    def test_has_human_cache_expires_after_one_second(self):
        """_has_human() re-runs subprocess after 1 second cache TTL expires."""
        proxy = TmuxProxy(session_name="gza-99")
        mock_result = MagicMock()
        mock_result.stdout = ""
        call_count = [0]

        def counting_run(*args, **kwargs):
            call_count[0] += 1
            return mock_result

        # TmuxProxy is created before the patch so __init__'s time.monotonic() call
        # uses the real clock. Within the patch block: first _has_human() gets 0.0
        # (sets cache at t=0), second call gets 2.0 (2.0 - 0.0 >= 1.0 → expired).
        with patch("gza.tmux_proxy.subprocess.run", side_effect=counting_run), \
             patch("gza.tmux_proxy.time.monotonic", side_effect=[0.0, 2.0]):
            proxy._has_human()  # first call — subprocess called, cache set at t=0
            proxy._has_human()  # second call at t=2 — cache expired, subprocess called again

        assert call_count[0] == 2, "subprocess.run should be called again after cache TTL"


class TestNonBlockingStdout:
    """Tests for non-blocking stdout writes in the proxy."""

    def test_write_stdout_drops_data_on_eagain(self):
        """_write_stdout silently drops data when stdout would block (EAGAIN)."""
        proxy = TmuxProxy(session_name="gza-99")
        with patch("gza.tmux_proxy.os.write", side_effect=OSError(errno.EAGAIN, "would block")):
            # Should not raise
            proxy._write_stdout(1, b"some output data")

    def test_write_stdout_drops_data_on_epipe(self):
        """_write_stdout silently drops data on broken pipe."""
        proxy = TmuxProxy(session_name="gza-99")
        with patch("gza.tmux_proxy.os.write", side_effect=OSError(errno.EPIPE, "broken pipe")):
            proxy._write_stdout(1, b"some output data")

    def test_write_stdout_writes_data_normally(self):
        """_write_stdout calls os.write with correct args."""
        proxy = TmuxProxy(session_name="gza-99")
        with patch("gza.tmux_proxy.os.write") as mock_write:
            proxy._write_stdout(42, b"hello")
            mock_write.assert_called_once_with(42, b"hello")

    def test_write_stdout_propagates_unexpected_errors(self):
        """_write_stdout re-raises non-EAGAIN/EPIPE errors."""
        proxy = TmuxProxy(session_name="gza-99")
        with patch("gza.tmux_proxy.os.write", side_effect=OSError(errno.EBADF, "bad fd")):
            with pytest.raises(OSError, match="bad fd"):
                proxy._write_stdout(1, b"data")

    def test_io_loop_does_not_deadlock_on_full_stdout(self):
        """Proxy keeps draining child PTY even when stdout buffer is full.

        Regression test: previously the proxy used blocking sys.stdout.buffer.write(),
        which would block when the tmux pane buffer was full, preventing the proxy
        from reading the child PTY, which in turn blocked the child on its own writes.
        """
        proxy = TmuxProxy(session_name="gza-99")

        child_output = b"x" * 8192
        read_call_count = [0]
        child_exited = [False]

        def fake_os_read(fd, size):
            read_call_count[0] += 1
            if read_call_count[0] == 1:
                return child_output
            # Second read: simulate EOF
            return b""

        def fake_reap(child_pid):
            child_exited[0] = True
            return 0

        # Mock _write_stdout to simulate EAGAIN without intercepting PTY writes
        with patch.object(proxy, "_has_human", return_value=False), \
             patch.object(proxy, "_reap", side_effect=fake_reap), \
             patch.object(proxy, "_write_stdout") as mock_write_stdout, \
             patch("gza.tmux_proxy.os.read", side_effect=fake_os_read), \
             patch("gza.tmux_proxy.os.waitpid", return_value=(0, 0)), \
             patch("gza.tmux_proxy.fcntl.fcntl"), \
             patch("gza.tmux_proxy.select.select", return_value=([3], [], [])), \
             patch("gza.tmux_proxy.sys.stdin") as mock_stdin, \
             patch("gza.tmux_proxy.sys.stdout") as mock_stdout:
            mock_stdin.isatty.return_value = False
            mock_stdout.fileno.return_value = 1
            result = proxy._io_loop(child_pid=123, pty_fd=3)

        assert child_exited[0], "Proxy should have reaped child instead of deadlocking"
        assert result == 0
        # Verify the proxy attempted to write child output to stdout
        mock_write_stdout.assert_called_once_with(1, child_output)
