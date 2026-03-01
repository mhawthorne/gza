"""Tests for the tmux proxy module."""

import subprocess
from unittest.mock import MagicMock, patch

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
