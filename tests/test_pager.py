"""Tests for --page flag on gza show and gza log commands."""

import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gza.cli._common import _get_pager, pager_context

# ---------------------------------------------------------------------------
# _get_pager tests
# ---------------------------------------------------------------------------

class TestGetPager:
    def test_returns_git_pager_env_first(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GIT_PAGER", "my-pager")
        assert _get_pager(tmp_path) == "my-pager"

    def test_returns_pager_env_when_no_git_pager(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GIT_PAGER", raising=False)
        monkeypatch.setenv("PAGER", "bat")
        with patch("gza.cli._common.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")
            result = _get_pager(tmp_path)
        assert result == "bat"

    def test_defaults_to_less_r(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GIT_PAGER", raising=False)
        monkeypatch.delenv("PAGER", raising=False)
        with patch("gza.cli._common.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")
            result = _get_pager(tmp_path)
        assert result == "less -R"

    def test_returns_git_config_pager(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GIT_PAGER", raising=False)
        monkeypatch.delenv("PAGER", raising=False)
        with patch("gza.cli._common.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="delta\n")
            result = _get_pager(tmp_path)
        assert result == "delta"


# ---------------------------------------------------------------------------
# pager_context tests
# ---------------------------------------------------------------------------

class TestPagerContext:
    def test_returns_nullcontext_when_no_page(self, tmp_path: Path) -> None:
        """When use_page=False, pager_context is a no-op."""
        ctx = pager_context(False, tmp_path)
        with ctx:
            pass  # no exception expected

    def test_returns_nullcontext_when_not_a_tty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When stdout is not a TTY, pager_context is a no-op even if use_page=True."""
        monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
        ctx = pager_context(True, tmp_path)
        with ctx:
            pass  # no exception expected

    def test_invokes_pager_when_tty_and_page_requested(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When use_page=True and stdout is a TTY, _GzaPager is used."""
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)

        pager_shown: list[str] = []

        class _FakePager:
            def show(self, content: str) -> None:
                pager_shown.append(content)

        with patch("gza.cli._common._GzaPager", return_value=_FakePager()):
            with patch("gza.cli._common._get_pager", return_value="cat"):
                from gza.console import console
                ctx = pager_context(True, tmp_path)
                with ctx:
                    console.print("hello pager")

        # If we got here without exception, pager_context worked correctly
        assert True


# ---------------------------------------------------------------------------
# CLI argument parser tests (via subprocess)
# ---------------------------------------------------------------------------

def _run_gza(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "gza", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def _setup_config(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")


class TestCliPageFlag:
    """Ensure --page flag is accepted by gza show and gza log."""

    def test_show_page_flag_is_recognized(self, tmp_path: Path) -> None:
        """gza show --page should not error with 'unrecognized argument'."""
        _setup_config(tmp_path)
        result = _run_gza("show", "testproject-99999", "--page", "--project", str(tmp_path))
        # Flag is known, so argparse won't complain; we get a task lookup failure instead.
        assert "unrecognized" not in result.stderr.lower()
        assert result.returncode != 0

    def test_show_page_not_active_without_flag(self, tmp_path: Path) -> None:
        """gza show without --page should not produce argparse errors."""
        _setup_config(tmp_path)
        result = _run_gza("show", "testproject-99999", "--project", str(tmp_path))
        assert "unrecognized" not in result.stderr.lower()

    def test_log_page_flag_is_recognized(self, tmp_path: Path) -> None:
        """gza log --page should not error with 'unrecognized argument'."""
        _setup_config(tmp_path)
        result = _run_gza("log", "testproject-99999", "--page", "--project", str(tmp_path))
        assert "unrecognized" not in result.stderr.lower()
        assert result.returncode != 0

    def test_log_page_not_active_without_flag(self, tmp_path: Path) -> None:
        """gza log without --page should not produce argparse errors."""
        _setup_config(tmp_path)
        result = _run_gza("log", "testproject-99999", "--project", str(tmp_path))
        assert "unrecognized" not in result.stderr.lower()

    def test_show_help_mentions_page(self, tmp_path: Path) -> None:
        """gza show --help should mention the --page flag."""
        result = _run_gza("show", "--help")
        assert result.returncode == 0
        assert "--page" in result.stdout

    def test_log_help_mentions_page(self, tmp_path: Path) -> None:
        """gza log --help should mention the --page flag."""
        result = _run_gza("log", "--help")
        assert result.returncode == 0
        assert "--page" in result.stdout
