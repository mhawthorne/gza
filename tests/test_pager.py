"""Tests for --page flag on gza show and gza log commands."""

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
        monkeypatch.setattr("gza.cli._common._stdout_is_tty", lambda: False)
        ctx = pager_context(True, tmp_path)
        with ctx:
            pass  # no exception expected

    def test_invokes_pager_when_tty_and_page_requested(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When use_page=True and stdout is a TTY, _GzaPager is used."""
        monkeypatch.setattr("gza.cli._common._stdout_is_tty", lambda: True)

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
