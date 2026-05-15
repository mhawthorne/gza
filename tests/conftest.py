"""Shared test fixtures."""

import os
from pathlib import Path

import pytest

UNIT_TEST_TIMEOUT_SECONDS = int(os.environ.get("GZA_UNIT_TEST_TIMEOUT_SECONDS", "1"))


def pytest_collection_modifyitems(items):
    """Apply the unit-suite watchdog unless a test sets its own timeout."""
    unit_timeout_marker = pytest.mark.timeout(UNIT_TEST_TIMEOUT_SECONDS, method="signal")
    for item in items:
        if item.get_closest_marker("timeout") is not None:
            continue
        item.add_marker(unit_timeout_marker)


@pytest.fixture(autouse=True)
def _disable_git_signing(tmp_path, monkeypatch):
    """Disable git commit signing for all tests.

    The CI/development environment may have global git config that enables
    commit signing (commit.gpgsign=true). This interferes with tests that
    create temporary git repos. Setting GIT_CONFIG_GLOBAL to an empty file
    prevents the global config from being inherited.
    """
    global_config = tmp_path / ".gitconfig-empty"
    global_config.write_text("")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(global_config))


@pytest.fixture(autouse=True)
def _isolate_home_dir(tmp_path: Path, monkeypatch):
    """Isolate HOME so user-level config tests do not read developer-machine state."""
    home_dir = tmp_path / ".isolated-home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))


@pytest.fixture(autouse=True)
def _isolate_rich_console(monkeypatch):
    """Pin Rich console color and width so test assertions are deterministic.

    Why: Rich auto-detects color and wraps to terminal width. Both vary across
    local terminals and CI, causing assertions on rendered output to flake when
    ANSI escape codes get inserted or messages wrap mid-token.
    """
    for name in ("FORCE_COLOR", "TTY_COMPATIBLE", "CLICOLOR_FORCE", "NO_COLOR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("COLUMNS", "200")

    from gza.cli import query as query_cli
    from gza.console import console

    for rich_console in (console, query_cli._stderr_console):
        monkeypatch.setattr(rich_console, "no_color", True)
        monkeypatch.setattr(rich_console, "_color_system", None, raising=False)
        monkeypatch.setattr(rich_console, "_width", 200, raising=False)
