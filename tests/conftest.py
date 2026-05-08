"""Shared test fixtures."""

import os

import pytest

UNIT_TEST_TIMEOUT_SECONDS = int(os.environ.get("GZA_UNIT_TEST_TIMEOUT_SECONDS", "1"))
FUNCTIONAL_TEST_TIMEOUT_SECONDS = int(os.environ.get("GZA_FUNCTIONAL_TEST_TIMEOUT_SECONDS", "2"))


def pytest_collection_modifyitems(items):
    """Apply the fast hang watchdog only to the unit-test suite."""
    unit_timeout_marker = pytest.mark.timeout(UNIT_TEST_TIMEOUT_SECONDS, method="signal")
    functional_timeout_marker = pytest.mark.timeout(FUNCTIONAL_TEST_TIMEOUT_SECONDS, method="signal")
    for item in items:
        if item.get_closest_marker("timeout") is None:
            if item.get_closest_marker("functional") is None:
                item.add_marker(unit_timeout_marker)
            else:
                item.add_marker(functional_timeout_marker)


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
