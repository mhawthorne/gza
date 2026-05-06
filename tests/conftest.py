"""Shared test fixtures."""


import pytest

UNIT_TEST_TIMEOUT_SECONDS = 1


def pytest_collection_modifyitems(items):
    """Apply the fast hang watchdog only to the unit-test suite."""
    timeout_marker = pytest.mark.timeout(UNIT_TEST_TIMEOUT_SECONDS, method="signal")
    for item in items:
        if item.get_closest_marker("timeout") is None:
            item.add_marker(timeout_marker)


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
def _isolate_rich_color_env(monkeypatch):
    """Keep Rich color-forcing environment changes from leaking across tests."""
    for name in ("FORCE_COLOR", "TTY_COMPATIBLE", "CLICOLOR_FORCE", "NO_COLOR"):
        monkeypatch.delenv(name, raising=False)

    from gza.console import console

    monkeypatch.setattr(console, "no_color", True)
