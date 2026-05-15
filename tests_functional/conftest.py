"""Shared functional test fixtures."""

import os
from pathlib import Path

import pytest

FUNCTIONAL_TEST_TIMEOUT_SECONDS = int(os.environ.get("GZA_FUNCTIONAL_TEST_TIMEOUT_SECONDS", "4"))


def pytest_collection_modifyitems(items):
    """Apply the functional-suite watchdog unless a test sets its own timeout."""
    timeout_marker = pytest.mark.timeout(FUNCTIONAL_TEST_TIMEOUT_SECONDS, method="signal")
    for item in items:
        if item.get_closest_marker("timeout") is None:
            item.add_marker(timeout_marker)


@pytest.fixture(autouse=True)
def _disable_git_signing(tmp_path, monkeypatch):
    """Keep global git signing config from breaking temporary repo commits."""
    global_config = tmp_path / ".gitconfig-empty"
    global_config.write_text("")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(global_config))


@pytest.fixture(autouse=True)
def _isolate_home_dir(tmp_path: Path, monkeypatch):
    """Isolate HOME so subprocess tests do not read developer-machine state."""
    home_dir = tmp_path / ".isolated-home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))
