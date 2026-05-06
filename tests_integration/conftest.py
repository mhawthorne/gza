"""Shared integration test fixtures."""

import pytest

INTEGRATION_TEST_TIMEOUT_SECONDS = 10


def pytest_collection_modifyitems(items):
    """Apply the slower hang watchdog only to the integration-test suite."""
    timeout_marker = pytest.mark.timeout(INTEGRATION_TEST_TIMEOUT_SECONDS, method="signal")
    for item in items:
        if item.get_closest_marker("timeout") is None:
            item.add_marker(timeout_marker)


@pytest.fixture(autouse=True)
def _disable_git_signing(tmp_path, monkeypatch):
    """Keep global git signing config from breaking temporary repo commits."""
    global_config = tmp_path / ".gitconfig-empty"
    global_config.write_text("")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(global_config))
