"""Shared integration test fixtures."""

import pytest

INTEGRATION_TEST_TIMEOUT_SECONDS = 10


def pytest_collection_modifyitems(items):
    """Apply the slower hang watchdog only to the integration-test suite."""
    timeout_marker = pytest.mark.timeout(INTEGRATION_TEST_TIMEOUT_SECONDS, method="signal")
    for item in items:
        if item.get_closest_marker("timeout") is None:
            item.add_marker(timeout_marker)
