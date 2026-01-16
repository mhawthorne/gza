"""Integration tests for worker spawning.

These tests verify that background workers can be spawned correctly.
They don't require Docker or API credentials, just a working Python environment.

Run with: uv run pytest tests_integration/test_workers.py -v -m integration
"""

import subprocess
import sys

import pytest


# Mark all tests as integration tests
pytestmark = pytest.mark.integration


class TestWorkerSpawning:
    """Tests for worker process spawning."""

    def test_python_module_invocation(self):
        """Test that 'python -m gza' works for spawning workers.

        This test verifies that the gza package can be invoked as a module,
        which is required for background worker spawning. The worker spawn
        command uses `sys.executable -m gza work --worker-mode ...`.
        """
        result = subprocess.run(
            [sys.executable, "-m", "gza", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0, f"Failed to run 'python -m gza': {result.stderr}"
        assert "usage" in result.stdout.lower(), f"Unexpected output: {result.stdout}"

    def test_worker_mode_flag_accepted(self):
        """Test that the --worker-mode flag is accepted (even if hidden).

        The --worker-mode flag is internal (hidden from help) but must be
        accepted by the parser for background workers to function.
        """
        # This should fail with "no pending tasks" not "unrecognized argument"
        result = subprocess.run(
            [sys.executable, "-m", "gza", "work", "--worker-mode", "."],
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should not fail due to unrecognized argument
        assert "unrecognized arguments: --worker-mode" not in result.stderr, \
            "--worker-mode flag not recognized by parser"
