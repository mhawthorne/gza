"""Shared test fixtures."""

import contextlib
import fcntl
import os
from pathlib import Path

import pytest

from checks.unit_suite_boundary import DEFAULT_PATHS, find_unit_suite_boundary_violations
from gza.pytest_timeout_diagnostics import register_sigterm_faulthandler

# NOTE: 2000ms is a short-term bridge. A 1s wall-clock per-test budget is
# inherently flaky under xdist contention (wall time inflates when workers
# compete for CPU). The durable fix is a CPU-time latency guard
# (time.process_time delta) plus a generous wall-clock hang-guard; see the
# follow-up task. Until then, 2s gives the heaviest in-process lifecycle tests
# (~0.8s solo) enough headroom to stop intermittently timing out.
UNIT_TEST_TIMEOUT_MS = int(os.environ.get("GZA_UNIT_TEST_TIMEOUT_MS", "2000"))
UNIT_TEST_TIMEOUT_SECONDS = UNIT_TEST_TIMEOUT_MS / 1000

register_sigterm_faulthandler()


def pytest_sessionstart(session: pytest.Session) -> None:
    """Fail fast if shell-backed CLI coverage drifts into the unit suite."""
    del session
    tests_root = DEFAULT_PATHS[0]
    violations = find_unit_suite_boundary_violations(tests_root)
    if not violations:
        return

    formatted = "\n".join(f"  - {violation.format()}" for violation in violations)
    raise pytest.UsageError(
        "Unit-suite boundary violation(s) detected. Use invoke_gza for in-process CLI coverage and "
        "move any subprocess-backed test to tests_functional/.\n"
        f"{formatted}"
    )


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

    Pinning ``no_color``/``_color_system`` on the console objects is necessary
    but not sufficient: code under test (e.g. ``Config.load`` ->
    ``set_config_no_color``) *restores* each console's ``_color_system`` from the
    value captured at import time in ``_REGISTERED_COLOR_SYSTEMS``. When the test
    process is attached to a TTY (e.g. ``bin/tests`` run interactively), that
    captured value is a real color system, so the restore re-enables ANSI
    mid-test and plaintext assertions on stderr fail. We therefore also pin the
    captured systems to ``None`` so any restore keeps output plain.
    """
    for name in ("FORCE_COLOR", "TTY_COMPATIBLE", "CLICOLOR_FORCE", "NO_COLOR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("COLUMNS", "200")

    import gza.console as console_module
    from gza.cli import query as query_cli
    from gza.console import console

    for rich_console in (console, query_cli._stderr_console):
        monkeypatch.setattr(rich_console, "no_color", True)
        monkeypatch.setattr(rich_console, "_color_system", None, raising=False)
        monkeypatch.setattr(rich_console, "_width", 200, raising=False)
        monkeypatch.setitem(console_module._REGISTERED_COLOR_SYSTEMS, rich_console, None)


def _clear_process_local_launch_state() -> None:
    from gza.concurrency import (
        _PROCESS_LOCKS,
        _PROCESS_LOCKS_GUARD,
        _RESERVED_LAUNCH_PERMITS,
        _RESERVED_LAUNCH_PERMITS_GUARD,
    )

    with _RESERVED_LAUNCH_PERMITS_GUARD:
        reserved_permits = list(_RESERVED_LAUNCH_PERMITS.values())
        _RESERVED_LAUNCH_PERMITS.clear()
    for permit in reserved_permits:
        with contextlib.suppress(Exception):
            permit.release()

    with _PROCESS_LOCKS_GUARD:
        held_lock_files = [
            state.lock_file
            for state in _PROCESS_LOCKS.values()
            if state.lock_file is not None
        ]
        _PROCESS_LOCKS.clear()
    for lock_file in held_lock_files:
        with contextlib.suppress(Exception):
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        with contextlib.suppress(Exception):
            lock_file.close()


@pytest.fixture(autouse=True)
def _isolate_launch_permit_state():
    _clear_process_local_launch_state()
    try:
        yield
    finally:
        _clear_process_local_launch_state()
