"""Shared test fixtures."""

import contextlib
import fcntl
import os
import time
import shlex
import subprocess
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from checks.unit_suite_boundary import DEFAULT_PATHS, find_unit_suite_boundary_violations
from gza.pytest_timeout_diagnostics import register_sigterm_faulthandler

# The unit suite uses two separate guards:
# - a generous wall-clock SIGALRM hang-guard that can still interrupt a stuck
#   process even when it is deadlocked or blocked in C code;
# - a post-hoc CPU-time budget that fails tests which finish after burning too
#   much in-process CPU, without false-positiving under xdist wall contention.
UNIT_TEST_HANG_GUARD_SECONDS = float(os.environ.get("GZA_UNIT_TEST_HANG_GUARD_SECONDS", "5"))
UNIT_TEST_CPU_BUDGET_SECONDS = float(os.environ.get("GZA_UNIT_TEST_CPU_BUDGET_SECONDS", "1.5"))
UNIT_RUNTIME_SUBPROCESS_GUARD_ENABLED = (
    os.environ.get("GZA_ENABLE_UNIT_SUBPROCESS_GUARD", "1") != "0"
)

# Keep the runtime guard on for the default unit lane. Any future exemption
# must stay narrow, temporary, and point at a dedicated follow-up implement
# task so real subprocess drift is still surfaced at author time.
UNIT_RUNTIME_SUBPROCESS_GUARD_EXEMPTIONS: dict[str, tuple[str, str]] = {
    "tests/test_lineage_query.py": (
        "gza-5361",
        "Temporary module-scoped exemption tracked by gza-5361, which converts this "
        "module's subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    # tests/cli/* offenders the guard surfaces; cleanup tracked by gza-5375.
    "tests/cli/test_advance_squash_threshold.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_config_cmds.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_execution.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_extract.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_git_ops.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_git_ops_merge_units.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_main.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_no_color.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_query.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/cli/test_tmux.py": (
        "gza-5375",
        "Temporary module-scoped exemption tracked by gza-5375, which converts the "
        "tests/cli/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    # tests/* offenders the guard surfaces; cleanup tracked by gza-5376.
    "tests/test_advance_engine.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/test_attach_wrapper.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/test_git.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/test_providers.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/test_query.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/test_recovery_engine.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/test_runner.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
    "tests/test_task_query.py": (
        "gza-5376",
        "Temporary module-scoped exemption tracked by gza-5376, which converts the "
        "tests/ subprocess/git tests to in-process mocks (or relocates them to "
        "tests_functional/) and then removes this exemption.",
    ),
}

register_sigterm_faulthandler()


def _subprocess_command_preview(command: object) -> str:
    if command is None:
        return "<unknown command>"
    if isinstance(command, bytes):
        return command.decode("utf-8", errors="replace")
    if isinstance(command, str):
        return command
    if isinstance(command, os.PathLike):
        return os.fspath(command)
    if isinstance(command, tuple | list):
        return shlex.join(
            os.fspath(part) if isinstance(part, os.PathLike) else str(part)
            for part in command
        )
    return repr(command)


def _is_git_subprocess_command(command: object) -> bool:
    if isinstance(command, bytes):
        command = command.decode("utf-8", errors="replace")
    if isinstance(command, str):
        stripped = command.lstrip()
        return stripped == "git" or stripped.startswith("git ")
    if isinstance(command, tuple | list) and command:
        head = command[0]
        if isinstance(head, os.PathLike):
            head = os.fspath(head)
        return head == "git"
    return False


def _build_unit_subprocess_guard_message(nodeid: str, command: object) -> str:
    preview = _subprocess_command_preview(command)
    operation = "real git command" if _is_git_subprocess_command(command) else "real subprocess"
    return (
        f"Unit-suite boundary violation in {nodeid}: this unit test invoked a {operation} "
        f"({preview}). Mock it or move the coverage to tests_functional/ and mark it "
        "@pytest.mark.functional."
    )


def _find_unit_runtime_subprocess_guard_exemption(nodeid: str) -> tuple[str, str] | None:
    if nodeid in UNIT_RUNTIME_SUBPROCESS_GUARD_EXEMPTIONS:
        return UNIT_RUNTIME_SUBPROCESS_GUARD_EXEMPTIONS[nodeid]
    module_nodeid = nodeid.split("::", 1)[0]
    return UNIT_RUNTIME_SUBPROCESS_GUARD_EXEMPTIONS.get(module_nodeid)


def install_unit_runtime_subprocess_guard(
    *,
    nodeid: str,
    fail: Any | None = None,
) -> contextlib.AbstractContextManager[None]:
    stack = ExitStack()
    if _find_unit_runtime_subprocess_guard_exemption(nodeid) is not None:
        return stack

    def _fail(command: object) -> None:
        message = _build_unit_subprocess_guard_message(nodeid, command)
        if fail is None:
            pytest.fail(message, pytrace=False)
        fail(message)

    def _command_from_call(*popenargs: object, **kwargs: object) -> object:
        if popenargs:
            return popenargs[0]
        return kwargs.get("args")

    def _guard_run(*popenargs: object, **kwargs: object):
        _fail(_command_from_call(*popenargs, **kwargs))

    def _guard_popen(*popenargs: object, **kwargs: object):
        _fail(_command_from_call(*popenargs, **kwargs))

    class _PopenGuard:
        # Keep import-time annotations like subprocess.Popen[Any] working while
        # still failing any attempt to instantiate a real child process.
        def __call__(self, *popenargs: object, **kwargs: object):
            _guard_popen(*popenargs, **kwargs)

        def __getitem__(self, _item: object):
            return self

        def __or__(self, _other: object):
            return self

        def __ror__(self, _other: object):
            return self

    stack.enter_context(patch.object(subprocess, "run", _guard_run))
    stack.enter_context(patch.object(subprocess, "Popen", _PopenGuard()))
    stack.enter_context(patch.object(subprocess, "check_call", _guard_run))
    stack.enter_context(patch.object(subprocess, "check_output", _guard_run))
    return stack


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
    unit_timeout_marker = pytest.mark.timeout(UNIT_TEST_HANG_GUARD_SECONDS, method="signal")
    for item in items:
        if item.get_closest_marker("timeout") is not None:
            continue
        item.add_marker(unit_timeout_marker)


def _fail_if_unit_test_exceeds_cpu_budget(nodeid: str, cpu_seconds: float) -> None:
    if cpu_seconds <= UNIT_TEST_CPU_BUDGET_SECONDS:
        return
    pytest.fail(
        f"{nodeid} exceeded the unit-test CPU budget: used {cpu_seconds:.3f}s CPU "
        f"with a {UNIT_TEST_CPU_BUDGET_SECONDS:.3f}s budget."
    )


def _watch_unit_test_cpu_budget(nodeid: str):
    start_cpu_seconds = time.process_time()
    yield
    _fail_if_unit_test_exceeds_cpu_budget(nodeid, time.process_time() - start_cpu_seconds)


@pytest.fixture(autouse=True)
def _enforce_unit_test_cpu_budget(request: pytest.FixtureRequest):
    yield from _watch_unit_test_cpu_budget(request.node.nodeid)


@pytest.fixture(autouse=True)
def _guard_unit_subprocesses(request: pytest.FixtureRequest):
    if not UNIT_RUNTIME_SUBPROCESS_GUARD_ENABLED:
        yield
        return
    with install_unit_runtime_subprocess_guard(nodeid=request.node.nodeid):
        yield


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
