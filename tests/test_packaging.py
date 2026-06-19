"""Packaging configuration regression tests."""

import ast
import contextlib
import importlib.util
import time
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from checks.unit_suite_boundary import find_unit_suite_boundary_violations


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _find_unit_suite_boundary_violations(tests_root: Path) -> list[str]:
    return [violation.format() for violation in find_unit_suite_boundary_violations(tests_root)]


def test_hatch_vcs_does_not_write_source_version_file() -> None:
    """Editable installs must not require writing src/gza/_version.py."""
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())

    hooks = config.get("tool", {}).get("hatch", {}).get("build", {}).get("hooks", {})
    assert "vcs" not in hooks


def test_pytest_timeout_watchdogs_are_scoped_by_suite() -> None:
    """pytest-timeout remains suite-scoped rather than globally configured."""
    repo_root = Path(__file__).resolve().parents[1]
    pyproject = repo_root / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())

    dependency_groups = config.get("dependency-groups", {})
    dev_deps = dependency_groups.get("dev", [])
    assert any(dep.startswith("pytest-timeout") for dep in dev_deps)

    pytest_options = config.get("tool", {}).get("pytest", {}).get("ini_options", {})
    assert "timeout" not in pytest_options

    functional_conftest_path = repo_root / "tests_functional" / "conftest.py"
    functional_conftest = ast.parse(
        functional_conftest_path.read_text(), filename=str(functional_conftest_path)
    )
    functional_timeout_calls = [
        node
        for node in ast.walk(functional_conftest)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "timeout"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "mark"
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == "pytest"
    ]
    assert len(functional_timeout_calls) == 1
    assert len(functional_timeout_calls[0].args) == 1
    assert isinstance(functional_timeout_calls[0].args[0], ast.Name)
    assert functional_timeout_calls[0].args[0].id == "FUNCTIONAL_TEST_TIMEOUT_SECONDS"

    integration_conftest_path = repo_root / "tests_integration" / "conftest.py"
    integration_conftest = ast.parse(
        integration_conftest_path.read_text(), filename=str(integration_conftest_path)
    )
    integration_timeout_calls = [
        node
        for node in ast.walk(integration_conftest)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "timeout"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "mark"
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == "pytest"
    ]
    assert len(integration_timeout_calls) == 1
    assert len(integration_timeout_calls[0].args) == 1
    assert isinstance(integration_timeout_calls[0].args[0], ast.Name)
    assert integration_timeout_calls[0].args[0].id == "INTEGRATION_TEST_TIMEOUT_SECONDS"


def test_unit_test_conftest_injects_only_unit_watchdog() -> None:
    """tests/conftest.py should assign the unit watchdog unless a test overrides it."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = _load_module(conftest_path, "tests_timeout_conftest")

    class FakeItem:
        def __init__(self, *, timeout: bool = False) -> None:
            self._timeout = timeout
            self.markers: list[pytest.MarkDecorator] = []

        def get_closest_marker(self, name: str):
            if name == "timeout" and self._timeout:
                return object()
            return None

        def add_marker(self, marker: pytest.MarkDecorator) -> None:
            self.markers.append(marker)

    plain_unit = FakeItem()
    plain_unit_2 = FakeItem()
    explicit_timeout = FakeItem(timeout=True)

    module.pytest_collection_modifyitems([plain_unit, plain_unit_2, explicit_timeout])

    assert len(plain_unit.markers) == 1
    assert plain_unit.markers[0].mark.name == "timeout"
    assert plain_unit.markers[0].mark.args == (module.UNIT_TEST_HANG_GUARD_SECONDS,)
    assert plain_unit.markers[0].mark.kwargs == {"method": "signal"}

    assert len(plain_unit_2.markers) == 1
    assert plain_unit_2.markers[0].mark.name == "timeout"
    assert plain_unit_2.markers[0].mark.args == (module.UNIT_TEST_HANG_GUARD_SECONDS,)
    assert plain_unit_2.markers[0].mark.kwargs == {"method": "signal"}

    assert explicit_timeout.markers == []


def test_unit_test_conftest_injects_timeout_when_default_env_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tests/conftest.py should still inject a watchdog when the override env is unset."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.delenv("GZA_UNIT_TEST_HANG_GUARD_SECONDS", raising=False)
    monkeypatch.delenv("GZA_UNIT_TEST_CPU_BUDGET_SECONDS", raising=False)
    module = _load_module(conftest_path, "tests_timeout_conftest_default")

    class FakeItem:
        def __init__(self) -> None:
            self.markers: list[pytest.MarkDecorator] = []

        def get_closest_marker(self, _name: str):
            return None

        def add_marker(self, marker: pytest.MarkDecorator) -> None:
            self.markers.append(marker)

    item = FakeItem()
    module.pytest_collection_modifyitems([item])

    assert module.UNIT_TEST_HANG_GUARD_SECONDS == 5
    assert module.UNIT_TEST_CPU_BUDGET_SECONDS == 1.5
    assert len(item.markers) == 1
    assert item.markers[0].mark.args == (module.UNIT_TEST_HANG_GUARD_SECONDS,)
    assert item.markers[0].mark.kwargs == {"method": "signal"}


def test_unit_test_conftest_uses_hang_guard_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests/conftest.py should use the hang-guard override for the timeout marker budget."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.setenv("GZA_UNIT_TEST_HANG_GUARD_SECONDS", "7.5")
    module = _load_module(conftest_path, "tests_timeout_conftest_hang_override")

    class FakeItem:
        def __init__(self) -> None:
            self.markers: list[pytest.MarkDecorator] = []

        def get_closest_marker(self, _name: str):
            return None

        def add_marker(self, marker: pytest.MarkDecorator) -> None:
            self.markers.append(marker)

    item = FakeItem()
    module.pytest_collection_modifyitems([item])

    assert module.UNIT_TEST_HANG_GUARD_SECONDS == 7.5
    assert len(item.markers) == 1
    assert item.markers[0].mark.args == (7.5,)
    assert item.markers[0].mark.kwargs == {"method": "signal"}


def test_unit_test_conftest_uses_cpu_budget_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests/conftest.py should read the CPU budget override from the environment."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_SECONDS", "1.75")
    module = _load_module(conftest_path, "tests_timeout_conftest_cpu_override")

    assert module.UNIT_TEST_CPU_BUDGET_SECONDS == 1.75


def test_unit_test_conftest_cpu_budget_failure_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests/conftest.py should fail slow unit tests with a CPU-budget-specific message."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_SECONDS", "1")
    module = _load_module(conftest_path, "tests_timeout_conftest_cpu_failure")

    with pytest.raises(pytest.fail.Exception, match="exceeded the unit-test CPU budget"):
        module._fail_if_unit_test_exceeds_cpu_budget("tests/example.py::test_slow", 1.25)


def test_unit_test_conftest_cpu_budget_ignores_wall_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wall-clock sleep with low CPU should not trip the post-hoc CPU budget guard."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_SECONDS", "0.05")
    module = _load_module(conftest_path, "tests_timeout_conftest_cpu_sleep")

    watchdog = module._watch_unit_test_cpu_budget("tests/example.py::test_sleep")
    next(watchdog)
    time.sleep(0.1)
    with contextlib.suppress(StopIteration):
        next(watchdog)


def test_unit_test_conftest_registers_sigterm_faulthandler(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests/conftest.py should enable SIGTERM faulthandler dumps for verify shutdowns."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    calls: list[bool] = []

    def _fake_register() -> bool:
        calls.append(True)
        return True

    monkeypatch.setattr("gza.pytest_timeout_diagnostics.register_sigterm_faulthandler", _fake_register)

    _load_module(conftest_path, "tests_timeout_conftest_sigterm")

    assert calls == [True]


def test_unit_test_conftest_rejects_boundary_violations(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests/conftest.py should fail collection loudly when shell-backed tests drift into tests/."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = _load_module(conftest_path, "tests_timeout_conftest_boundary_guard")
    monkeypatch.setattr(
        module,
        "find_unit_suite_boundary_violations",
        lambda _path: [SimpleNamespace(format=lambda: "tests/example.py:7 forbidden helper")],
    )

    with pytest.raises(pytest.UsageError, match="Unit-suite boundary violation"):
        module.pytest_sessionstart(SimpleNamespace())


def test_functional_suite_conftest_injects_functional_watchdog() -> None:
    """tests_functional/conftest.py should assign the functional watchdog unless overridden."""
    repo_root = Path(__file__).resolve().parents[1]
    conftest_path = repo_root / "tests_functional" / "conftest.py"
    module = _load_module(conftest_path, "tests_functional_timeout_conftest")

    class FakeItem:
        def __init__(self, *, timeout: bool = False) -> None:
            self._timeout = timeout
            self.markers: list[pytest.MarkDecorator] = []

        def get_closest_marker(self, name: str):
            if name == "timeout" and self._timeout:
                return object()
            return None

        def add_marker(self, marker: pytest.MarkDecorator) -> None:
            self.markers.append(marker)

    plain_functional = FakeItem()
    explicit_timeout = FakeItem(timeout=True)

    module.pytest_collection_modifyitems([plain_functional, explicit_timeout])

    assert len(plain_functional.markers) == 1
    assert plain_functional.markers[0].mark.name == "timeout"
    assert plain_functional.markers[0].mark.args == (module.FUNCTIONAL_TEST_TIMEOUT_SECONDS,)
    assert plain_functional.markers[0].mark.kwargs == {"method": "signal"}
    assert explicit_timeout.markers == []


def test_functional_suite_conftest_registers_sigterm_faulthandler(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests_functional/conftest.py should enable SIGTERM faulthandler dumps for verify shutdowns."""
    repo_root = Path(__file__).resolve().parents[1]
    conftest_path = repo_root / "tests_functional" / "conftest.py"
    calls: list[bool] = []

    def _fake_register() -> bool:
        calls.append(True)
        return True

    monkeypatch.setattr("gza.pytest_timeout_diagnostics.register_sigterm_faulthandler", _fake_register)

    _load_module(conftest_path, "tests_functional_sigterm_conftest")

    assert calls == [True]


def test_pytest_suite_conftests_do_not_register_sigterm_faulthandler_inline() -> None:
    """Pytest suite conftests should route SIGTERM registration through the shared helper."""
    repo_root = Path(__file__).resolve().parents[1]

    for relative_path in ("tests/conftest.py", "tests_functional/conftest.py"):
        conftest_path = repo_root / relative_path
        conftest = ast.parse(conftest_path.read_text(), filename=str(conftest_path))
        direct_registrations = [
            node
            for node in ast.walk(conftest)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "register"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "faulthandler"
        ]
        assert direct_registrations == [], f"{relative_path} should use register_sigterm_faulthandler()"


def test_functional_subprocess_timeouts_within_watchdog() -> None:
    """tests_functional subprocess.run(timeout=N) calls must stay within the suite watchdog."""
    repo_root = Path(__file__).resolve().parents[1]
    conftest_path = repo_root / "tests_functional" / "conftest.py"
    conftest_module = ast.parse(conftest_path.read_text(), filename=str(conftest_path))

    functional_budget: int | float | None = None
    for node in ast.walk(conftest_module):
        if not (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "FUNCTIONAL_TEST_TIMEOUT_SECONDS"
        ):
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, (int, float)):
            functional_budget = node.value.value
            continue
        if (
            isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "int"
            and len(node.value.args) == 1
            and isinstance(node.value.args[0], ast.Call)
            and isinstance(node.value.args[0].func, ast.Attribute)
            and node.value.args[0].func.attr == "get"
            and len(node.value.args[0].args) == 2
            and isinstance(node.value.args[0].args[1], ast.Constant)
        ):
            functional_budget = int(node.value.args[0].args[1].value)

    assert functional_budget is not None, "FUNCTIONAL_TEST_TIMEOUT_SECONDS not found in tests_functional/conftest.py"

    inversions: list[str] = []
    tests_root = repo_root / "tests_functional"
    for test_file in tests_root.rglob("test_*.py"):
        module = ast.parse(test_file.read_text(), filename=str(test_file))
        for inner in ast.walk(module):
            if not isinstance(inner, ast.Call):
                continue
            if not (
                isinstance(inner.func, ast.Attribute)
                and inner.func.attr == "run"
                and isinstance(inner.func.value, ast.Name)
                and inner.func.value.id == "subprocess"
            ):
                continue
            for kw in inner.keywords:
                if (
                    kw.arg == "timeout"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, (int, float))
                    and kw.value.value > functional_budget
                ):
                    inversions.append(
                        f"{test_file}:{inner.lineno} subprocess.run(timeout={kw.value.value}) "
                        f"> FUNCTIONAL_TEST_TIMEOUT_SECONDS={functional_budget}"
                    )

    assert not inversions, (
        "Inner subprocess.run timeouts exceed the functional watchdog; the watchdog will fire first "
        "and the inner timeout can never trip:\n  " + "\n  ".join(inversions)
    )


def test_unit_suite_keeps_subprocess_and_real_shell_tests_out_of_tests_dir() -> None:
    """Unit tests and test fixtures should keep subprocesses and direct shell commands out."""
    repo_root = Path(__file__).resolve().parents[1]
    violations = _find_unit_suite_boundary_violations(repo_root / "tests")

    assert not violations, "Unit suite boundary violations found:\n  " + "\n  ".join(violations)


def test_unit_suite_boundary_flags_unmarked_direct_git_run(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_real_shell.py"
    nested_test.write_text(
        "from gza.git import Git\n\n"
        "def test_real_git_shell(tmp_path):\n"
        "    git = Git(tmp_path)\n"
        "    git._run('init', '-b', 'main')\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:5 direct Git._run shell command belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_whitespace_formatted_git_run(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_real_shell_spacing.py"
    nested_test.write_text(
        "from gza.git import Git\n\n"
        "def test_real_git_shell(tmp_path):\n"
        "    git = Git(tmp_path)\n"
        "    git._run ('status')\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:5 direct Git._run shell command belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_spaced_dot_git_run(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_real_shell_spaced_dot.py"
    nested_test.write_text(
        "from gza.git import Git\n\n"
        "def test_real_git_shell(tmp_path):\n"
        "    git = Git(tmp_path)\n"
        "    git . _run('status')\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:5 direct Git._run shell command belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_whitespace_formatted_cli_subprocess(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_cli_subprocess_spacing.py"
    module_name = "g" "za"
    nested_test.write_text(
        "import subprocess\n"
        "import sys\n\n"
        "def test_cli_subprocess_spacing():\n"
        f"    subprocess.run ([sys.executable, '-m', '{module_name}', 'next'])\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:5 CLI subprocess invocation belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_direct_git_subprocess(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_git_subprocess.py"
    nested_test.write_text(
        "import subprocess\n\n"
        "def test_git_subprocess(tmp_path):\n"
        "    subprocess.run(['git', 'status'], cwd=tmp_path)\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:4 direct git subprocess belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_generic_subprocess_popen(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_background_worker.py"
    nested_test.write_text(
        "import subprocess\n\n"
        "def test_background_worker_probe():\n"
        "    subprocess.Popen(['sleep', '1'])\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:4 subprocess-backed test belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_subprocess_alias_popen(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_background_worker_alias.py"
    nested_test.write_text(
        "import subprocess as sp\n\n"
        "def test_background_worker_probe():\n"
        "    sp.Popen(['sleep', '1'])\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:4 subprocess-backed test belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_looped_git_subprocess(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_git_loop.py"
    nested_test.write_text(
        "import subprocess\n\n"
        "def test_git_subprocess(tmp_path):\n"
        "    for cmd in (['git', 'status'], ['git', 'branch']):\n"
        "        subprocess.run(cmd, cwd=tmp_path)\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:5 direct git subprocess belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_spaced_dot_cli_subprocess(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_cli_subprocess_spaced_dot.py"
    module_name = "g" "za"
    nested_test.write_text(
        "import subprocess\n"
        "import sys\n\n"
        "def test_cli_subprocess_spacing():\n"
        f"    subprocess . run([sys.executable, '-m', '{module_name}', 'next'])\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:5 CLI subprocess invocation belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_nested_shell_backed_conftest(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_conftest = nested / "conftest.py"
    nested_conftest.write_text(
        "from gza.git import Git\n\n"
        "def build_repo(tmp_path):\n"
        "    git = Git(tmp_path)\n"
        "    git._run('init')\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_conftest}:5 direct Git._run shell command belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_non_collected_helper_shell_body(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_hidden_helper.py"
    nested_test.write_text(
        "from gza.git import Git\n\n"
        "def _functional_test_hidden(tmp_path):\n"
        "    git = Git(tmp_path)\n"
        "    git._run('status')\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:5 direct Git._run shell command belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_subprocess_helper_definition(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    helpers = tests_root / "helpers"
    helpers.mkdir(parents=True)
    helper = helpers / "cli.py"
    helper_name = "run_" "gza_subprocess"
    module_name = "g" "za"
    helper.write_text(
        "import subprocess\nimport sys\n\n"
        f"def {helper_name}(*args):\n"
        f"    return subprocess.run([sys.executable, '-m', '{module_name}', *args])\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{helper}:5 CLI subprocess invocation belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_subprocess_helper_call_and_import(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    helpers = tests_root / "helpers"
    helpers.mkdir(parents=True)
    helper_name = "run_" "gza_subprocess"
    (helpers / "cli.py").write_text(f"def {helper_name}(*args):\n    return args\n")
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_cli.py"
    nested_test.write_text(
        f"from tests.helpers.cli import {helper_name}\n\n"
        "def test_cli_roundtrip():\n"
        f"    {helper_name}('next')\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:1 CLI subprocess helper belongs in tests_functional/",
        f"{nested_test}:4 CLI subprocess helper belongs in tests_functional/"
    ]


def test_unit_suite_boundary_flags_legacy_inprocess_cli_helper_name(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    helpers = tests_root / "helpers"
    helpers.mkdir(parents=True)
    (helpers / "cli.py").write_text("def invoke_gza(*args):\n    return args\n")
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_cli.py"
    helper_name = "run_" "gza"
    nested_test.write_text(
        f"from tests.helpers.cli import {helper_name}\n\n"
        "def test_cli_roundtrip():\n"
        f"    {helper_name}('next')\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)
    expected = "legacy " + ("run_" "gza") + " helper is banned in tests/; use invoke_gza or move the test to tests_functional/"

    assert violations == [
        f"{nested_test}:1 {expected}",
        f"{nested_test}:4 {expected}",
    ]


def test_unit_suite_boundary_flags_tests_functional_imports(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_imports.py"
    nested_test.write_text(
        "from tests_functional.git_helpers import init_basic_repo\n\n"
        "def test_x(tmp_path):\n"
        "    assert init_basic_repo is not None\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:1 unit tests must not import tests_functional modules"
    ]


def test_unit_suite_boundary_allows_dedicated_git_run_unit_tests(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    (nested / "test_git.py").write_text(
        "class TestGitRun:\n"
        "    def test_run_successful_command(self, tmp_path):\n"
        "        git = object()\n"
        "        git._run('status')\n"
    )

    assert _find_unit_suite_boundary_violations(tests_root) == []
