"""Packaging configuration regression tests."""

import ast
import importlib.util
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
            self.stash: dict[object, object] = {}

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
    assert plain_unit.markers[0].mark.args == (module.UNIT_TEST_HANG_TIMEOUT_SECONDS,)
    assert plain_unit.markers[0].mark.kwargs == {"method": "signal"}

    assert len(plain_unit_2.markers) == 1
    assert plain_unit_2.markers[0].mark.name == "timeout"
    assert plain_unit_2.markers[0].mark.args == (module.UNIT_TEST_HANG_TIMEOUT_SECONDS,)
    assert plain_unit_2.markers[0].mark.kwargs == {"method": "signal"}

    assert explicit_timeout.markers == []


def test_unit_test_conftest_runtime_subprocess_guard_fails_real_git() -> None:
    """The unit-lane autouse guard should fail loudly on real git subprocesses."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = _load_module(conftest_path, "tests_runtime_guard_conftest")

    class GuardTriggered(Exception):
        pass

    def _fail(message: str) -> None:
        raise GuardTriggered(message)

    with module.install_unit_runtime_subprocess_guard(
        nodeid="tests/demo/test_probe.py::test_real_git_shell",
        fail=_fail,
    ):
        with pytest.raises(GuardTriggered, match="Unit-suite boundary violation") as exc_info:
            module.subprocess.run(["git", "status"])

    assert "real git command" in str(exc_info.value)
    assert "mock it or move the coverage to tests_functional/" in str(exc_info.value).lower()


def test_unit_test_conftest_runtime_subprocess_guard_is_on_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The autouse runtime subprocess guard should protect the default unit lane."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"

    monkeypatch.delenv("GZA_ENABLE_UNIT_SUBPROCESS_GUARD", raising=False)
    enabled_module = _load_module(conftest_path, "tests_runtime_guard_default_on_conftest")
    assert enabled_module.UNIT_RUNTIME_SUBPROCESS_GUARD_ENABLED is True

    class GuardTriggered(Exception):
        pass

    def _fail(message: str) -> None:
        raise GuardTriggered(message)

    fake_request = SimpleNamespace(
        node=SimpleNamespace(nodeid="tests/demo/test_probe.py::test_real_subprocess"),
    )
    real_install = enabled_module.install_unit_runtime_subprocess_guard
    guard = enabled_module._guard_unit_subprocesses.__wrapped__(fake_request)
    try:
        with pytest.raises(GuardTriggered, match="Unit-suite boundary violation"):
            with enabled_module.patch.object(
                enabled_module, "install_unit_runtime_subprocess_guard"
            ) as install:
                install.return_value = real_install(
                    nodeid=fake_request.node.nodeid,
                    fail=_fail,
                )
                next(guard)
                install.assert_called_once_with(nodeid=fake_request.node.nodeid)
                enabled_module.subprocess.run(["python", "-c", "print('boom')"])
    finally:
        guard.close()


def test_unit_test_conftest_runtime_subprocess_guard_can_be_explicitly_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A narrow env override can disable the guard for emergency triage only."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"

    monkeypatch.setenv("GZA_ENABLE_UNIT_SUBPROCESS_GUARD", "0")
    disabled_module = _load_module(conftest_path, "tests_runtime_guard_disabled_conftest")
    assert disabled_module.UNIT_RUNTIME_SUBPROCESS_GUARD_ENABLED is False


def test_unit_test_conftest_runtime_subprocess_guard_exemptions_are_explicit_and_narrow() -> None:
    """Temporary exemptions must stay module-scoped and carry follow-up context."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = _load_module(conftest_path, "tests_runtime_guard_exemptions_conftest")

    exemptions = module.UNIT_RUNTIME_SUBPROCESS_GUARD_EXEMPTIONS

    # Each known offender module the guard surfaces keeps a narrow, module-scoped
    # exemption pointing at the follow-up implement task that will clean it up.
    assert "tests/cli/test_advance_auto_plans.py" not in exemptions
    assert "tests/cli/test_advance_squash_threshold.py" not in exemptions
    assert "tests/cli/test_config_cmds.py" not in exemptions
    assert "tests/cli/test_execution.py" not in exemptions
    assert "tests/cli/test_extract.py" not in exemptions
    assert "tests/cli/test_git_ops.py" not in exemptions
    assert "tests/cli/test_git_ops_merge_units.py" not in exemptions
    assert "tests/cli/test_main.py" not in exemptions
    assert "tests/cli/test_no_color.py" not in exemptions
    assert "tests/cli/test_query.py" not in exemptions
    assert "tests/cli/test_tmux.py" not in exemptions
    assert "tests/test_lineage_query.py" not in exemptions

    # Every exemption must stay module-scoped (a tests/ path, no ``::`` nodeid),
    # cite a real follow-up task ID (never a placeholder, gza-5177 B3), and carry
    # actionable cleanup context referencing that task.
    for module_path, value in exemptions.items():
        assert module_path.startswith("tests/") and "::" not in module_path, (
            f"exemption key {module_path!r} must be a module-scoped tests/ path"
        )
        task_id, reason = value
        assert task_id.startswith("gza-") and task_id[len("gza-") :].isdigit(), (
            f"exemption for {module_path} must cite a real gza-<n> follow-up task, got {task_id!r}"
        )
        assert task_id in reason, (
            f"exemption reason for {module_path} should reference its follow-up task {task_id}"
        )


def test_unit_test_conftest_runtime_subprocess_guard_exemptions_match_module_prefix() -> None:
    """Module-scoped exemptions should match child nodeids but not unrelated tests."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = _load_module(conftest_path, "tests_runtime_guard_exemption_lookup_conftest")

    assert (
        module._find_unit_runtime_subprocess_guard_exemption(
            "tests/cli/test_advance_auto_plans.py::test_example"
        )
        is None
    )
    assert (
        module._find_unit_runtime_subprocess_guard_exemption(
            "tests/cli/test_watch.py::test_example"
        )
        is None
    )
    assert (
        module._find_unit_runtime_subprocess_guard_exemption(
            "tests/demo/test_probe.py::test_real_subprocess"
        )
        is None
    )


def test_unit_test_conftest_injects_timeout_when_default_env_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tests/conftest.py should still inject a watchdog when the override env is unset."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.delenv("GZA_UNIT_TEST_HANG_TIMEOUT_MS", raising=False)
    monkeypatch.delenv("GZA_UNIT_TEST_CPU_BUDGET_MS", raising=False)
    module = _load_module(conftest_path, "tests_timeout_conftest_default")

    class FakeItem:
        def __init__(self) -> None:
            self.markers: list[pytest.MarkDecorator] = []
            self.stash: dict[object, object] = {}

        def get_closest_marker(self, _name: str):
            return None

        def add_marker(self, marker: pytest.MarkDecorator) -> None:
            self.markers.append(marker)

    item = FakeItem()
    module.pytest_collection_modifyitems([item])

    assert len(item.markers) == 1
    assert item.markers[0].mark.args == (module.UNIT_TEST_HANG_TIMEOUT_SECONDS,)
    assert item.markers[0].mark.kwargs == {"method": "signal"}


def test_unit_test_conftest_uses_hang_guard_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests/conftest.py should use the hang-guard override for the timeout marker budget."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.setenv("GZA_UNIT_TEST_HANG_TIMEOUT_MS", "35000")
    module = _load_module(conftest_path, "tests_timeout_conftest_hang_override")

    class FakeItem:
        def __init__(self) -> None:
            self.markers: list[pytest.MarkDecorator] = []
            self.stash: dict[object, object] = {}

        def get_closest_marker(self, _name: str):
            return None

        def add_marker(self, marker: pytest.MarkDecorator) -> None:
            self.markers.append(marker)

    item = FakeItem()
    module.pytest_collection_modifyitems([item])

    assert module.UNIT_TEST_HANG_TIMEOUT_MS == 35000
    assert module.UNIT_TEST_HANG_TIMEOUT_SECONDS == 35
    assert len(item.markers) == 1
    assert item.markers[0].mark.args == (35,)
    assert item.markers[0].mark.kwargs == {"method": "signal"}


def test_unit_test_conftest_uses_cpu_budget_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """tests/conftest.py should read the CPU budget override from the environment."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_MS", "1750")
    module = _load_module(conftest_path, "tests_timeout_conftest_cpu_override")

    assert module.UNIT_TEST_CPU_BUDGET_MS == 1750


def test_unit_test_conftest_uses_cpu_budget_marker_override() -> None:
    """tests/conftest.py should allow narrow per-test CPU budget overrides."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = _load_module(conftest_path, "tests_timeout_conftest_cpu_marker_override")

    class FakeItem:
        def __init__(self) -> None:
            self.stash: dict[object, object] = {}

        def get_closest_marker(self, name: str):
            if name == "cpu_budget":
                return pytest.mark.cpu_budget(ms=2250).mark
            return None

    assert module._cpu_budget_ms(FakeItem()) == 2250


def test_unit_test_conftest_cpu_budget_message_is_clear() -> None:
    """tests/conftest.py should explain CPU budget failures clearly."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = _load_module(conftest_path, "tests_timeout_conftest_cpu_failure")

    message = module._format_cpu_violation("tests/example.py::test_slow", 1250.4, 1000)
    assert "CPU latency budget exceeded: tests/example.py::test_slow" in message
    assert "consumed 1250.4ms CPU" in message
    assert "budget 1000ms" in message
    assert "@pytest.mark.cpu_budget(ms=...)" in message


def test_unit_test_conftest_validates_positive_integer_env_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tests/conftest.py should reject invalid timeout-budget env overrides."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_MS", "0")
    with pytest.raises(ValueError, match="GZA_UNIT_TEST_CPU_BUDGET_MS must be a positive integer"):
        _load_module(conftest_path, "tests_timeout_conftest_bad_cpu_env")

    monkeypatch.delenv("GZA_UNIT_TEST_CPU_BUDGET_MS", raising=False)
    monkeypatch.setenv("GZA_UNIT_TEST_HANG_TIMEOUT_MS", "abc")
    with pytest.raises(ValueError, match="GZA_UNIT_TEST_HANG_TIMEOUT_MS must be a positive integer"):
        _load_module(conftest_path, "tests_timeout_conftest_bad_hang_env")


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
            self.stash: dict[object, object] = {}

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
    module = _load_module(conftest_path, "tests_functional_watchdog_budget_conftest")
    functional_budget = module.FUNCTIONAL_TEST_TIMEOUT_SECONDS

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


@pytest.mark.cpu_budget(ms=2000)
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


def test_unit_suite_boundary_flags_direct_subprocess_run_import_alias(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_run_alias.py"
    nested_test.write_text(
        "from subprocess import run\n\n"
        "def test_git_subprocess(tmp_path):\n"
        "    run(['git', 'status'], cwd=tmp_path)\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:1 direct subprocess callable imports are banned in tests/ because they bypass the unit-suite runtime guard; import subprocess and patch the seam instead, or move the test to tests_functional/",
        f"{nested_test}:4 subprocess.run alias calls belong in tests_functional/ unless patched through an in-process seam",
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


def test_unit_suite_boundary_flags_direct_subprocess_popen_import_alias(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_popen_alias.py"
    nested_test.write_text(
        "from subprocess import Popen as pop\n\n"
        "def test_git_subprocess(tmp_path):\n"
        "    pop(['git', 'status'], cwd=tmp_path)\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:1 direct subprocess callable imports are banned in tests/ because they bypass the unit-suite runtime guard; import subprocess and patch the seam instead, or move the test to tests_functional/",
        f"{nested_test}:4 subprocess.Popen alias calls belong in tests_functional/ unless patched through an in-process seam",
    ]


def test_unit_suite_boundary_flags_direct_subprocess_check_output_import_alias(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_check_output_alias.py"
    nested_test.write_text(
        "from subprocess import check_output\n\n"
        "def test_non_git_subprocess(tmp_path):\n"
        "    check_output(['python', '-c', 'print(1)'], cwd=tmp_path)\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:1 direct subprocess callable imports are banned in tests/ because they bypass the unit-suite runtime guard; import subprocess and patch the seam instead, or move the test to tests_functional/",
        f"{nested_test}:4 subprocess.check_output alias calls belong in tests_functional/ unless patched through an in-process seam",
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


def test_unit_suite_boundary_flags_module_alias_subprocess_calls(tmp_path: Path) -> None:
    tests_root = tmp_path / "tests"
    nested = tests_root / "cli"
    nested.mkdir(parents=True)
    nested_test = nested / "test_module_alias.py"
    nested_test.write_text(
        "import subprocess as sp\n\n"
        "def test_git_subprocess(tmp_path):\n"
        "    sp.run(['git', 'status'], cwd=tmp_path)\n"
    )

    violations = _find_unit_suite_boundary_violations(tests_root)

    assert violations == [
        f"{nested_test}:4 direct git subprocess belongs in tests_functional/"
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
