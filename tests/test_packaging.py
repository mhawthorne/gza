"""Packaging configuration regression tests."""

import ast
import importlib.util
import tomllib
from pathlib import Path

import pytest


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    assert plain_unit.markers[0].mark.args == (module.UNIT_TEST_TIMEOUT_SECONDS,)
    assert plain_unit.markers[0].mark.kwargs == {"method": "signal"}

    assert len(plain_unit_2.markers) == 1
    assert plain_unit_2.markers[0].mark.name == "timeout"
    assert plain_unit_2.markers[0].mark.args == (module.UNIT_TEST_TIMEOUT_SECONDS,)
    assert plain_unit_2.markers[0].mark.kwargs == {"method": "signal"}

    assert explicit_timeout.markers == []


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


def test_github_test_workflow_uses_shared_test_script() -> None:
    workflow = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "test.yml"
    workflow_text = workflow.read_text()

    assert "run: ./bin/tests" in workflow_text
    # CI pins workers to `auto` because bin/tests now defaults to ~75% of cores so
    # busy laptops have headroom for Docker / gza / etc. CI runners aren't busy
    # with anything else, so they should saturate.
    assert 'PYTEST_XDIST_WORKERS: "auto"' in workflow_text
