"""Packaging configuration regression tests."""

import ast
import tomllib
from pathlib import Path


def test_hatch_vcs_does_not_write_source_version_file() -> None:
    """Editable installs must not require writing src/gza/_version.py."""
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())

    hooks = config.get("tool", {}).get("hatch", {}).get("build", {}).get("hooks", {})
    assert "vcs" not in hooks


def test_pytest_timeout_watchdogs_are_scoped_by_suite() -> None:
    """Test suites should fail hung tests without applying a global watchdog."""
    repo_root = Path(__file__).resolve().parents[1]
    pyproject = repo_root / "pyproject.toml"
    pyproject_text = pyproject.read_text()
    config = tomllib.loads(pyproject_text)

    dependency_groups = config.get("dependency-groups", {})
    dev_deps = dependency_groups.get("dev", [])
    assert any(dep.startswith("pytest-timeout>=") for dep in dev_deps)

    pytest_options = config.get("tool", {}).get("pytest", {}).get("ini_options", {})
    assert "timeout" not in pytest_options
    assert "timeout_method" not in pytest_options

    unit_conftest = (repo_root / "tests" / "conftest.py").read_text()
    assert "UNIT_TEST_TIMEOUT_SECONDS = 1" in unit_conftest
    assert "pytest.mark.timeout(UNIT_TEST_TIMEOUT_SECONDS, method=\"signal\")" in unit_conftest

    integration_conftest = (repo_root / "tests_integration" / "conftest.py").read_text()
    assert "INTEGRATION_TEST_TIMEOUT_SECONDS = 10" in integration_conftest
    assert (
        "pytest.mark.timeout(INTEGRATION_TEST_TIMEOUT_SECONDS, method=\"signal\")"
        in integration_conftest
    )


def test_unit_tests_do_not_carry_pytest_timeout_overrides() -> None:
    """Unit tests should not rely on per-test timeout overrides."""
    tests_root = Path(__file__).resolve().parents[1] / "tests"
    timeout_overrides: list[str] = []

    for test_file in tests_root.rglob("test_*.py"):
        module = ast.parse(test_file.read_text(), filename=str(test_file))
        for node in ast.walk(module):
            if not isinstance(node, ast.Call):
                continue
            if not (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "timeout"
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "mark"
                and isinstance(node.func.value.value, ast.Name)
                and node.func.value.value.id == "pytest"
            ):
                continue
            timeout_overrides.append(f"{test_file}:{node.lineno}")

    assert not timeout_overrides, f"Found timeout overrides in tests/: {timeout_overrides}"
