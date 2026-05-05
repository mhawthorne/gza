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


def test_pytest_timeout_watchdog_is_enabled_by_default() -> None:
    """Pytest should fail hung tests quickly unless they opt into a higher limit."""
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    pyproject_text = pyproject.read_text()
    config = tomllib.loads(pyproject_text)

    dependency_groups = config.get("dependency-groups", {})
    dev_deps = dependency_groups.get("dev", [])
    assert any(dep.startswith("pytest-timeout>=") for dep in dev_deps)

    pytest_options = config.get("tool", {}).get("pytest", {}).get("ini_options", {})
    assert pytest_options.get("timeout") == 5
    assert pytest_options.get("timeout_method") == "thread"
    assert "bounded" in pyproject_text
    assert "rather than weakening the suite-wide 5s default" in pyproject_text


def test_explicit_pytest_timeout_overrides_are_bounded_when_present() -> None:
    """Per-test timeout overrides must keep the watchdog enabled instead of disabling it."""
    tests_root = Path(__file__).resolve().parents[1] / "tests"

    for test_file in tests_root.rglob("test_*.py"):
        module = ast.parse(test_file.read_text(), filename=str(test_file))
        for node in ast.walk(module):
            if not isinstance(node, ast.FunctionDef):
                continue
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call):
                    continue
                if not (
                    isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr == "timeout"
                    and isinstance(decorator.func.value, ast.Attribute)
                    and decorator.func.value.attr == "mark"
                    and isinstance(decorator.func.value.value, ast.Name)
                    and decorator.func.value.value.id == "pytest"
                ):
                    continue
                assert decorator.args, f"{test_file}:{node.lineno} timeout override must pass a bounded value"
                value = decorator.args[0]
                assert isinstance(value, ast.Constant) and isinstance(value.value, (int, float)), (
                    f"{test_file}:{node.lineno} timeout override must use a numeric bound"
                )
                assert value.value > 0, f"{test_file}:{node.lineno} timeout override must be > 0"
