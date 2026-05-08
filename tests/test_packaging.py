"""Packaging configuration regression tests."""

import ast
import os
import subprocess
import tomllib
from pathlib import Path

import pytest


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
    assert pytest_options["timeout_method"] == "signal"

    unit_conftest = (repo_root / "tests" / "conftest.py").read_text()
    assert (
        'UNIT_TEST_TIMEOUT_SECONDS = int(os.environ.get("GZA_UNIT_TEST_TIMEOUT_SECONDS", "1"))'
        in unit_conftest
    )
    assert (
        'FUNCTIONAL_TEST_TIMEOUT_SECONDS = int(os.environ.get("GZA_FUNCTIONAL_TEST_TIMEOUT_SECONDS", "2"))'
        in unit_conftest
    )
    assert "pytest.mark.timeout(UNIT_TEST_TIMEOUT_SECONDS, method=\"signal\")" in unit_conftest
    assert "pytest.mark.timeout(FUNCTIONAL_TEST_TIMEOUT_SECONDS, method=\"signal\")" in unit_conftest

    integration_conftest = (repo_root / "tests_integration" / "conftest.py").read_text()
    assert "INTEGRATION_TEST_TIMEOUT_SECONDS = 10" in integration_conftest
    assert (
        "pytest.mark.timeout(INTEGRATION_TEST_TIMEOUT_SECONDS, method=\"signal\")"
        in integration_conftest
    )


def test_unit_tests_do_not_carry_per_test_pytest_timeout_overrides() -> None:
    """Unit tests should rely on the central suite timeout fixture."""
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


def test_functional_subprocess_timeouts_within_watchdog() -> None:
    """subprocess.run(timeout=N) inside @pytest.mark.functional tests must not exceed the watchdog."""
    repo_root = Path(__file__).resolve().parents[1]
    conftest_path = repo_root / "tests" / "conftest.py"

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
        # Direct constant assignment, e.g. `FUNCTIONAL_TEST_TIMEOUT_SECONDS = 2`.
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, (int, float)):
            functional_budget = node.value.value
            continue
        # Env-overridable form: `int(os.environ.get("...", "2"))` — pull the literal default.
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
            default_literal = node.value.args[0].args[1].value
            functional_budget = int(default_literal)
    assert functional_budget is not None, "FUNCTIONAL_TEST_TIMEOUT_SECONDS not found in tests/conftest.py"

    def has_functional_marker(decorators: list[ast.expr]) -> bool:
        for decorator in decorators:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            if (
                isinstance(target, ast.Attribute)
                and target.attr == "functional"
                and isinstance(target.value, ast.Attribute)
                and target.value.attr == "mark"
                and isinstance(target.value.value, ast.Name)
                and target.value.value.id == "pytest"
            ):
                return True
        return False

    inversions: list[str] = []
    tests_root = repo_root / "tests"
    for test_file in tests_root.rglob("test_*.py"):
        module = ast.parse(test_file.read_text(), filename=str(test_file))
        functional_funcs: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
        for top in module.body:
            if isinstance(top, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if has_functional_marker(top.decorator_list):
                    functional_funcs.append(top)
            elif isinstance(top, ast.ClassDef):
                class_marked = has_functional_marker(top.decorator_list)
                for child in top.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
                        class_marked or has_functional_marker(child.decorator_list)
                    ):
                        functional_funcs.append(child)

        for func in functional_funcs:
            for inner in ast.walk(func):
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


def test_unit_test_conftest_assigns_only_central_timeout_marker() -> None:
    """tests/conftest.py should own the unit-suite timeout marker."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = ast.parse(conftest_path.read_text(), filename=str(conftest_path))

    timeout_calls: list[int] = []
    collection_hooks: list[int] = []

    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == "pytest_collection_modifyitems":
            collection_hooks.append(node.lineno)
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
        timeout_calls.append(node.lineno)

    assert len(collection_hooks) == 1
    assert len(timeout_calls) == 2


def _run_bin_tests(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "bin" / "tests"
    tmp_path.mkdir(parents=True, exist_ok=True)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uv = fake_bin / "uv"
    uv_log = tmp_path / "uv.log"
    uv_log.write_text("")
    fake_uv.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >>\"$FAKE_UV_LOG\"\n"
        "exit 0\n"
    )
    fake_uv.chmod(0o755)

    env = os.environ.copy()
    # Scrub envs read by bin/tests so assertions exercise its built-in defaults
    # regardless of how the parent test runner was invoked.
    env.pop("PYTEST_XDIST_WORKERS", None)
    env.pop("PYTEST_FAULTHANDLER_TIMEOUT", None)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_UV_LOG"] = str(uv_log)

    return subprocess.run(
        ["bash", str(script), *args],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.mark.functional
def test_bin_tests_default_run_skips_integration_pytest(tmp_path: Path) -> None:
    result = _run_bin_tests(tmp_path)

    assert result.returncode == 0
    assert (tmp_path / "uv.log").read_text().splitlines() == [
        "run ruff check src/gza/",
        "run ty check src/gza/",
        "run mypy src/gza/",
        "run python -m checks",
        'run pytest tests/ -n auto --dist loadscope -x -o faulthandler_timeout=2',
    ]


def test_github_test_workflow_uses_shared_test_script() -> None:
    workflow = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "test.yml"
    workflow_text = workflow.read_text()

    assert "run: ./bin/tests" in workflow_text
    assert "PYTEST_XDIST_WORKERS" not in workflow_text


@pytest.mark.functional
def test_bin_tests_integration_flag_runs_integration_pytest(tmp_path: Path) -> None:
    long_result = _run_bin_tests(tmp_path / "long", "--integration")
    short_result = _run_bin_tests(tmp_path / "short", "-i")

    assert long_result.returncode == 0
    assert short_result.returncode == 0
    assert (tmp_path / "long" / "uv.log").read_text().splitlines()[-1] == "run pytest tests_integration -xv"
    assert (tmp_path / "short" / "uv.log").read_text().splitlines()[-1] == "run pytest tests_integration -xv"


@pytest.mark.functional
def test_bin_tests_unknown_argument_exits_with_usage_and_no_invocations(tmp_path: Path) -> None:
    result = _run_bin_tests(tmp_path, "--wat")
    script = Path(__file__).resolve().parents[1] / "bin" / "tests"

    assert result.returncode == 2
    assert (tmp_path / "uv.log").read_text() == ""
    assert result.stderr.splitlines()[-3:] == [
        f"+ echo 'Usage: {script} [-i|--integration]'",
        f"Usage: {script} [-i|--integration]",
        "+ exit 2",
    ]
