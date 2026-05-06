"""Packaging configuration regression tests."""

import ast
import os
import subprocess
import tomllib
from pathlib import Path


def test_hatch_vcs_does_not_write_source_version_file() -> None:
    """Editable installs must not require writing src/gza/_version.py."""
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())

    hooks = config.get("tool", {}).get("hatch", {}).get("build", {}).get("hooks", {})
    assert "vcs" not in hooks


def test_pytest_timeout_watchdogs_are_scoped_by_suite() -> None:
    """Unit tests should keep the suite-wide watchdog in central pytest config."""
    repo_root = Path(__file__).resolve().parents[1]
    pyproject = repo_root / "pyproject.toml"
    pyproject_text = pyproject.read_text()
    config = tomllib.loads(pyproject_text)

    dependency_groups = config.get("dependency-groups", {})
    dev_deps = dependency_groups.get("dev", [])
    assert any(dep.startswith("pytest-timeout>=") for dep in dev_deps)

    pytest_options = config.get("tool", {}).get("pytest", {}).get("ini_options", {})
    assert pytest_options["timeout"] == 5
    assert pytest_options["timeout_method"] == "thread"

    unit_conftest = (repo_root / "tests" / "conftest.py").read_text()
    assert "pytest.mark.timeout(" not in unit_conftest
    assert "pytest_collection_modifyitems" not in unit_conftest

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


def test_unit_test_conftest_does_not_assign_timeout_markers() -> None:
    """tests/conftest.py should not inject timeout markers during collection."""
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

    assert not collection_hooks, (
        f"tests/conftest.py unexpectedly defines pytest_collection_modifyitems at {collection_hooks}"
    )
    assert not timeout_calls, (
        f"tests/conftest.py unexpectedly assigns pytest timeout markers at {timeout_calls}"
    )


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
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_UV_LOG"] = str(uv_log)

    return subprocess.run(
        ["bash", str(script), *args],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )


def test_bin_tests_default_run_skips_integration_pytest(tmp_path: Path) -> None:
    result = _run_bin_tests(tmp_path)

    assert result.returncode == 0
    assert (tmp_path / "uv.log").read_text().splitlines() == [
        "run ruff check src/gza/",
        "run ty check src/gza/",
        "run mypy src/gza/",
        "run python -m checks",
        'run pytest tests/ -n 8 --dist loadscope -x -o faulthandler_timeout=2',
    ]


def test_bin_tests_integration_flag_runs_integration_pytest(tmp_path: Path) -> None:
    long_result = _run_bin_tests(tmp_path / "long", "--integration")
    short_result = _run_bin_tests(tmp_path / "short", "-i")

    assert long_result.returncode == 0
    assert short_result.returncode == 0
    assert (tmp_path / "long" / "uv.log").read_text().splitlines()[-1] == "run pytest tests_integration -xv"
    assert (tmp_path / "short" / "uv.log").read_text().splitlines()[-1] == "run pytest tests_integration -xv"


def test_bin_tests_unknown_argument_exits_with_usage_and_no_invocations(tmp_path: Path) -> None:
    result = _run_bin_tests(tmp_path, "--wat")
    script = Path(__file__).resolve().parents[1] / "bin" / "tests"

    assert result.returncode == 2
    assert (tmp_path / "uv.log").read_text() == ""
    assert result.stderr.splitlines()[-3:] == [
        "+ echo 'Usage: /workspace/bin/tests [-i|--integration]'",
        f"Usage: {script} [-i|--integration]",
        "+ exit 2",
    ]
