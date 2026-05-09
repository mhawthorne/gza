"""Packaging configuration regression tests."""

import ast
import os
import subprocess
import tomllib
from pathlib import Path

import pytest

_PYTEST_TIMEOUT_DECORATOR = "@pytest.mark." "timeout"


def _find_timeout_decorator_occurrences(root: Path) -> list[str]:
    """Return every file/line under root that contains a timeout decorator."""
    occurrences: list[str] = []
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        file_text = path.read_text(encoding="utf-8", errors="ignore")
        for lineno, line in enumerate(file_text.splitlines(), start=1):
            if _PYTEST_TIMEOUT_DECORATOR in line:
                occurrences.append(f"{path.relative_to(root)}:{lineno}")
    return occurrences


def test_hatch_vcs_does_not_write_source_version_file() -> None:
    """Editable installs must not require writing src/gza/_version.py."""
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())

    hooks = config.get("tool", {}).get("hatch", {}).get("build", {}).get("hooks", {})
    assert "vcs" not in hooks


def test_pytest_timeout_watchdogs_are_scoped_by_suite() -> None:
    """pytest-timeout remains available without a global unit-suite watchdog."""
    repo_root = Path(__file__).resolve().parents[1]
    pyproject = repo_root / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())

    dependency_groups = config.get("dependency-groups", {})
    dev_deps = dependency_groups.get("dev", [])
    assert any(dep.startswith("pytest-timeout") for dep in dev_deps)

    pytest_options = config.get("tool", {}).get("pytest", {}).get("ini_options", {})
    assert "timeout" not in pytest_options

    unit_conftest_path = repo_root / "tests" / "conftest.py"
    unit_conftest = ast.parse(unit_conftest_path.read_text(), filename=str(unit_conftest_path))
    unit_timeout_calls = [
        node
        for node in ast.walk(unit_conftest)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "timeout"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "mark"
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == "pytest"
    ]
    assert len(unit_timeout_calls) == 1
    assert len(unit_timeout_calls[0].args) == 1
    assert isinstance(unit_timeout_calls[0].args[0], ast.Name)
    assert unit_timeout_calls[0].args[0].id == "FUNCTIONAL_TEST_TIMEOUT_SECONDS"

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


def test_unit_tests_do_not_carry_per_test_pytest_timeout_overrides() -> None:
    """Unit tests should not carry timeout decorators anywhere under tests/."""
    tests_root = Path(__file__).resolve().parents[1] / "tests"
    timeout_overrides = _find_timeout_decorator_occurrences(tests_root)

    assert not timeout_overrides, f"Found timeout overrides in tests/: {timeout_overrides}"


def test_timeout_decorator_scan_covers_all_files_under_tests_tree(tmp_path: Path) -> None:
    """The timeout-decorator guard should catch non-test helper files too."""
    tests_root = tmp_path / "tests"
    (tests_root / "cli").mkdir(parents=True)
    (tests_root / "helpers").mkdir()
    (tests_root / "cli" / "conftest.py").write_text(
        "import pytest\n\n"
        + _PYTEST_TIMEOUT_DECORATOR
        + "(5, method='signal')\n"
        "def pytest_collection_modifyitems(items):\n"
        "    return None\n"
    )
    (tests_root / "helpers" / "timeout_notes.txt").write_text(
        "Do not add "
        + _PYTEST_TIMEOUT_DECORATOR
        + " to helper modules.\n"
    )

    assert _find_timeout_decorator_occurrences(tests_root) == [
        "cli/conftest.py:3",
        "helpers/timeout_notes.txt:1",
    ]


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


def test_unit_test_conftest_skips_unit_timeout_injection() -> None:
    """tests/conftest.py should not inject timeout markers into plain unit tests."""
    conftest_path = Path(__file__).resolve().parents[1] / "tests" / "conftest.py"
    module = ast.parse(conftest_path.read_text(), filename=str(conftest_path))

    timeout_targets: list[str] = []
    collection_hooks: list[int] = []
    assigned_names: set[str] = set()
    marker_lookups: list[str] = []

    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == "pytest_collection_modifyitems":
            collection_hooks.append(node.lineno)
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assigned_names.add(target.id)
        if not isinstance(node, ast.Call):
            continue
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "get_closest_marker"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            marker_lookups.append(node.args[0].value)
        if not (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "timeout"
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "mark"
            and isinstance(node.func.value.value, ast.Name)
            and node.func.value.value.id == "pytest"
        ):
            continue
        assert len(node.args) == 1
        assert isinstance(node.args[0], ast.Name)
        timeout_targets.append(node.args[0].id)

    assert len(collection_hooks) == 1
    assert "UNIT_TEST_TIMEOUT_SECONDS" not in assigned_names
    assert timeout_targets == ["FUNCTIONAL_TEST_TIMEOUT_SECONDS"]
    assert "functional" in marker_lookups
    assert "timeout" in marker_lookups


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
    # Pin envs read by bin/tests so assertions are host-independent. The script's
    # default worker count is derived from CPU count, which varies by machine; we
    # exercise the literal-passthrough path here and leave default-derivation to
    # its own test.
    env["PYTEST_XDIST_WORKERS"] = "auto"
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
    # CI pins workers to `auto` because bin/tests now defaults to ~75% of cores so
    # busy laptops have headroom for Docker / gza / etc. CI runners aren't busy
    # with anything else, so they should saturate.
    assert 'PYTEST_XDIST_WORKERS: "auto"' in workflow_text


@pytest.mark.functional
def test_bin_tests_defaults_workers_to_three_quarters_of_cores(tmp_path: Path) -> None:
    """bin/tests should default to ~75% of cores when PYTEST_XDIST_WORKERS is unset.

    Why: dev laptops run Docker, gza, etc. alongside the test suite — saturating
    every core (`-n auto`) starves those siblings and trips the functional-test
    watchdog under fork/exec contention. CI overrides via env to keep `auto`.
    """
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "bin" / "tests"
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

    # Stub getconf to report a deterministic core count. The script reads CPU count
    # via `getconf _NPROCESSORS_ONLN`; intercepting that is the cleanest way to
    # assert the percentage formula without depending on the host's actual cores.
    fake_getconf = fake_bin / "getconf"
    fake_getconf.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "_NPROCESSORS_ONLN" ]; then echo 16; exit 0; fi\n'
        'exec /usr/bin/getconf "$@"\n'
    )
    fake_getconf.chmod(0o755)

    env = os.environ.copy()
    env.pop("PYTEST_XDIST_WORKERS", None)
    env.pop("PYTEST_FAULTHANDLER_TIMEOUT", None)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_UV_LOG"] = str(uv_log)

    result = subprocess.run(
        ["bash", str(script)],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    pytest_invocation = (tmp_path / "uv.log").read_text().splitlines()[-1]
    # 16 cores * 3 / 4 = 12 workers
    assert pytest_invocation == (
        "run pytest tests/ -n 12 --dist loadscope -x -o faulthandler_timeout=2"
    )


@pytest.mark.functional
@pytest.mark.parametrize("flag", ["--integration", "-i"])
def test_bin_tests_integration_flag_runs_integration_pytest(tmp_path: Path, flag: str) -> None:
    result = _run_bin_tests(tmp_path, flag)

    assert result.returncode == 0
    assert (tmp_path / "uv.log").read_text().splitlines()[-1] == "run pytest tests_integration -xv"


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
