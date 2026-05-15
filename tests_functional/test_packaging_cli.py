"""Functional regression tests for test runner shell scripts."""

import os
import subprocess
from pathlib import Path

import pytest


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


def test_bin_tests_default_run_splits_unit_and_functional_pytest(tmp_path: Path) -> None:
    result = _run_bin_tests(tmp_path)

    assert result.returncode == 0
    assert (tmp_path / "uv.log").read_text().splitlines() == [
        "run ruff check src/gza/",
        "run ty check src/gza/",
        "run mypy src/gza/",
        "run python -m checks",
        'run pytest tests/ -n auto --dist loadscope -x -o faulthandler_timeout=2',
        'run pytest tests_functional/ -x -o faulthandler_timeout=2',
    ]


def test_bin_tests_defaults_workers_to_three_quarters_of_cores(tmp_path: Path) -> None:
    """bin/tests should default to ~75% of cores when PYTEST_XDIST_WORKERS is unset."""
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

    pytest_invocations = (tmp_path / "uv.log").read_text().splitlines()[-2:]
    assert pytest_invocations == [
        "run pytest tests/ -n 12 --dist loadscope -x -o faulthandler_timeout=2",
        "run pytest tests_functional/ -x -o faulthandler_timeout=2",
    ]


@pytest.mark.parametrize("flag", ["--integration", "-i"])
def test_bin_tests_integration_flag_runs_integration_pytest(tmp_path: Path, flag: str) -> None:
    result = _run_bin_tests(tmp_path, flag)

    assert result.returncode == 0
    assert (tmp_path / "uv.log").read_text().splitlines()[-1] == "run pytest tests_integration -xv"


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
