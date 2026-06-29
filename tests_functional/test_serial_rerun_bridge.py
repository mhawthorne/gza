"""End-to-end coverage for the guarded unit serial-rerun bridge."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


_TEST_UNIT_SUBPROCESS_TIMEOUT_SECONDS = 15


def _repo_env() -> dict[str, str]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.pop("PYTEST_XDIST_WORKER", None)
    env.pop("PYTEST_XDIST_WORKER_COUNT", None)
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")
    return env


def _run_test_unit(
    suite_dir: Path,
    *extra_args: str,
    env_updates: dict[str, str] | None = None,
    use_verify_phase: bool = False,
    timeout_seconds: float = _TEST_UNIT_SUBPROCESS_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    env = _repo_env()
    if env_updates:
        env.update(env_updates)
    module_cmd = [
        sys.executable,
        "-m",
        "gza.test_serial_rerun",
        "--summary",
        "--",
        str(suite_dir),
        "-n",
        "2",
        "--dist",
        "loadscope",
        "-o",
        "faulthandler_timeout=60",
        *extra_args,
    ]
    if use_verify_phase:
        cmd = [sys.executable, "-m", "gza.tools.verify_phase", "unit", "--", *module_cmd]
    else:
        cmd = module_cmd
    return subprocess.run(
        cmd,
        cwd=suite_dir.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def _run_test_functional(
    suite_dir: Path,
    *extra_args: str,
    env_updates: dict[str, str] | None = None,
    use_verify_phase: bool = False,
    timeout_seconds: float = _TEST_UNIT_SUBPROCESS_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    env = _repo_env()
    if env_updates:
        env.update(env_updates)
    module_cmd = [
        sys.executable,
        "-m",
        "gza.test_serial_rerun",
        "--phase",
        "functional",
        "--summary",
        "--",
        str(suite_dir),
        "-n",
        "2",
        "--dist",
        "loadscope",
        "-o",
        "faulthandler_timeout=60",
        *extra_args,
    ]
    if use_verify_phase:
        cmd = [sys.executable, "-m", "gza.tools.verify_phase", "functional", "--", *module_cmd]
    else:
        cmd = module_cmd
    return subprocess.run(
        cmd,
        cwd=suite_dir.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


@pytest.mark.timeout(60, method="signal")
def test_parallel_only_watchdog_failure_passes_via_serial_rerun_and_preserves_unit_phase_line(tmp_path: Path) -> None:
    suite_dir = tmp_path / "parallel_only_watchdog"
    suite_dir.mkdir()
    (suite_dir / "test_parallel_only_watchdog.py").write_text(
        "import os\n"
        "import time\n"
        "import pytest\n\n"
        "@pytest.mark.timeout(1, method='signal')\n"
        "def test_parallel_only_watchdog_probe():\n"
        "    if int(os.environ.get('PYTEST_XDIST_WORKER_COUNT', '0')) > 1:\n"
        "        time.sleep(2)\n",
        encoding="utf-8",
    )

    # This subprocess performs two pytest invocations (parallel pass + serial rerun).
    # Under full-suite xdist contention that can exceed a brittle 4s child timeout,
    # so keep the subprocess bounded but give it headroom well below the test's
    # explicit 60s watchdog.
    result = _run_test_unit(suite_dir, use_verify_phase=True)

    assert result.returncode == 0, result.stderr
    assert "gza-verify phase=passed name=unit duration_seconds=" in result.stdout
    assert "latency: " in result.stdout
    assert "PARALLEL-ONLY FAILURE (passed serially):" in result.stderr


@pytest.mark.timeout(60, method="signal")
def test_over_cap_failures_fail_without_serial_rerun(tmp_path: Path) -> None:
    suite_dir = tmp_path / "over_cap_suite"
    suite_dir.mkdir()
    for index in range(3):
        (suite_dir / f"test_fail_{index}.py").write_text(
            f"def test_fail_{index}():\n"
            f"    assert False, 'boom-{index}'\n",
            encoding="utf-8",
        )

    result = _run_test_unit(suite_dir, env_updates={"GZA_UNIT_RERUN_CAP": "2"})

    assert result.returncode != 0
    assert "NOT masking - over cap (3 > 2)" in result.stderr
    assert "re-running serially" not in result.stderr


@pytest.mark.timeout(60, method="signal")
def test_genuinely_broken_test_still_fails_phase_and_logs_confirmed_failure(tmp_path: Path) -> None:
    suite_dir = tmp_path / "broken_suite"
    suite_dir.mkdir()
    (suite_dir / "test_broken.py").write_text(
        "def test_broken():\n"
        "    assert False, 'still broken'\n",
        encoding="utf-8",
    )

    result = _run_test_unit(suite_dir, use_verify_phase=True)

    assert result.returncode != 0
    assert "gza-verify phase=failed name=unit duration_seconds=" in result.stdout
    assert "CONFIRMED FAILURE (failed serially too):" in result.stderr


@pytest.mark.timeout(60, method="signal")
def test_green_parallel_path_keeps_summary_and_skips_rerun(tmp_path: Path) -> None:
    suite_dir = tmp_path / "green_suite"
    suite_dir.mkdir()
    (suite_dir / "test_green.py").write_text(
        "def test_green():\n"
        "    assert True\n",
        encoding="utf-8",
    )

    result = _run_test_unit(suite_dir)

    assert result.returncode == 0, result.stderr
    assert "latency: " in result.stdout
    assert "re-running serially" not in result.stderr
    assert "PARALLEL-ONLY FAILURE" not in result.stderr


@pytest.mark.timeout(60, method="signal")
def test_parallel_only_invalid_path_probe_passes_via_functional_serial_rerun(tmp_path: Path) -> None:
    suite_dir = tmp_path / "functional_invalid_path_probe"
    suite_dir.mkdir()
    (suite_dir / "test_invalid_path_probe.py").write_text(
        "import os\n\n"
        "def test_invalid_path_probe():\n"
        "    if int(os.environ.get('PYTEST_XDIST_WORKER_COUNT', '0')) > 1:\n"
        "        raise AssertionError(\n"
        "            \"git worktree list --porcelain failed: fatal: Invalid path '/gza-git': No such file or directory\"\n"
        "        )\n",
        encoding="utf-8",
    )

    result = _run_test_functional(suite_dir, use_verify_phase=True)

    assert result.returncode == 0, result.stderr
    assert "gza-verify phase=passed name=functional duration_seconds=" in result.stdout
    assert "functional-rerun: PARALLEL-ONLY FAILURE (passed serially):" in result.stderr
    assert "Invalid path '/gza-git': No such file or directory" in result.stderr
