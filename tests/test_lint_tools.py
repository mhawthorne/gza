"""Smoke tests verifying ruff and ty are installed and pass on the project source."""

import subprocess
import sys


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True)


def test_ruff_installed() -> None:
    result = _run([sys.executable, "-m", "ruff", "--version"])
    assert result.returncode == 0, f"ruff --version failed: {result.stderr}"


def test_ty_installed() -> None:
    result = _run(["uv", "run", "ty", "--version"])
    assert result.returncode == 0, f"ty --version failed: {result.stderr}"


def test_ruff_check_passes() -> None:
    result = _run(["uv", "run", "ruff", "check", "src/gza/"])
    assert result.returncode == 0, f"ruff check failed:\n{result.stdout}\n{result.stderr}"


def test_ty_check_passes() -> None:
    result = _run(["uv", "run", "ty", "check", "src/gza/"])
    assert result.returncode == 0, f"ty check failed:\n{result.stdout}\n{result.stderr}"
