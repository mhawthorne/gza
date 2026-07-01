"""Integration smoke tests for external lint/type-check tools."""

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True)


def _venv_tool(name: str) -> str:
    repo_root = Path(__file__).resolve().parents[1]
    project_venv_tool = repo_root / ".venv" / "bin" / name
    if project_venv_tool.exists():
        return str(project_venv_tool)
    return str(Path(sys.executable).resolve().parent / name)


def test_ruff_installed() -> None:
    result = _run([sys.executable, "-m", "ruff", "--version"])
    assert result.returncode == 0, f"ruff --version failed: {result.stderr}"


def test_ty_installed() -> None:
    result = _run([_venv_tool("ty"), "--version"])
    assert result.returncode == 0, f"ty --version failed: {result.stderr}"


def test_ruff_check_passes() -> None:
    result = _run([sys.executable, "-m", "ruff", "check", "src/gza/"])
    assert result.returncode == 0, f"ruff check failed:\n{result.stdout}\n{result.stderr}"


def test_ruff_check_watch_cli_module_passes() -> None:
    result = _run(["uv", "run", "ruff", "check", "src/gza/cli/watch.py"])
    assert result.returncode == 0, f"ruff check failed for watch CLI module:\n{result.stdout}\n{result.stderr}"


def test_watch_cli_imports_are_isort_clean() -> None:
    result = _run([_venv_tool("ruff"), "check", "--select", "I001", "src/gza/cli/watch.py"])
    assert result.returncode == 0, f"ruff import-order check failed:\n{result.stdout}\n{result.stderr}"


def test_watch_cli_mypy_passes() -> None:
    result = _run([_venv_tool("mypy"), "src/gza/cli/watch.py"])
    assert result.returncode == 0, f"mypy watch.py failed:\n{result.stdout}\n{result.stderr}"


def test_ty_check_passes() -> None:
    result = _run([_venv_tool("ty"), "check", "src/gza/"])
    assert result.returncode == 0, f"ty check failed:\n{result.stdout}\n{result.stderr}"
