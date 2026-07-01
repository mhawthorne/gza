"""Focused regression coverage for lint failures that block the verify gate."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def _venv_tool(repo_root: Path, name: str) -> Path:
    project_venv_tool = repo_root / ".venv" / "bin" / name
    if project_venv_tool.exists():
        return project_venv_tool
    return Path(sys.executable).resolve().parent / name


@pytest.mark.timeout(30, method="signal")
def test_watch_cli_module_passes_ruff_gate(tmp_path: Path) -> None:
    del tmp_path
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [str(_venv_tool(repo_root, "ruff")), "check", "src/gza/cli/watch.py"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.timeout(30, method="signal")
def test_main_integration_verify_unit_test_passes_ruff_gate(tmp_path: Path) -> None:
    del tmp_path
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [str(_venv_tool(repo_root, "ruff")), "check", "tests/test_main_integration_verify.py"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.timeout(30, method="signal")
def test_watch_cli_module_passes_mypy_gate(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["MYPY_CACHE_DIR"] = str(tmp_path / ".mypy_cache")
    result = subprocess.run(
        [str(_venv_tool(repo_root, "mypy")), "src/gza/cli/watch.py"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=20,
        env=env,
    )

    assert result.returncode == 0, result.stdout + result.stderr
