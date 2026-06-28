"""Functional coverage for venv-aware verification script entrypoints."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest


def _make_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _setup_verify_script_fixture(tmp_path: Path) -> Path:
    fixture_root = tmp_path / "verify-fixture"
    (fixture_root / "bin").mkdir(parents=True)
    repo_root = Path(__file__).resolve().parents[1]
    (fixture_root / "bin" / "tests").write_text((repo_root / "bin" / "tests").read_text(encoding="utf-8"), encoding="utf-8")
    (fixture_root / "bin" / "tests").chmod(0o755)
    (fixture_root / "bin" / "test-unit").write_text(
        (repo_root / "bin" / "test-unit").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (fixture_root / "bin" / "test-unit").chmod(0o755)
    return fixture_root


def _write_fake_venv_python(path: Path, log_path: Path) -> None:
    _make_executable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf 'python %s\\n' \"$*\" >> {str(log_path)!r}\n"
        "if [[ \"${1:-}\" == \"-m\" && \"${2:-}\" == \"gza.tools.verify_phase\" ]]; then\n"
        f"  exec {sys.executable!r} \"$@\"\n"
        "fi\n",
    )


def _write_fake_passthrough_tool(path: Path, log_path: Path, name: str) -> None:
    _make_executable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '{name} %s\\n' \"$*\" >> {str(log_path)!r}\n",
    )


def _write_fake_getconf(path: Path, *, cpu_count: int) -> None:
    _make_executable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "if [[ \"${1:-}\" != \"_NPROCESSORS_ONLN\" ]]; then\n"
        "  exit 2\n"
        "fi\n"
        f"printf '%s\\n' {cpu_count!r}\n",
    )


def _write_fake_uv(path: Path, log_path: Path) -> None:
    _make_executable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf 'uv %s\\n' \"$*\" >> {str(log_path)!r}\n"
        "if [[ \"${1:-}\" == \"run\" && \"${2:-}\" == \"python\" && \"${3:-}\" == \"-m\" && \"${4:-}\" == \"gza.tools.verify_phase\" ]]; then\n"
        f"  shift 2\n  exec {sys.executable!r} \"$@\"\n"
        "fi\n",
    )


def _write_real_venv_python(path: Path) -> None:
    _make_executable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "if [[ \"${1:-}\" == \"-m\" && \"${2:-}\" == \"gza.tools.verify_phase\" ]]; then\n"
        f"  exec {sys.executable!r} \"$@\"\n"
        "fi\n",
    )


def _write_fake_python(path: Path, log_path: Path) -> None:
    _make_executable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf 'python %s\\n' \"$*\" >> {str(log_path)!r}\n",
    )


@pytest.mark.timeout(30, method="signal")
def test_full_verify_defaults_to_ci_parity_xdist_worker_count_on_high_core_machine(tmp_path: Path) -> None:
    fixture_root = _setup_verify_script_fixture(tmp_path)
    tool_log = fixture_root / "venv-tools.log"

    venv_bin = fixture_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    _write_fake_venv_python(venv_bin / "python", tool_log)
    _write_fake_passthrough_tool(fixture_root / "bin" / "test-unit", tool_log, "test-unit")
    for tool_name in ("ruff", "ty", "mypy", "pytest"):
        _write_fake_passthrough_tool(venv_bin / tool_name, tool_log, tool_name)

    fake_bin = fixture_root / "fake-bin"
    fake_bin.mkdir()
    _write_fake_getconf(fake_bin / "getconf", cpu_count=16)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"

    result = subprocess.run(
        ["bash", "bin/tests"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert result.returncode == 0, result.stderr
    assert "cores=16 xdist_workers=2" in result.stdout
    tool_invocations = tool_log.read_text(encoding="utf-8")
    assert "test-unit --summary -- tests/ -n 2 --dist loadscope --durations=25 -o faulthandler_timeout=60" in tool_invocations
    assert "pytest tests_functional/ -n 2 --dist loadscope -x --durations=25 -o faulthandler_timeout=60" in tool_invocations


@pytest.mark.timeout(30, method="signal")
def test_full_verify_uses_project_venv_for_test_latency_when_available(tmp_path: Path) -> None:
    fixture_root = _setup_verify_script_fixture(tmp_path)
    tool_log = fixture_root / "venv-tools.log"
    uv_log = fixture_root / "uv.log"
    repo_root = Path(__file__).resolve().parents[1]

    venv_bin = fixture_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    _write_fake_venv_python(venv_bin / "python", tool_log)
    _write_fake_passthrough_tool(fixture_root / "bin" / "test-unit", tool_log, "test-unit")
    for tool_name in ("ruff", "ty", "mypy", "pytest"):
        _write_fake_passthrough_tool(venv_bin / tool_name, tool_log, tool_name)

    fake_bin = fixture_root / "fake-bin"
    fake_bin.mkdir()
    _write_fake_uv(fake_bin / "uv", uv_log)
    uv_log.write_text("", encoding="utf-8")

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["PYTEST_XDIST_WORKERS"] = "7"
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")
    result = subprocess.run(
        ["bash", "bin/tests"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert result.returncode == 0, result.stderr
    assert "use_venv=1" in result.stdout
    tool_invocations = tool_log.read_text(encoding="utf-8")
    assert "python -m gza.tools.verify_phase unit -- ./bin/test-unit --summary -- tests/ -n 7 --dist loadscope --durations=25 -o faulthandler_timeout=60" in tool_invocations
    assert "test-unit --summary -- tests/ -n 7 --dist loadscope --durations=25 -o faulthandler_timeout=60" in tool_invocations
    assert (
        "python -m gza.tools.verify_phase functional -- pytest tests_functional/ -n 7 --dist loadscope -x --durations=25 -o faulthandler_timeout=60"
        in tool_invocations
    )
    assert uv_log.read_text(encoding="utf-8") == ""


@pytest.mark.timeout(30, method="signal")
def test_full_verify_falls_back_to_uv_for_test_latency_without_project_venv(tmp_path: Path) -> None:
    fixture_root = _setup_verify_script_fixture(tmp_path)
    uv_log = fixture_root / "uv.log"
    tool_log = fixture_root / "tools.log"
    repo_root = Path(__file__).resolve().parents[1]

    fake_bin = fixture_root / "fake-bin"
    fake_bin.mkdir()
    _write_fake_uv(fake_bin / "uv", uv_log)
    _write_fake_python(fake_bin / "python", tool_log)
    _write_fake_passthrough_tool(fixture_root / "bin" / "test-unit", tool_log, "test-unit")
    for tool_name in ("ruff", "ty", "mypy", "pytest"):
        _write_fake_passthrough_tool(fake_bin / tool_name, tool_log, tool_name)
    uv_log.write_text("", encoding="utf-8")

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["PYTEST_XDIST_WORKERS"] = "7"
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")
    result = subprocess.run(
        ["bash", "bin/tests"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert result.returncode == 0, result.stderr
    assert "use_venv=0" in result.stdout
    uv_invocations = uv_log.read_text(encoding="utf-8")
    assert "uv run python -m gza.tools.verify_phase ruff -- uv run ruff check src/gza/" in uv_invocations
    assert "uv run python -m gza.tools.verify_phase unit -- ./bin/test-unit --summary -- tests/ -n 7 --dist loadscope --durations=25 -o faulthandler_timeout=60" in uv_invocations
    assert "uv run python -m gza.tools.verify_phase functional -- uv run pytest tests_functional/ -n 7 --dist loadscope -x --durations=25 -o faulthandler_timeout=60" in uv_invocations


@pytest.mark.timeout(30, method="signal")
def test_full_verify_only_runs_integration_suite_with_integration_flag(tmp_path: Path) -> None:
    fixture_root = _setup_verify_script_fixture(tmp_path)
    tool_log = fixture_root / "venv-tools.log"

    venv_bin = fixture_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    _write_fake_venv_python(venv_bin / "python", tool_log)
    _write_fake_passthrough_tool(fixture_root / "bin" / "test-unit", tool_log, "test-unit")
    for tool_name in ("ruff", "ty", "mypy", "pytest"):
        _write_fake_passthrough_tool(venv_bin / tool_name, tool_log, tool_name)

    env = os.environ.copy()
    env["PYTEST_XDIST_WORKERS"] = "3"

    default_result = subprocess.run(
        ["bash", "bin/tests"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert default_result.returncode == 0, default_result.stderr
    default_invocations = tool_log.read_text(encoding="utf-8")
    assert "pytest tests_functional/ -n 3 --dist loadscope -x --durations=25 -o faulthandler_timeout=60" in default_invocations
    assert "pytest tests_integration -xv" not in default_invocations

    tool_log.write_text("", encoding="utf-8")

    integration_result = subprocess.run(
        ["bash", "bin/tests", "--integration"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert integration_result.returncode == 0, integration_result.stderr
    integration_invocations = tool_log.read_text(encoding="utf-8")
    assert "pytest tests_integration -xv" in integration_invocations


@pytest.mark.timeout(30, method="signal")
def test_quick_verify_omits_tree_fingerprint_when_gitdir_is_unavailable(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_root = _setup_verify_script_fixture(tmp_path)
    (fixture_root / ".git").write_text("gitdir: /definitely/missing/gitdir\n", encoding="utf-8")

    venv_bin = fixture_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    _write_real_venv_python(venv_bin / "python")
    _write_fake_passthrough_tool(venv_bin / "ruff", fixture_root / "tool.log", "ruff")

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")
    result = subprocess.run(
        ["bash", "bin/tests", "--quick"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert result.returncode == 0, result.stderr
    assert "gza-verify phase=start name=ruff" in result.stdout
    assert "gza-verify phase=passed name=ruff duration_seconds=" in result.stdout
    assert "tree_fingerprint=None" not in result.stdout
    assert "tree_fingerprint=" not in result.stdout
    assert "failed to compute exact tree fingerprint for timeout resume checkpoints" in result.stderr
    assert "not a git repository" in result.stderr
