"""Functional coverage for venv-aware verification script entrypoints."""

from __future__ import annotations

import os
import re
import shutil
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

def _write_fake_ruff_failure(path: Path, log_path: Path, *, exit_code: int = 1) -> None:
    _make_executable(
        path,
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf 'ruff %s\\n' \"$*\" >> {str(log_path)!r}\n"
        f"exit {exit_code}\n",
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


def _current_env_tool(name: str) -> str:
    sibling_tool = Path(sys.executable).resolve().parent / name
    if sibling_tool.is_file():
        return str(sibling_tool)

    resolved = shutil.which(name)
    assert resolved is not None, f"{name} not found in current test environment"
    return resolved


def _watch_module_ruff_command(repo_root: Path) -> list[str]:
    ruff_bin = repo_root / ".venv" / "bin" / "ruff"
    if ruff_bin.is_file() and os.access(ruff_bin, os.X_OK):
        return [str(ruff_bin), "check", "src/gza/cli/watch.py"]
    return ["uv", "run", "ruff", "check", "src/gza/cli/watch.py"]


def _repo_ruff_command(repo_root: Path) -> list[str]:
    venv_bin = repo_root / ".venv" / "bin"
    if venv_bin.is_dir():
        return [str(venv_bin / "ruff")]
    return ["uv", "run", "ruff"]


def _verify_ruff_targets() -> list[str]:
    return ["src/gza/", "tests/test_main_integration_verify.py"]


def _full_verify_ruff_command(repo_root: Path) -> list[str]:
    ruff_bin = repo_root / ".venv" / "bin" / "ruff"
    if ruff_bin.is_file() and os.access(ruff_bin, os.X_OK):
        return [str(ruff_bin), "check", *_verify_ruff_targets()]
    return ["uv", "run", "ruff", "check", *_verify_ruff_targets()]


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
    assert (
        "uv run python -m gza.tools.verify_phase ruff -- "
        "uv run ruff check src/gza/ tests/test_main_integration_verify.py"
    ) in uv_invocations
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


@pytest.mark.timeout(30, method="signal")
def test_quick_verify_stops_after_ruff_failure(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_root = _setup_verify_script_fixture(tmp_path)
    tool_log = fixture_root / "tool.log"

    venv_bin = fixture_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    _write_real_venv_python(venv_bin / "python")
    _write_fake_ruff_failure(venv_bin / "ruff", tool_log)

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

    assert result.returncode == 1
    assert "gza-verify phase=start name=ruff" in result.stdout
    assert "gza-verify phase=failed name=ruff duration_seconds=" in result.stdout
    assert "gza-verify phase=start name=checks" not in result.stdout
    assert tool_log.read_text(encoding="utf-8").splitlines() == [
        "ruff check src/gza/ tests/test_main_integration_verify.py"
    ]


@pytest.mark.timeout(30, method="signal")
def test_verify_phase_ruff_passes_for_watch_cli_module_on_real_repo() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "gza.tools.verify_phase",
            "ruff",
            "--",
            *_watch_module_ruff_command(repo_root),
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert "gza-verify phase=start name=ruff" in result.stdout
    match = re.search(
        r"gza-verify phase=passed name=ruff duration_seconds=[0-9.]+(?: tree_fingerprint=[0-9a-f]{64})?",
        result.stdout,
    )
    assert match is not None, result.stdout


@pytest.mark.timeout(30, method="signal")
def test_quick_verify_falls_back_to_uv_run_ruff_without_project_venv(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_root = _setup_verify_script_fixture(tmp_path)
    uv_log = fixture_root / "uv.log"

    fake_bin = fixture_root / "fake-bin"
    fake_bin.mkdir()
    _write_fake_uv(fake_bin / "uv", uv_log)
    uv_log.write_text("", encoding="utf-8")

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")
    result = subprocess.run(
        ["bash", "bin/tests", "--quick"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert not (fixture_root / ".venv" / "bin" / "ruff").exists()
    assert result.returncode == 0, result.stderr
    assert "gza-verify phase=start name=ruff" in result.stdout
    assert "gza-verify phase=passed name=ruff duration_seconds=" in result.stdout
    uv_invocations = uv_log.read_text(encoding="utf-8")
    assert (
        "uv run python -m gza.tools.verify_phase ruff -- "
        "uv run ruff check src/gza/ tests/test_main_integration_verify.py"
    ) in uv_invocations


@pytest.mark.timeout(30, method="signal")
def test_full_verify_stops_after_ruff_failure(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_root = _setup_verify_script_fixture(tmp_path)
    tool_log = fixture_root / "tool.log"

    venv_bin = fixture_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    _write_fake_venv_python(venv_bin / "python", tool_log)
    _write_fake_ruff_failure(venv_bin / "ruff", tool_log)
    for tool_name in ("ty", "mypy", "pytest"):
        _write_fake_passthrough_tool(venv_bin / tool_name, tool_log, tool_name)
    _write_fake_passthrough_tool(fixture_root / "bin" / "test-unit", tool_log, "test-unit")

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")
    result = subprocess.run(
        ["bash", "bin/tests"],
        cwd=fixture_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=4,
    )

    assert result.returncode == 1
    assert "gza-verify phase=start name=ruff" in result.stdout
    assert "gza-verify phase=failed name=ruff duration_seconds=" in result.stdout
    assert "gza-verify phase=start name=ty" not in result.stdout
    assert "gza-verify phase=start name=unit" not in result.stdout
    tool_invocations = tool_log.read_text(encoding="utf-8")
    assert "ruff check src/gza/ tests/test_main_integration_verify.py" in tool_invocations
    assert "ty check src/gza/" not in tool_invocations
    assert "test-unit --summary -- tests/" not in tool_invocations


@pytest.mark.functional
@pytest.mark.timeout(30, method="signal")
def test_ruff_check_passes_for_watch_cli_module() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [*_repo_ruff_command(repo_root), "check", "src/gza/cli/watch.py"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_watch_module_ruff_command_matches_bin_tests_modes(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    venv_bin = repo_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)

    ruff_bin = venv_bin / "ruff"
    ruff_bin.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    ruff_bin.chmod(0o755)

    assert _watch_module_ruff_command(repo_root) == [str(ruff_bin), "check", "src/gza/cli/watch.py"]

    ruff_bin.chmod(0o644)
    assert _watch_module_ruff_command(repo_root) == ["uv", "run", "ruff", "check", "src/gza/cli/watch.py"]


def test_full_verify_ruff_command_matches_bin_tests_modes(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    venv_bin = repo_root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)

    ruff_bin = venv_bin / "ruff"
    ruff_bin.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    ruff_bin.chmod(0o755)

    assert _full_verify_ruff_command(repo_root) == [
        str(ruff_bin),
        "check",
        "src/gza/",
        "tests/test_main_integration_verify.py",
    ]

    ruff_bin.chmod(0o644)
    assert _full_verify_ruff_command(repo_root) == [
        "uv",
        "run",
        "ruff",
        "check",
        "src/gza/",
        "tests/test_main_integration_verify.py",
    ]


@pytest.mark.timeout(30, method="signal")
def test_verify_phase_ruff_passes_for_full_src_tree_on_real_repo() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "gza.tools.verify_phase",
            "ruff",
            "--",
            *_full_verify_ruff_command(repo_root),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert "gza-verify phase=start name=ruff" in result.stdout
    match = re.search(
        r"gza-verify phase=passed name=ruff duration_seconds=[0-9.]+(?: tree_fingerprint=[0-9a-f]{64})?",
        result.stdout,
    )
    assert match is not None, result.stdout
