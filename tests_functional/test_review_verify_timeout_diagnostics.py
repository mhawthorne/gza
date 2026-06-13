"""Functional coverage for review verify timeout diagnostics."""

from __future__ import annotations

import shlex
import sys
import time
from pathlib import Path

import pytest

from gza.runner import _run_review_verify_command


def _process_is_running(pid: int) -> bool:
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return False
    state_line = next(
        (line for line in status_path.read_text(encoding="utf-8").splitlines() if line.startswith("State:")),
        None,
    )
    if state_line is None:
        return True
    return "\tZ" not in state_line


@pytest.mark.timeout(5)
def test_run_review_verify_command_captures_sigterm_dump_before_forced_kill(tmp_path: Path) -> None:
    script = tmp_path / "verify_sigterm_dump.py"
    script.write_text(
        "import signal\n"
        "import time\n"
        "\n"
        "def handle(_signum, _frame):\n"
        "    print('SIGTERM_SENTINEL', flush=True)\n"
        "    time.sleep(10)\n"
        "\n"
        "signal.signal(signal.SIGTERM, handle)\n"
        "print('STARTED', flush=True)\n"
        "time.sleep(10)\n",
        encoding="utf-8",
    )

    result = _run_review_verify_command(
        f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}",
        cwd=tmp_path,
        timeout_seconds=0.1,
        timeout_grace_seconds=0.1,
    )

    assert result.status == "failed"
    assert result.exit_status == "timed out"
    assert result.output is not None
    assert "SIGTERM_SENTINEL" in result.output
    assert "sent SIGTERM, waited 0.1s, then sent SIGKILL" in result.output


@pytest.mark.timeout(5)
def test_run_review_verify_command_reports_graceful_exit_without_forced_kill(tmp_path: Path) -> None:
    script = tmp_path / "verify_sigterm_exit.py"
    script.write_text(
        "import signal\n"
        "import sys\n"
        "import time\n"
        "\n"
        "def handle(_signum, _frame):\n"
        "    print('SIGTERM_GRACEFUL_EXIT', flush=True)\n"
        "    sys.exit(0)\n"
        "\n"
        "signal.signal(signal.SIGTERM, handle)\n"
        "print('STARTED', flush=True)\n"
        "time.sleep(10)\n",
        encoding="utf-8",
    )

    result = _run_review_verify_command(
        f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}",
        cwd=tmp_path,
        timeout_seconds=0.1,
        timeout_grace_seconds=0.2,
    )

    assert result.status == "failed"
    assert result.exit_status == "timed out"
    assert result.output is not None
    assert "SIGTERM_GRACEFUL_EXIT" in result.output
    assert "sent SIGTERM, waited 0.2s, and the process group exited during grace" in result.output
    assert "SIGKILL" not in result.output


@pytest.mark.timeout(10)
def test_run_review_verify_command_drains_large_sigterm_output_during_grace(tmp_path: Path) -> None:
    script = tmp_path / "verify_sigterm_large_output.py"
    script.write_text(
        "import os\n"
        "import signal\n"
        "import time\n"
        "\n"
        "PAYLOAD = b'X' * (2 * 1024 * 1024)\n"
        "\n"
        "def handle(_signum, _frame):\n"
        "    os.write(1, b'BEGIN_LARGE_SIGTERM_OUTPUT\\n')\n"
        "    os.write(1, PAYLOAD)\n"
        "    os.write(1, b'\\nEND_LARGE_SIGTERM_OUTPUT\\n')\n"
        "    raise SystemExit(0)\n"
        "\n"
        "signal.signal(signal.SIGTERM, handle)\n"
        "print('STARTED', flush=True)\n"
        "time.sleep(10)\n",
        encoding="utf-8",
    )

    result = _run_review_verify_command(
        f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}",
        cwd=tmp_path,
        timeout_seconds=0.1,
        timeout_grace_seconds=1.0,
    )

    assert result.status == "failed"
    assert result.exit_status == "timed out"
    assert result.output is not None
    assert "BEGIN_LARGE_SIGTERM_OUTPUT" in result.output
    assert "END_LARGE_SIGTERM_OUTPUT" in result.output
    assert "sent SIGTERM, waited 1.0s, and the process group exited during grace" in result.output
    assert "SIGKILL" not in result.output


@pytest.mark.timeout(5)
def test_run_review_verify_command_forces_kill_when_inherited_pipe_descendant_survives_grace(
    tmp_path: Path,
) -> None:
    child_pid_file = tmp_path / "child.pid"
    script = tmp_path / "verify_inherited_pipe_descendant.py"
    script.write_text(
        "import os\n"
        "import signal\n"
        "import subprocess\n"
        "import sys\n"
        "import time\n"
        "from pathlib import Path\n"
        "\n"
        "child_pid_file = Path(sys.argv[1])\n"
        "child = subprocess.Popen(\n"
        "    [\n"
        "        sys.executable,\n"
        "        '-c',\n"
        "        \"import signal, time; signal.signal(signal.SIGTERM, lambda *_: time.sleep(10)); time.sleep(10)\",\n"
        "    ],\n"
        ")\n"
        "child_pid_file.write_text(str(child.pid), encoding='utf-8')\n"
        "\n"
        "def handle(_signum, _frame):\n"
        "    print(f'ROOT_SIGTERM child_pid={child.pid}', flush=True)\n"
        "    sys.exit(0)\n"
        "\n"
        "signal.signal(signal.SIGTERM, handle)\n"
        "print('ROOT_STARTED', flush=True)\n"
        "time.sleep(10)\n",
        encoding="utf-8",
    )

    result = _run_review_verify_command(
        f"{shlex.quote(sys.executable)} {shlex.quote(str(script))} {shlex.quote(str(child_pid_file))}",
        cwd=tmp_path,
        timeout_seconds=0.1,
        timeout_grace_seconds=0.1,
    )

    assert result.status == "failed"
    assert result.exit_status == "timed out"
    assert result.output is not None
    assert "ROOT_SIGTERM" in result.output
    assert "sent SIGTERM, waited 0.1s, then sent SIGKILL" in result.output

    child_pid = int(child_pid_file.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if not _process_is_running(child_pid):
            break
        time.sleep(0.01)
    else:
        pytest.fail(f"inherited-pipe descendant survived forced kill: pid={child_pid}")
