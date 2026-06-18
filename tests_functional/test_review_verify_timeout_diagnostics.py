"""Functional coverage for review verify timeout diagnostics."""

from __future__ import annotations

import fcntl
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from gza.runner import _run_review_verify_command


pytestmark = [
    pytest.mark.functional,
    pytest.mark.skipif(os.name != "posix", reason="review verify descendant termination requires POSIX signals"),
]


def _write_timeout_fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    child_script = tmp_path / "child.py"
    parent_script = tmp_path / "parent.py"
    pid_file = tmp_path / "child.pid"
    lock_file = tmp_path / "child.lock"
    child_script.write_text(
        "import fcntl\n"
        "import os\n"
        "import signal\n"
        "import sys\n"
        "import time\n"
        "from pathlib import Path\n"
        "pid_file = Path(sys.argv[1])\n"
        "lock_path = Path(sys.argv[2])\n"
        "pid_file.write_text(str(os.getpid()), encoding='utf-8')\n"
        "lock_fd = open(lock_path, 'w', encoding='utf-8')\n"
        "fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print('child-ready', flush=True)\n"
        "while True:\n"
        "    time.sleep(0.1)\n",
        encoding="utf-8",
    )
    parent_script.write_text(
        "import subprocess\n"
        "import sys\n"
        "child_script = sys.argv[1]\n"
        "pid_file = sys.argv[2]\n"
        "lock_file = sys.argv[3]\n"
        "child = subprocess.Popen([sys.executable, child_script, pid_file, lock_file])\n"
        "print(f'child-pid={child.pid}', flush=True)\n"
        "child.wait()\n",
        encoding="utf-8",
    )
    return parent_script, child_script, pid_file, lock_file


def _wait_for_file(path: Path, *, timeout_seconds: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists() and path.read_text(encoding="utf-8").strip():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {path}")


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _child_lock_held(lock_file: Path) -> bool:
    with open(lock_file, "a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False


def _wait_for_process_exit(pid: int, *, timeout_seconds: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _process_exists(pid):
            return True
        time.sleep(0.05)
    return not _process_exists(pid)


def _wait_for_lock_release(lock_file: Path, *, timeout_seconds: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if lock_file.exists() and not _child_lock_held(lock_file):
            return True
        time.sleep(0.05)
    return lock_file.exists() and not _child_lock_held(lock_file)


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
def test_parent_only_sigterm_control_leaves_descendant_alive(tmp_path: Path) -> None:
    """Portable PID probing must detect the survivor if only the parent is signaled."""
    parent_script, child_script, pid_file, lock_file = _write_timeout_fixture(tmp_path)
    process = subprocess.Popen(
        [sys.executable, str(parent_script), str(child_script), str(pid_file), str(lock_file)],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_file(pid_file)
        child_pid = int(pid_file.read_text(encoding="utf-8").strip())
        os.kill(process.pid, signal.SIGTERM)
        process.wait(timeout=0.5)
        assert _process_exists(child_pid), "portable PID probe should catch a surviving descendant"
        assert _child_lock_held(lock_file), "child lock should still be held when only the parent dies"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if pid_file.exists():
            child_pid = int(pid_file.read_text(encoding="utf-8").strip())
            if _process_exists(child_pid):
                os.kill(child_pid, signal.SIGKILL)
                _wait_for_process_exit(child_pid)
        if lock_file.exists():
            _wait_for_lock_release(lock_file)


@pytest.mark.timeout(5)
def test_run_review_verify_command_forces_kill_when_inherited_pipe_descendant_survives_grace(
    tmp_path: Path,
) -> None:
    parent_script, child_script, pid_file, lock_file = _write_timeout_fixture(tmp_path)
    verify_command = " ".join(
        [
            shlex.quote(sys.executable),
            shlex.quote(str(parent_script)),
            shlex.quote(str(child_script)),
            shlex.quote(str(pid_file)),
            shlex.quote(str(lock_file)),
        ]
    )

    result = _run_review_verify_command(
        verify_command,
        cwd=tmp_path,
        timeout_seconds=0.2,
        timeout_grace_seconds=0.1,
    )

    assert result.status == "failed"
    assert result.exit_status == "timed out"
    assert "sent SIGTERM, waited 0.1s, then sent SIGKILL" in (result.output or "")
    assert _wait_for_lock_release(lock_file), "child lock should release after timeout kill"
