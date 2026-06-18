"""Internal verification phase wrapper."""

from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path

from gza.git import Git, GitError
from gza.runner import _compute_tree_fingerprint

_PHASE_NAME_RE = re.compile(r"[A-Za-z0-9_.-]+")
_TREE_FINGERPRINT_RE = re.compile(r"[0-9a-f]{64}")


def _usage() -> int:
    print("Usage: python -m gza.tools.verify_phase <name> -- <cmd...>", file=sys.stderr)
    return 2


def _result_line(status: str, name: str, duration_seconds: float, tree_fingerprint: str | None) -> str:
    line = f"gza-verify phase={status} name={name} duration_seconds={duration_seconds:.6f}"
    if tree_fingerprint:
        line += f" tree_fingerprint={tree_fingerprint}"
    return line


def _warn(message: str) -> None:
    print(f"verify_phase: {message}", file=sys.stderr, flush=True)


def _launch_failure_exit_code(exc: OSError) -> int:
    return 127 if isinstance(exc, FileNotFoundError) else 126


def _current_tree_fingerprint() -> str | None:
    try:
        fingerprint = _compute_tree_fingerprint(Git(Path.cwd()))
    except (FileNotFoundError, NotADirectoryError, GitError) as exc:
        _warn(f"skipping tree fingerprint because git metadata is unavailable: {exc}")
        return None
    except Exception as exc:
        _warn(f"unexpected tree fingerprint failure: {exc!r}")
        return None
    if fingerprint is None:
        return None
    if _TREE_FINGERPRINT_RE.fullmatch(fingerprint):
        return fingerprint
    _warn(f"skipping invalid tree fingerprint {fingerprint!r}; expected 64 lowercase hex characters")
    return None


def _run_command(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, check=False)


def _emit_phase_result(name: str, status: str, start: float) -> None:
    duration_seconds = time.monotonic() - start
    print(_result_line(status, name, duration_seconds, _current_tree_fingerprint()), flush=True)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) < 3 or "--" not in args:
        return _usage()

    separator_index = args.index("--")
    if separator_index != 1 or not args[0] or separator_index == len(args) - 1:
        return _usage()

    phase_name = args[0]
    if not _PHASE_NAME_RE.fullmatch(phase_name):
        _warn(f"invalid phase name {phase_name!r}; expected [A-Za-z0-9_.-]+")
        return _usage()
    command = args[separator_index + 1 :]

    print(f"gza-verify phase=start name={phase_name}", flush=True)
    start = time.monotonic()
    try:
        completed = _run_command(command)
    except OSError as exc:
        _warn(f"failed to launch command {command!r}: {exc}")
        _emit_phase_result(phase_name, "failed", start)
        return _launch_failure_exit_code(exc)

    status = "passed" if completed.returncode == 0 else "failed"
    _emit_phase_result(phase_name, status, start)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
