"""Shared CLI test helpers."""

import io
import os
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from gza.cli import main as cli_main


class _PatchedStdin(io.StringIO):
    def __init__(self, value: str, *, isatty: bool) -> None:
        super().__init__(value)
        self._isatty = isatty

    def isatty(self) -> bool:
        return self._isatty


def run_gza(
    *args: str,
    cwd: Path | None = None,
    stdin_input: str | None = None,
    stdin_isatty: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Run gza CLI in-process and capture stdout/stderr like subprocess.run."""
    stdout = io.StringIO()
    stderr = io.StringIO()
    old_cwd = Path.cwd()
    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    try:
        if cwd is not None:
            os.chdir(cwd)
        stdin = _PatchedStdin(stdin_input or "", isatty=stdin_isatty)
        with (
            patch.dict(os.environ, run_env, clear=True),
            patch.object(sys, "argv", ["gza", *args]),
            patch("sys.stdin", stdin),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            try:
                returncode = cli_main()
            except SystemExit as exc:
                code = exc.code
                returncode = code if isinstance(code, int) else 1
    finally:
        os.chdir(old_cwd)

    return subprocess.CompletedProcess(
        args=["uv", "run", "gza", *args],
        returncode=returncode,
        stdout=stdout.getvalue(),
        stderr=stderr.getvalue(),
    )


def capture_background_worker_spawns() -> tuple[list[dict[str, object]], object]:
    """Return a recorder and fake background-worker spawn function."""
    calls: list[dict[str, object]] = []

    def fake_spawn(worker_args, _config, task_id, **kwargs):
        calls.append(
            {
                "task_id": task_id,
                "worker_args": worker_args,
                "kwargs": kwargs,
            }
        )
        return 0

    return calls, fake_spawn
