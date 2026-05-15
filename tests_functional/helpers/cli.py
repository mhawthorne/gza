"""CLI helpers for functional tests that intentionally spawn subprocesses."""

import os
import subprocess
import sys
from pathlib import Path


def run_gza_subprocess(
    *args: str,
    cwd: Path | None = None,
    stdin_input: str | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run gza in a subprocess using the active test interpreter."""
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    for name in ("FORCE_COLOR", "TTY_COMPATIBLE", "CLICOLOR_FORCE"):
        run_env.pop(name, None)
    run_env.setdefault("NO_COLOR", "1")
    return subprocess.run(
        [sys.executable, "-m", "gza", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
        input=stdin_input,
        env=run_env,
        timeout=timeout,
    )
