#!/usr/bin/env python3
"""Overnight unattended reviver for parked gza tasks.

YOU launch this (it runs ``gza fix`` / ``gza iterate``) — the "automated tool the
user runs" pattern, like ``gza watch -y``. It dispatches on the ``next_action``
field that ``gza incomplete --json`` already computes:

  * review/improve no-progress backstop or retry-limit (``next_action=skip`` with
    the matching reason) → ``gza fix -b`` (a fresh attempt; iterate is a no-op there)
  * lifecycle steps (``resume`` / ``create_review`` / ``improve``) → ``gza iterate -b``
  * everything else (GIT_ERROR / needs_rebase / awaiting_human / merge) → leave parked

Don't-fix-twice-in-a-row guard: a ``fix`` only advances a unit when it makes a
real code change. If the unit's most recent task is *already* a fix, re-fixing
just burns cost — so it is skipped. The check is read straight from gza state
(the newest ``member_ids`` task's type), so there is no external state file. This
mirrors what ``watch`` itself would enforce when this logic moves in-tree.

Usage:
    scripts/revive_stuck.py                       # 15-min cycles, 1 task/cycle
    scripts/revive_stuck.py --batch 4 --interval 600
    scripts/revive_stuck.py --once --dry-run      # show decisions, run nothing
"""

from __future__ import annotations

import argparse
import json
import random
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _gza(args: list[str], project: Path) -> tuple[int, str, str]:
    """Run ``uv run gza <args>`` from the project root; return (rc, stdout, stderr).

    stdout and stderr are kept separate so JSON parsing isn't corrupted by a
    stderr warning (e.g. a worktree-metadata warning).
    """
    proc = subprocess.run(
        ["uv", "run", "gza", *args],
        cwd=project,
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout or "", proc.stderr or ""


def _seq(task_id: str) -> int:
    """Numeric suffix of a gza-NNNN id (newest task in a unit = highest)."""
    tail = task_id.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else -1


def _latest_task_is_fix(row: dict, project: Path) -> bool:
    """True if the unit's most recent task (highest member id) is a ``fix``."""
    members = row.get("member_ids") or []
    if not members:
        return False
    latest = max(members, key=_seq)
    _rc, out, err = _gza(["show", latest], project)
    for raw in (out + err).splitlines():
        line = _ANSI.sub("", raw).strip()
        if line.lower().startswith("type:"):
            return line.split(":", 1)[1].strip().lower() == "fix"
    return False


def _classify(row: dict, project: Path) -> tuple[list[str] | None, str]:
    """Return (gza CLI args or None, a short decision label for the log)."""
    task_id = row.get("id")
    if not task_id:
        return None, "no id"
    action = row.get("next_action") or ""
    reason = (row.get("next_action_reason") or "").lower()

    # Parked review/improve loop (3-cycle backstop) or retry-cap → `fix`.
    if action == "skip" and (
        "durable progress" in reason
        or "no-op improve" in reason
        or "retry limit reached" in reason
    ):
        if _latest_task_is_fix(row, project):
            return None, "leave (latest task is already a fix — won't fix twice in a row)"
        return ["fix", "-b", task_id], "fix"
    # Lifecycle steps iterate can drive.
    if action == "resume":
        return ["iterate", "-b", "--resume", task_id], "iterate --resume"
    if action in ("create_review", "improve", "create_improve"):
        return ["iterate", "-b", task_id], f"iterate ({action})"
    # GIT_ERROR / needs_rebase / supersedes / awaiting_human / merge → leave it.
    return None, f"leave ({action or 'unknown'})"


def _log(log_path: Path, message: str) -> None:
    line = f"{datetime.now():%F %T} {message}"
    print(line, flush=True)
    with log_path.open("a") as fh:
        fh.write(line + "\n")


def _classified_rows(project: Path) -> list[tuple[str, list[str] | None, str, str]]:
    """Return [(task_id, cmd_or_None, label, reason), ...] for every live row."""
    rc, out, _err = _gza(["incomplete", "--json", "--last", "0"], project)
    if rc != 0:
        return []
    try:
        rows = json.loads(out)
    except json.JSONDecodeError:
        return []
    result: list[tuple[str, list[str] | None, str, str]] = []
    for row in rows:
        if (row.get("status") or "") == "dropped":
            continue
        cmd, label = _classify(row, project)
        result.append((row.get("id") or "?", cmd, label, row.get("next_action_reason") or ""))
    return result


def _run_pass(project: Path, log_path: Path, attempted: set[str], dry_run: bool, batch: int) -> bool:
    """One cycle: log every row's decision, then dispatch up to ``batch`` fresh ones."""
    rows = _classified_rows(project)

    actionable: list[tuple[str, list[str], str]] = []
    for task_id, cmd, label, reason in rows:
        if cmd is None:
            _log(log_path, f"SKIP   {task_id}: {label} [{reason}]")
        elif task_id in attempted:
            _log(log_path, f"SKIP   {task_id}: already attempted this pass")
        else:
            actionable.append((task_id, cmd, label))

    if not actionable:
        return False

    for task_id, cmd, label in random.sample(actionable, min(batch, len(actionable))):
        attempted.add(task_id)
        _log(log_path, f"SELECT {task_id}: {label}  ->  gza {' '.join(cmd)}")
        if dry_run:
            _log(log_path, "       [dry-run] not executed")
            continue
        rc, out, err = _gza(cmd, project)
        _log(log_path, f"       -> exit {rc}")
        combined = (out + err).strip()
        for line in (combined.splitlines()[-1:] if combined else []):
            _log(log_path, f"       {line}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=int, default=900, help="Seconds between cycles (default 900 = 15m)")
    parser.add_argument("--batch", type=int, default=1, help="How many tasks to dispatch per cycle (default 1)")
    parser.add_argument("--once", action="store_true", help="Run a single pass, then exit")
    parser.add_argument("--dry-run", action="store_true", help="Show decisions; execute nothing")
    parser.add_argument("--project", type=Path, default=REPO_ROOT, help="Project dir to run gza from (default: repo root)")
    parser.add_argument("--log", type=Path, default=Path.home() / "revive-stuck.log", help="Log file path")
    args = parser.parse_args()

    project: Path = args.project.resolve()
    _log(args.log, f"── revive-stuck started (interval {args.interval}s, batch {args.batch}, "
                   f"project {project}, dry_run={args.dry_run})")

    attempted: set[str] = set()
    try:
        while True:
            acted = _run_pass(project, args.log, attempted, args.dry_run, args.batch)
            if not acted:
                _log(args.log, "no fresh actionable task this pass; resetting attempted set")
                attempted.clear()
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        _log(args.log, "stopped (interrupt)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
