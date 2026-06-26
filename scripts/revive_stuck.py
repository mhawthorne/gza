#!/usr/bin/env python3
"""Overnight unattended reviver for parked gza tasks.

YOU launch this (it runs ``gza iterate``) — the "automated tool the user runs"
pattern, like ``gza watch -y``. It does NOT guess ``--resume`` vs ``--retry`` vs
neither: it dispatches on the ``next_action`` field that ``gza incomplete --json``
already computes, and SKIPS anything the system has parked for manual
intervention (``next_action`` = skip / needs_rebase / needs_discussion /
awaiting_human). ``merge`` is also skipped on purpose — promoting an approved
unit is watch's / your gate, not a blind loop's.

Reality check: most "needs attention" rows are ``next_action=skip`` — the system
already decided automatic recovery won't help (3-cycle no-progress backstop,
retry-limit reached, GIT_ERROR awaiting the worktree-isolation fix). This will
NOT force those; reviving them needs a content-changing rebase or the systemic
fixes to land, not an iterate flag. It only touches genuinely-actionable rows.

Why it shells out to ``gza`` instead of importing the query internals: the
``next_action`` decision is computed by the CLI with config+git wiring (merge /
rebase readiness depends on it), and the action itself is a ``gza iterate``
subprocess regardless — so this stays a thin, robust CLI orchestrator.

Usage:
    scripts/revive_stuck.py                  # 15-min cycles, one actionable task each
    scripts/revive_stuck.py --interval 600   # 10-min cycles
    scripts/revive_stuck.py --once           # a single pass, then exit
    scripts/revive_stuck.py --dry-run        # show picks, run nothing
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _classify(row: dict) -> list[str] | None:
    """Return the gza CLI args to revive this row, or None to leave it parked.

    Dispatch is driven by ``next_action`` plus a parse of ``next_action_reason``:
    the review/improve no-progress backstop shows up as ``next_action=skip`` and
    needs ``gza fix`` (iterate is a no-op there); lifecycle steps use ``iterate``.
    """
    task_id = row.get("id")
    if not task_id:
        return None
    action = row.get("next_action") or ""
    reason = (row.get("next_action_reason") or "").lower()

    # Review/improve loop parked by the 3-cycle no-progress backstop → `fix`.
    if action == "skip" and ("durable progress" in reason or "no-op improve" in reason):
        return ["fix", "-b", task_id]
    # Lifecycle steps iterate can drive.
    if action == "resume":
        return ["iterate", "-b", "--resume", task_id]
    if action in ("create_review", "improve", "create_improve"):
        return ["iterate", "-b", task_id]
    # GIT_ERROR / needs_rebase / retry-limit / supersedes / awaiting_human / merge → leave it.
    return None


def _log(log_path: Path, message: str) -> None:
    line = f"{datetime.now():%F %T} {message}"
    print(line, flush=True)
    with log_path.open("a") as fh:
        fh.write(line + "\n")


def _gza(args: list[str], project: Path) -> tuple[int, str]:
    """Run ``uv run gza <args>`` from the project root; return (rc, combined output)."""
    proc = subprocess.run(
        ["uv", "run", "gza", *args],
        cwd=project,
        capture_output=True,
        text=True,
    )
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _actionable_rows(project: Path) -> list[tuple[str, list[str]]]:
    """Return [(task_id, gza_cli_args), ...] for rows this reviver can act on."""
    rc, out = _gza(["incomplete", "--json", "--last", "0"], project)
    if rc != 0:
        return []
    try:
        rows = json.loads(out)
    except json.JSONDecodeError:
        return []
    result: list[tuple[str, list[str]]] = []
    for row in rows:
        if (row.get("status") or "") == "dropped":
            continue
        cmd = _classify(row)
        if cmd is not None:
            result.append((row["id"], cmd))
    return result


def _run_pass(project: Path, log_path: Path, attempted: set[str], dry_run: bool) -> bool:
    """One cycle: pick a random fresh actionable task and iterate it.

    Returns True if it acted (or would have, under --dry-run), False if nothing
    fresh remained (caller resets the attempted set for another pass).
    """
    candidates = [r for r in _actionable_rows(project) if r[0] not in attempted]
    if not candidates:
        return False

    task_id, cmd = random.choice(candidates)
    attempted.add(task_id)

    if dry_run:
        _log(log_path, f"[dry-run] would run: gza {' '.join(cmd)}")
        return True

    _log(log_path, f"gza {' '.join(cmd)}")
    rc, out = _gza(cmd, project)
    _log(log_path, f"  -> exit {rc}")
    tail = out.strip().splitlines()[-1:] if out.strip() else []
    for line in tail:
        _log(log_path, f"  {line}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=int, default=900, help="Seconds between cycles (default 900 = 15m)")
    parser.add_argument("--once", action="store_true", help="Run a single pass, then exit")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run; execute nothing")
    parser.add_argument("--project", type=Path, default=REPO_ROOT, help="Project dir to run gza from (default: repo root)")
    parser.add_argument("--log", type=Path, default=Path.home() / "revive-stuck.log", help="Log file path")
    args = parser.parse_args()

    project: Path = args.project.resolve()
    _log(args.log, f"── revive-stuck started (interval {args.interval}s, project {project}, dry_run={args.dry_run})")

    attempted: set[str] = set()
    try:
        while True:
            acted = _run_pass(project, args.log, attempted, args.dry_run)
            if not acted:
                _log(args.log, "no fresh actionable task (rest are parked: skip/needs_rebase/awaiting_human); resetting pass")
                attempted.clear()
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        _log(args.log, "stopped (interrupt)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
