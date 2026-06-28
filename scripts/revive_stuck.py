#!/usr/bin/env python3
"""Overnight unattended reviver for parked gza tasks.

YOU launch this (it runs ``gza advance``) — the "automated tool the user runs"
pattern, like ``gza watch -y``. It dispatches on the ``next_action`` field that
``gza incomplete --json`` already computes, and acts ONLY on mechanical steps:

  * lifecycle / recovery steps that ``advance`` owns (``resume`` / ``create_review``
    / ``improve`` / ``create_improve`` / ``retry`` / ``reconcile_branch_divergence``)
    → ``gza advance <id> -y`` (one step; watch finishes it)
  * everything else → leave parked

Judgment parks are deliberately NOT touched here: the system itself flags them as
needing judgment (``needs_discussion`` no-op-improve / review-max-cycles /
circuit-breaker, ``skip`` retry-limit-reached / GIT_ERROR, ``needs_rebase``,
``awaiting_human``), and a deterministic force past the guardrail is a blind
override, not a decision. Those are the job of the separate budget-capped
LLM-judge gate (specs/features/stuck-unit-resolution-judge.md). Moot units are
left to auto-resolve to ``redundant`` — never dropped here.

Usage:
    scripts/revive_stuck.py                       # 15-min cycles, 1 task/cycle
    scripts/revive_stuck.py --batch 4 --interval 600
    scripts/revive_stuck.py --once --dry-run      # show decisions, run nothing
    scripts/revive_stuck.py --tag system          # only revive tasks tagged `system`
    scripts/revive_stuck.py --tag a --tag b       # match-ANY (repeatable or comma-separated)

Tag filtering is a short-term client-side narrowing of ``gza incomplete --json`` on the
lineage-owner tags; it will be replaced when ``gza incomplete`` accepts ``--tag`` natively
(gza-6466), matching the ``watch`` / ``queue`` argument convention.
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


def _parse_tags(raw_tags: list[str] | None) -> set[str] | None:
    """Normalize repeatable ``--tag`` values (comma-separated allowed) to a set.

    Returns None when no tags were given. Short-term client-side filter until
    ``gza incomplete`` learns ``--tag`` natively (tracked by gza-6466).
    """
    if not raw_tags:
        return None
    tags = {
        part.strip().lower()
        for raw in raw_tags
        for part in str(raw).split(",")
        if part.strip()
    }
    return tags or None


def _classify(row: dict) -> tuple[list[str] | None, str]:
    """Return (gza CLI args or None, a short decision label for the log)."""
    task_id = row.get("id")
    if not task_id:
        return None, "no id"
    action = row.get("next_action") or ""

    # Mechanical lifecycle / recovery steps that `advance` owns: one transition,
    # then watch carries it. `retry` re-arms an infra death (e.g. WORKER_DIED);
    # `reconcile_branch_divergence` realigns diverged local/origin refs.
    if action in (
        "resume",
        "create_review",
        "improve",
        "create_improve",
        "retry",
        "reconcile_branch_divergence",
    ):
        return ["advance", task_id, "-y"], f"advance ({action})"

    # Everything else is left parked. The judgment buckets in particular
    # (needs_discussion no-op-improve / review-max-cycles / circuit-breaker,
    # skip retry-limit-reached / GIT_ERROR, needs_rebase, awaiting_human, moot)
    # are deliberately NOT force-advanced here — the system flags them as needing
    # judgment, which is the LLM-judge gate's job (see
    # specs/features/stuck-unit-resolution-judge.md). Moot is left to auto-resolve
    # to `redundant`, never dropped here.
    return None, f"leave ({action or 'unknown'})"


def _log(log_path: Path, message: str) -> None:
    line = f"{datetime.now():%F %T} {message}"
    print(line, flush=True)
    with log_path.open("a") as fh:
        fh.write(line + "\n")


def _row_matches_tags(row: dict, tag_filters: set[str] | None) -> bool:
    """Match-ANY (OR) on the lineage-owner tags; True when no filter is set."""
    if tag_filters is None:
        return True
    owner_tags = {str(t).strip().lower() for t in (row.get("tags") or [])}
    return not owner_tags.isdisjoint(tag_filters)


def _classified_rows(
    project: Path, tag_filters: set[str] | None
) -> tuple[list[tuple[str, list[str] | None, str, str]], int]:
    """Return (classified live rows, total live row count before tag filtering).

    The total is every non-dropped incomplete row; the returned list is narrowed
    to rows matching ``tag_filters`` (match-ANY) when one is given.
    """
    rc, out, _err = _gza(["incomplete", "--json", "--last", "0"], project)
    if rc != 0:
        return [], 0
    try:
        rows = json.loads(out)
    except json.JSONDecodeError:
        return [], 0
    result: list[tuple[str, list[str] | None, str, str]] = []
    total = 0
    for row in rows:
        if (row.get("status") or "") == "dropped":
            continue
        total += 1
        if not _row_matches_tags(row, tag_filters):
            continue
        cmd, label = _classify(row)
        result.append((row.get("id") or "?", cmd, label, row.get("next_action_reason") or ""))
    return result, total


def _run_pass(
    project: Path,
    log_path: Path,
    attempted: set[str],
    dry_run: bool,
    batch: int,
    tag_filters: set[str] | None,
) -> bool:
    """One cycle: log every row's decision, then dispatch up to ``batch`` fresh ones."""
    rows, total = _classified_rows(project, tag_filters)
    if tag_filters:
        _log(log_path, f"found {total} incomplete task(s); {len(rows)} match tag {sorted(tag_filters)}")
    else:
        _log(log_path, f"found {total} incomplete task(s)")

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
    parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        metavar="TAG",
        help="Only revive tasks whose lineage-owner tags match (repeatable; comma-separated "
             "values allowed). Match-ANY. Short-term client-side filter until `gza incomplete "
             "--tag` lands (gza-6466).",
    )
    args = parser.parse_args()

    project: Path = args.project.resolve()
    tag_filters = _parse_tags(args.tags)
    _log(args.log, f"── revive-stuck started (interval {args.interval}s, batch {args.batch}, "
                   f"project {project}, dry_run={args.dry_run}"
                   + (f", tags={sorted(tag_filters)}" if tag_filters else "") + ")")

    attempted: set[str] = set()
    try:
        while True:
            acted = _run_pass(project, args.log, attempted, args.dry_run, args.batch, tag_filters)
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
