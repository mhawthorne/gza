#!/usr/bin/env python3
"""Report how much rework each merged merge unit needed before it landed.

Where merge_unit_metrics.py answers "what did landing cost" (runtime, tokens,
USD), this answers "how much friction did it hit" -- review iteration counts,
how often improve/verify_fix/fix/rebase were needed, and active vs elapsed
time to land (the gap between them is queue/idle time).

Intended as a baseline snapshot: run it, change something, run it again and
compare. Filter by tag to scope to one release.

Usage:
    scripts/pipeline_friction.py --tag v0.5.1
    scripts/pipeline_friction.py --tag v0.5.1 --json
    scripts/pipeline_friction.py --since 2026-08-01 --verbose
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_DB = Path.home() / "work" / "supreme" / "gza" / ".gza" / "gza.db"

# Task types counted as rework signals, in report order.
REWORK_TYPES = ("review", "improve", "verify_fix", "fix", "rebase")


def percentile(values: list[float], pct: float) -> float | None:
    """Linear-interpolated percentile; returns None for an empty series."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * pct
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def load_units(
    db: Path, tag: str | None, since: str | None, project_id: str | None
) -> list[dict]:
    """Return one row per merged merge unit, with its attached tasks."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    where = ["mu.state = 'merged'"]
    params: list[object] = []
    if project_id:
        where.append("mu.project_id = ?")
        params.append(project_id)
    if since:
        where.append("COALESCE(mu.merged_at, mu.updated_at) >= ?")
        params.append(since)
    if tag:
        # A unit is in scope when ANY attached task carries the tag; tags are
        # normally set on the implement task, not on spawned reviews/improves.
        where.append(
            """mu.id IN (
                SELECT mut2.merge_unit_id
                  FROM merge_unit_tasks mut2
                  JOIN task_tags tt
                    ON tt.task_id = mut2.task_id
                   AND tt.project_id = mut2.project_id
                 WHERE tt.tag = ?
            )"""
        )
        params.append(tag)

    rows = conn.execute(
        f"""
        SELECT mu.id             AS unit_id,
               mu.source_branch  AS source_branch,
               mu.merged_at      AS merged_at,
               mu.merge_source   AS merge_source,
               mu.owner_task_id  AS owner_task_id,
               mu.diff_lines_added   AS lines_added,
               mu.diff_lines_removed AS lines_removed,
               t.id              AS task_id,
               t.task_type       AS task_type,
               t.status          AS task_status,
               t.created_at      AS created_at,
               t.started_at      AS started_at,
               t.duration_seconds AS duration_seconds
          FROM merge_units mu
          JOIN merge_unit_tasks mut
            ON mut.merge_unit_id = mu.id
           AND mut.project_id = mu.project_id
          JOIN tasks t
            ON t.id = mut.task_id
           AND t.project_id = mut.project_id
         WHERE {' AND '.join(where)}
        """,
        params,
    ).fetchall()
    conn.close()

    units: dict[str, dict] = {}
    for row in rows:
        unit = units.setdefault(
            row["unit_id"],
            {
                "unit_id": row["unit_id"],
                "source_branch": row["source_branch"],
                "merged_at": row["merged_at"],
                "merge_source": row["merge_source"],
                "owner_task_id": row["owner_task_id"],
                "lines_added": row["lines_added"] or 0,
                "lines_removed": row["lines_removed"] or 0,
                "counts": Counter(),
                "first_start": None,
                "active_seconds": 0.0,
            },
        )
        unit["counts"][row["task_type"]] += 1
        # Elapsed is anchored on real starts only; a task that never started
        # contributes no clock, so queue time before first execution is excluded.
        started = parse_ts(row["started_at"])
        if started and (unit["first_start"] is None or started < unit["first_start"]):
            unit["first_start"] = started
        if row["duration_seconds"]:
            unit["active_seconds"] += float(row["duration_seconds"])

    for unit in units.values():
        merged = parse_ts(unit["merged_at"])
        unit["hours_active"] = unit["active_seconds"] / 3600
        if merged and unit["first_start"]:
            unit["hours_elapsed"] = (merged - unit["first_start"]).total_seconds() / 3600
        else:
            unit["hours_elapsed"] = None
        if unit["hours_elapsed"] and unit["hours_elapsed"] > 0:
            # Share of calendar time with no agent running on this unit.
            idle = max(0.0, unit["hours_elapsed"] - unit["hours_active"])
            unit["pct_idle"] = 100.0 * idle / unit["hours_elapsed"]
        else:
            unit["pct_idle"] = None

    return sorted(units.values(), key=lambda u: u["merged_at"] or "")


def summarize(units: list[dict]) -> dict:
    total = len(units)
    if not total:
        return {"total_units": 0}

    reviews = [u["counts"]["review"] for u in units]
    improves = [u["counts"]["improve"] for u in units]
    elapsed = [u["hours_elapsed"] for u in units if u["hours_elapsed"] is not None]
    active = [u["hours_active"] for u in units]
    idle_pcts = [u["pct_idle"] for u in units if u["pct_idle"] is not None]

    summary: dict = {
        "total_units": total,
        "review_counts": {
            "mean": sum(reviews) / total,
            "median": percentile([float(r) for r in reviews], 0.5),
            "p90": percentile([float(r) for r in reviews], 0.9),
            "max": max(reviews),
            "distribution": dict(sorted(Counter(reviews).items())),
        },
        "improve_counts": {
            "mean": sum(improves) / total,
            "median": percentile([float(i) for i in improves], 0.5),
            "max": max(improves),
        },
        "pct_needing": {
            task_type: 100.0
            * sum(1 for u in units if u["counts"][task_type] > 0)
            / total
            for task_type in REWORK_TYPES
        },
        "pct_clean_first_pass": 100.0
        * sum(1 for u in units if u["counts"]["review"] <= 1 and not u["counts"]["improve"])
        / total,
        "hours_active": {
            "median": percentile(active, 0.5),
            "p90": percentile(active, 0.9),
            "max": max(active) if active else None,
            "total": sum(active),
        },
        "hours_elapsed": {
            "median": percentile(elapsed, 0.5),
            "p90": percentile(elapsed, 0.9),
            "max": max(elapsed) if elapsed else None,
            "total": sum(elapsed),
        },
        "pct_idle": {
            "median": percentile(idle_pcts, 0.5),
            "p90": percentile(idle_pcts, 0.9),
        },
        "units_missing_elapsed": total - len(elapsed),
        "merge_source": dict(Counter(u["merge_source"] or "(blank)" for u in units)),
    }
    return summary


def fmt(value: float | None, suffix: str = "") -> str:
    return "n/a" if value is None else f"{value:.1f}{suffix}"


def print_report(units: list[dict], summary: dict, tag: str | None, verbose: bool) -> None:
    scope = f"tag {tag}" if tag else "all tags"
    total = summary.get("total_units", 0)
    print(f"Merged merge units ({scope}): {total}")
    if not total:
        return

    rc = summary["review_counts"]
    print("\nReview iterations before merge")
    print(f"  mean {rc['mean']:.2f}   median {fmt(rc['median'])}   p90 {fmt(rc['p90'])}   max {rc['max']}")
    print("  distribution (reviews -> units):")
    for count, n in rc["distribution"].items():
        bar = "#" * min(n, 60)
        print(f"    {count:>2}: {n:>4}  {bar}")

    ic = summary["improve_counts"]
    print(f"\nImprove tasks per unit: mean {ic['mean']:.2f}   median {fmt(ic['median'])}   max {ic['max']}")

    print("\nUnits requiring at least one:")
    for task_type in REWORK_TYPES:
        print(f"  {task_type:<12} {summary['pct_needing'][task_type]:>5.1f}%")
    print(f"\nClean first pass (<=1 review, no improve): {summary['pct_clean_first_pass']:.1f}%")

    ha, he, pi = summary["hours_active"], summary["hours_elapsed"], summary["pct_idle"]
    print("\nHours per unit (active = summed task runtime, elapsed = first start -> merged)")
    print(f"  active   median {fmt(ha['median'])}   p90 {fmt(ha['p90'])}   max {fmt(ha['max'])}   total {fmt(ha['total'])}")
    print(f"  elapsed  median {fmt(he['median'])}   p90 {fmt(he['p90'])}   max {fmt(he['max'])}   total {fmt(he['total'])}")
    print(f"  idle     median {fmt(pi['median'], '%')}   p90 {fmt(pi['p90'], '%')}  (share of elapsed with nothing running)")
    if summary["units_missing_elapsed"]:
        print(f"  note: {summary['units_missing_elapsed']} unit(s) had no started task; excluded from elapsed")

    print(f"\nMerge source: {summary['merge_source']}")

    if verbose:
        print("\nPer-unit detail (worst review counts first):")
        print(f"  {'unit':<14} {'rev':>3} {'imp':>3} {'vfix':>4} {'fix':>3} {'reb':>3} {'act':>6} {'elap':>7} {'idle':>6}  branch")
        for unit in sorted(units, key=lambda u: -u["counts"]["review"]):
            c = unit["counts"]
            print(
                f"  {unit['unit_id']:<14} {c['review']:>3} {c['improve']:>3} "
                f"{c['verify_fix']:>4} {c['fix']:>3} {c['rebase']:>3} "
                f"{fmt(unit['hours_active']):>6} {fmt(unit['hours_elapsed']):>7} "
                f"{fmt(unit['pct_idle'], '%'):>6}  {unit['source_branch']}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tag", help="Only units with this tag on an attached task")
    parser.add_argument("--since", help="Only units merged on/after this ISO date")
    parser.add_argument("--project-id", help="Restrict to one project id")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help=f"DB path (default: {DEFAULT_DB})")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a text report")
    parser.add_argument("--verbose", "-v", action="store_true", help="Include per-unit detail rows")
    args = parser.parse_args()

    if not args.db.exists():
        parser.error(f"database not found: {args.db}")

    units = load_units(args.db, args.tag, args.since, args.project_id)
    summary = summarize(units)

    if args.json:
        print(json.dumps({"summary": summary, "units": [
            {k: v for k, v in u.items() if k != "counts"} | {"counts": dict(u["counts"])}
            for u in units
        ]}, indent=2, default=str))
    else:
        print_report(units, summary, args.tag, args.verbose)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
