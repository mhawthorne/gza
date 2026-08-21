#!/usr/bin/env python3
"""Report pipeline effectiveness metrics per merge unit.

For each merged merge unit in a time window, computes the cost of landing it:
total agent runtime, runtime percentiles across its tasks, token totals and
percentiles, USD cost, and the wall-clock calendar time from the first task
starting to the unit being merged.

Usage:
    python scripts/merge_unit_metrics.py --days 14
    python scripts/merge_unit_metrics.py --since 2026-08-01 --json
    python scripts/merge_unit_metrics.py --unit mu-123 --detail
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_DB = Path.home() / "work" / "supreme" / "gza" / ".gza" / "gza.db"


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
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def load_rows(db: Path, project_id: str | None, since: str | None, unit_ids: list[str]) -> list[dict]:
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
    if unit_ids:
        where.append("mu.id IN (%s)" % ",".join("?" * len(unit_ids)))
        params.extend(unit_ids)
    rows = conn.execute(
        f"""
        SELECT mu.id            AS unit_id,
               mu.project_id    AS project_id,
               mu.source_branch AS source_branch,
               mu.target_branch AS target_branch,
               mu.owner_task_id AS owner_task_id,
               mu.merged_at     AS merged_at,
               mu.merge_source  AS merge_source,
               mu.diff_lines_added   AS lines_added,
               mu.diff_lines_removed AS lines_removed,
               t.id             AS task_id,
               t.task_type      AS task_type,
               t.status         AS task_status,
               t.started_at     AS started_at,
               t.completed_at   AS completed_at,
               t.duration_seconds AS duration_seconds,
               t.input_tokens   AS input_tokens,
               t.output_tokens  AS output_tokens,
               t.cost_usd       AS cost_usd
        FROM merge_units mu
        JOIN merge_unit_tasks mut
          ON mut.project_id = mu.project_id AND mut.merge_unit_id = mu.id
        JOIN tasks t
          ON t.project_id = mut.project_id AND t.id = mut.task_id
        WHERE {" AND ".join(where)}
        ORDER BY mu.merged_at, mu.id, t.created_at
        """,
        params,
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def summarize_unit(unit_id: str, rows: list[dict]) -> dict:
    durations = [r["duration_seconds"] for r in rows if r["duration_seconds"] is not None]
    tokens = [
        (r["input_tokens"] or 0) + (r["output_tokens"] or 0)
        for r in rows
        if r["input_tokens"] is not None or r["output_tokens"] is not None
    ]
    starts = [ts for r in rows if (ts := parse_ts(r["started_at"]))]
    merged = parse_ts(rows[0]["merged_at"])
    calendar = (merged - min(starts)).total_seconds() if merged and starts else None
    by_type: dict[str, float] = defaultdict(float)
    for r in rows:
        by_type[r["task_type"] or "unknown"] += r["duration_seconds"] or 0.0
    return {
        "unit_id": unit_id,
        "owner_task_id": rows[0]["owner_task_id"],
        "source_branch": rows[0]["source_branch"],
        "target_branch": rows[0]["target_branch"],
        "merged_at": rows[0]["merged_at"],
        "merge_source": rows[0]["merge_source"],
        "lines_changed": (rows[0]["lines_added"] or 0) + (rows[0]["lines_removed"] or 0),
        "task_count": len(rows),
        "task_types": dict(sorted(by_type.items(), key=lambda kv: -kv[1])),
        "runtime_total_s": sum(durations),
        "runtime_median_s": percentile(durations, 0.5),
        "runtime_p90_s": percentile(durations, 0.9),
        "runtime_max_s": max(durations) if durations else None,
        "tokens_total": sum(tokens),
        "tokens_median": percentile([float(v) for v in tokens], 0.5),
        "tokens_p90": percentile([float(v) for v in tokens], 0.9),
        "cost_usd": sum(r["cost_usd"] or 0.0 for r in rows),
        "calendar_s": calendar,
        "efficiency": (sum(durations) / calendar) if calendar else None,
    }


def fmt_dur(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400 * 2:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def fmt_tokens(value: float | None) -> str:
    if value is None:
        return "-"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f}k"
    return f"{value:.0f}"


def print_table(units: list[dict], detail: bool) -> None:
    """Render the per-unit table, sizing every column to its widest cell.

    Unit ids carry a per-project prefix (``gza-mu-1`` vs ``gzaserver-mu-12``), so
    a fixed width either truncates or wastes space once more than one project is
    in the window.
    """
    columns = [
        ("unit", "<", lambda u: u["unit_id"]),
        ("tasks", ">", lambda u: str(u["task_count"])),
        ("run", ">", lambda u: fmt_dur(u["runtime_total_s"])),
        ("med", ">", lambda u: fmt_dur(u["runtime_median_s"])),
        ("p90", ">", lambda u: fmt_dur(u["runtime_p90_s"])),
        ("tokens", ">", lambda u: fmt_tokens(u["tokens_total"])),
        ("tok p90", ">", lambda u: fmt_tokens(u["tokens_p90"])),
        ("cost", ">", lambda u: f"${u['cost_usd']:,.2f}"),
        ("calendar", ">", lambda u: fmt_dur(u["calendar_s"])),
        ("eff", ">", lambda u: f"{u['efficiency'] * 100:.0f}%" if u["efficiency"] else "-"),
    ]
    table = [[cell(u) for _, _, cell in columns] for u in units]
    widths = [
        max(len(title), *(len(row[i]) for row in table)) if table else len(title)
        for i, (title, _, _) in enumerate(columns)
    ]

    def render(cells: list[str]) -> str:
        return " ".join(
            f"{cell:{align}{width}}"
            for cell, (_, align, _), width in zip(cells, columns, widths)
        ).rstrip()

    header = render([title for title, _, _ in columns])
    print(header)
    print("-" * len(header))
    indent = " " * (widths[0] + 1)
    for u, cells in zip(units, table):
        print(render(cells))
        if detail:
            types = ", ".join(f"{k}={fmt_dur(v)}" for k, v in u["task_types"].items())
            print(f"{indent}{u['source_branch']} -> {u['target_branch']}  [{types}]")


def print_fleet(units: list[dict]) -> None:
    if not units:
        print("no merged merge units in window")
        return
    tasks = [float(u["task_count"]) for u in units]
    runtimes = [u["runtime_total_s"] for u in units]
    calendars = [u["calendar_s"] for u in units if u["calendar_s"] is not None]
    tokens = [float(u["tokens_total"]) for u in units]
    print()
    print(f"merge units merged: {len(units)}")
    print(f"total agent runtime: {fmt_dur(sum(runtimes))}   total tokens: {fmt_tokens(sum(tokens))}   total cost: ${sum(u['cost_usd'] for u in units):,.2f}")
    for label, series, fmt in (
        ("tasks per unit", tasks, lambda v: f"{v:.1f}"),
        ("runtime per unit", runtimes, fmt_dur),
        ("tokens per unit", tokens, fmt_tokens),
        ("calendar per unit", calendars, fmt_dur),
    ):
        print(
            f"  {label:<18} median {fmt(percentile(series, 0.5)):>8}"
            f"  p90 {fmt(percentile(series, 0.9)):>8}"
            f"  p99 {fmt(percentile(series, 0.99)):>8}"
            f"  max {fmt(max(series)) if series else '-':>8}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", type=Path, default=Path(os.environ.get("GZA_DB", DEFAULT_DB)))
    ap.add_argument("--project-id", default=None, help="restrict to one project id")
    ap.add_argument("--days", type=float, default=14.0, help="window of merged units (default 14)")
    ap.add_argument("--since", default=None, help="ISO timestamp; overrides --days")
    ap.add_argument("--unit", action="append", default=[], help="specific merge unit id (repeatable)")
    ap.add_argument("--sort", choices=("merged", "runtime", "calendar", "tokens", "tasks"), default="merged")
    ap.add_argument("--limit", type=int, default=0, help="show only the top N rows after sorting")
    ap.add_argument("--detail", action="store_true", help="show branches and per-task-type runtime")
    ap.add_argument("--json", action="store_true", dest="as_json")
    args = ap.parse_args()

    if not args.db.exists():
        print(f"db not found: {args.db}")
        return 1
    since = args.since
    if since is None and not args.unit:
        since = (datetime.now(timezone.utc) - timedelta(days=args.days)).strftime("%Y-%m-%d %H:%M:%S")

    rows = load_rows(args.db, args.project_id, since, args.unit)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["unit_id"]].append(row)
    units = [summarize_unit(uid, urows) for uid, urows in grouped.items()]

    keys = {
        "merged": lambda u: u["merged_at"] or "",
        "runtime": lambda u: -(u["runtime_total_s"] or 0),
        "calendar": lambda u: -(u["calendar_s"] or 0),
        "tokens": lambda u: -(u["tokens_total"] or 0),
        "tasks": lambda u: -u["task_count"],
    }
    units.sort(key=keys[args.sort])
    if args.limit:
        units = units[: args.limit]

    if args.as_json:
        print(json.dumps(units, indent=2))
    else:
        print_table(units, args.detail)
        print_fleet(units)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
