#!/usr/bin/env python3
"""Analyze review/improve cycle counts per implementation task.

Shows median, p90, and max cycle counts grouped by week.
Chains of implement tasks (via based_on) are treated as one logical implementation.
Percentile stats only include tasks that had at least one review.
"""

import argparse
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median


def get_db_path() -> Path:
    return Path(".gza") / "gza.db"


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def percentile(sorted_vals: list[int], p: float) -> int:
    """Return the value at the p-th percentile (nearest-rank method)."""
    if not sorted_vals:
        return 0
    k = max(0, min(len(sorted_vals) - 1, int(len(sorted_vals) * p / 100 + 0.5) - 1))
    return sorted_vals[k]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "start",
        nargs="?",
        default=None,
        help="Start date (YYYYmmdd). Default: 14 days ago",
    )
    parser.add_argument(
        "end",
        nargs="?",
        default=None,
        help="End date (YYYYmmdd). Default: today",
    )
    args = parser.parse_args()

    today = date.today()
    end_date = parse_date(args.end) if args.end else today
    start_date = parse_date(args.start) if args.start else end_date - timedelta(days=14)

    start_iso = start_date.isoformat()
    end_iso = (end_date + timedelta(days=1)).isoformat()

    db_path = get_db_path()
    if not db_path.exists():
        print(f"Database not found at {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Load all tasks for chain resolution
    rows = conn.execute(
        "SELECT id, task_type, based_on, created_at FROM tasks"
    ).fetchall()
    tasks = {r["id"]: dict(r) for r in rows}

    def find_root_impl(task_id: int, visited: set | None = None) -> int | None:
        """Walk up based_on chain to find the root implement task."""
        if visited is None:
            visited = set()
        if task_id in visited:
            return None
        visited.add(task_id)

        task = tasks.get(task_id)
        if task is None:
            return None

        if task["based_on"]:
            parent = tasks.get(task["based_on"])
            if parent and parent["task_type"] == "implement":
                root = find_root_impl(parent["id"], visited)
                return root if root is not None else parent["id"]

        if task["task_type"] == "implement":
            return task_id
        return None

    # Find all review/improve tasks in the date range
    ri_tasks = conn.execute(
        """
        SELECT id, task_type, based_on, created_at
        FROM tasks
        WHERE task_type IN ('review', 'improve')
          AND created_at >= ? AND created_at < ?
          AND based_on IS NOT NULL
        ORDER BY id
        """,
        (start_iso, end_iso),
    ).fetchall()

    conn.close()

    # Count reviews per root impl
    root_reviews: dict[int, list[str]] = defaultdict(list)

    for ri in ri_tasks:
        root = find_root_impl(ri["based_on"])
        if root is None:
            continue
        if ri["task_type"] == "review":
            root_reviews[root].append(ri["created_at"])

    # Find all root implement tasks in range (including those continued by improve chains)
    root_impls_in_range: list[dict] = []
    seen_roots: set[int] = set()
    for t in tasks.values():
        if t["task_type"] not in ("implement", "improve"):
            continue
        if t["created_at"] is None or t["created_at"] < start_iso or t["created_at"] >= end_iso:
            continue
        root = find_root_impl(t["id"])
        if root is not None and root not in seen_roots:
            seen_roots.add(root)
            root_impls_in_range.append(tasks[root])

    total_impls = len(root_impls_in_range)
    total_reviews = sum(len(dates) for dates in root_reviews.values())
    reviewed_impls = {r for r in root_reviews if r in seen_roots}
    review_pct = (len(reviewed_impls) / total_impls * 100) if total_impls else 0

    def week_label(created_at: str) -> str:
        dt = datetime.fromisoformat(created_at)
        monday = dt.date() - timedelta(days=dt.weekday())
        sunday = monday + timedelta(days=6)
        return f"{monday.strftime('%b %d')} - {sunday.strftime('%b %d')}"

    def week_sort_key(label: str) -> date:
        parts = label.split(" - ")
        return datetime.strptime(parts[0] + f" {today.year}", "%b %d %Y").date()

    # Group by week — only reviewed impls for cycle stats
    week_data: dict[str, dict] = defaultdict(lambda: {"impls": 0, "reviews": 0, "reviewed_cycles": []})

    for impl in root_impls_in_range:
        wk = week_label(impl["created_at"])
        week_data[wk]["impls"] += 1

    for root_id, review_dates in root_reviews.items():
        if root_id not in seen_roots:
            continue
        earliest = min(review_dates)
        wk = week_label(tasks[root_id]["created_at"])
        week_data[wk]["reviews"] += len(review_dates)
        week_data[wk]["reviewed_cycles"].append(len(review_dates))

    sorted_weeks = sorted(week_data.keys(), key=week_sort_key)

    # Print summary
    print(f"\nReview cycle stats ({start_date} to {end_date})")
    print(f"  Implement tasks: {total_impls}")
    print(f"  Total reviews:   {total_reviews}")
    print(f"  Reviewed:        {len(reviewed_impls)}/{total_impls} ({review_pct:.0f}%)")

    # Print table (cycle stats only for reviewed tasks)
    print(f"\n{'Week':<22} {'Impls':>5} {'Rvws':>5} {'Rv%':>5} {'Med':>5} {'P90':>5} {'Max':>5}")
    print("-" * 56)

    all_reviewed_cycles: list[int] = []
    total_row_impls = 0
    total_row_reviews = 0

    for wk in sorted_weeks:
        d = week_data[wk]
        total_row_impls += d["impls"]
        total_row_reviews += d["reviews"]
        cycles = sorted(d["reviewed_cycles"])
        all_reviewed_cycles.extend(cycles)
        rv_pct = (len(cycles) / d["impls"] * 100) if d["impls"] else 0

        if cycles:
            med = int(median(cycles))
            p90 = percentile(cycles, 90)
            mx = max(cycles)
            print(f"{wk:<22} {d['impls']:>5} {d['reviews']:>5} {rv_pct:>4.0f}% {med:>5} {p90:>5} {mx:>5}")
        else:
            print(f"{wk:<22} {d['impls']:>5} {d['reviews']:>5} {rv_pct:>4.0f}%     -     -     -")

    if len(sorted_weeks) > 1:
        all_reviewed_cycles.sort()
        print("-" * 56)
        rv_pct = (len(all_reviewed_cycles) / total_row_impls * 100) if total_row_impls else 0
        if all_reviewed_cycles:
            med = int(median(all_reviewed_cycles))
            p90 = percentile(all_reviewed_cycles, 90)
            mx = max(all_reviewed_cycles)
            print(f"{'Total':<22} {total_row_impls:>5} {total_row_reviews:>5} {rv_pct:>4.0f}% {med:>5} {p90:>5} {mx:>5}")
        else:
            print(f"{'Total':<22} {total_row_impls:>5} {total_row_reviews:>5} {rv_pct:>4.0f}%     -     -     -")

    # Distribution (reviewed tasks only)
    if all_reviewed_cycles:
        dist = Counter(all_reviewed_cycles)
        print(f"\nCycle distribution (reviewed tasks only):")
        for count in sorted(dist.keys()):
            bar = "#" * dist[count]
            print(f"  {count} cycles: {dist[count]:>3} impls  {bar}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
