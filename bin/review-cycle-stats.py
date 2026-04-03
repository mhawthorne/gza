#!/usr/bin/env python3
"""Analyze review/improve cycle counts per implementation task.

Shows median, p90, and max cycle counts grouped by week.
Chains of implement tasks (via based_on/depends_on) are treated as one logical implementation.
Percentile stats only include tasks that had at least one review.
"""

import argparse
import re
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


def _count_section_items(content: str, header_pattern: str) -> int:
    """Count top-level list items under a section matching header_pattern.

    Looks for a line matching the pattern, then counts top-level list items:
    - Bullets at column 0: '- item'
    - Numbered items: '1. item'

    Skips blank lines, indented continuation/sub-items, and '- None.'.
    Stops at the next section header or unrecognized top-level line.
    """
    lines = content.split("\n")
    i = 0
    count = 0
    while i < len(lines):
        if re.match(header_pattern, lines[i].strip(), re.IGNORECASE):
            i += 1
            in_numbered = False
            while i < len(lines):
                line = lines[i]
                stripped = line.strip()
                if stripped == "":
                    i += 1
                    continue
                # Numbered item at any indent (1. ..., 2. ...)
                if re.match(r"^\d+\.\s", stripped):
                    count += 1
                    in_numbered = True
                    i += 1
                # Top-level bullet (no leading whitespace)
                elif line.startswith("- "):
                    if in_numbered:
                        # Sub-detail of a numbered item, skip
                        i += 1
                    else:
                        item_text = stripped[2:].strip().rstrip(".")
                        if item_text.lower() != "none":
                            count += 1
                        i += 1
                # Indented line — continuation/sub-item, skip
                elif line.startswith("  ") or line.startswith("\t"):
                    i += 1
                else:
                    break
        else:
            i += 1
    return count


def count_review_issues(content: str) -> tuple[int, int]:
    """Parse review markdown and return (must_fix_count, suggestion_count).

    Supports two formats:
    - Claude: ### M1/### 1./### Issue 1 headings for must-fix, ### S1 for suggestions
    - Codex: 'Must-fix issues' / 'Suggestions' plain-text headers with bullet lists

    Returns (0, 0) if content is empty or unparseable.
    """
    if not content:
        return 0, 0

    # Claude format: ### headings
    must_fix = len(re.findall(r"^###\s+(?:M?\d+[\.\s—–-]|Issue\s+\d+)", content, re.MULTILINE))
    suggestions = len(re.findall(r"^###\s+S\d+[\.\s—–-]", content, re.MULTILINE))

    # Codex format: plain-text section headers with bullet lists
    if must_fix == 0:
        must_fix = _count_section_items(content, r"^(?:#+\s*)?must[- ]?fix(?:\s+issues?)?$")
    if suggestions == 0:
        suggestions = _count_section_items(content, r"^(?:#+\s*)?suggestions?$")

    return must_fix, suggestions


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
    parser.add_argument(
        "--issues",
        action="store_true",
        help="Show per-model issue counts parsed from review content",
    )
    args = parser.parse_args()

    today = date.today()
    end_date = parse_date(args.end) if args.end else today + timedelta(days=1)
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
        "SELECT id, task_type, based_on, depends_on, created_at, model FROM tasks"
    ).fetchall()
    tasks = {r["id"]: dict(r) for r in rows}

    def find_root_impl(task_id: int, visited: set | None = None) -> int | None:
        """Walk up based_on/depends_on chains to find the root implement task."""
        if visited is None:
            visited = set()
        if task_id in visited:
            return None
        visited.add(task_id)

        task = tasks.get(task_id)
        if task is None:
            return None

        # Walk up via based_on (lineage chain) or depends_on (dependency link)
        for parent_id in (task["based_on"], task["depends_on"]):
            if parent_id:
                parent = tasks.get(parent_id)
                if parent and parent["task_type"] == "implement":
                    root = find_root_impl(parent["id"], visited)
                    return root if root is not None else parent["id"]

        if task["task_type"] == "implement":
            return task_id
        return None

    # Find all review/improve tasks in the date range
    ri_tasks = conn.execute(
        """
        SELECT id, task_type, based_on, depends_on, created_at, model
        FROM tasks
        WHERE task_type IN ('review', 'improve')
          AND created_at >= ? AND created_at < ?
          AND (based_on IS NOT NULL OR depends_on IS NOT NULL)
        ORDER BY id
        """,
        (start_iso, end_iso),
    ).fetchall()

    # Load review content for issue counting (only when --issues)
    review_content: dict[int, str] = {}
    if args.issues:
        content_rows = conn.execute(
            """
            SELECT id, output_content
            FROM tasks
            WHERE task_type = 'review'
              AND output_content IS NOT NULL
              AND created_at >= ? AND created_at < ?
            """,
            (start_iso, end_iso),
        ).fetchall()
        review_content = {r["id"]: r["output_content"] for r in content_rows}

    conn.close()

    # Count reviews per root impl, tracking models
    root_reviews: dict[int, list[str]] = defaultdict(list)
    root_review_models: dict[int, list[str | None]] = defaultdict(list)

    for ri in ri_tasks:
        root = find_root_impl(ri["depends_on"] or ri["based_on"])
        if root is None:
            continue
        if ri["task_type"] == "review":
            root_reviews[root].append(ri["created_at"])
            root_review_models[root].append(ri["model"])

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
        # Group by when the impl was created, matching the impl grouping above
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
        total = len(all_reviewed_cycles)
        print(f"\nCycle distribution (reviewed tasks only):")
        for count in sorted(dist.keys()):
            pct = dist[count] / total * 100
            bar = "#" * dist[count]
            print(f"  {count} cycles: {dist[count]:>3} ({pct:4.0f}%)  {bar}")

    # Per-model review cycle stats
    # For each root impl, determine the predominant review model
    model_cycles: dict[str, list[int]] = defaultdict(list)
    for root_id in reviewed_impls:
        models = root_review_models.get(root_id, [])
        cycle_count = len(root_reviews[root_id])
        # Use the most common review model for this impl
        model_counts = Counter(m for m in models if m)
        if model_counts:
            model = model_counts.most_common(1)[0][0]
        else:
            model = "unknown"
        model_cycles[model].append(cycle_count)

    def _cycle_stats(vals: list[int]) -> str:
        """Format med/p75/p90/max as a compact string."""
        return f"{int(median(vals))}/{percentile(vals, 75)}/{percentile(vals, 90)}/{max(vals)}"

    if model_cycles:
        print(f"\n{'Review model':<35} {'Impls':>5}  {'med/p75/p90/max':>16}")
        print("-" * 60)
        for model in sorted(model_cycles):
            cycles = sorted(model_cycles[model])
            print(f"{model:<35} {len(cycles):>5}  {_cycle_stats(cycles):>16}")

    # Per-model issue counts (parsed from review markdown)
    if args.issues and review_content:
        print(f"\nParsing issue counts from {len(review_content)} review(s)...")
        model_issues: dict[str, list[tuple[int, int]]] = defaultdict(list)
        unparsed: list[int] = []
        for ri in ri_tasks:
            if ri["task_type"] != "review":
                continue
            content = review_content.get(ri["id"])
            if content is None:
                continue
            must_fix, sugg = count_review_issues(content)
            if must_fix == 0 and sugg == 0:
                unparsed.append(ri["id"])
            model = ri["model"] or "unknown"
            model_issues[model].append((must_fix, sugg))
        if unparsed:
            ids = ", ".join(f"#{i}" for i in unparsed)
            print(f"\nwarning: could not parse issues from {len(unparsed)} review(s): {ids}", file=sys.stderr)

        if model_issues:
            print(f"\nIssue counts per review (parsed from markdown)")
            def _stats(vals: list[int]) -> str:
                """Format med/p75/p90/max as a compact string."""
                return f"{int(median(vals))}/{percentile(vals, 75)}/{percentile(vals, 90)}/{max(vals)}"

            print(f"{'Review model':<35} {'Rvws':>5}  {'Must-fix':>16}  {'Suggestions':>16}")
            print(f"{'':35} {'':>5}  {'med/p75/p90/max':>16}  {'med/p75/p90/max':>16}")
            print("-" * 77)
            all_fixes: list[int] = []
            all_suggs: list[int] = []
            for model in sorted(model_issues):
                pairs = model_issues[model]
                n = len(pairs)
                fixes = sorted(mf for mf, _ in pairs)
                suggs = sorted(sg for _, sg in pairs)
                all_fixes.extend(fixes)
                all_suggs.extend(suggs)
                print(f"{model:<35} {n:>5}  {_stats(fixes):>16}  {_stats(suggs):>16}")
            if len(model_issues) > 1:
                all_fixes.sort()
                all_suggs.sort()
                print("-" * 77)
                print(f"{'Total':<35} {len(all_fixes):>5}  {_stats(all_fixes):>16}  {_stats(all_suggs):>16}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
