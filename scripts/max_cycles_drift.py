#!/usr/bin/env python3
"""Quantify the "drifting blocker" pattern in max-cycles review/improve churn.

For implementation tasks that hit >= N review cycles, parse each review's
Blockers section, extract cited file paths, and report what fraction of
consecutive review pairs cite files in the same module. High overlap means
the reviewer keeps probing the same surface area each cycle (the gza-3310
shape); low overlap means scattered findings.

Used to decide whether a "smarter review" prompt change (enumerate the whole
class of issue in one blocker, not the first instance) would pay back.

Usage:
    scripts/max_cycles_drift.py
    scripts/max_cycles_drift.py --min-cycles 10
    scripts/max_cycles_drift.py --window 10 --db .gza/gza.db
"""

import argparse
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

FILE_RE = re.compile(
    r"\b((?:src|tests|docs|bin|scripts|\.gza|\.claude)/[\w./\-]+"
    r"\.(?:py|md|sh|yaml|yml|toml|json|txt))(?::\d+(?:-\d+)?)?"
)

BLOCKERS_SECTION_RE = re.compile(r"##\s*Blockers\b(.*?)(?=\n##\s|\Z)", re.S | re.I)


def files_in(text: str) -> set[str]:
    if not text:
        return set()
    return set(FILE_RE.findall(text))


def module_of(path: str) -> str:
    """Group by first 3 path segments. For top-level src/gza/*.py this is the
    file itself; for nested paths like src/gza/skills/foo/SKILL.md it's the
    parent module."""
    parts = path.split("/")
    if len(parts) <= 3:
        return path
    return "/".join(parts[:3])


def blockers_text(review_output: str) -> str:
    if not review_output:
        return ""
    m = BLOCKERS_SECTION_RE.search(review_output)
    return m.group(1) if m else review_output


def analyze(db_path: Path, project_id: str, min_cycles: int, window: int) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute(
        """
        SELECT r.based_on, COUNT(*) AS n, t.completed_at
        FROM tasks r
        JOIN tasks t ON t.project_id=r.project_id AND t.id=r.based_on
        WHERE r.task_type='review' AND r.project_id=? AND r.based_on IS NOT NULL
        GROUP BY r.based_on
        HAVING n >= ?
        ORDER BY t.completed_at
        """,
        (project_id, min_cycles),
    )
    impls = [(row[0], row[2]) for row in cur.fetchall()]

    cur.execute(
        """
        SELECT COUNT(DISTINCT t.id) FROM tasks t
        JOIN tasks r ON r.project_id=t.project_id AND r.based_on=t.id AND r.task_type='review'
        WHERE t.project_id=? AND t.task_type='implement' AND t.completed_at IS NOT NULL
        """,
        (project_id,),
    )
    impls_with_reviews = cur.fetchone()[0]
    overall_rate = (len(impls) / impls_with_reviews * 100) if impls_with_reviews else 0
    print(
        f"Stuck (>= {min_cycles} review cycles): {len(impls)} of {impls_with_reviews} "
        f"impl tasks that entered the review loop ({overall_rate:.1f}%)\n"
    )

    summary = []
    for impl_id, impl_completed in impls:
        cur.execute(
            """
            SELECT id, created_at, output_content
            FROM tasks
            WHERE task_type='review' AND project_id=? AND based_on=?
            ORDER BY created_at
            LIMIT ?
            """,
            (project_id, impl_id, window),
        )
        reviews = cur.fetchall()

        per_review_modules = []
        for _rid, _ts, content in reviews:
            files = files_in(blockers_text(content or ""))
            per_review_modules.append({module_of(f) for f in files})

        consecutive_shared = 0
        consecutive_total = 0
        for i in range(1, len(per_review_modules)):
            prev, curr = per_review_modules[i - 1], per_review_modules[i]
            if not prev or not curr:
                continue
            consecutive_total += 1
            if prev & curr:
                consecutive_shared += 1

        all_modules: Counter[str] = Counter()
        for ms in per_review_modules:
            all_modules.update(ms)
        total = sum(all_modules.values())
        top_mod, top_count = (all_modules.most_common(1)[0] if all_modules else (None, 0))
        top_share = (top_count / total * 100) if total else 0
        empty = sum(1 for ms in per_review_modules if not ms)

        summary.append(
            {
                "impl_id": impl_id,
                "completed_at": impl_completed,
                "n": len(reviews),
                "empty": empty,
                "shared": consecutive_shared,
                "pairs": consecutive_total,
                "overlap_pct": (consecutive_shared / consecutive_total * 100)
                if consecutive_total
                else 0,
                "top_mod": top_mod,
                "top_share_pct": top_share,
                "uniq_mods": len(all_modules),
            }
        )

    print(
        f"{'completed':10} {'impl':10} {'n':>3} {'empty':>5} {'cons_overlap':>14} "
        f"{'top_share':>9} {'uniq':>5}  top_module"
    )
    print("-" * 120)
    for s in summary:
        date = (s["completed_at"] or "")[:10] or "unknown"
        print(
            f"{date:10} {s['impl_id']:10} {s['n']:>3} {s['empty']:>5} "
            f"{s['shared']:>3}/{s['pairs']:<3} ({s['overlap_pct']:>4.0f}%)  "
            f"{s['top_share_pct']:>7.0f}%  {s['uniq_mods']:>5}  {s['top_mod']}"
        )

    valid = [s for s in summary if s["pairs"] > 0]
    if not valid:
        print("\nNo tasks with parseable consecutive review pairs.")
        return

    avg_overlap = sum(s["overlap_pct"] for s in valid) / len(valid)
    high = sum(1 for s in valid if s["overlap_pct"] >= 70)
    very_high = sum(1 for s in valid if s["overlap_pct"] >= 90)
    avg_top_share = sum(s["top_share_pct"] for s in valid) / len(valid)
    concentrated = sum(1 for s in valid if s["top_share_pct"] >= 50)

    print("\n--- Aggregate ---")
    print(f"Parseable tasks: {len(valid)}/{len(summary)}")
    print(f"Avg consecutive-review module overlap: {avg_overlap:.0f}%")
    print(
        f">=70% consecutive overlap (drift shape): "
        f"{high}/{len(valid)} ({high/len(valid)*100:.0f}%)"
    )
    print(
        f">=90% consecutive overlap (heavy drift): "
        f"{very_high}/{len(valid)} ({very_high/len(valid)*100:.0f}%)"
    )
    print(f"\nAvg top-module share of citations: {avg_top_share:.0f}%")
    print(
        f"Top module >=50% of citations (single-file churn): "
        f"{concentrated}/{len(valid)} ({concentrated/len(valid)*100:.0f}%)"
    )

    # Monthly trend: rate of max-cycles tasks + drift shape over time.
    cur.execute(
        """
        SELECT substr(t.completed_at, 1, 7) AS month, COUNT(DISTINCT t.id)
        FROM tasks t
        JOIN tasks r ON r.project_id=t.project_id AND r.based_on=t.id AND r.task_type='review'
        WHERE t.project_id=? AND t.task_type='implement' AND t.completed_at IS NOT NULL
        GROUP BY month
        ORDER BY month
        """,
        (project_id,),
    )
    impl_totals = dict(cur.fetchall())

    by_month: dict[str, list[dict]] = {}
    for s in valid:
        month = (s["completed_at"] or "")[:7] or "unknown"
        by_month.setdefault(month, []).append(s)

    print("\n--- Monthly trend ---")
    print(
        f"{'month':8} {'stuck':>5} {'total_impl':>10} {'stuck_rate':>10} "
        f"{'avg_overlap':>11} {'drift>=70%':>10}"
    )
    print("-" * 70)
    for month in sorted(by_month):
        rows = by_month[month]
        total = impl_totals.get(month, 0)
        rate = (len(rows) / total * 100) if total else 0
        avg_ov = sum(r["overlap_pct"] for r in rows) / len(rows)
        drift = sum(1 for r in rows if r["overlap_pct"] >= 70)
        print(
            f"{month:8} {len(rows):>5} {total:>10} {rate:>9.1f}% "
            f"{avg_ov:>10.0f}% {drift:>3}/{len(rows):<3}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=Path(".gza/gza.db"), help="Path to gza.db (default: .gza/gza.db)")
    parser.add_argument("--project-id", default="gza", help="Project ID to analyze (default: gza)")
    parser.add_argument("--min-cycles", type=int, default=10, help="Only include impl tasks with >= this many reviews (default: 10)")
    parser.add_argument("--window", type=int, default=10, help="Reviews per task to analyze, ordered by created_at (default: 10)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        return 1

    analyze(args.db, args.project_id, args.min_cycles, args.window)
    return 0


if __name__ == "__main__":
    sys.exit(main())
