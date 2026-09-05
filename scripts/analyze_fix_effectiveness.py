#!/usr/bin/env python3
"""Regenerate docs/internal/gza-fix-effectiveness.md from the live task DB.

Usage: uv run python scripts/analyze_fix_effectiveness.py docs/internal/gza-fix-effectiveness.md
"""
import collections
import datetime
import math
import os
import sqlite3
import sys
from pathlib import Path

from gza.review_verdict import _extract_verdict

DB = os.environ.get("GZA_DB_PATH", str(Path.home() / "work/supreme/gza/.gza/gza.db"))
con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
con.row_factory = sqlite3.Row
con.execute("PRAGMA busy_timeout=30000")

# The re-review bound (2dab013c) reached main in merge aa9aeda2. Units merged after
# this instant are the first that could have been reviewed under the narrowed rule.
REREVIEW_BOUND_LANDED_AT = os.environ.get("GZA_REREVIEW_BOUND_LANDED_AT", "2026-09-04T15:08:45")


def reviews_per_merged_unit(con):
    """Return (merged_at, review_count) for every merged unit that has an implement."""
    rows = con.execute("""
        SELECT mu.id, mu.merged_at,
               SUM(CASE WHEN t.task_type = 'review' THEN 1 ELSE 0 END) AS reviews
        FROM merge_units mu
        JOIN merge_unit_tasks mut
          ON mut.merge_unit_id = mu.id AND mut.project_id = mu.project_id
        JOIN tasks t
          ON t.id = mut.task_id AND t.project_id = mut.project_id
        WHERE mu.state = 'merged' AND mu.merged_at IS NOT NULL
        GROUP BY mu.id, mu.merged_at
        HAVING SUM(CASE WHEN t.task_type = 'implement' THEN 1 ELSE 0 END) > 0
    """).fetchall()
    return [(r["merged_at"], r["reviews"]) for r in rows]


def _pct(values, q):
    """Nearest-rank percentile. Review counts are small integers with a long tail,
    so a mean is dominated by a few runaway units; p50/p90 describe the typical
    unit and the bad tail separately."""
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


def convergence_section(con):
    data = reviews_per_merged_unit(con)
    lines = ["## Convergence: reviews per merged unit\n"]
    lines.append(
        "The churn this report describes shows up here. A unit that converges is reviewed "
        "once or twice; one that does not accumulates reviews. `2dab013c` narrowed what a "
        "re-review may block on, so p90 in particular should fall for units merged after it landed "
        f"(`{REREVIEW_BOUND_LANDED_AT}`).\n"
    )

    by_month = collections.defaultdict(list)
    for merged_at, reviews in data:
        by_month[merged_at[:7]].append(reviews)
    lines.append("| month merged | units | p50 reviews | p90 reviews | max |")
    lines.append("|---|---|---|---|---|")
    for month in sorted(by_month):
        counts = by_month[month]
        lines.append(
            f"| {month} | {len(counts)} | {_pct(counts, 50)} | {_pct(counts, 90)} | "
            f"{max(counts)} |"
        )
    lines.append("")

    before = [r for merged_at, r in data if merged_at < REREVIEW_BOUND_LANDED_AT]
    after = [r for merged_at, r in data if merged_at >= REREVIEW_BOUND_LANDED_AT]
    lines.append(
        f"**Before the bound:** {len(before)} units, "
        f"p50 {_pct(before, 50)} / p90 {_pct(before, 90)} reviews."
    )
    if after:
        lines.append(
            f"**After the bound:** {len(after)} units, "
            f"p50 {_pct(after, 50)} / p90 {_pct(after, 90)} reviews."
        )
        if len(after) < 30:
            lines.append(
                f"\n> Only {len(after)} units have merged since the change. Too few to "
                "conclude anything; re-run once this reaches ~50."
            )
    else:
        lines.append(
            "**After the bound:** no units merged yet. Re-run this once some have; "
            "that is the measurement, not the prose above."
        )
    lines.append("")
    return lines

def cohort(src):
    # null predates the trigger_source column; every null row carries a review dep,
    # so all of them are the manual churn-rescue shape.
    return "churn-rescue" if src in (None, "manual") else "watch-rework"

fixes = con.execute("""SELECT id, based_on impl, depends_on prev_review, status,
    trigger_source, created_at, completed_at FROM tasks WHERE task_type='fix' ORDER BY created_at""").fetchall()

def next_review(impl, after):
    if not impl or not after:
        return None
    return con.execute("""SELECT id, status, output_content FROM tasks WHERE task_type='review'
        AND (depends_on=? OR based_on=?) AND created_at > ? ORDER BY created_at ASC LIMIT 1""",
        (impl, impl, after)).fetchone()

def merged(impl):
    r = con.execute("""SELECT mu.state FROM merge_units mu
      JOIN merge_unit_tasks mut ON mut.merge_unit_id=mu.id AND mut.project_id=mu.project_id
      WHERE mut.task_id=? ORDER BY mu.updated_at DESC LIMIT 1""", (impl,)).fetchone()
    return bool(r and r["state"] == "merged")

data = collections.defaultdict(lambda: {"rows": [], "outcomes": collections.Counter()})
for f in fixes:
    c = cohort(f["trigger_source"])
    rev = next_review(f["impl"], f["completed_at"]) if f["status"] == "completed" else None
    if f["status"] != "completed":
        out = f"fix-{f['status']}"
    elif rev is None:
        out = "no-review-after"
    elif rev["status"] != "completed":
        out = f"review-{rev['status']}"
    else:
        out = _extract_verdict(rev["output_content"] or "") or "unparsed"
    data[c]["outcomes"][out] += 1
    data[c]["rows"].append((f["id"], f["impl"], f["created_at"][:10], rev["id"] if rev else None, out))

VERDICTS = ("APPROVED","APPROVED_WITH_FOLLOWUPS","CHANGES_REQUESTED","NEEDS_DISCUSSION")
lines = []
lines.append("# Does `gza fix` lead to a passing review?\n")
lines.append(f"Generated {datetime.date.today().isoformat()} from `.gza/gza.db` "
             f"(all {len(fixes)} `fix` tasks ever created). Verdicts parsed with "
             "`gza.review_verdict._extract_verdict`, not string matching.\n")
lines.append("## Cohorts\n")
lines.append("`fix` serves two unrelated jobs. They must not be pooled.\n")
lines.append("| cohort | trigger_source | n | what it does |")
lines.append("|---|---|---|---|")
lines.append(f"| churn-rescue | `manual`, and `NULL` (pre-dates the column) | {sum(data['churn-rescue']['outcomes'].values())} | `gza fix` — rescue a task stuck in review/improve churn |")
lines.append(f"| watch-rework | `watch`, `watch-pre-merge-integration-verify-rework` | {sum(data['watch-rework']['outcomes'].values())} | merged tree failed verify; repair the branch |")
lines.append("")

for name in ("churn-rescue", "watch-rework"):
    d = data[name]
    lines.append(f"## {name}\n")
    lines.append("| outcome of the next review | n |")
    lines.append("|---|---|")
    for k, v in d["outcomes"].most_common():
        lines.append(f"| {k} | {v} |")
    judged = [r for r in d["rows"] if r[4] in VERDICTS]
    ok = [r for r in judged if r[4].startswith("APPROVED")]
    lines.append("")
    if judged:
        lines.append(f"**Reached a verdict: {len(judged)}. Approved: {len(ok)} ({100*len(ok)/len(judged):.0f}%).**\n")
    else:
        lines.append("**No fix in this cohort was followed by a completed review.**\n")

impls = collections.Counter(f["impl"] for f in fixes if cohort(f["trigger_source"])=="churn-rescue" and f["impl"])
rep = {k: v for k, v in impls.items() if v > 1}
m = sum(1 for i in impls if merged(i))
ctrl = con.execute("""SELECT mu.state, COUNT(*) n FROM merge_units mu
  JOIN merge_unit_tasks mut ON mut.merge_unit_id=mu.id AND mut.project_id=mu.project_id
  JOIN tasks t ON t.id=mut.task_id
  WHERE t.task_type='implement' AND t.id NOT IN (SELECT based_on FROM tasks WHERE task_type='fix' AND based_on IS NOT NULL)
  GROUP BY mu.state""").fetchall()
ct = sum(r["n"] for r in ctrl)
cm = sum(r["n"] for r in ctrl if r["state"] == "merged")

lines.append("## Did the task eventually merge? (churn-rescue only)\n")
lines.append(f"- impls that received at least one `gza fix`: **{m}/{len(impls)} ({100*m/len(impls):.0f}%)** merged")
lines.append(f"- impls that never needed one: **{cm}/{ct} ({100*cm/ct:.0f}%)** merged")
lines.append(f"- impls needing more than one fix: **{len(rep)}/{len(impls)}** (max {max(impls.values())})\n")
lines.append("> Not a controlled comparison: you only reach for `fix` on a task that is already "
             "churning, so this cohort is harder by construction. The gap is mostly selection.\n")

lines.append("## Why the next review fails\n")
lines.append("Ten before/after blocker pairs were read by hand (automated prose and symbol "
             "matching proved unreliable, giving 79% and 6% on the same data).\n")
lines.append("- ~7 of 10: the new review's blockers concerned **different code** than the original complaint.")
lines.append("- ~3 of 10: a genuine repeat of the same issue.\n")
lines.append("The fix generally does close the blocker it was handed. The next reviewer, by design "
             "independent and with no memory of the last round, then finds something else. Chains "
             "like `gza-2969 -> 2974 -> 2980 -> 3008` show each round's new blocker becoming the next "
             "round's input.\n")
lines.append("## Conclusion\n")
lines.append("`gza fix` does not reliably produce a passing review, but the cause is not the fix "
             "agent. It is an unbounded re-reviewer: the blocker set was resampled from the whole "
             "diff every round instead of shrinking, so the loop had no reason to terminate.\n")
lines.append("Addressed in `2dab013c`, which scopes blocker criterion (3) on a re-review to code "
             "changed since the round that already reviewed it. These numbers are the pre-change "
             "baseline; re-run this to measure the effect.\n")
lines.extend(convergence_section(con))
lines.append("## Per-task detail (churn-rescue)\n")
lines.append("| fix | impl | created | next review | outcome |")
lines.append("|---|---|---|---|---|")
for r in data["churn-rescue"]["rows"]:
    lines.append(f"| {r[0]} | {r[1] or '-'} | {r[2]} | {r[3] or '-'} | {r[4]} |")

out = "\n".join(lines) + "\n"
open(sys.argv[1], "w").write(out)
print(out[:1800])
