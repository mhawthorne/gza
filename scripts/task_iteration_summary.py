#!/usr/bin/env python3
"""Summarize iterate review/improve cycles for an implementation task.

Walks the resume/retry chain from the given impl task, lists each
review/improve cycle with blocker/follow-up counts, and flags recurring
issues and regressions by fuzzy-matching blocker text across reviews.

Usage:
    scripts/task_iteration_summary.py gza-1141
    scripts/task_iteration_summary.py gza-1141 --threshold 0.7
    scripts/task_iteration_summary.py gza-1141 --verbose
"""

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from gza.db import SqliteTaskStore, Task
from gza.git import Git, parse_diff_numstat
from gza.review_verdict import parse_review_verdict


ITEM_HEADER_RE = re.compile(r"^###\s+([BF])(\d+)\s*$")
SECTION_RE = re.compile(
    r"^-?\s*(Evidence|Impact|Required fix|Recommended follow-up|Required tests|Recommended tests):\s*(.*)$"
)
# Matches the Codex aggregate usage payload in log files. Claude emits
# cache_read_input_tokens / cache_creation_input_tokens under similar objects;
# today only Codex is confirmed, but the regex is permissive on key ordering.
USAGE_RE = re.compile(
    r'"usage"\s*:\s*\{[^{}]*?"input_tokens"\s*:\s*(\d+)[^{}]*?"cached_input_tokens"\s*:\s*(\d+)'
)

# Words ignored when computing similarity between review items. These are either
# English filler or review-format boilerplate that shows up in every item.
SIMILARITY_STOPWORDS = frozenset({
    "about", "after", "also", "another", "any", "been", "before", "being",
    "both", "because", "but", "could", "can", "doing", "does", "each", "even",
    "evidence", "fix", "follow", "followup", "for", "from", "has", "have",
    "impact", "into", "just", "must", "new", "not", "now", "old", "only",
    "onto", "other", "over", "recommended", "required", "same", "should",
    "since", "such", "still", "test", "tests", "than", "that", "the", "their",
    "them", "then", "there", "these", "they", "this", "those", "through",
    "under", "when", "whether", "which", "while", "whose", "will", "with",
    "would", "what",
})


@dataclass
class ReviewItem:
    kind: str   # "B" or "F"
    num: int    # 1, 2, ...
    evidence: str
    impact: str

    @property
    def label(self) -> str:
        return f"{self.kind}{self.num}"


@dataclass
class ReviewCycle:
    review: Task
    verdict: str | None
    blockers: list[ReviewItem]
    followups: list[ReviewItem]
    improves: list[Task]  # chronological; failed -> resumed chain lives here


@dataclass
class RunRow:
    """One agent run (a review or a single improve attempt) = one display row.

    A cycle expands to one review row followed by one row per improve attempt,
    including failed/timed-out attempts and their resumes. ``cycle`` is set only
    on the review row (the start of the cycle); continuation rows leave it None.
    """
    cycle: int | None       # cycle number on the review row; None on improve rows
    task: Task              # the review or improve task this row represents
    kind: str               # "review" or "improve"
    result: str             # review verdict, or improve status ("completed"/"TIMEOUT"/…)
    blockers: int | None    # review rows only
    followups: int | None   # review rows only
    delta: tuple[int, int] | None = None  # (files, lines) this improve introduced


def _resolve_effective_impl(store: SqliteTaskStore, task_id: str) -> Task:
    """Walk forward through the resume/retry chain to the latest impl row."""
    task = store.get(task_id)
    if task is None:
        raise SystemExit(f"Task {task_id} not found.")
    if task.task_type != "implement":
        raise SystemExit(f"Task {task_id} is {task.task_type}, expected implement.")
    while True:
        children = [
            t for t in store.get_based_on_children(task.id)
            if t.task_type == "implement"
        ]
        if not children:
            return task
        children.sort(key=lambda t: (t.created_at or 0))
        task = children[-1]


def _read_review_body(review: Task) -> str | None:
    if review.output_content:
        return review.output_content
    if review.report_file:
        path = Path(review.report_file)
        if path.exists():
            return path.read_text()
    return None


def _parse_items(body: str) -> tuple[list[ReviewItem], list[ReviewItem]]:
    """Return (blockers, follow-ups) extracted from a review markdown body."""
    blockers: list[ReviewItem] = []
    followups: list[ReviewItem] = []
    lines = body.splitlines()
    i = 0
    while i < len(lines):
        m = ITEM_HEADER_RE.match(lines[i].strip())
        if not m:
            i += 1
            continue
        kind, num = m.group(1), int(m.group(2))
        i += 1
        sections: dict[str, list[str]] = {}
        current: str | None = None
        while i < len(lines):
            stripped = lines[i].strip()
            if ITEM_HEADER_RE.match(stripped) or stripped.startswith("## "):
                break
            sec = SECTION_RE.match(stripped)
            if sec:
                current = sec.group(1)
                inline = sec.group(2).strip()
                bucket = sections.setdefault(current, [])
                if inline:
                    bucket.append(re.sub(r"^-\s+", "", inline))
            elif current is not None and stripped:
                sections[current].append(re.sub(r"^-\s+", "", stripped))
            i += 1
        item = ReviewItem(
            kind=kind,
            num=num,
            evidence=" ".join(sections.get("Evidence", [])),
            impact=" ".join(sections.get("Impact", [])),
        )
        (blockers if kind == "B" else followups).append(item)
    blockers.sort(key=lambda it: it.num)
    followups.sort(key=lambda it: it.num)
    return blockers, followups


def _load_cycles(store: SqliteTaskStore, impl: Task) -> list[ReviewCycle]:
    assert impl.id is not None
    reviews = [r for r in store.get_reviews_for_task(impl.id) if r.status == "completed"]
    reviews.sort(key=lambda t: (t.completed_at or 0, t.id or ""))
    cycles: list[ReviewCycle] = []
    for review in reviews:
        assert review.id is not None
        body = _read_review_body(review) or ""
        verdict = parse_review_verdict(body)
        blockers, followups = _parse_items(body)
        improves = store.get_improve_tasks_for(impl.id, review.id)
        improves.sort(key=lambda t: (t.created_at or 0))
        cycles.append(ReviewCycle(
            review=review,
            verdict=verdict,
            blockers=blockers,
            followups=followups,
            improves=improves,
        ))
    return cycles


def _fmt_ts(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M")


def _work_result(task: Task) -> str:
    """Outcome word for a work run (implement/improve); the reason goes in Error."""
    return "completed" if task.status == "completed" else task.status


def _error_reason(task: Task) -> str:
    """Failure reason for a run that errored (TIMEOUT, MAX_STEPS, …), else empty.

    Completed runs and normal reviews have none. A failed run with no recorded
    reason still shows as failed via the Result column.
    """
    if task.status == "completed":
        return ""
    return task.failure_reason or ""


def _fmt_delta(delta: tuple[int, int] | None) -> tuple[str, str]:
    if delta is None:
        return "-", "-"
    files, lines = delta
    return str(files), str(lines)


def _diff_two(git: Git | None, a: str | None, b: str | None) -> tuple[int, int] | None:
    """(files, total_lines) for the direct tree diff between two commits.

    Two-dot range: compares the endpoints directly rather than from their
    merge-base, so a rebase between the two snapshots doesn't shift the base
    and inflate the count (it still counts any main changes the rebase pulled
    in — an inherent limit). Returns None when a SHA is missing or unresolvable.
    """
    if git is None or not a or not b:
        return None
    result = git.get_diff_numstat_result(f"{a}..{b}")
    if result.returncode != 0:
        return None
    stdout = result.stdout if isinstance(result.stdout, str) else ""
    files, added, removed = parse_diff_numstat(stdout.strip())
    return files, added + removed


def _implement_delta(git: Git | None, cycles: list["ReviewCycle"]) -> tuple[int, int] | None:
    """Change the implementation introduced: base -> the HEAD its first review saw."""
    first = cycles[0].review
    return _diff_two(git, first.review_verify_base_sha, first.review_verify_head_sha)


def _cycle_delta(
    git: Git | None, cycles: list["ReviewCycle"], i: int
) -> tuple[int, int] | None:
    """(files, total_lines) introduced by the improve that ran after review ``i``.

    Reviews capture the impl HEAD at review time, and exactly one improve runs
    between two consecutive reviews, so the increment is the diff of review i's
    HEAD vs. review i+1's HEAD. Returns None when either SHA is missing (legacy
    reviews) or there is no following review (the latest improve isn't bounded).
    """
    cur = cycles[i].review.review_verify_head_sha
    nxt = cycles[i + 1].review.review_verify_head_sha if i + 1 < len(cycles) else None
    return _diff_two(git, cur, nxt)


def _delta_owner(work: list[Task]) -> int | None:
    """Index of the run in a work chain that actually produced the commits.

    The increment belongs to the run that committed — the last attempt with
    commits, else the last completed attempt. Failed/timed-out attempts that
    landed nothing get no delta.
    """
    committed = [j for j, t in enumerate(work) if t.has_commits]
    if committed:
        return committed[-1]
    completed = [j for j, t in enumerate(work) if t.status == "completed"]
    return completed[-1] if completed else None


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s}s" if m else f"{s}s"


def _sum_durations(tasks: list[Task]) -> float | None:
    values = [t.duration_seconds for t in tasks if t.duration_seconds is not None]
    return sum(values) if values else None


def _read_cache_usage(task: Task) -> tuple[int, int] | None:
    """Return (input_tokens, cached_input_tokens) from the task's log, or None.

    Codex emits exactly one `usage` block per run, so we just scan for the
    first match. If the task failed before reporting usage, returns None.
    """
    if not task.log_file:
        return None
    path = Path(task.log_file)
    if not path.exists():
        return None
    try:
        content = path.read_text(errors="replace")
    except OSError:
        return None
    m = USAGE_RE.search(content)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _combine_usage(tasks: list[Task]) -> tuple[int, int] | None:
    usages = [_read_cache_usage(task) for task in tasks]
    present = [usage for usage in usages if usage is not None]
    if not present:
        return None
    return sum(total for total, _ in present), sum(cached for _, cached in present)


def _fmt_tokens(usage: tuple[int, int] | None) -> str:
    if usage is None:
        return "-"
    total, _ = usage
    if total >= 1_000_000:
        return f"{total / 1_000_000:.1f}M"
    if total >= 1_000:
        return f"{total / 1_000:.1f}k"
    return str(total)


def _tokens(s: str) -> frozenset[str]:
    """Content tokens for similarity. Keeps ≥4-char identifiers; drops stopwords."""
    return frozenset(
        w for w in re.findall(r"[a-zA-Z_][a-zA-Z0-9_]{3,}", s.lower())
        if w not in SIMILARITY_STOPWORDS
    )


def _similarity(a: frozenset[str], b: frozenset[str]) -> float:
    """Overlap coefficient: |A ∩ B| / min(|A|, |B|).

    Chosen over Jaccard because review prose is paraphrased each cycle — the
    symmetric penalty in Jaccard punishes genuine recurrences that get
    rewritten. Overlap is more forgiving of length differences.
    """
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def _item_fingerprint(item: ReviewItem) -> frozenset[str]:
    return _tokens(f"{item.impact} {item.evidence}")


def _find_recurring(cycles: list[ReviewCycle], threshold: float) -> list[tuple[int, ReviewItem, int, ReviewItem, float]]:
    """Pairs of (earlier_cycle_idx, earlier_item, later_cycle_idx, later_item, score).
    Each later item is matched against its closest earlier counterpart; only
    the best match per later item is reported, above threshold."""
    results: list[tuple[int, ReviewItem, int, ReviewItem, float]] = []
    fingerprints = [
        [(it, _item_fingerprint(it)) for it in c.blockers + c.followups]
        for c in cycles
    ]
    for later_idx in range(1, len(cycles)):
        for later_item, later_fp in fingerprints[later_idx]:
            best: tuple[float, int, ReviewItem] | None = None
            for earlier_idx in range(later_idx):
                for earlier_item, earlier_fp in fingerprints[earlier_idx]:
                    score = _similarity(earlier_fp, later_fp)
                    if best is None or score > best[0]:
                        best = (score, earlier_idx, earlier_item)
            if best and best[0] >= threshold:
                results.append((best[1], best[2], later_idx, later_item, best[0]))
    return results


def _find_new_in_latest(cycles: list[ReviewCycle], threshold: float) -> list[ReviewItem]:
    if len(cycles) < 2:
        return []
    latest = cycles[-1]
    latest_items = [(it, _item_fingerprint(it)) for it in latest.blockers + latest.followups]
    prior_fps = [
        _item_fingerprint(it)
        for c in cycles[:-1]
        for it in c.blockers + c.followups
    ]
    new: list[ReviewItem] = []
    for item, fp in latest_items:
        if not any(_similarity(fp, prior) >= threshold for prior in prior_fps):
            new.append(item)
    return new


def _truncate(s: str, n: int) -> str:
    s = s.strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _emit_cycle(
    rows: list[RunRow],
    cycle_num: int,
    work: list[Task],
    review: ReviewCycle | None,
    delta: tuple[int, int] | None,
) -> None:
    """Append one cycle's rows: its work run(s) followed by the review of that work.

    Every work attempt — including failed/timed-out ones and their resumes —
    gets its own row; the increment is attributed to whichever attempt committed.
    The cycle number labels the cycle's first row.
    """
    first = len(rows)
    owner = _delta_owner(work)
    for j, task in enumerate(work):
        kind = "implement" if task.task_type == "implement" else "improve"
        rows.append(RunRow(
            cycle=None,
            task=task,
            kind=kind,
            result=_work_result(task),
            blockers=None,
            followups=None,
            delta=delta if j == owner else None,
        ))
    if review is not None:
        rows.append(RunRow(
            cycle=None,
            task=review.review,
            kind="review",
            result=review.verdict or "-",
            blockers=len(review.blockers),
            followups=len(review.followups),
        ))
    if len(rows) > first:
        rows[first].cycle = cycle_num


def _build_rows(
    cycles: list[ReviewCycle], impl: Task, git: Git | None
) -> list[RunRow]:
    """Group runs into work->review cycles, one row per run.

    A cycle is the work that produced a state plus the review of that state, so:
    cycle 1 = the implementation + its first review; cycle k = the improve(s)
    that responded to review k-1 + review k. Improves done after the final
    review (not yet re-reviewed) form a trailing review-less cycle.
    """
    rows: list[RunRow] = []
    # Cycle 1: the implementation, reviewed by the first review.
    _emit_cycle(rows, 1, [impl], cycles[0], _implement_delta(git, cycles))
    # Cycles 2..N: improve(s) answering the prior review, then the next review.
    for m in range(1, len(cycles)):
        _emit_cycle(rows, m + 1, cycles[m - 1].improves, cycles[m],
                    _cycle_delta(git, cycles, m - 1))
    # Trailing: improve(s) answering the final review, not yet re-reviewed.
    if cycles[-1].improves:
        _emit_cycle(rows, len(cycles) + 1, cycles[-1].improves, None,
                    _cycle_delta(git, cycles, len(cycles) - 1))
    return rows


def _net_change(rows: list[RunRow]) -> tuple[int, int] | None:
    """Final branch-vs-main total: the last run that recorded diff stats.

    rows are in chronological order, so the last run with stored stats reflects
    the branch's net change against main after the whole effort.
    """
    last: Task | None = None
    for r in rows:
        if r.task.diff_files_changed is not None:
            last = r.task
    if last is None:
        return None
    lines = (last.diff_lines_added or 0) + (last.diff_lines_removed or 0)
    return last.diff_files_changed, lines


def _render_table(rows: list[RunRow]) -> None:
    header = (
        f"{'Cycle':<6}{'Task':<12}{'Kind':<11}{'When':<17}{'Result':<18}"
        f"{'B':>3}{'F':>3}  {'ΔFiles':>7}{'ΔLines':>8}  {'Runtime':>9}{'Tokens':>9}  {'Error'}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        cycle = str(r.cycle) if r.cycle is not None else ""
        when = _fmt_ts(r.task.completed_at or r.task.started_at)
        blockers = str(r.blockers) if r.blockers is not None else ""
        followups = str(r.followups) if r.followups is not None else ""
        # Per-run change; reviews produce no diff so leave it blank.
        dfiles, dlines = _fmt_delta(r.delta) if r.kind != "review" else ("", "")
        dur = _fmt_duration(r.task.duration_seconds)
        tokens = _fmt_tokens(_read_cache_usage(r.task))
        error = _truncate(_error_reason(r.task), 40)
        print(
            f"{cycle:<6}{r.task.id or '-':<12}{r.kind:<11}{when:<17}{r.result:<18}"
            f"{blockers:>3}{followups:>3}  {dfiles:>7}{dlines:>8}  {dur:>9}{tokens:>9}  {error}"
        )
    print("-" * len(header))
    print()

    all_tasks = [r.task for r in rows]
    n_cycles = max((r.cycle for r in rows if r.cycle is not None), default=0)
    net = _net_change(rows)
    print("Totals")
    print(f"  Cycles:      {n_cycles}  ({len(rows)} runs)")
    print(f"  Runtime:     {_fmt_duration(_sum_durations(all_tasks))}")
    print(f"  Tokens:      {_fmt_tokens(_combine_usage(all_tasks))}")
    if net is not None:
        print(f"  Net change:  {net[0]} files, {net[1]} lines vs main")
    print()
    print("ΔFiles/ΔLines = change introduced by that run (implement: vs base; improve:")
    print("diff of bracketing review HEADs). '-' when review SHAs weren't captured or the")
    print("run isn't bounded by a following review.")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id", help="Implementation task id (e.g. gza-1141)")
    parser.add_argument("--threshold", type=float, default=0.28,
                        help="Overlap-coefficient threshold for recurring/regression detection (default: 0.28)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print every blocker/follow-up title per review")
    parser.add_argument("--width", type=int, default=110,
                        help="Truncation width for item impact text (default: 110)")
    args = parser.parse_args()

    store = SqliteTaskStore.default()
    root = store.get(args.task_id)
    if root is None:
        print(f"Task {args.task_id} not found.", file=sys.stderr)
        return 1
    if root.task_type != "implement":
        print(f"Task {args.task_id} is {root.task_type}, expected implement.", file=sys.stderr)
        return 1

    impl = _resolve_effective_impl(store, args.task_id)
    chain_note = "" if impl.id == root.id else f" → resumed/retried as {impl.id} ({impl.status})"
    print(f"Impl: {root.id} ({root.status}){chain_note}")
    print()

    cycles = _load_cycles(store, impl)
    if not cycles:
        print("No completed review cycles yet.")
        return 0

    try:
        git: Git | None = Git(Path.cwd())
    except Exception:
        git = None

    rows = _build_rows(cycles, impl, git)
    _render_table(rows)

    if args.verbose:
        for idx, c in enumerate(cycles, start=1):
            print(f"Cycle {idx} — {c.review.id} ({c.verdict}):")
            for item in c.blockers + c.followups:
                print(f"  {item.label}: {_truncate(item.impact or item.evidence, args.width)}")
            print()

    recurring = _find_recurring(cycles, args.threshold)
    if recurring:
        print(f"Recurring issues (similarity ≥ {args.threshold:.2f}):")
        for earlier_idx, earlier_item, later_idx, later_item, score in recurring:
            earlier_id = cycles[earlier_idx].review.id
            later_id = cycles[later_idx].review.id
            print(
                f"  C{earlier_idx + 1} {earlier_item.label} ({earlier_id})"
                f" ≈ C{later_idx + 1} {later_item.label} ({later_id})  [sim {score:.2f}]"
            )
            print(f"    → {_truncate(later_item.impact or later_item.evidence, args.width)}")
        print()

    regressions = _find_new_in_latest(cycles, args.threshold)
    if regressions and len(cycles) >= 2:
        latest_id = cycles[-1].review.id
        print(f"New in latest review ({latest_id}) — no close match in prior cycles:")
        for item in regressions:
            print(f"  {item.label}: {_truncate(item.impact or item.evidence, args.width)}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
