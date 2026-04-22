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
from pathlib import Path

from gza.db import SqliteTaskStore, Task


ITEM_HEADER_RE = re.compile(r"^###\s+([BF])(\d+)\s*$")
SECTION_RE = re.compile(r"^(Evidence|Impact|Required fix|Recommended follow-up|Required tests|Recommended tests):\s*(.*)$")
VERDICT_RE = re.compile(r"^Verdict:\s*(\S.*)$", re.MULTILINE)
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
                    bucket.append(inline)
            elif current is not None and stripped:
                sections[current].append(stripped)
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
        verdict_match = VERDICT_RE.search(body)
        verdict = verdict_match.group(1).strip() if verdict_match else None
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


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s}s" if m else f"{s}s"


def _fmt_cost(cost: float | None) -> str:
    return f"${cost:.2f}" if cost else "-"


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


def _fmt_cache(usage: tuple[int, int] | None) -> str:
    if usage is None:
        return "-"
    total, cached = usage
    if total <= 0:
        return "-"
    return f"{100 * cached / total:4.1f}%"


def _fmt_improve(improves: list[Task]) -> str:
    if not improves:
        return "—"
    if len(improves) == 1:
        t = improves[0]
        return f"{t.id} [{t.status}]"
    # chain: show last + annotate predecessor failure modes
    head = improves[-1]
    prior = improves[:-1]
    prior_str = ", ".join(
        f"{t.id} {t.failure_reason or t.status}" for t in prior
    )
    return f"{head.id} [{head.status}] (after {prior_str})"


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

    header = f"{'Cycle':<6}{'Review':<12}{'Verdict':<20}{'B':>3}{'F':>3}  {'Improve':<48}{'Dur':>8}  {'Cost':>7}  {'Cache':>6}"
    print(header)
    print("-" * len(header))
    for idx, c in enumerate(cycles, start=1):
        improve_str = _fmt_improve(c.improves)
        last_improve = c.improves[-1] if c.improves else None
        dur = _fmt_duration(last_improve.duration_seconds) if last_improve else "-"
        cost = _fmt_cost(last_improve.cost_usd) if last_improve else "-"
        cache = _fmt_cache(_read_cache_usage(last_improve)) if last_improve else "-"
        print(
            f"{idx:<6}{c.review.id or '-':<12}{(c.verdict or '-'):<20}"
            f"{len(c.blockers):>3}{len(c.followups):>3}  "
            f"{improve_str:<48}{dur:>8}  {cost:>7}  {cache:>6}"
        )
    print()

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
