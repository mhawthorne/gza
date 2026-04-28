#!/usr/bin/env python3
"""Backfill stored review scores for completed review tasks with parseable content.

Usage:
    uv run python scripts/backfill_review_scores.py
    uv run python scripts/backfill_review_scores.py --dry-run
    uv run python scripts/backfill_review_scores.py --force
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from gza.db import SqliteTaskStore
from gza.review_verdict import get_backfillable_review_score


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing review_score values.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute scores even when review_score is already populated.",
    )
    args = parser.parse_args()

    project_dir = Path.cwd()
    store = SqliteTaskStore.default(project_dir)
    reviews = store.get_history(limit=None, status="completed", task_type="review")
    reviews.sort(key=lambda task: (task.completed_at or task.created_at, task.id or ""))

    scanned = len(reviews)
    updated = 0
    skipped_existing = 0
    skipped_unbackfillable = 0

    for review in reviews:
        if review.review_score is not None and not args.force:
            skipped_existing += 1
            continue

        score = get_backfillable_review_score(project_dir, review)
        if score is None:
            skipped_unbackfillable += 1
            continue

        if review.review_score == score:
            skipped_existing += 1
            continue

        if args.dry_run:
            print(f"would set {review.id}: {review.review_score} -> {score}")
        else:
            review.review_score = score
            store.update(review)
            print(f"set {review.id}: {score}")
        updated += 1

    mode = "dry-run" if args.dry_run else "applied"
    print(
        f"{mode}: scanned={scanned} updated={updated} "
        f"skipped_existing={skipped_existing} skipped_unbackfillable={skipped_unbackfillable}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
