#!/usr/bin/env python3
"""Show (or clear) review_cleared_at on an impl task.

Usage:
    scripts/review_state.py <impl_task_id>           # show
    scripts/review_state.py <impl_task_id> --clear   # clear, then show
"""

import argparse
import sys

from gza.db import SqliteTaskStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id", help="Implementation task ID (e.g. gza-1146)")
    parser.add_argument(
        "--clear", action="store_true", help="Clear review_cleared_at on the task"
    )
    args = parser.parse_args()

    store = SqliteTaskStore.default()
    task = store.get(args.task_id)
    if task is None:
        print(f"Task {args.task_id} not found.", file=sys.stderr)
        return 1

    if args.clear:
        before = task.review_cleared_at
        store.clear_review_state(args.task_id)
        after = store.get(args.task_id).review_cleared_at
        print(f"Task {args.task_id}: review_cleared_at {before} -> {after}")
    else:
        print(f"Task {args.task_id}: review_cleared_at={task.review_cleared_at}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
