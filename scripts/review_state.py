#!/usr/bin/env python3
"""Show (or clear, or force-approve) review state on an impl task.

Usage:
    scripts/review_state.py <impl_task_id>                      # show
    scripts/review_state.py <impl_task_id> --clear              # clear, then show
    scripts/review_state.py <impl_task_id> --force-approve "reason"
        # Insert a completed review task with Verdict: APPROVED, so
        # `gza advance` treats the implementation as having a passing
        # review and proceeds to rebase/verify/merge gating instead of
        # re-running a real review. This is a manual operator override,
        # not a substitute for review -- use only when you've personally
        # confirmed the code is fine and just want to skip a re-review.
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git
from gza.runner import _compute_slug_override, generate_slug, get_task_output_paths


def _force_approve(store: SqliteTaskStore, config: Config, task_id: str, reason: str) -> int:
    impl = store.get(task_id)
    if impl is None:
        print(f"Task {task_id} not found.", file=sys.stderr)
        return 1
    if impl.task_type != "implement":
        print(f"Task {task_id} is a {impl.task_type!r}, not an implement task.", file=sys.stderr)
        return 1

    origin = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    body = (
        "## Summary\n\n"
        f"Manually force-approved via scripts/review_state.py on {origin}.\n\n"
        f"Reason: {reason}\n\n"
        "## Blockers\n\nNone (operator override -- not independently re-verified).\n\n"
        "## Verdict\n\nVerdict: APPROVED\n"
    )
    output_content = f"<!-- origin: scripts/review_state.py --force-approve ({origin}) -->\n" + body

    created = store.add(
        prompt=f"Manual force-approve override for {task_id}",
        task_type="review",
        based_on=task_id,
        depends_on=task_id,
        same_branch=True,
        tags=list(impl.tags),
    )
    assert created.id is not None

    if created.slug is None:
        slug_override = _compute_slug_override(created, store)
        created.slug = generate_slug(
            created.prompt,
            existing_id=None,
            log_path=config.log_path,
            git=Git(config.project_dir),
            store=store,
            exclude_task_id=created.id,
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=created.task_type_hint,
        )
        store.update(created)

    report_path, _summary_path = get_task_output_paths(created, config.project_dir)
    assert report_path is not None
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(output_content)

    created.report_file = str(report_path.relative_to(config.project_dir))
    created.status = "completed"
    created.completed_at = datetime.now(timezone.utc)
    created.output_content = output_content
    store.update(created)

    print(f"Task {task_id}: force-approved via review {created.id} ({created.report_file})")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id", help="Implementation task ID (e.g. gza-1146)")
    parser.add_argument(
        "--clear", action="store_true", help="Clear review_cleared_at on the task"
    )
    parser.add_argument(
        "--force-approve",
        metavar="REASON",
        help="Insert a completed APPROVED review for this task (manual override)",
    )
    args = parser.parse_args()

    config = Config.load(Path.cwd())
    store = SqliteTaskStore.from_config(config)
    task = store.get(args.task_id)
    if task is None:
        print(f"Task {args.task_id} not found.", file=sys.stderr)
        return 1

    if args.force_approve is not None:
        return _force_approve(store, config, args.task_id, args.force_approve)

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
