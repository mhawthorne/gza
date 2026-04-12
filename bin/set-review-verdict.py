#!/usr/bin/env python3
"""Force-update the review verdict for a task by re-parsing or explicitly setting it.

Usage:
    # Re-parse verdict from review task's output_content:
    python bin/set-review-verdict.py gza-409

    # Explicitly set a verdict (also backfills output_content if it is NULL):
    python bin/set-review-verdict.py gza-409 --verdict CHANGES_REQUESTED

    # Extract review content from the task's log file and backfill output_content:
    python bin/set-review-verdict.py gza-409 --from-log

    # Dry-run (show what would happen):
    python bin/set-review-verdict.py gza-409 --dry-run

The verdict is extracted dynamically by `gza show` / `gza advance` from
output_content. This script's job is to patch output_content so the
regex-based extractors can find the verdict reliably.
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

from gza.config import Config
from gza.db import InvalidTaskIdError, resolve_task_id
from gza.runner import extract_content_from_log

VALID_VERDICTS = ("APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION")


def get_db_path() -> Path:
    return Path(".gza") / "gza.db"


def extract_verdict(content: str) -> str | None:
    """Extract verdict using the same logic as the codebase."""
    match = re.search(
        r"\*{0,2}Verdict\*{0,2}:\s*\*{0,2}(APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)\*{0,2}",
        content,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).upper()
    match = re.search(
        r"##\s+Verdict\s*\n+\s*\*{0,2}(APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)\*{0,2}",
        content,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).upper()
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task_id", type=str, help="Full prefixed review task ID (for example: gza-1234)")
    parser.add_argument("--verdict", choices=VALID_VERDICTS, help="Explicitly set this verdict (skip parsing)")
    parser.add_argument("--from-log", action="store_true", help="Extract review content from the task's log file and backfill output_content")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without writing")
    args = parser.parse_args()

    if args.verdict and args.from_log:
        print("--verdict and --from-log are mutually exclusive", file=sys.stderr)
        return 1

    db_path = get_db_path()
    if not db_path.exists():
        print(f"Database not found at {db_path}", file=sys.stderr)
        return 1

    config = Config.load(Path("."))
    try:
        resolved_task_id = resolve_task_id(args.task_id, config.project_prefix)
    except InvalidTaskIdError as e:
        print(str(e), file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    row = conn.execute(
        "SELECT id, task_type, output_content, log_file FROM tasks WHERE id = ?",
        (resolved_task_id,),
    ).fetchone()
    if not row:
        print(f"Task {args.task_id} not found", file=sys.stderr)
        return 1

    if row["task_type"] != "review":
        print(f"Task {args.task_id} is type '{row['task_type']}', not 'review'", file=sys.stderr)
        return 1

    # Determine content and verdict
    new_output_content: str | None = None

    if args.from_log:
        log_file_rel = row["log_file"]
        if not log_file_rel:
            print(f"Task {args.task_id} has no log_file recorded", file=sys.stderr)
            return 1
        log_path = Path(log_file_rel)
        if not log_path.is_absolute():
            log_path = Path(".") / log_file_rel
        if not log_path.exists():
            print(f"Log file not found: {log_path}", file=sys.stderr)
            return 1
        content = extract_content_from_log(log_path)
        if not content:
            print(f"No 'result' entry with text found in log file {log_path}", file=sys.stderr)
            return 1
        print(f"Extracted content from log ({len(content)} chars)")
        verdict = extract_verdict(content)
        if not verdict:
            print("Could not parse verdict from extracted log content", file=sys.stderr)
            print("Use --verdict to set it explicitly", file=sys.stderr)
            return 1
        print(f"Parsed verdict from log content: {verdict}")
        new_output_content = content

    elif args.verdict:
        verdict = args.verdict
        print(f"Using explicit verdict: {verdict}")
        # Backfill output_content when it is NULL so get_review_verdict() can find the verdict.
        if not row["output_content"]:
            new_output_content = f"**Verdict: {verdict}**"
            print("output_content is NULL — will backfill with synthetic verdict block")

    else:
        content = row["output_content"] or ""
        verdict = extract_verdict(content)
        if not verdict:
            print(f"Could not parse verdict from output_content of task {args.task_id}", file=sys.stderr)
            print("Use --verdict to set it explicitly, or --from-log to extract from the log file", file=sys.stderr)
            return 1
        print(f"Parsed verdict from output_content: {verdict}")

    # Backfill output_content if needed
    if new_output_content is not None:
        old_content = row["output_content"]
        if args.dry_run:
            print(f"Would update tasks {args.task_id}: output_content {old_content!r:.80} -> {new_output_content!r:.80}")
        else:
            conn.execute(
                "UPDATE tasks SET output_content = ? WHERE id = ?",
                (new_output_content, resolved_task_id),
            )
            print(f"Updated tasks {args.task_id}: output_content backfilled ({len(new_output_content)} chars)")

    # Show current state for verification
    print(f"\nVerdict for review task {args.task_id}: {verdict}")
    print("Note: gza show/advance also extract the verdict dynamically from output_content.")

    if not args.dry_run:
        conn.commit()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
