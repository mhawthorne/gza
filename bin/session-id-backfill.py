#!/usr/bin/env python3
"""Backfill session_id from log files into the tasks DB."""

import argparse
import json
import sqlite3
import sys

DB_PATH = ".gza/gza.db"


def get_session_id_from_log(log_file: str) -> str | None:
    """Read the first line of a JSONL log and extract session_id."""
    try:
        with open(log_file) as f:
            first_line = f.readline()
        data = json.loads(first_line)
        return data.get("session_id")
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        print(f"  error reading log: {e}", file=sys.stderr)
        return None


def process_task(conn: sqlite3.Connection, task_id: int, *, dry_run: bool, force: bool) -> bool:
    """Process a single task. Returns True if session_id was found."""
    row = conn.execute(
        "SELECT id, session_id, log_file FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()

    if row is None:
        print(f"task {task_id}: not found", file=sys.stderr)
        return False

    _, existing_session_id, log_file = row

    if existing_session_id and not force:
        print(f"task {task_id}: already has session_id {existing_session_id}, skipping")
        return True

    if not log_file:
        print(f"task {task_id}: no log_file set", file=sys.stderr)
        return False

    session_id = get_session_id_from_log(log_file)
    if not session_id:
        print(f"task {task_id}: no session_id found in log", file=sys.stderr)
        return False

    print(f"task {task_id}: {session_id}" + (" (dry run)" if dry_run else ""))

    if not dry_run:
        conn.execute("UPDATE tasks SET session_id = ? WHERE id = ?", (session_id, task_id))
        conn.commit()

    return True


def main():
    parser = argparse.ArgumentParser(description="Backfill session_id from log files")
    parser.add_argument("task_ids", nargs="*", type=int, help="Task IDs to process")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing to DB")
    parser.add_argument("--force", action="store_true", help="Overwrite existing session_id")
    parser.add_argument("--all", action="store_true", help="Process all tasks with missing session_id")
    args = parser.parse_args()

    if not args.task_ids and not args.all:
        parser.error("provide task IDs or --all")

    conn = sqlite3.connect(DB_PATH)

    if args.all:
        rows = conn.execute(
            "SELECT id FROM tasks WHERE session_id IS NULL AND log_file IS NOT NULL ORDER BY id"
        ).fetchall()
        task_ids = [r[0] for r in rows]
        print(f"found {len(task_ids)} tasks with missing session_id")
    else:
        task_ids = args.task_ids

    ok = 0
    for task_id in task_ids:
        if process_task(conn, task_id, dry_run=args.dry_run, force=args.force):
            ok += 1

    conn.close()
    print(f"\nprocessed {ok}/{len(task_ids)} tasks")


if __name__ == "__main__":
    main()
