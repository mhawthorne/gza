#!/usr/bin/env python3
"""Show all tasks downstream of a given task ID."""

import argparse
import os
import sqlite3
from pathlib import Path


def get_db_path() -> Path:
    return Path(".gza") / "gza.db"


def get_downstream_tasks(task_id: int) -> list[tuple]:
    """Get all tasks downstream of the given task ID (via based_on or depends_on)."""
    conn = sqlite3.connect(get_db_path())
    c = conn.cursor()
    c.execute(
        """
        WITH RECURSIVE downstream(id) AS (
            SELECT ?
            UNION
            SELECT t.id FROM tasks t, downstream d
            WHERE t.based_on = d.id OR t.depends_on = d.id
        )
        SELECT t.id, t.based_on, t.depends_on, t.status, t.task_type,
               t.started_at, t.completed_at, t.num_turns, t.session_id, t.prompt
        FROM tasks t
        WHERE t.id IN downstream
        ORDER BY t.id
        """,
        (task_id,),
    )
    rows = c.fetchall()
    conn.close()
    return rows


def main():
    parser = argparse.ArgumentParser(description="Show all tasks downstream of a given task ID")
    parser.add_argument("task_id", type=int, help="Root task ID")
    parser.add_argument("--show-prompt", action="store_true", help="Show task prompts on a separate line")
    parser.add_argument("--prompt-truncation", type=int, default=None, help="Truncate prompts to N chars (0 or negative for full prompt)")
    args = parser.parse_args()

    db_path = get_db_path()
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return 1

    rows = get_downstream_tasks(args.task_id)
    if not rows:
        print(f"No tasks found for task #{args.task_id}")
        return 1

    header = f"{'ID':>4}  {'Based':>5}  {'Deps':>5}  {'Status':<10}  {'Type':<12}  {'Started':<17}  {'Completed':<17}  {'Turns':>5}  {'Session ID':<40}"
    print(header)
    print("-" * len(header))
    for row in rows:
        id_, based_on, depends_on, status, task_type, started_at, completed_at, num_turns, session_id, prompt = row
        based_str = str(based_on) if based_on else "-"
        deps_str = str(depends_on) if depends_on else "-"
        task_type = task_type or "-"
        started_str = started_at[:16] if started_at else "-"
        completed_str = completed_at[:16] if completed_at else "-"
        turns_str = str(num_turns) if num_turns else "-"
        session_str = session_id if session_id else "-"
        print(f"{id_:>4}  {based_str:>5}  {deps_str:>5}  {status:<10}  {task_type:<12}  {started_str:<17}  {completed_str:<17}  {turns_str:>5}  {session_str:<40}")
        if args.show_prompt and prompt:
            truncate_len = args.prompt_truncation if args.prompt_truncation is not None else len(header)
            if truncate_len > 0:
                display_prompt = prompt.split('\n')[0]
                if len(display_prompt) > truncate_len:
                    display_prompt = display_prompt[:truncate_len]
            else:
                display_prompt = prompt
            print(display_prompt)
            print()

    return 0


if __name__ == "__main__":
    exit(main())
