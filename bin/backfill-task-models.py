#!/usr/bin/env python3
"""Backfill the model and provider columns on tasks by parsing log file init lines."""

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path


def get_db_path() -> Path:
    return Path(".gza") / "gza.db"


def infer_provider_from_model(model: str) -> str | None:
    """Infer provider name from model identifier patterns."""
    if re.match(r"claude-", model, re.IGNORECASE):
        return "claude"
    if re.match(r"gpt-|o1|o3", model, re.IGNORECASE):
        return "codex"
    if re.match(r"gemini-", model, re.IGNORECASE):
        return "gemini"
    return None


def extract_model_and_provider_from_log(log_path: Path) -> tuple[str | None, str | None]:
    """Extract model and provider from the init JSON line in a log file."""
    try:
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    if d.get("type") == "system" and d.get("subtype") == "init":
                        model = d.get("model")
                        provider = d.get("provider")
                        if model and not provider:
                            provider = infer_provider_from_model(model)
                        return model, provider
                except json.JSONDecodeError:
                    continue
    except (OSError, UnicodeDecodeError):
        pass
    return None, None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be updated"
    )
    args = parser.parse_args()

    db_path = get_db_path()
    if not db_path.exists():
        print(f"Database not found at {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, log_file FROM tasks WHERE log_file IS NOT NULL AND (model IS NULL OR provider IS NULL)"
    ).fetchall()

    updated = 0
    missing = 0

    for row in rows:
        log_file = row["log_file"]
        log_path = Path(log_file)

        model, provider = extract_model_and_provider_from_log(log_path)
        if model is None and provider is None:
            missing += 1
            continue

        if args.dry_run:
            print(f"  Task {row['id']}: model={model} provider={provider}")
        else:
            conn.execute(
                "UPDATE tasks SET model = COALESCE(model, ?), provider = COALESCE(provider, ?) WHERE id = ?",
                (model, provider, row["id"]),
            )
        updated += 1

    if not args.dry_run:
        conn.commit()

    conn.close()

    action = "Would update" if args.dry_run else "Updated"
    print(f"{action} {updated} tasks ({missing} logs missing/unparseable)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
