#!/usr/bin/env python3
"""Show git statistics for today: lines added, deleted, and commit count."""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta


def run_git(args: list[str]) -> str:
    """Run a git command and return the output."""
    result = subprocess.run(
        ["git"] + args,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error running git: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def get_author() -> str:
    """Get the current git user name."""
    return run_git(["config", "user.name"]).strip()


def get_branches_created_between(start: datetime, end: datetime) -> int:
    """Count branches created between start and end times."""
    branch_result = subprocess.run(
        [
            "git", "for-each-ref",
            "--sort=-creatordate",
            "--format=%(creatordate:unix) %(refname:short)",
            "refs/heads/",
        ],
        capture_output=True,
        text=True,
    )
    if branch_result.returncode != 0:
        return 0

    start_ts = start.timestamp()
    end_ts = end.timestamp()

    count = 0
    for line in branch_result.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split(" ", 1)
        if len(parts) >= 1:
            try:
                branch_time = int(parts[0])
                if start_ts <= branch_time < end_ts:
                    count += 1
            except ValueError:
                continue

    return count


def get_branches_created(since: str) -> int:
    """Count branches created since the given time using reflog."""
    now = datetime.now()
    if since == "midnight":
        cutoff = datetime(now.year, now.month, now.day)
    elif "hour" in since:
        hours = int(since.split()[0])
        cutoff = now - timedelta(hours=hours)
    elif "day" in since:
        days = int(since.split()[0])
        cutoff = now - timedelta(days=days)
    elif "week" in since:
        weeks = int(since.split()[0])
        cutoff = now - timedelta(weeks=weeks)
    else:
        cutoff = datetime(now.year, now.month, now.day)

    return get_branches_created_between(cutoff, now)


def get_stats_for_range(author: str, since: str, until: str | None = None) -> dict:
    """Get git stats for a time range."""
    log_args = [
        "log",
        f"--since={since}",
        f"--author={author}",
        "--pretty=tformat:",
        "--numstat",
    ]
    if until:
        log_args.insert(2, f"--until={until}")

    numstat_output = run_git(log_args)

    added = 0
    deleted = 0
    files_changed = set()

    for line in numstat_output.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) >= 3:
            if parts[0] != "-":
                added += int(parts[0])
            if parts[1] != "-":
                deleted += int(parts[1])
            files_changed.add(parts[2])

    commit_args = [
        "log",
        f"--since={since}",
        f"--author={author}",
        "--oneline",
    ]
    if until:
        commit_args.insert(2, f"--until={until}")

    commit_output = run_git(commit_args)
    commits = len([line for line in commit_output.strip().split("\n") if line])

    return {
        "added": added,
        "deleted": deleted,
        "files": len(files_changed),
        "commits": commits,
    }


def get_stats(since: str, author: str) -> dict:
    """Get git stats since the given time."""
    stats = get_stats_for_range(author, since)
    stats["branches"] = get_branches_created(since)
    return stats


def print_stats(label: str, stats: dict) -> None:
    """Print stats for a single period."""
    net = stats["added"] - stats["deleted"]
    net_str = f"+{net}" if net >= 0 else str(net)
    branches_str = f"  Branches: {stats['branches']}" if "branches" in stats else ""

    print(f"{label}")
    print(f"  Commits:  {stats['commits']}")
    if branches_str:
        print(branches_str)
    print(f"  Files:    {stats['files']}")
    print(f"  Added:    +{stats['added']}")
    print(f"  Deleted:  -{stats['deleted']}")
    print(f"  Net:      {net_str}")


def get_per_day_stats(days: int, author: str) -> list[tuple[str, dict]]:
    """Get stats broken down by day."""
    results = []
    now = datetime.now()
    today = datetime(now.year, now.month, now.day)

    for i in range(days):
        day_start = today - timedelta(days=i)
        day_end = day_start + timedelta(days=1)

        since_str = day_start.strftime("%Y-%m-%d 00:00:00")
        until_str = day_end.strftime("%Y-%m-%d 00:00:00")

        stats = get_stats_for_range(author, since_str, until_str)
        stats["branches"] = get_branches_created_between(day_start, day_end)

        day_label = day_start.strftime("%a %Y-%m-%d")
        if i == 0:
            day_label += " (today)"
        elif i == 1:
            day_label += " (yesterday)"

        results.append((day_label, stats))

    return results


def main():
    parser = argparse.ArgumentParser(description="Show git statistics")
    parser.add_argument(
        "--since",
        default="midnight",
        help="Show stats since this time (default: midnight)",
    )
    parser.add_argument(
        "--author",
        default=None,
        help="Filter by author (default: current git user)",
    )
    parser.add_argument(
        "--per-day",
        action="store_true",
        help="Show stats broken down per day (use with --since)",
    )
    args = parser.parse_args()

    author = args.author or get_author()

    if args.per_day:
        # Calculate how many days to show based on --since
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)

        since = args.since
        if since == "midnight":
            num_days = 1
        elif "hour" in since:
            hours = int(since.split()[0])
            num_days = max(1, (hours + 23) // 24)  # Round up to days
        elif "day" in since:
            num_days = int(since.split()[0])
        elif "week" in since:
            num_days = int(since.split()[0]) * 7
        elif "month" in since:
            num_days = int(since.split()[0]) * 30
        else:
            num_days = 7  # Default fallback

        print(f"Git stats per day for {author} (last {num_days} days):\n")
        daily_stats = get_per_day_stats(num_days, author)

        totals = {"commits": 0, "branches": 0, "files": 0, "added": 0, "deleted": 0}

        for day_label, stats in daily_stats:
            print_stats(day_label, stats)
            print()
            totals["commits"] += stats["commits"]
            totals["branches"] += stats["branches"]
            totals["added"] += stats["added"]
            totals["deleted"] += stats["deleted"]

        # Print totals
        print("-" * 40)
        totals["files"] = "—"  # Can't sum files (would double-count)
        print_stats("TOTAL", totals)
    else:
        stats = get_stats(args.since, author)
        print_stats(f"Git stats since {args.since} for {author}:", stats)


if __name__ == "__main__":
    main()
