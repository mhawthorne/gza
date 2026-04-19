#!/usr/bin/env python3
"""Triage recently-failed tasks by lineage merge status.

For each failed task (bounded by --limit or --since), resolves the
lineage root, groups by merged vs unmerged, and prints a summary so
you can decide the next step. Merged roots get a one-line "done";
unmerged roots get a lineage tree.

Usage:
    scripts/triage_failed.py --limit 20
    scripts/triage_failed.py --since 2026-04-15
    scripts/triage_failed.py --since 2026-04-15 --limit 50
"""

import argparse
import sys
from datetime import UTC, datetime

from gza.db import SqliteTaskStore, Task
from gza.query import build_lineage_tree, resolve_lineage_root


def _parse_since(value: str) -> datetime:
    fmt_candidates = ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S")
    for fmt in fmt_candidates:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Cannot parse --since value: {value!r}")


def _render_tree(store: SqliteTaskStore, root: Task) -> str:
    """Minimal ASCII lineage rendering — avoids pulling in Rich."""
    tree = build_lineage_tree(store, root, max_depth=None)
    lines: list[str] = []

    def _label(task: Task) -> str:
        status = task.status or "unknown"
        if task.status == "failed" and task.failure_reason:
            status = f"failed({task.failure_reason})"
        merge = f" [merged]" if task.merge_status == "merged" else ""
        first_line = (task.prompt or "").split("\n", 1)[0].strip()
        if len(first_line) > 60:
            first_line = first_line[:60] + "…"
        return f"{task.id} [{task.task_type or '?'}] {status}{merge}  '{first_line}'"

    def _walk(node, prefix: str) -> None:
        for i, child in enumerate(node.children):
            is_last = i == len(node.children) - 1
            branch = "└── " if is_last else "├── "
            lines.append(f"{prefix}{branch}{_label(child.task)}")
            _walk(child, prefix + ("    " if is_last else "│   "))

    lines.append(_label(tree.task))
    _walk(tree, "")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="Max failed tasks to inspect (newest first)")
    parser.add_argument("--since", type=_parse_since, default=None, help="Only failed tasks with completed_at >= this (UTC)")
    args = parser.parse_args()

    if args.limit is None and args.since is None:
        parser.error("Specify at least one of --limit or --since")

    store = SqliteTaskStore.default()
    failed = store.get_history(
        limit=args.limit,
        status="failed",
        since=args.since,
    )
    if not failed:
        print("No failed tasks match the filter.")
        return 0

    # Group by lineage root.
    roots_by_id: dict[str, Task] = {}
    failures_by_root: dict[str, list[Task]] = {}
    for task in failed:
        root = resolve_lineage_root(store, task)
        if root.id is None:
            continue
        roots_by_id.setdefault(root.id, root)
        failures_by_root.setdefault(root.id, []).append(task)

    merged: list[tuple[Task, list[Task]]] = []
    unmerged: list[tuple[Task, list[Task]]] = []
    for root_id, root in roots_by_id.items():
        entries = failures_by_root[root_id]
        # A root is "merged" if itself or any implement descendant is merged.
        # Simplest: check root.merge_status; also walk impl descendants.
        root_is_merged = root.merge_status == "merged"
        if not root_is_merged:
            tree = build_lineage_tree(store, root, max_depth=None)
            stack = [tree]
            while stack:
                node = stack.pop()
                if node.task.task_type == "implement" and node.task.merge_status == "merged":
                    root_is_merged = True
                    break
                stack.extend(node.children)
        if root_is_merged:
            merged.append((root, entries))
        else:
            unmerged.append((root, entries))

    print(f"Inspected {len(failed)} failed task(s) → {len(roots_by_id)} lineage root(s).\n")

    if merged:
        print(f"== Merged ({len(merged)}): no action needed ==")
        for root, entries in merged:
            failed_ids = ", ".join(t.id or "?" for t in entries)
            print(f"  {root.id} [{root.task_type}] merged — failed descendants: {failed_ids}")
        print()

    if unmerged:
        print(f"== Unmerged ({len(unmerged)}): needs attention ==")
        for root, entries in unmerged:
            failed_ids = ", ".join(t.id or "?" for t in entries)
            print(f"\n- {root.id} [{root.task_type}] (failed descendants: {failed_ids})")
            print(_render_tree(store, root))
    return 0


if __name__ == "__main__":
    sys.exit(main())
