"""Presentation helpers for TaskQuery results."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Literal

from gza.console import shorten_prompt
from gza.query import TaskLineageNode
from gza.task_query import LineageRow, TaskQueryResult, TaskRow

PresentationMode = Literal["flat", "grouped", "tree", "one_line", "json"]


def render_query_result(result: TaskQueryResult, mode: PresentationMode) -> str:
    """Render a query result into human-readable text."""
    if mode == "json":
        return json.dumps(result.to_json(), indent=2, default=_json_default)

    if mode == "one_line":
        return _render_one_line(result)

    if mode == "tree":
        return _render_tree(result)

    if mode == "grouped":
        return _render_grouped(result)

    return _render_flat(result)


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _render_flat(result: TaskQueryResult) -> str:
    lines: list[str] = []
    for row in result.rows:
        if isinstance(row, TaskRow):
            task_row: TaskRow = row
            task = task_row.task
            status = str(task_row.values.get("status", task.status))
            task_id = task.id or "unknown"
            prompt = shorten_prompt(task.prompt, 100)
            lines.append(f"{status:<11} {task_id} {prompt}")
        else:
            lineage_row: LineageRow = row
            owner = lineage_row.owner_task
            status = str(lineage_row.values.get("status", owner.status))
            owner_id = owner.id or "unknown"
            prompt = shorten_prompt(owner.prompt, 100)
            lines.append(f"{status:<11} {owner_id} {prompt}")
    return "\n".join(lines)


def _render_grouped(result: TaskQueryResult) -> str:
    return _render_flat(result)


def _render_tree(result: TaskQueryResult) -> str:
    lines: list[str] = []
    for index, row in enumerate(result.rows):
        if not isinstance(row, LineageRow) or row.tree is None:
            continue
        if index > 0:
            lines.append("-" * 32)

        def _walk(node: TaskLineageNode, prefix: str = "", is_last: bool = True) -> None:
            task = node.task
            connector = ""
            child_prefix = ""
            if prefix:
                connector = "└── " if is_last else "├── "
                child_prefix = prefix + ("    " if is_last else "│   ")
            task_id = task.id or "unknown"
            status = task.status
            prompt = shorten_prompt(task.prompt, 90)
            lines.append(f"{prefix}{connector}{status:<11} {task_id} {prompt}")
            for child_index, child in enumerate(node.children):
                _walk(child, child_prefix, child_index == len(node.children) - 1)

        _walk(row.tree)
    return "\n".join(lines)


def _render_one_line(result: TaskQueryResult) -> str:
    lines: list[str] = []
    for row in result.rows:
        if isinstance(row, LineageRow):
            lineage_row: LineageRow = row
            owner = lineage_row.owner_task
            owner_id = owner.id or "unknown"
            reason = lineage_row.values.get("next_action_reason")
            unresolved_parts: list[str] = []
            unresolved_ids = {task.id for task in lineage_row.unresolved_tasks if task.id is not None}
            for task in lineage_row.unresolved_tasks:
                if task.id is None:
                    continue
                snippet = task.prompt
                if task.status == "failed" and task.failure_reason and task.failure_reason != "UNKNOWN":
                    snippet = f"{snippet} ({task.failure_reason})"
                elif task.status == "dropped":
                    snippet = f"{snippet} (dropped)"
                unresolved_parts.append(snippet)
            unresolved_text = ""
            if unresolved_parts:
                unresolved_text = " | unresolved: " + "; ".join(unresolved_parts)
            context_parts: list[str] = []
            for task in lineage_row.members:
                if task.id is None or task.id in unresolved_ids:
                    continue
                if task.prompt == owner.prompt:
                    continue
                context_parts.append(task.prompt)
            context_text = ""
            if context_parts:
                context_text = " | context: " + "; ".join(context_parts)

            if isinstance(reason, str) and reason:
                lines.append(f"{owner_id}: {reason} — {owner.prompt}{unresolved_text}{context_text}")
                continue

            unresolved_count = len(lineage_row.unresolved_tasks)
            lines.append(
                f"{owner_id}: unresolved lineage ({unresolved_count} task{'s' if unresolved_count != 1 else ''})"
                f" — {owner.prompt}{unresolved_text}{context_text}"
            )
            continue

        task_row: TaskRow = row
        task = task_row.task
        lines.append(f"{task.id or 'unknown'}: {task.status}")

    return "\n".join(lines)


__all__ = ["render_query_result"]
