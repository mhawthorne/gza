"""Presentation helpers for TaskQuery results."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Literal

import gza.colors as _colors
from gza.console import shorten_prompt, truncate
from gza.db import Task as DbTask
from gza.lineage_grouping import format_lineage_summary
from gza.query import TaskLineageNode
from gza.task_query import (
    LineageRow,
    TaskQueryResult,
    TaskRow,
    projection_fields,
)

PresentationMode = Literal["flat", "blocks", "grouped", "lineage", "tree", "one_line", "json", "rich"]


def render_query_result(result: TaskQueryResult, mode: PresentationMode) -> str:
    """Render a query result into human-readable text."""
    if mode == "json":
        return json.dumps(result.to_json(), indent=2, default=_json_default)

    if mode == "one_line":
        return _render_one_line(result)

    if mode == "blocks":
        return _render_blocks(result)

    if mode == "rich":
        return _render_rich(result)

    if mode in {"tree", "lineage"}:
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
    incomplete_view = bool(
        result.query.lifecycle_state and "incomplete" in result.query.lifecycle_state
    )
    for index, row in enumerate(result.rows):
        if not isinstance(row, LineageRow) or row.tree is None:
            continue
        if index > 0:
            lines.append("-" * 32)

        unresolved_ids = {
            task.id for task in row.unresolved_tasks
            if task.id is not None
        }

        def _walk(
            node: TaskLineageNode,
            prefix: str = "",
            *,
            is_last: bool = True,
            is_root: bool = False,
        ) -> None:
            task = node.task
            connector = "" if is_root else ("└── " if is_last else "├── ")
            child_prefix = prefix + ("    " if is_last else "│   ")
            task_id = task.id or "unknown"
            status = task.status
            if incomplete_view and task.id is not None and task.id not in unresolved_ids:
                status = "resolved"
            prompt = shorten_prompt(task.prompt, 90)
            lines.append(f"{prefix}{connector}{status:<11} {task_id} {prompt}")
            for child_index, child in enumerate(node.children):
                _walk(
                    child,
                    child_prefix,
                    is_last=child_index == len(node.children) - 1,
                    is_root=False,
                )

        _walk(row.tree, is_root=True)
    return "\n".join(lines)


def _render_one_line(result: TaskQueryResult) -> str:
    lines: list[str] = []
    incomplete_view = bool(
        result.query.lifecycle_state and "incomplete" in result.query.lifecycle_state
    )
    for row in result.rows:
        if isinstance(row, LineageRow):
            lineage_row: LineageRow = row
            owner = lineage_row.owner_task
            values = lineage_row.values
            owner_id = str(values.get("id", owner.id or "unknown"))
            owner_prompt = _headline_prompt(str(values.get("prompt", owner.prompt)))
            reason = lineage_row.values.get("next_action_reason")
            unresolved_text = _render_unresolved_summary(lineage_row.unresolved_tasks)

            if isinstance(reason, str) and reason:
                if incomplete_view:
                    parked_prereq_reason = (
                        "SKIP: legacy dependency-merge failure is parked; "
                        "wait for dependency merge state to reconcile"
                    )
                    if reason == parked_prereq_reason:
                        reason = "SKIP: legacy prereq parked"
                prefix = f"{owner_id}: {reason} — "
                lines.append(f"{prefix}{owner_prompt}{unresolved_text}")
                continue

            unresolved_count = len(lineage_row.unresolved_tasks)
            lines.append(
                f"{owner_id}: unresolved lineage ({unresolved_count} task{'s' if unresolved_count != 1 else ''})"
                f" — {owner_prompt}{unresolved_text}"
            )
            continue

        task_row: TaskRow = row
        task = task_row.task
        lines.append(f"{task.id or 'unknown'}: {task.status}")

    return "\n".join(lines)


def _render_unresolved_summary(unresolved_tasks: tuple[DbTask, ...]) -> str:
    if len(unresolved_tasks) <= 1:
        return ""

    parts: list[str] = []
    for task in unresolved_tasks:
        task_id = getattr(task, "id", None) or "unknown"
        reason = _unresolved_reason(task)
        parts.append(f"{task_id} ({reason})" if reason else task_id)
    return " | unresolved: " + "; ".join(parts)


def _unresolved_reason(task: object) -> str | None:
    status = getattr(task, "status", None)
    if status == "failed":
        failure_reason = getattr(task, "failure_reason", None)
        if isinstance(failure_reason, str) and failure_reason and failure_reason != "UNKNOWN":
            return failure_reason
        return None
    if status == "completed":
        completion_reason = getattr(task, "completion_reason", None)
        if isinstance(completion_reason, str) and completion_reason:
            return completion_reason
        return None
    if status == "dropped":
        return "dropped"
    return None


def _render_rich(result: TaskQueryResult) -> str:
    lines: list[str] = []
    total_rows = len(result.rows)
    for index, row in enumerate(result.rows):
        if not isinstance(row, LineageRow):
            continue
        values = row.values
        task = row.owner_task
        if index > 0:
            lines.append("")
            lines.append("-" * 32)
            lines.append("")

        header_bits = [str(values.get("id", task.id or "unknown"))]
        completed_at = values.get("completed_at")
        if isinstance(completed_at, datetime):
            header_bits.append(f"({completed_at.strftime('%Y-%m-%d %H:%M')})")
        header_prompt = _headline_prompt(str(values.get("prompt", task.prompt)))
        lines.append(f"⚡ {' '.join(header_bits)} {header_prompt}".rstrip())

        if values.get("lineage_task_count") is not None:
            lines.append(f"lineage: {format_lineage_summary(values)}")

        branch = values.get("branch")
        if branch:
            branch_bits = [str(branch)]
            commit_count = values.get("commit_count")
            if commit_count is not None:
                branch_bits.append(f"{commit_count} commit{'s' if commit_count != 1 else ''}")
            files_changed = values.get("files_changed")
            insertions = values.get("insertions")
            deletions = values.get("deletions")
            if files_changed is not None and insertions is not None and deletions is not None:
                branch_bits.append(f"+{insertions}/-{deletions} LOC, {files_changed} files")
            if values.get("branch_deleted"):
                branch_bits.append("branch deleted")
            if values.get("has_conflicts"):
                branch_bits.append("has conflicts")
            lines.append(f"branch: {', '.join(branch_bits)}")
            if values.get("has_conflicts"):
                conflicts_text = _style_unmerged_field_value(
                    "has_conflicts",
                    "has conflicts",
                    "has conflicts",
                )
                lines.append(f"merge: {conflicts_text}")

        pr_url = values.get("pr_url")
        if pr_url:
            lines.append(f"pr: {pr_url}")

        review_status = values.get("review_status")
        if review_status:
            review_line = str(review_status)
            review_detail = values.get("review_detail")
            if review_detail:
                review_line = f"{review_line} ({review_detail})"
            review_verdict = values.get("review_verdict")
            if review_verdict:
                score = values.get("review_score")
                review_badge = str(review_verdict)
                if score is not None:
                    review_badge = f"{review_badge} ({score})"
                review_badge = _style_unmerged_field_value("review_verdict", review_badge, review_verdict)
                review_line = f"{review_line} [{review_badge}]"
            lines.append(f"review: {review_line}")

        report_file = values.get("report_file")
        if report_file:
            lines.append(f"report: {report_file}")

        stats = values.get("stats")
        if stats:
            lines.append(f"stats: {stats}")

        failure_reason = values.get("failure_reason")
        completion_reason = values.get("completion_reason")
        if failure_reason:
            lines.append(f"failure: {failure_reason}")
        elif completion_reason:
            lines.append(f"completion: {completion_reason}")

    if total_rows == 0:
        return ""
    return "\n".join(lines)


def _projection_field_order(result: TaskQueryResult) -> tuple[str, ...]:
    scope: Literal["tasks", "lineages"] = (
        "lineages" if any(isinstance(row, LineageRow) for row in result.rows) else "tasks"
    )
    return projection_fields(result.query.projection, scope=scope)


def _render_blocks(result: TaskQueryResult) -> str:
    fields = _projection_field_order(result)
    if not fields:
        return ""

    lines: list[str] = []
    single_field = len(fields) == 1
    for index, row in enumerate(result.rows):
        if index > 0 and not single_field:
            lines.append("-" * 32)
        values = row.values
        if single_field:
            rendered = _render_field_value(fields[0], values.get(fields[0]))
            lines.append(rendered)
            continue
        for field_name in fields:
            lines.append(f"{field_name}: {_render_field_value(field_name, values.get(field_name))}")
    return "\n".join(lines)


def _render_field_value(field_name: str, value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return ", ".join("" if item is None else str(item) for item in value)
    if isinstance(value, tuple):
        return ", ".join("" if item is None else str(item) for item in value)
    return _style_unmerged_field_value(field_name, str(value), value)


def _headline_prompt(prompt: str) -> str:
    first_line = next((line.strip() for line in prompt.splitlines() if line.strip()), "")
    return truncate(first_line, 100) if first_line else shorten_prompt(prompt, 100)


def _style_unmerged_field_value(field_name: str, display_value: str, raw_value: object) -> str:
    color = _colors.get_unmerged_field_value_color(field_name, raw_value)
    if not color:
        return display_value
    return f"[{color}]{display_value}[/{color}]"


__all__ = ["render_query_result"]
