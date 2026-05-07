"""Presentation helpers for TaskQuery results."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Literal

from gza.console import shorten_prompt
from gza.query import TaskLineageNode
from gza.task_query import LineageRow, TaskQueryResult, TaskRow

PresentationMode = Literal["flat", "grouped", "lineage", "tree", "one_line", "json", "rich"]


def render_query_result(result: TaskQueryResult, mode: PresentationMode) -> str:
    """Render a query result into human-readable text."""
    if mode == "json":
        return json.dumps(result.to_json(), indent=2, default=_json_default)

    if mode == "one_line":
        return _render_one_line(result)

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
                elif task.status == "completed" and task.completion_reason:
                    snippet = f"{snippet} ({task.completion_reason})"
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
        lines.append(f"⚡ {' '.join(header_bits)} {values.get('prompt', task.prompt)}".rstrip())

        tree_text = values.get("lineage_text")
        if not isinstance(tree_text, str):
            tree_text = _render_tree(
                TaskQueryResult(query=result.query, rows=(row,), total_count=1)
            )
        if tree_text:
            lines.append("lineage:")
            lines.extend(tree_text.splitlines())

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
                if score is not None:
                    review_line = f"{review_line} [{review_verdict} ({score})]"
                else:
                    review_line = f"{review_line} [{review_verdict}]"
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


__all__ = ["render_query_result"]
