"""Shared rendering helpers for queue-style task listings."""

from __future__ import annotations

from dataclasses import dataclass

from rich.text import Text

import gza.colors as _colors

from ..console import prompt_available_width, shorten_prompt
from ..db import Task as DbTask


@dataclass(frozen=True)
class QueueRenderRow:
    """Display-ready queue row data shared by `gza queue` and `gza next`."""

    task: DbTask
    position_text: str
    blocked: bool = False
    blocked_by_text: str | None = None


@dataclass(frozen=True)
class QueueRenderWidths:
    """Fixed-width columns for queue row alignment."""

    position: int
    task_id: int
    task_type: int


def _format_prompt_for_width(prompt: str, *, prefix: int = 0, suffix: int = 0) -> str:
    available = prompt_available_width(prefix=prefix, suffix=suffix)
    return shorten_prompt(prompt, available)


def queue_render_widths(rows: list[QueueRenderRow]) -> QueueRenderWidths:
    """Return shared column widths for a queue listing."""
    if not rows:
        return QueueRenderWidths(position=1, task_id=1, task_type=len("[implement]"))
    return QueueRenderWidths(
        position=max(1, max(len(row.position_text) for row in rows)),
        task_id=max(len(str(row.task.id)) for row in rows),
        task_type=max(len(f"[{row.task.task_type}]") for row in rows),
    )


def build_queue_row_renderables(
    row: QueueRenderRow,
    *,
    widths: QueueRenderWidths,
) -> tuple[Text, Text | None]:
    """Build first/second-line renderables for a queue row."""
    colors = _colors.QUEUE_COLORS
    task = row.task
    task_id = str(task.id)
    type_chip = f"[{task.task_type}]"
    prefix = (
        f"{row.position_text:>{widths.position}}  "
        f"{task_id:<{widths.task_id}}  "
        f"{type_chip:<{widths.task_type}}  "
    )
    prompt = _format_prompt_for_width(task.prompt, prefix=len(prefix))

    first_line = Text()
    first_line.append(
        f"{row.position_text:>{widths.position}}",
        style=colors.blocked_marker if row.blocked else colors.position,
    )
    first_line.append("  ")
    first_line.append(f"{task_id:<{widths.task_id}}", style=colors.task_id)
    first_line.append("  ")
    first_line.append(f"{type_chip:<{widths.task_type}}", style=colors.task_type)
    first_line.append("  ")
    first_line.append(prompt, style=colors.prompt)

    extras: list[tuple[str, str]] = []
    if task.urgent:
        extras.append(("[urgent]", colors.urgent))
    if task.queue_position is not None:
        extras.append((f"[#{task.queue_position}]", colors.explicit_position))
    if row.blocked and row.blocked_by_text:
        extras.append((row.blocked_by_text, colors.blocked_by))

    if not extras:
        return first_line, None

    second_line = Text(" " * len(prefix))
    for idx, (text, style) in enumerate(extras):
        if idx:
            second_line.append("  ")
        second_line.append(text, style=style)
    return first_line, second_line


def build_queue_summary(message: str) -> Text:
    """Return a themed summary line for queue listings."""
    return Text(message, style=_colors.QUEUE_COLORS.summary)


def build_blocked_count_summary(count: int) -> Text:
    """Return a themed blocked-count summary for `gza next`."""
    plural = "tasks" if count != 1 else "task"
    return Text(f"({count} {plural} blocked by dependencies)", style=_colors.QUEUE_COLORS.blocked_by)


def print_queue_rows(
    render_console,
    rows: list[QueueRenderRow],
    *,
    widths: QueueRenderWidths,
) -> None:
    """Print queue rows to a Rich-compatible console."""
    for row in rows:
        first_line, second_line = build_queue_row_renderables(row, widths=widths)
        render_console.print(first_line)
        if second_line is not None:
            render_console.print(second_line)
