"""Multi-task live log viewer — ``gza tv``."""

import argparse
import json
import os
import time
from pathlib import Path

from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape as rich_escape
from rich.panel import Panel
from rich.text import Text

import gza.colors as _colors
from ..config import Config
from ..console import console, format_duration, truncate
from ..db import SqliteTaskStore, Task as DbTask
from ..workers import WorkerRegistry
from ._common import get_store, resolve_id
from .log import _resolve_task_log_path

# Sensible upper bound — beyond this the rows become unreadable.
MAX_SLOTS = 8
DEFAULT_SLOTS = 4


def _status_color(status: str) -> str:
    """Return the themed color for a task status string."""
    return getattr(_colors.STATUS_COLORS, status.replace("-", "_"), _colors.SHOW_COLORS.value)


def _themed_console() -> Console:
    """Build a throwaway Console with the active Rich theme for rendering."""
    from ..colors import build_rich_theme
    return Console(theme=build_rich_theme(), highlight=True)


def _extract_log_lines(log_path: Path, n: int) -> list[str]:
    """Read the last *n* meaningful display lines from a JSONL task log."""
    if not log_path.exists():
        return ["(log file not found)"]

    try:
        with open(log_path) as f:
            raw_lines = f.readlines()
    except OSError:
        return ["(unable to read log)"]

    display: list[str] = []
    for raw in raw_lines:
        raw = raw.strip()
        if not raw or raw.startswith("---"):
            continue
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            display.append(raw[:200])
            continue

        display.extend(_summarize_entry(entry))

    return display[-n:] if display else ["(no log output)"]


def _summarize_entry(entry: dict) -> list[str]:
    """Turn a single JSON log entry into zero or more display lines.

    Handles both provider log formats:
    - Codex/OpenAI: ``item.completed`` with ``agent_message`` / ``command_execution``
    - Claude: ``assistant`` with ``content`` blocks (``text`` / ``tool_use``)
    """
    etype = entry.get("type")

    # --- Codex / OpenAI format ---
    if etype == "item.completed":
        item = entry.get("item", {})
        if not isinstance(item, dict):
            return []
        itype = item.get("type")
        if itype == "agent_message":
            text = item.get("text", "")
            if isinstance(text, str) and text.strip():
                return _text_to_lines(text)
            return []
        if itype == "command_execution":
            cmd = item.get("command", "")
            lines: list[str] = []
            if cmd:
                short = _strip_shell_wrapper(cmd)
                lines.append(f"→ $ {truncate(short, 120)}")
            output = item.get("aggregated_output", "")
            if isinstance(output, str) and output.strip():
                for ol in output.strip().splitlines()[-3:]:
                    if ol.strip():
                        lines.append(f"  {truncate(ol.strip(), 120)}")
            return lines
        return []

    # --- Claude format ---
    if etype == "assistant":
        message = entry.get("message", {})
        content = message.get("content", [])
        lines = []
        if isinstance(content, str):
            lines.extend(_text_to_lines(content))
        elif isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "text":
                    text = block.get("text", "").strip()
                    if text:
                        lines.extend(_text_to_lines(text))
                elif block.get("type") == "tool_use":
                    name = block.get("name", "tool")
                    inp = block.get("input", {})
                    lines.append(f"→ {_tool_one_liner(name, inp)}")
        return lines

    if etype == "gza":
        msg = entry.get("message", "")
        subtype = entry.get("subtype", "")
        prefix = f"[gza:{subtype}]" if subtype else "[gza]"
        return [f"{prefix} {msg}"] if msg else []

    if etype == "system":
        subtype = entry.get("subtype", "")
        if subtype == "init":
            model = entry.get("model", "unknown")
            return [f"Session initialized (model: {model})"]

    return []


def _text_to_lines(text: str, max_lines: int = 6) -> list[str]:
    """Extract the last *max_lines* non-empty lines from a block of text."""
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    lines = lines[-max_lines:]
    return [l[:200] for l in lines]


def _strip_shell_wrapper(cmd: str) -> str:
    """Strip ``/bin/bash -lc '...'`` wrapper to show the inner command."""
    for prefix in ('/bin/bash -lc "', "/bin/bash -lc '", '/bin/sh -c "', "/bin/sh -c '"):
        if cmd.startswith(prefix):
            inner = cmd[len(prefix):]
            if inner and inner[-1] in ('"', "'"):
                inner = inner[:-1]
            return inner
    return cmd


def _tool_one_liner(name: str, inp: dict) -> str:
    """Compact tool-use summary."""
    if name == "Bash":
        cmd = str(inp.get("command", ""))
        return f"$ {truncate(cmd, 120)}" if cmd else "$ (bash)"
    if name in ("Read", "Edit", "Write"):
        path = str(inp.get("file_path", ""))
        return f"{name} {path}"
    if name == "Grep":
        pattern = str(inp.get("pattern", ""))
        path = str(inp.get("path", ""))
        return f"Grep {truncate(pattern, 40)} [{path}]"
    if name == "Glob":
        return f"Glob {inp.get('pattern', '')}"
    return name


def _lines_per_panel(n_tasks: int) -> int:
    """Compute how many log lines each panel gets based on terminal height."""
    try:
        term_height = os.get_terminal_size().lines
    except OSError:
        term_height = 40
    # Each panel has 2 lines of border (top + bottom) + content lines.
    # Reserve 1 line for any trailing cursor / prompt.
    available = term_height - 1
    if n_tasks <= 0:
        return 10
    # panel overhead: top border + bottom border = 2 lines per panel
    content_lines = (available - (n_tasks * 2)) // n_tasks
    return max(3, content_lines)


def _build_task_panel(
    task: DbTask,
    log_path: Path | None,
    n_lines: int,
    width: int,
) -> Panel:
    """Build a Rich Panel for one task's TV row."""
    rc = _colors.RUNNER_COLORS
    sc = _status_color(task.status or "unknown")

    # --- subtitle with metadata ---
    parts: list[str] = []
    if task.task_type:
        parts.append(f"[{rc.task_type}]{task.task_type}[/{rc.task_type}]")
    parts.append(f"[{sc}]{task.status or 'unknown'}[/{sc}]")
    if task.duration_seconds is not None:
        parts.append(f"[{rc.value}]{format_duration(task.duration_seconds)}[/{rc.value}]")
    subtitle = "  ".join(parts)

    # --- log body ---
    # Render lines through a themed console so Rich highlighting applies
    # (numbers, paths, strings pick up the gza rich theme overrides).
    if log_path and log_path.exists():
        lines = _extract_log_lines(log_path, n_lines)
    else:
        lines = ["(no log available)"]

    # Pad to n_lines so panels are uniform height
    while len(lines) < n_lines:
        lines.insert(0, "")

    themed = _themed_console()
    body_parts: list[Text] = []
    for line in lines:
        with themed.capture() as cap:
            themed.print(rich_escape(truncate(line, width - 6)), end="", soft_wrap=True)
        # Re-parse the ANSI output from the themed console into a Rich Text
        body_parts.append(Text.from_ansi(cap.get()))

    body = Text("\n").join(body_parts)

    # --- title ---
    task_id = task.id or "?"
    prompt_short = truncate(task.prompt or "", 60)
    title = f"[{_colors.TASK_COLORS.task_id}]{task_id}[/{_colors.TASK_COLORS.task_id}]  {prompt_short}"

    return Panel(
        body,
        title=title,
        title_align="left",
        subtitle=subtitle,
        subtitle_align="right",
        border_style=_colors.TASK_COLORS.task_id,
        width=width,
        padding=(0, 1),
    )


def _render_all(
    tasks: list[DbTask],
    log_paths: list[Path | None],
    n_lines: int,
) -> Group:
    """Render all task panels as a Rich Group."""
    width = console.width or 120
    panels = [
        _build_task_panel(task, lp, n_lines, width)
        for task, lp in zip(tasks, log_paths)
    ]
    return Group(*panels)


def _auto_select_tasks(
    store: SqliteTaskStore,
    max_slots: int,
) -> list[DbTask]:
    """Pick up to *max_slots* in-progress tasks, most recently started first."""
    in_progress = store.get_in_progress()
    # get_in_progress returns oldest-first; we want newest-first for TV
    in_progress.reverse()
    return in_progress[:max_slots]


def cmd_tv(args: argparse.Namespace) -> int:
    """Live multi-task log dashboard."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    registry = WorkerRegistry(config.workers_path)

    task_ids_raw: list[str] = args.task_ids or []
    max_slots: int = args.number or DEFAULT_SLOTS
    explicit_ids = len(task_ids_raw) > 0

    if explicit_ids:
        # Resolve specific tasks
        if len(task_ids_raw) > MAX_SLOTS:
            print(f"Error: too many tasks (max {MAX_SLOTS})")
            return 1
        tasks: list[DbTask] = []
        for raw_id in task_ids_raw:
            task_id = resolve_id(config, raw_id)
            task = store.get(task_id)
            if not task:
                print(f"Error: task {raw_id} not found")
                return 1
            tasks.append(task)
    else:
        # Auto-select running tasks
        tasks = _auto_select_tasks(store, max_slots)
        if not tasks:
            print("No in-progress tasks found.")
            return 0

    n_tasks = len(tasks)
    n_lines = _lines_per_panel(n_tasks)

    # Resolve initial log paths
    log_paths: list[Path | None] = []
    for task in tasks:
        log_path, _startup = _resolve_task_log_path(config, registry, task)
        log_paths.append(log_path)

    try:
        with Live(
            _render_all(tasks, log_paths, n_lines),
            console=console,
            refresh_per_second=2,
            screen=True,
        ) as live:
            while True:
                time.sleep(0.5)

                # Refresh task metadata
                for i in range(len(tasks)):
                    refreshed = store.get(tasks[i].id)  # type: ignore[arg-type]
                    if refreshed:
                        tasks[i] = refreshed

                # Recalculate lines (terminal may have resized)
                n_lines = _lines_per_panel(len(tasks))

                # Re-resolve log paths (may appear after task starts)
                for i in range(len(tasks)):
                    if log_paths[i] is None or not log_paths[i].exists():  # type: ignore[union-attr]
                        lp, _ = _resolve_task_log_path(config, registry, tasks[i])
                        log_paths[i] = lp

                live.update(_render_all(tasks, log_paths, n_lines))

                # Exit when all displayed tasks are done
                if not any(t.status == "in_progress" for t in tasks):
                    time.sleep(1)
                    live.update(_render_all(tasks, log_paths, n_lines))
                    break

    except KeyboardInterrupt:
        pass

    return 0
