"""Multi-task live log viewer — ``gza tv``."""

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape as rich_escape
from rich.panel import Panel
from rich.text import Text

import gza.colors as _colors

from ..config import Config
from ..console import console, format_duration, shorten_prompt, truncate
from ..db import SqliteTaskStore, Task as DbTask
from ..providers.output_formatter import format_token_count
from ..workers import WorkerRegistry
from ._common import get_store, resolve_id
from .log import _resolve_task_log_path


@dataclass
class _LogStats:
    step_count: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0

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


def _scan_log(log_path: Path, n: int) -> tuple[list[str], _LogStats]:
    """Read the last *n* display lines and accumulate stats from a JSONL task log."""
    stats = _LogStats()
    if not log_path.exists():
        return ["(log file not found)"], stats

    try:
        with open(log_path) as f:
            raw_lines = f.readlines()
    except OSError:
        return ["(unable to read log)"], stats

    seen_msg_ids: set[str] = set()
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

        prev_steps = stats.step_count
        _accumulate_stats(entry, stats, seen_msg_ids)
        if stats.step_count > prev_steps and display:
            display.append("")
        display.extend(_summarize_entry(entry))

    if not display:
        display = ["(no log output)"]
    return display[-n:], stats


def _accumulate_stats(entry: dict, stats: _LogStats, seen_msg_ids: set[str]) -> None:
    """Update running stats from a single log entry (mirrors ``_LiveLogPrinter``)."""
    etype = entry.get("type")
    if etype == "assistant":
        message = entry.get("message", {}) or {}
        msg_id = message.get("id")
        if msg_id and msg_id not in seen_msg_ids:
            seen_msg_ids.add(msg_id)
            stats.step_count += 1
            usage = message.get("usage", {}) or {}
            stats.total_tokens += usage.get("input_tokens", 0) or 0
            stats.total_tokens += usage.get("cache_creation_input_tokens", 0) or 0
            stats.total_tokens += usage.get("cache_read_input_tokens", 0) or 0
            stats.total_tokens += usage.get("output_tokens", 0) or 0
    elif etype == "turn.started":
        stats.step_count += 1
    elif etype == "result":
        # Final cost, when provider emits it.
        cost = entry.get("total_cost_usd") or entry.get("cost_usd")
        if isinstance(cost, (int, float)):
            stats.cost_usd = float(cost)


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
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    lines = lines[-max_lines:]
    return [line[:200] for line in lines]


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


def _task_elapsed_seconds(task: DbTask) -> float | None:
    """Live elapsed time: use recorded duration when set, else now - started_at."""
    if task.duration_seconds is not None:
        return task.duration_seconds
    if task.started_at is not None:
        started = task.started_at
        if started.tzinfo is None:
            started = started.replace(tzinfo=UTC)
        return (datetime.now(UTC) - started).total_seconds()
    return None


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

    # --- log body & live stats ---
    # Render lines through a themed console so Rich highlighting applies
    # (numbers, paths, strings pick up the gza rich theme overrides).
    if log_path and log_path.exists():
        lines, log_stats = _scan_log(log_path, n_lines)
    else:
        lines, log_stats = ["(no log available)"], _LogStats()

    # --- subtitle with metadata ---
    parts: list[str] = []
    if task.task_type:
        parts.append(f"[{rc.task_type}]{task.task_type}[/{rc.task_type}]")
    parts.append(f"[{sc}]{task.status or 'unknown'}[/{sc}]")

    elapsed = _task_elapsed_seconds(task)
    if elapsed is not None:
        parts.append(f"[{rc.value}]{format_duration(elapsed)}[/{rc.value}]")

    steps = task.num_steps_reported or task.num_steps_computed or log_stats.step_count
    if steps:
        parts.append(f"[{rc.value}]{steps} steps[/{rc.value}]")

    if log_stats.total_tokens:
        parts.append(f"[{rc.value}]{format_token_count(log_stats.total_tokens)}[/{rc.value}]")

    cost = task.cost_usd if task.cost_usd is not None else log_stats.cost_usd
    if cost:
        parts.append(f"[{rc.value}]${cost:.2f}[/{rc.value}]")

    subtitle = "  ".join(parts)

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

    # --- title: prompt styled in themed "prompt" color (pink in minimal),
    # expanded to fill the panel width using the centralized width helper.
    task_id = task.id or "?"
    prompt_color = _colors.TASK_COLORS.prompt
    # Panel borders (2) + padding (2) + 2 spaces between id and prompt.
    prefix_chars = len(task_id) + 2
    suffix_chars = 0
    # Fit the prompt into the panel's width specifically (not the whole
    # terminal), since each row may be narrower than the full screen.
    border_and_padding = 4
    available = max(20, width - border_and_padding - prefix_chars - suffix_chars)
    prompt_display = shorten_prompt(task.prompt or "", available=available)
    title = (
        f"[{_colors.TASK_COLORS.task_id}]{task_id}[/{_colors.TASK_COLORS.task_id}]  "
        f"[{prompt_color}]{rich_escape(prompt_display)}[/{prompt_color}]"
    )

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
                    current_task_id = tasks[i].id
                    if current_task_id is None:
                        continue
                    refreshed = store.get(current_task_id)
                    if refreshed:
                        tasks[i] = refreshed

                # Recalculate lines (terminal may have resized)
                n_lines = _lines_per_panel(len(tasks))

                # Re-resolve log paths (may appear after task starts)
                for i in range(len(tasks)):
                    log_path = log_paths[i]
                    if log_path is None or not log_path.exists():
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
