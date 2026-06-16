"""Multi-task live log viewer — ``gza tv``."""

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape as rich_escape
from rich.panel import Panel
from rich.text import Text

import gza.colors as _colors

from ..config import Config
from ..console import build_console, console, format_duration, shorten_prompt, truncate
from ..db import SqliteTaskStore, Task as DbTask
from ..providers.log_renderers import UnknownLogProviderError, get_log_renderer
from ..providers.log_rendering import RenderStats
from ..providers.output_formatter import format_token_count
from ..workers import WorkerRegistry
from ._common import get_store, resolve_id
from .log import _resolve_task_log_path

# Module-local bindings so tests can patch without touching stdlib globals.
_sleep = time.sleep
_get_terminal_size = os.get_terminal_size


# Sensible upper bound — beyond this the rows become unreadable.
MAX_SLOTS = 8
DEFAULT_MIN_SLOTS = 1
DEFAULT_MAX_SLOTS = 4
SHRINK_TICKS = 5
# Number of ticks a just-completed task stays on screen after transitioning from
# in_progress.  At 0.5 s/tick this gives ~1.5 s of post-completion visibility so
# closing log lines can render before the panel falls off.
LINGER_TICKS = 3
# Render into this percentage of the terminal so a tmux status bar / shell
# cursor can never clip the bottom panel. The leftover margin is intentional
# headroom. Integer percent (not a float) to keep the budget math exact.
HEIGHT_PERCENT = 95
# The single global header row above all panels.
HEADER_LINES = 1


def _status_color(status: str) -> str:
    """Return the themed color for a task status string."""
    return getattr(_colors.STATUS_COLORS, status.replace("-", "_"), _colors.SHOW_COLORS.value)


def _themed_console() -> Console:
    """Build a throwaway Console with the active Rich theme for rendering."""
    from ..colors import build_rich_theme
    return build_console(theme=build_rich_theme(), highlight=True)


def _scan_log(log_path: Path, n: int, provider: str | None, configured_model: str | None = None) -> tuple[list[str], RenderStats]:
    """Read the last *n* display lines and accumulate stats from a JSONL task log."""
    try:
        renderer = get_log_renderer(provider, configured_model=configured_model, verbose=False)
    except UnknownLogProviderError as exc:
        return [f"Error: {exc}"], RenderStats()
    if not log_path.exists():
        return ["(log file not found)"], renderer.stats

    try:
        with open(log_path) as f:
            raw_lines = f.readlines()
    except OSError:
        return ["(unable to read log)"], renderer.stats

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
        if not isinstance(entry, dict):
            display.append(truncate(str(entry), 200))
            continue

        prev_steps = renderer.stats.step_count
        rendered = renderer.handle_tv(entry)
        if rendered.starts_step and renderer.stats.step_count > prev_steps and display:
            display.append("")
        display.extend(rendered.tv_lines)

    if not display:
        display = ["(no log output)"]
    return display[-n:], renderer.stats


def _task_elapsed_seconds(task: DbTask) -> float | None:
    """Elapsed runtime shown in a panel's metadata.

    Only a live ``in_progress`` task ticks against the wall clock. Once a task
    finishes (completed/failed/etc.) the timer freezes: prefer the recorded
    ``duration_seconds``, else ``completed_at - started_at``. Without this a
    failed task with no recorded duration would increment forever.
    """
    if task.duration_seconds is not None:
        return task.duration_seconds
    if task.started_at is None:
        return None
    started = task.started_at
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    if task.status == "in_progress":
        return (datetime.now(UTC) - started).total_seconds()
    # Finished without a recorded duration: freeze at the completion time.
    if task.completed_at is not None:
        completed = task.completed_at
        if completed.tzinfo is None:
            completed = completed.replace(tzinfo=UTC)
        return (completed - started).total_seconds()
    return None


def _lines_per_panel(n_tasks: int) -> int:
    """Compute how many log lines each panel gets based on terminal height.

    Sizes panels to fit within ``HEIGHT_PERCENT`` of the terminal. The math is
    exact: total rendered height is ``HEADER_LINES + n*(content + 2)`` which is
    ``<= budget`` by construction, so with fixed-height, non-wrapping panels the
    ``Live(screen=True)`` view never overflows and clips the bottom row.
    """
    try:
        term_height = _get_terminal_size().lines
    except OSError:
        term_height = 40
    budget = term_height * HEIGHT_PERCENT // 100
    # Reserve the global header row; the rest is split across panels.
    available = budget - HEADER_LINES
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
    """Build a Rich Panel for one task's TV row.

    All metadata (id, type, status, elapsed, steps, tokens, cost) lives on the
    panel's top border alongside the prompt, so every task reads top-down in one
    glance. The panel is given a fixed height and a non-wrapping body so wide
    characters in the logs can never grow it past its budgeted size.
    """
    rc = _colors.RUNNER_COLORS
    tc = _colors.TASK_COLORS
    sc = _status_color(task.status or "unknown")

    # --- log body & live stats ---
    # Render lines through a themed console so Rich highlighting applies
    # (numbers, paths, strings pick up the gza rich theme overrides).
    if log_path and log_path.exists():
        lines, log_stats = _scan_log(log_path, n_lines, task.provider, task.model)
    else:
        lines, log_stats = ["(no log available)"], RenderStats()

    # --- metadata: task id + live stats, all on the top border ---
    task_id = task.id or "?"
    meta = Text()
    meta.append(task_id, style=tc.task_id)
    if task.task_type:
        meta.append("  ")
        meta.append(task.task_type, style=rc.task_type)
    meta.append("  ")
    meta.append(task.status or "unknown", style=sc)

    elapsed = _task_elapsed_seconds(task)
    if elapsed is not None:
        meta.append("  ")
        meta.append(format_duration(elapsed), style=rc.value)

    steps = task.num_steps_reported or task.num_steps_computed or log_stats.step_count
    if steps:
        meta.append("  ")
        meta.append(f"{steps} steps", style=rc.value)

    total_tokens = log_stats.input_tokens + log_stats.output_tokens
    if total_tokens:
        meta.append("  ")
        meta.append(format_token_count(total_tokens), style=rc.value)

    cost = task.cost_usd if task.cost_usd is not None else log_stats.cost_usd
    if cost:
        meta.append("  ")
        meta.append(f"${cost:.2f}", style=rc.value)

    # --- prompt fills the title width remaining after the metadata ---
    # Reserve the border corners plus the dashes/spaces Rich wraps a title in.
    # If the prompt still overruns, Rich crops the title tail (never the meta).
    title_overhead = 8
    available = max(20, width - title_overhead - meta.cell_len)
    prompt_display = shorten_prompt(task.prompt or "", available=available)
    title = meta.copy()
    if prompt_display:
        title.append("  ")
        title.append(prompt_display, style=tc.prompt)

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
    # Never wrap: a wide char must be cropped, not pushed onto a second visual
    # row that would grow the panel past its fixed height and clip the screen.
    body.no_wrap = True
    body.overflow = "crop"

    return Panel(
        body,
        title=title,
        title_align="left",
        border_style=tc.task_id,
        width=width,
        height=n_lines + 2,
        padding=(0, 1),
    )


def _render_all(
    tasks: list[DbTask],
    log_paths: list[Path | None],
    n_lines: int,
    *,
    running_count: int,
    min_slots: int,
    max_slots: int,
    explicit_ids: bool,
) -> Group:
    """Render all task panels as a Rich Group."""
    slot_count = len(tasks)

    if explicit_ids:
        slot_text = Text.assemble(
            ("slots: ", _colors.RUNNER_COLORS.label),
            (str(slot_count), _colors.RUNNER_COLORS.value),
            (" (explicit)", _colors.RUNNER_COLORS.label),
        )
    else:
        slot_text = Text.assemble(
            ("slots: ", _colors.RUNNER_COLORS.label),
            (str(slot_count), _colors.RUNNER_COLORS.value),
            (" (", _colors.RUNNER_COLORS.label),
            ("min ", _colors.RUNNER_COLORS.label),
            (str(min_slots), _colors.RUNNER_COLORS.value),
            (", ", _colors.RUNNER_COLORS.label),
            ("max ", _colors.RUNNER_COLORS.label),
            (str(max_slots), _colors.RUNNER_COLORS.value),
            (")", _colors.RUNNER_COLORS.label),
        )

    header = Text.assemble(
        ("Running: ", _colors.RUNNER_COLORS.label),
        (str(running_count), _colors.RUNNER_COLORS.value),
        ("  |  ", _colors.RUNNER_COLORS.label),
    )
    header.append_text(slot_text)

    if not tasks:
        return Group(
            header,
            Panel(
                Text("Waiting for in-progress tasks...", justify="center"),
                title="gza tv",
                title_align="left",
                border_style=_colors.TASK_COLORS.task_id,
                padding=(1, 2),
            )
        )

    width = console.width or 120
    panels = [
        _build_task_panel(task, lp, n_lines, width)
        for task, lp in zip(tasks, log_paths)
    ]
    return Group(header, *panels)


def _auto_select_tasks(
    store: SqliteTaskStore,
    max_slots: int,
    linger_ids: set[str] | None = None,
) -> list[DbTask]:
    """Pick tasks for auto mode, filling slots with live tasks first and recent finished tasks second.

    ``linger_ids`` names tasks that just transitioned to a terminal status while
    on-screen.  They are injected into the selected set (displacing the
    lowest-priority in-progress tasks if necessary) so their final log lines
    render before the panel falls off.
    """
    linger_ids = linger_ids or set()

    in_progress = store.get_in_progress()
    # get_in_progress returns oldest-first; we want newest-first for TV
    in_progress.reverse()

    in_progress_ids = {t.id for t in in_progress if t.id is not None}

    # Fetch linger tasks that are no longer in in_progress
    linger_tasks: list[DbTask] = []
    for linger_id in linger_ids:
        if linger_id not in in_progress_ids:
            task = store.get(linger_id)
            if task is not None:
                linger_tasks.append(task)

    # Prefer most-recently-completed linger tasks so that when more linger tasks exist
    # than available slots, the newest ones take priority.
    linger_tasks.sort(
        key=lambda t: t.completed_at or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )

    # Reserve slots for linger tasks; in-progress fills the remainder.
    ip_limit = max(max_slots - len(linger_tasks), 0)
    selected = list(in_progress[:ip_limit])
    seen_ids = {task.id for task in selected if task.id is not None}

    for task in linger_tasks:
        if task.id is not None and task.id not in seen_ids:
            selected.append(task)
            seen_ids.add(task.id)
            if len(selected) >= max_slots:
                break

    if len(selected) >= max_slots:
        return selected

    for task in store.get_history(limit=max_slots * 3):
        if task.id is not None and task.id in seen_ids:
            continue
        selected.append(task)
        if task.id is not None:
            seen_ids.add(task.id)
        if len(selected) >= max_slots:
            break

    return selected


def _resolve_slot_bounds(
    number: int | None,
    min_slots: int | None,
    max_slots: int | None,
) -> tuple[int, int]:
    """Resolve slot bounds from CLI flags."""
    if number is not None and (min_slots is not None or max_slots is not None):
        raise ValueError("`-n/--number` cannot be combined with `--min` or `--max`.")

    if number is not None:
        min_resolved = number
        max_resolved = number
    else:
        min_resolved = min_slots if min_slots is not None else DEFAULT_MIN_SLOTS
        max_resolved = max_slots if max_slots is not None else DEFAULT_MAX_SLOTS

    if min_resolved < 1:
        raise ValueError("`--min` must be >= 1.")
    if max_resolved < min_resolved:
        raise ValueError("`--max` must be >= `--min`.")
    if max_resolved > MAX_SLOTS:
        raise ValueError(f"`--max` cannot exceed {MAX_SLOTS}.")

    return min_resolved, max_resolved


def _can_pad_to_min(store: SqliteTaskStore, running_count: int, min_slots: int) -> bool:
    """Whether auto-select mode can fill up to min slots with fallback tasks."""
    if running_count == 0:
        return True
    if min_slots <= running_count:
        return True
    return len(_auto_select_tasks(store, min_slots)) >= min_slots


def _desired_slot_count(
    running_count: int,
    min_slots: int,
    max_slots: int,
    can_pad_to_min: bool,
) -> int:
    """Compute desired slot count before hysteresis."""
    if running_count > max_slots:
        return max_slots
    if running_count < min_slots:
        if running_count > 0 and not can_pad_to_min:
            return running_count
        return min_slots
    return running_count


def _next_slot_count(
    running_count: int,
    current_rendered_count: int,
    ticks_below: int,
    *,
    min_slots: int,
    max_slots: int,
    can_pad_to_min: bool,
) -> tuple[int, int]:
    """Apply asymmetric hysteresis to slot resizing."""
    desired_count = _desired_slot_count(
        running_count=running_count,
        min_slots=min_slots,
        max_slots=max_slots,
        can_pad_to_min=can_pad_to_min,
    )

    if desired_count > current_rendered_count:
        return desired_count, 0
    if desired_count < current_rendered_count:
        next_ticks_below = ticks_below + 1
        if next_ticks_below >= SHRINK_TICKS:
            return desired_count, 0
        return current_rendered_count, next_ticks_below
    return current_rendered_count, 0


def _resolve_log_paths(
    config: Config,
    registry: WorkerRegistry,
    tasks: list[DbTask],
) -> list[Path | None]:
    """Resolve log paths for the current set of displayed tasks."""
    log_paths: list[Path | None] = []
    for task in tasks:
        log_path, _startup = _resolve_task_log_path(config, registry, task)
        log_paths.append(log_path)
    return log_paths


def cmd_tv(args: argparse.Namespace) -> int:
    """Live multi-task log dashboard."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    registry = WorkerRegistry(config.workers_path)

    task_ids_raw: list[str] = args.task_ids or []
    number: int | None = getattr(args, "number", None)
    min_arg: int | None = getattr(args, "min_slots", None)
    max_arg: int | None = getattr(args, "max_slots", None)
    try:
        min_slots, max_slots = _resolve_slot_bounds(number, min_arg, max_arg)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

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
        rendered_slots = len(tasks)
        ticks_below = 0
        running_count = len(store.get_in_progress())
    else:
        _ip_init = store.get_in_progress()
        running_count = len(_ip_init)
        prev_in_progress_ids: set[str] = {t.id for t in _ip_init if t.id is not None}
        linger_remaining: dict[str, int] = {}
        can_pad_to_min = _can_pad_to_min(store, running_count, min_slots)
        rendered_slots = _desired_slot_count(running_count, min_slots, max_slots, can_pad_to_min)
        ticks_below = 0
        tasks = _auto_select_tasks(store, rendered_slots)

    n_tasks = len(tasks)
    n_lines = _lines_per_panel(n_tasks)

    # Resolve initial log paths
    log_paths = _resolve_log_paths(config, registry, tasks)

    try:
        with Live(
            _render_all(
                tasks,
                log_paths,
                n_lines,
                running_count=running_count,
                min_slots=min_slots,
                max_slots=max_slots,
                explicit_ids=explicit_ids,
            ),
            console=console,
            refresh_per_second=2,
            screen=True,
        ) as live:
            while True:
                _sleep(0.5)

                if explicit_ids:
                    # Refresh task metadata for the fixed set of requested tasks.
                    for i in range(len(tasks)):
                        current_task_id = tasks[i].id
                        if current_task_id is None:
                            continue
                        refreshed = store.get(current_task_id)
                        if refreshed:
                            tasks[i] = refreshed
                    running_count = len(store.get_in_progress())
                else:
                    in_progress_tasks = store.get_in_progress()
                    running_count = len(in_progress_tasks)
                    in_progress_ids = {t.id for t in in_progress_tasks if t.id is not None}

                    # Detect tasks that left in_progress while on-screen and add
                    # them to the linger set so their final log lines can render.
                    for tid in prev_in_progress_ids:
                        if tid not in in_progress_ids and tid not in linger_remaining:
                            refreshed = store.get(tid)
                            if refreshed is not None and refreshed.status not in {"in_progress", "pending"}:
                                linger_remaining[tid] = LINGER_TICKS

                    # Capture linger set before decrement so freshly-added tasks
                    # are included on the tick they transitioned.
                    linger_ids = set(linger_remaining.keys())

                    can_pad_to_min = _can_pad_to_min(store, running_count, min_slots)
                    rendered_slots, ticks_below = _next_slot_count(
                        running_count,
                        rendered_slots,
                        ticks_below,
                        min_slots=min_slots,
                        max_slots=max_slots,
                        can_pad_to_min=can_pad_to_min,
                    )
                    # In auto-select mode, repoll the live task set so finished
                    # tasks fall off the screen and newly running tasks replace them.
                    # Linger tasks (just completed) stay visible for LINGER_TICKS
                    # extra ticks so closing log lines have time to render.
                    tasks = _auto_select_tasks(store, rendered_slots, linger_ids=linger_ids)
                    log_paths = _resolve_log_paths(config, registry, tasks)

                    # Advance linger countdown; expire entries that hit zero.
                    prev_in_progress_ids = in_progress_ids
                    linger_remaining = {
                        tid: count - 1
                        for tid, count in linger_remaining.items()
                        if count > 1
                    }

                # Recalculate lines (terminal may have resized)
                n_lines = _lines_per_panel(len(tasks))

                # Re-resolve log paths (may appear after task starts)
                for i in range(len(tasks)):
                    log_path = log_paths[i]
                    if log_path is None or not log_path.exists():
                        lp, _ = _resolve_task_log_path(config, registry, tasks[i])
                        log_paths[i] = lp

                live.update(
                    _render_all(
                        tasks,
                        log_paths,
                        n_lines,
                        running_count=running_count,
                        min_slots=min_slots,
                        max_slots=max_slots,
                        explicit_ids=explicit_ids,
                    )
                )

    except KeyboardInterrupt:
        pass

    return 0
