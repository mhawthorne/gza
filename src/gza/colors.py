"""Centralized color definitions for all gza CLI output.

This module provides a single source of truth for all colors used in gza's
terminal output, covering: task history/stats, task status, work output,
log display, next/pending lists, and review verdicts. Colors are chosen for
readability on both dark and light terminal backgrounds.

## Terminal Compatibility Notes
- ``#ff99cc`` (pink): Chosen specifically for prompt text — readable on
  both dark (white-on-dark) and light (dark-on-light) terminals where pure
  white or pure black would be invisible.
- ``#aaaaaa`` (gray_secondary): Soft gray visible on dark backgrounds; avoids
  the harsh contrast of white on light backgrounds.
- ``cyan``, ``green``, ``red``, ``yellow``: Standard ANSI colors that adapt
  reasonably well across terminal themes.
- ``dim``: Inherits from the terminal foreground color and applies a dimming
  effect, making it universally readable regardless of background.
- ``bold``: Also inherits from the terminal foreground, so it adapts to dark
  and light terminals without hardcoding a color.

## Usage
Import the typed dataclass singletons for attribute-style access::

    from gza.colors import TASK_COLORS
    color = TASK_COLORS.prompt  # "#ff99cc"

Or import the ``*_dict`` variants for dict-style access (drop-in replacements
for the old inline dictionaries)::

    from gza.colors import TASK_COLORS_DICT
    color = TASK_COLORS_DICT["prompt"]  # "#ff99cc"

## Theme System
A theme is a named set of partial color overrides layered on top of defaults.
Use :func:`set_theme` to activate a named theme or ad-hoc overrides::

    from gza.colors import set_theme
    set_theme('blue')
    set_theme('blue', {'task_id': '#ff0000'})

Built-in themes: ``'default_dark'``, ``'selective_neon'``, ``'blue'``.

Use :meth:`Theme.uniform` to create a theme that sets every field to a single color.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Base palette — logical semantic names mapped to Rich color strings
# ---------------------------------------------------------------------------

# Readable on both dark and light backgrounds (pure white vanishes on light;
# pure black on dark).
pink: str = "#ff99cc"

blue_neon: str = "#00ffff"
blue_bright: str = "#00aaff"

pink_neon: str = "#ffaaff"

# Soft gray used for secondary/metadata text (task IDs, dates, labels).
# Visible on dark terminals without the harsh contrast of white on light ones.
gray_secondary: str = "#aaaaaa"

gray_light1: str = "#eeeeee"

# Standard ANSI colors — adapt reasonably to most terminal themes.
blue_step: str = "blue"
cyan: str = "cyan"

# green_success: str = "green"
green_success: str = "#00ff88"

yellow_warning: str = "yellow"
red_error: str = "red"
magenta_tool: str = "magenta"

# Semantic modifiers — inherit from the terminal's own foreground color.
bold_heading: str = "bold"
dim_secondary: str = "dim"

# Composite styles.
bold_cyan_heading: str = "bold cyan"
bold_red_error: str = "bold red"
dim_yellow_note: str = "dim yellow"

purple: str = "#cc88ff"
orange: str = "#ffcc44"


# ---------------------------------------------------------------------------
# Per-domain color dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaskColors:
    """Colors for task history / stats output (``gza history``, ``gza stats``)."""

    task_id: str = gray_secondary    # light gray for task ID and date
    prompt: str = pink        # pink for prompt text
    branch: str = cyan        # cyan for branch name
    stats: str = cyan         # cyan for stats line
    success: str = green_success     # green for completed (✓)
    failure: str = red_error         # red for failed (✗)
    unmerged: str = yellow_warning   # yellow for unmerged (⚡)
    orphaned: str = yellow_warning   # yellow for orphaned (⚠)
    lineage: str = gray_secondary    # light gray for lineage relationship labels
    header: str = bold_heading       # bold for section headers
    label: str = gray_secondary      # light gray for labels
    value: str = "white"             # white for values


@dataclass(frozen=True)
class StatusColors:
    """Colors for task/worker status values (``gza ps`` and lineage trees)."""

    completed: str = green_success   # green
    failed: str = red_error          # red
    pending: str = yellow_warning    # yellow
    in_progress: str = cyan   # cyan
    unmerged: str = yellow_warning   # yellow
    dropped: str = red_error         # red
    stale: str = yellow_warning      # yellow
    unknown: str = yellow_warning    # yellow
    running: str = green_success     # green


@dataclass(frozen=True)
class WorkOutputColors:
    """Colors for live provider stream output (agent work log)."""

    step_header: str = blue_step       # blue for step/turn headers
    assistant_text: str = green_success  # green for assistant messages
    tool_use: str = magenta_tool       # magenta for tool-use events
    error: str = bold_red_error        # bold red for errors
    todo_pending: str = "white"        # white for pending todo items
    todo_in_progress: str = yellow_warning  # yellow for in-progress todos
    todo_completed: str = green_success     # green for completed todos


@dataclass(frozen=True)
class ShowColors:
    """Colors for the ``gza show`` command task detail view."""

    heading: str = bold_cyan_heading  # bold cyan for headings
    section: str = dim_secondary      # dim for section separators
    label: str = dim_secondary        # dim for field labels
    value: str = bold_heading         # bold adapts to terminal background
    task_id: str = dim_secondary      # dim adapts to terminal background
    prompt: str = pink         # pink works on dark and light backgrounds
    branch: str = cyan         # cyan visible on dark and light
    stats: str = cyan          # cyan visible on dark and light
    status_pending: str = yellow_warning   # yellow for pending
    status_running: str = cyan      # cyan for running/in_progress
    status_completed: str = green_success  # green for completed
    status_failed: str = red_error         # red for failed
    status_default: str = bold_heading     # bold adapts to terminal background


@dataclass(frozen=True)
class UnmergedColors:
    """Colors for the ``gza unmerged`` command task list."""

    task_id: str = blue_neon
    prompt: str = pink_neon
    stats: str = purple
    branch: str = cyan        # cyan for branch name
    review_approved: str = green_success      # green for approved
    review_changes: str = yellow_warning      # yellow for changes requested
    review_discussion: str = cyan      # cyan for discussion
    review_none: str = dim_yellow_note        # dim yellow for no review


@dataclass(frozen=True)
class LineageColors:
    """Colors for lineage tree rendering (``_format_lineage`` and ``gza lineage``)."""

    task_id: str = blue_bright       # blue for task ID in tree nodes
    task_type: str = orange
    # annotation: str = dim_secondary  # dim for annotation metadata
    annotation: str = gray_light1
    connector: str = dim_secondary   # dim for tree branch connectors (├── └──)
    # cmd_lineage-specific colors
    type_label: str = "magenta"      # magenta for task type in gza lineage
    stats: str = cyan
    prompt: str = pink        # pink for prompt text
    relationship: str = dim_secondary  # dim for relationship labels
    target_highlight: str = bold_heading  # bold for the target task


@dataclass(frozen=True)
class NextColors:
    """Colors for the ``gza next`` command pending-task list."""

    task_id: str = blue_neon
    prompt: str = pink_neon
    type: str = gray_light1
    blocked: str = yellow_warning    # yellow for blocked indicator
    index: str = gray_light1


# ---------------------------------------------------------------------------
# BaseColors — cross-context defaults for fields shared across multiple classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BaseColors:
    """Default color for each field that appears in multiple domain color classes.

    These values are used by :class:`Theme` to apply cross-cutting overrides —
    setting ``base.task_id`` in a theme changes ``task_id`` in *every* domain
    class that has that field, unless a domain-specific override is also set.
    """

    task_id: str = gray_secondary
    prompt: str = pink
    stats: str = cyan
    branch: str = cyan
    label: str = gray_secondary
    value: str = "white"
    heading: str = bold_cyan_heading


# ---------------------------------------------------------------------------
# Theme model
# ---------------------------------------------------------------------------


def _all_fields(color: str, cls: type) -> dict[str, str]:
    """Return a dict mapping every dataclass field name in *cls* to *color*."""
    return {f.name: color for f in dataclasses.fields(cls)}


@dataclass(frozen=True)
class Theme:
    """A named set of partial color overrides layered on top of per-domain defaults.

    Resolution priority (highest wins):

    1. Ad-hoc per-field overrides from the config ``colors:`` key (highest).
    2. Per-domain dicts (``task``, ``show``, etc.) — domain-specific overrides
       that take precedence over ``base`` for that class.
    3. ``base`` dict — cross-cutting overrides for :class:`BaseColors` fields;
       applied to every domain class that has the named field.
    4. Dataclass field defaults — the hardcoded default values in each class.
    """

    name: str
    base: dict[str, str] = field(default_factory=dict)
    task: dict[str, str] = field(default_factory=dict)
    status: dict[str, str] = field(default_factory=dict)
    work_output: dict[str, str] = field(default_factory=dict)
    show: dict[str, str] = field(default_factory=dict)
    unmerged: dict[str, str] = field(default_factory=dict)
    lineage: dict[str, str] = field(default_factory=dict)
    next_colors: dict[str, str] = field(default_factory=dict)

    @classmethod
    def uniform(cls, name: str, color: str) -> "Theme":
        """Create a theme that sets every field in every domain class to *color*."""
        af = _all_fields  # local alias for brevity
        return cls(
            name=name,
            base=af(color, BaseColors),
            task=af(color, TaskColors),
            status=af(color, StatusColors),
            work_output=af(color, WorkOutputColors),
            show=af(color, ShowColors),
            unmerged=af(color, UnmergedColors),
            lineage=af(color, LineageColors),
            next_colors=af(color, NextColors),
        )


# ---------------------------------------------------------------------------
# Built-in themes
# ---------------------------------------------------------------------------

_dd = Theme.uniform("default_dark", gray_light1)
_THEME_DEFAULT_DARK = dataclasses.replace(
    _dd,
    base={**_dd.base, "value": "white", "heading": "white"},
    task={**_dd.task, "header": "white"},
    work_output={**_dd.work_output, "error": "white", "todo_pending": "white"},
    show={**_dd.show, "heading": "white", "status_default": "white"},
    lineage={**_dd.lineage, "target_highlight": "white"},
    next_colors={**_dd.next_colors, "type": "white"},
)

_THEME_SELECTIVE_NEON = Theme(
    name="selective_neon",
    base={
        "task_id": blue_neon,
        "heading": pink_neon,
    },
    work_output={
        "step_header": blue_neon,
        "error": bold_red_error,
    },
    show={
        "heading": pink_neon,
    },
)

_THEME_BLUE = Theme(
    name="blue",
    base={
        "task_id": blue_bright,
        "branch": blue_neon,
        "stats": blue_step,
    },
    task={
        "stats": blue_bright,
    },
    show={
        "heading": bold_cyan_heading,
    },
)

#: Registry of all built-in themes, keyed by name.
BUILT_IN_THEMES: dict[str, Theme] = {
    t.name: t for t in [_THEME_DEFAULT_DARK, _THEME_SELECTIVE_NEON, _THEME_BLUE]
}

# ---------------------------------------------------------------------------
# Theme application helpers
# ---------------------------------------------------------------------------

_BASE_COLOR_FIELDS: frozenset[str] = frozenset(
    f.name for f in dataclasses.fields(BaseColors)
)

def _apply_domain_theme(
    default_instance: Any,
    domain_overrides: dict[str, str],
    base_overrides: dict[str, str],
    color_overrides: dict[str, str],
) -> Any:
    """Return a new frozen dataclass instance with theme and config overrides applied.

    Priority (highest to lowest):

    1. ``color_overrides`` — ad-hoc per-field config overrides.
    2. ``domain_overrides`` — theme's domain-specific overrides.
    3. ``base_overrides`` — theme's BaseColors overrides (only for shared fields).
    4. ``default_instance`` field values — hardcoded dataclass defaults.
    """
    overrides: dict[str, Any] = {}
    for f in dataclasses.fields(default_instance):
        name = f.name
        if name in color_overrides:
            overrides[name] = color_overrides[name]
        elif name in domain_overrides:
            overrides[name] = domain_overrides[name]
        elif name in _BASE_COLOR_FIELDS and name in base_overrides:
            overrides[name] = base_overrides[name]
    if overrides:
        return dataclasses.replace(default_instance, **overrides)
    return default_instance


def _build_themed_instances(
    theme_name: str | None,
    color_overrides: dict[str, str],
) -> dict[str, Any]:
    """Build all themed color singletons and return them in a dict."""
    theme = BUILT_IN_THEMES.get(theme_name) if theme_name else None
    base_ov = theme.base if theme is not None else {}
    _no: dict[str, str] = {}

    task_c = _apply_domain_theme(TaskColors(), theme.task if theme else _no, base_ov, color_overrides)
    status_c = _apply_domain_theme(StatusColors(), theme.status if theme else _no, base_ov, color_overrides)
    work_c = _apply_domain_theme(WorkOutputColors(), theme.work_output if theme else _no, base_ov, color_overrides)
    show_c = _apply_domain_theme(ShowColors(), theme.show if theme else _no, base_ov, color_overrides)
    unmerged_c = _apply_domain_theme(UnmergedColors(), theme.unmerged if theme else _no, base_ov, color_overrides)
    lineage_c = _apply_domain_theme(LineageColors(), theme.lineage if theme else _no, base_ov, color_overrides)
    next_c = _apply_domain_theme(NextColors(), theme.next_colors if theme else _no, base_ov, color_overrides)

    return {
        "TASK_COLORS": task_c,
        "STATUS_COLORS": status_c,
        "WORK_OUTPUT_COLORS": work_c,
        "SHOW_COLORS": show_c,
        "UNMERGED_COLORS": unmerged_c,
        "LINEAGE_COLORS": lineage_c,
        "NEXT_COLORS": next_c,
        "TASK_COLORS_DICT": dataclasses.asdict(task_c),
        "STATUS_COLORS_DICT": dataclasses.asdict(status_c),
        "WORK_OUTPUT_COLORS_DICT": dataclasses.asdict(work_c),
        "SHOW_COLORS_DICT": dataclasses.asdict(show_c),
        "UNMERGED_COLORS_DICT": dataclasses.asdict(unmerged_c),
        "LINEAGE_COLORS_DICT": dataclasses.asdict(lineage_c),
        "NEXT_COLORS_DICT": dataclasses.asdict(next_c),
        "LINEAGE_STATUS_COLORS": {
            "completed": status_c.completed,
            "failed": status_c.failed,
            "pending": status_c.pending,
            "in_progress": status_c.in_progress,
            "unmerged": status_c.unmerged,
            "dropped": status_c.dropped,
        },
        "PS_STATUS_COLORS": {
            "running": status_c.running,
            "in_progress": status_c.in_progress,
            "completed": status_c.completed,
            "failed": status_c.failed,
            "failed(startup)": status_c.failed,
            "stale": status_c.stale,
            "unknown": status_c.unknown,
        },
    }


def set_theme(
    theme_name: str | None = None,
    color_overrides: dict[str, str] | None = None,
) -> None:
    """Apply a named theme and optional ad-hoc overrides to all module-level singletons.

    Args:
        theme_name: Name of a built-in theme (``'default_dark'``, ``'selective_neon'``,
                    ``'blue'``), or ``None`` to use per-class defaults.
        color_overrides: Optional mapping of field-name → Rich color string applied on
                         top of the theme.  The same key applies to *every* domain class
                         that has a field with that name.

    This function replaces the module-level singletons (``TASK_COLORS``,
    ``TASK_COLORS_DICT``, etc.) in place so that code which has already imported
    a singleton via ``import gza.colors as c`` and accesses ``c.TASK_COLORS``
    sees the updated value.  Code that captured the singleton via
    ``from gza.colors import TASK_COLORS`` at an earlier import will retain the
    old value.
    """
    global TASK_COLORS, STATUS_COLORS, WORK_OUTPUT_COLORS, SHOW_COLORS
    global UNMERGED_COLORS, LINEAGE_COLORS, NEXT_COLORS
    global TASK_COLORS_DICT, STATUS_COLORS_DICT, WORK_OUTPUT_COLORS_DICT, SHOW_COLORS_DICT
    global UNMERGED_COLORS_DICT, LINEAGE_COLORS_DICT, NEXT_COLORS_DICT
    global LINEAGE_STATUS_COLORS, PS_STATUS_COLORS

    inst = _build_themed_instances(theme_name, color_overrides or {})
    TASK_COLORS = inst["TASK_COLORS"]
    STATUS_COLORS = inst["STATUS_COLORS"]
    WORK_OUTPUT_COLORS = inst["WORK_OUTPUT_COLORS"]
    SHOW_COLORS = inst["SHOW_COLORS"]
    UNMERGED_COLORS = inst["UNMERGED_COLORS"]
    LINEAGE_COLORS = inst["LINEAGE_COLORS"]
    NEXT_COLORS = inst["NEXT_COLORS"]
    TASK_COLORS_DICT = inst["TASK_COLORS_DICT"]
    STATUS_COLORS_DICT = inst["STATUS_COLORS_DICT"]
    WORK_OUTPUT_COLORS_DICT = inst["WORK_OUTPUT_COLORS_DICT"]
    SHOW_COLORS_DICT = inst["SHOW_COLORS_DICT"]
    UNMERGED_COLORS_DICT = inst["UNMERGED_COLORS_DICT"]
    LINEAGE_COLORS_DICT = inst["LINEAGE_COLORS_DICT"]
    NEXT_COLORS_DICT = inst["NEXT_COLORS_DICT"]
    LINEAGE_STATUS_COLORS = inst["LINEAGE_STATUS_COLORS"]
    PS_STATUS_COLORS = inst["PS_STATUS_COLORS"]


# ---------------------------------------------------------------------------
# Module-level singleton instances (typed, attribute-style access)
# ---------------------------------------------------------------------------
# Initialized with per-class defaults. Call set_theme() to apply a theme at
# runtime (Config.load() does this automatically).

TASK_COLORS: TaskColors = TaskColors()
STATUS_COLORS: StatusColors = StatusColors()
WORK_OUTPUT_COLORS: WorkOutputColors = WorkOutputColors()
SHOW_COLORS: ShowColors = ShowColors()
UNMERGED_COLORS: UnmergedColors = UnmergedColors()
LINEAGE_COLORS: LineageColors = LineageColors()
NEXT_COLORS: NextColors = NextColors()

# ---------------------------------------------------------------------------
# Dict variants (drop-in replacements for the old inline dictionaries)
# ---------------------------------------------------------------------------

TASK_COLORS_DICT: dict[str, str] = dataclasses.asdict(TASK_COLORS)
STATUS_COLORS_DICT: dict[str, str] = dataclasses.asdict(STATUS_COLORS)
WORK_OUTPUT_COLORS_DICT: dict[str, str] = dataclasses.asdict(WORK_OUTPUT_COLORS)
SHOW_COLORS_DICT: dict[str, str] = dataclasses.asdict(SHOW_COLORS)
UNMERGED_COLORS_DICT: dict[str, str] = dataclasses.asdict(UNMERGED_COLORS)
LINEAGE_COLORS_DICT: dict[str, str] = dataclasses.asdict(LINEAGE_COLORS)
NEXT_COLORS_DICT: dict[str, str] = dataclasses.asdict(NEXT_COLORS)

# Lineage-status dict (subset of StatusColors, keyed by status string)
LINEAGE_STATUS_COLORS: dict[str, str] = {
    "completed": STATUS_COLORS.completed,
    "failed": STATUS_COLORS.failed,
    "pending": STATUS_COLORS.pending,
    "in_progress": STATUS_COLORS.in_progress,
    "unmerged": STATUS_COLORS.unmerged,
    "dropped": STATUS_COLORS.dropped,
}

# PS-command status dict (subset of StatusColors, keyed by status string)
PS_STATUS_COLORS: dict[str, str] = {
    "running": STATUS_COLORS.running,
    "in_progress": STATUS_COLORS.in_progress,
    "completed": STATUS_COLORS.completed,
    "failed": STATUS_COLORS.failed,
    "failed(startup)": STATUS_COLORS.failed,
    "stale": STATUS_COLORS.stale,
    "unknown": STATUS_COLORS.unknown,
}

# Log-command task-status colors (keyed by task.status string).
# Note: 'unmerged' maps to green (treated as successfully merged for display
# purposes) and 'in_progress' maps to yellow — both differ from STATUS_COLORS
# which uses yellow/cyan respectively for those states.
# Intentionally excluded from set_theme(): these use semantic ANSI colors
# (green_success, red_error, yellow_warning) that should remain stable across
# themes to preserve clear pass/fail readability in log output.
LOG_TASK_STATUS_COLORS: dict[str, str] = {
    "completed": green_success,
    "unmerged": green_success,
    "failed": red_error,
    "dropped": red_error,
    "in_progress": yellow_warning,
}

# Log-command worker-status colors (keyed by worker.status string).
# Intentionally excluded from set_theme() — same rationale as LOG_TASK_STATUS_COLORS.
LOG_WORKER_STATUS_COLORS: dict[str, str] = {
    "running": yellow_warning,
    "in_progress": yellow_warning,
    "completed": green_success,
    "failed": red_error,
    "stale": yellow_warning,
}

# Review-verdict colors for the runner's post-task verdict display.
# Keys are uppercase verdict strings as returned by parse_review_verdict().
# Intentionally excluded from set_theme() — same rationale as LOG_TASK_STATUS_COLORS.
REVIEW_VERDICT_COLORS: dict[str, str] = {
    "APPROVED": green_success,
    "CHANGES_REQUESTED": yellow_warning,
    "NEEDS_DISCUSSION": blue_step,
}

# Cycle-status colors for the ``gza show`` cycle state display.
# Keys are cycle status strings as stored in the database.
# Intentionally excluded from set_theme() — same rationale as LOG_TASK_STATUS_COLORS.
CYCLE_STATUS_COLORS: dict[str, str] = {
    "active": cyan,
    "approved": green_success,
    "maxed_out": yellow_warning,
    "blocked": red_error,
}

