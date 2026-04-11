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
from typing import Any

# ---------------------------------------------------------------------------
# Base palette — logical semantic names mapped to Rich color strings
# ---------------------------------------------------------------------------

# Readable on both dark and light backgrounds (pure white vanishes on light;
# pure black on dark).

white_bright: str = "bright_white"
white: str = "white"

blue: str = "blue"
blue_neon: str = "#00ffff"
blue_bright: str = "#00ccff"
blue_light: str = "#88ccff"

pink: str = "#ff99cc"
pink_light: str = "#ffaacc"
pink_neon: str = "#ffaaff"

# Soft gray used for secondary/metadata text (task IDs, dates, labels).
# Visible on dark terminals without the harsh contrast of white on light ones.
gray_secondary: str = "#aaaaaa"

gray_light1: str = "#f0f0f0"
gray_light2: str = "#eeeeee"

# Standard ANSI colors — adapt reasonably to most terminal themes.
cyan: str = "cyan"

# green_success: str = "green"
green_success: str = "#00ff88"

yellow_warning: str = "yellow"
red_error: str = "#ff88aa"
magenta_tool: str = "magenta"

# Semantic modifiers — inherit from the terminal's own foreground color.
bold_heading: str = "bold"

# Composite styles.
bold_cyan_heading: str = "bold cyan"
bold_red_error: str = "bold red"
dim_yellow_note: str = "dim yellow"

purple: str = "#cc88ff"
orange: str = "#ffbb44"

# Default color for all fields when no theme is applied.
default_color: str = "bright_white"


# ---------------------------------------------------------------------------
# Per-domain color dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaskColors:
    """Colors for task history / stats output (``gza history``, ``gza stats``)."""

    task_id: str = default_color
    prompt: str = default_color
    branch: str = default_color
    stats: str = default_color
    success: str = default_color
    failure: str = default_color
    unmerged: str = default_color
    orphaned: str = default_color
    lineage: str = default_color
    date: str = default_color
    file: str = default_color
    header: str = default_color
    label: str = default_color
    value: str = default_color


@dataclass(frozen=True)
class StatusColors:
    """Colors for task/worker status values (``gza ps`` and lineage trees)."""

    completed: str = default_color
    failed: str = default_color
    pending: str = default_color
    in_progress: str = default_color
    unmerged: str = default_color
    dropped: str = default_color
    stale: str = default_color
    unknown: str = default_color
    running: str = default_color


@dataclass(frozen=True)
class TaskStreamColors:
    """Colors for live provider stream output (agent task log)."""

    step_header: str = default_color
    assistant_text: str = default_color
    tool_use: str = default_color
    error: str = default_color
    todo_pending: str = default_color
    todo_in_progress: str = default_color
    todo_completed: str = default_color


@dataclass(frozen=True)
class ShowColors:
    """Colors for the ``gza show`` command task detail view."""

    heading: str = default_color
    section: str = default_color
    label: str = default_color
    value: str = default_color
    task_id: str = default_color
    prompt: str = default_color
    branch: str = default_color
    stats: str = default_color
    status_pending: str = default_color
    status_running: str = default_color
    status_completed: str = default_color
    status_failed: str = default_color
    status_default: str = default_color


@dataclass(frozen=True)
class UnmergedColors:
    """Colors for the ``gza unmerged`` command task list."""

    task_id: str = default_color
    prompt: str = default_color
    stats: str = default_color
    branch: str = default_color
    date: str = default_color
    review_approved: str = default_color
    review_changes: str = default_color
    review_discussion: str = default_color
    review_none: str = default_color


@dataclass(frozen=True)
class LineageColors:
    """Colors for lineage tree rendering (``_format_lineage`` and ``gza lineage``)."""

    task_id: str = default_color
    task_type: str = default_color
    annotation: str = default_color
    connector: str = default_color
    type_label: str = default_color
    stats: str = default_color
    prompt: str = default_color
    relationship: str = default_color
    target_highlight: str = default_color


@dataclass(frozen=True)
class NextColors:
    """Colors for the ``gza next`` command pending-task list."""

    task_id: str = default_color
    prompt: str = default_color
    type: str = default_color
    blocked: str = default_color
    index: str = default_color


@dataclass(frozen=True)
class RunnerColors:
    """Colors for post-task runner output (stats, headers, next steps)."""

    label: str = default_color
    value: str = default_color
    success: str = default_color
    error: str = default_color
    warning: str = default_color
    heading: str = default_color
    task_id: str = default_color
    task_type: str = default_color
    slug: str = default_color
    next_cmd: str = default_color
    next_comment: str = default_color
    estimated: str = default_color
    commits_yes: str = default_color


@dataclass(frozen=True)
class AdvanceColors:
    """Colors for ``gza advance`` action type indicators."""

    merge: str = default_color
    error: str = default_color
    waiting: str = default_color
    default: str = default_color


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

    task_id: str = default_color
    prompt: str = default_color
    stats: str = default_color
    branch: str = default_color
    label: str = default_color
    value: str = default_color
    heading: str = default_color


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
    task_stream: dict[str, str] = field(default_factory=dict)
    show: dict[str, str] = field(default_factory=dict)
    unmerged: dict[str, str] = field(default_factory=dict)
    lineage: dict[str, str] = field(default_factory=dict)
    next_colors: dict[str, str] = field(default_factory=dict)
    runner: dict[str, str] = field(default_factory=dict)
    advance: dict[str, str] = field(default_factory=dict)
    # Rich style-name overrides (e.g. "repr.number", "repr.path"). Applied to
    # Consoles built via :func:`build_rich_theme`, currently only the provider
    # stream console. Keys match Rich's own style names exactly.
    rich: dict[str, str] = field(default_factory=dict)

    @classmethod
    def uniform(cls, name: str, color: str) -> Theme:
        """Create a theme that sets every field in every domain class to *color*."""
        af = _all_fields  # local alias for brevity
        return cls(
            name=name,
            base=af(color, BaseColors),
            task=af(color, TaskColors),
            status=af(color, StatusColors),
            task_stream=af(color, TaskStreamColors),
            show=af(color, ShowColors),
            unmerged=af(color, UnmergedColors),
            lineage=af(color, LineageColors),
            next_colors=af(color, NextColors),
            runner=af(color, RunnerColors),
            advance=af(color, AdvanceColors),
        )


# ---------------------------------------------------------------------------
# Built-in themes
# ---------------------------------------------------------------------------

_THEME_DEFAULT_DARK = Theme.uniform("default_dark", gray_light1)

_THEME_MINIMAL = Theme(
    name="minimal",
    base={
        "task_id": blue_bright,
        "prompt": pink_neon,
        # "file": blue_bright,
        # "branch": blue_bright,
    },
    lineage={
        "task_id": blue_bright,
    },
    task={
        "success": green_success,
        "failure": red_error,
        "running": green_success,
        "pending": yellow_warning,
        "dropped": red_error,
        "stale": yellow_warning,
        "unknown": red_error,
        "unmerged": f"{green_success} bold",
        "orphaned": yellow_warning,
    },
    runner={
        "success": green_success,
        "error": red_error,
        "warning": yellow_warning,
        "slug": gray_secondary,
        # "heading": bold_cyan_heading,
        # "task_type": magenta_tool,
    },
    advance={
        "merge": green_success,
        "error": red_error,
        "waiting": yellow_warning,
        # "default": cyan,
    },
    unmerged={
        "review_changes": orange,
        "review_discussion": blue,
        "review_none": yellow_warning,
        "review_approved": green_success,
    },
    task_stream={
        "step_header": f"{pink_light} bold",
    },
    rich={
        "repr.number": orange,
        "repr.path": orange,
        "repr.filename": orange,
        "repr.str": purple,
        "repr.url": purple,
        "repr.call": purple,
    },
)

_THEME_SELECTIVE_NEON = Theme(
    name="selective_neon",
    base={
        "task_id": blue_neon,
        "heading": pink_neon,
    },
    task_stream={
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
        "stats": blue,
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
    t.name: t for t in [_THEME_DEFAULT_DARK, _THEME_MINIMAL, _THEME_SELECTIVE_NEON, _THEME_BLUE]
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
    stream_c = _apply_domain_theme(TaskStreamColors(), theme.task_stream if theme else _no, base_ov, color_overrides)
    show_c = _apply_domain_theme(ShowColors(), theme.show if theme else _no, base_ov, color_overrides)
    unmerged_c = _apply_domain_theme(UnmergedColors(), theme.unmerged if theme else _no, base_ov, color_overrides)
    lineage_c = _apply_domain_theme(LineageColors(), theme.lineage if theme else _no, base_ov, color_overrides)
    next_c = _apply_domain_theme(NextColors(), theme.next_colors if theme else _no, base_ov, color_overrides)
    runner_c = _apply_domain_theme(RunnerColors(), theme.runner if theme else _no, base_ov, color_overrides)
    advance_c = _apply_domain_theme(AdvanceColors(), theme.advance if theme else _no, base_ov, color_overrides)
    rich_styles = dict(theme.rich) if theme else {}

    return {
        "TASK_COLORS": task_c,
        "STATUS_COLORS": status_c,
        "TASK_STREAM_COLORS": stream_c,
        "SHOW_COLORS": show_c,
        "UNMERGED_COLORS": unmerged_c,
        "LINEAGE_COLORS": lineage_c,
        "NEXT_COLORS": next_c,
        "RUNNER_COLORS": runner_c,
        "ADVANCE_COLORS": advance_c,
        "TASK_COLORS_DICT": dataclasses.asdict(task_c),
        "STATUS_COLORS_DICT": dataclasses.asdict(status_c),
        "TASK_STREAM_COLORS_DICT": dataclasses.asdict(stream_c),
        "SHOW_COLORS_DICT": dataclasses.asdict(show_c),
        "UNMERGED_COLORS_DICT": dataclasses.asdict(unmerged_c),
        "LINEAGE_COLORS_DICT": dataclasses.asdict(lineage_c),
        "NEXT_COLORS_DICT": dataclasses.asdict(next_c),
        "RUNNER_COLORS_DICT": dataclasses.asdict(runner_c),
        "ADVANCE_COLORS_DICT": dataclasses.asdict(advance_c),
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
        "RICH_STYLES_DICT": rich_styles,
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

    Dict singletons are updated **in place** (clear + update) so that code
    which captured a reference via ``from gza.colors import TASK_COLORS_DICT``
    sees the themed values.  Dataclass singletons are replaced (frozen
    dataclasses cannot be mutated), so code using ``import gza.colors as c``
    and ``c.TASK_COLORS`` is preferred for those.
    """
    global TASK_COLORS, STATUS_COLORS, TASK_STREAM_COLORS, SHOW_COLORS
    global UNMERGED_COLORS, LINEAGE_COLORS, NEXT_COLORS, RUNNER_COLORS, ADVANCE_COLORS

    inst = _build_themed_instances(theme_name, color_overrides or {})

    # Frozen dataclass singletons — must replace the module-level name.
    TASK_COLORS = inst["TASK_COLORS"]
    STATUS_COLORS = inst["STATUS_COLORS"]
    TASK_STREAM_COLORS = inst["TASK_STREAM_COLORS"]
    SHOW_COLORS = inst["SHOW_COLORS"]
    UNMERGED_COLORS = inst["UNMERGED_COLORS"]
    LINEAGE_COLORS = inst["LINEAGE_COLORS"]
    NEXT_COLORS = inst["NEXT_COLORS"]
    RUNNER_COLORS = inst["RUNNER_COLORS"]
    ADVANCE_COLORS = inst["ADVANCE_COLORS"]

    # Dict singletons — update in place so ``from`` imports see new values.
    for name in (
        "TASK_COLORS_DICT", "STATUS_COLORS_DICT", "TASK_STREAM_COLORS_DICT",
        "SHOW_COLORS_DICT", "UNMERGED_COLORS_DICT", "LINEAGE_COLORS_DICT",
        "NEXT_COLORS_DICT", "RUNNER_COLORS_DICT", "ADVANCE_COLORS_DICT",
        "LINEAGE_STATUS_COLORS", "PS_STATUS_COLORS", "RICH_STYLES_DICT",
    ):
        target = globals()[name]
        target.clear()
        target.update(inst[name])


# ---------------------------------------------------------------------------
# Module-level singleton instances (typed, attribute-style access)
# ---------------------------------------------------------------------------
# Initialized with per-class defaults. Call set_theme() to apply a theme at
# runtime (Config.load() does this automatically).

TASK_COLORS: TaskColors = TaskColors()
STATUS_COLORS: StatusColors = StatusColors()
TASK_STREAM_COLORS: TaskStreamColors = TaskStreamColors()
SHOW_COLORS: ShowColors = ShowColors()
UNMERGED_COLORS: UnmergedColors = UnmergedColors()
LINEAGE_COLORS: LineageColors = LineageColors()
NEXT_COLORS: NextColors = NextColors()
RUNNER_COLORS: RunnerColors = RunnerColors()
ADVANCE_COLORS: AdvanceColors = AdvanceColors()

# ---------------------------------------------------------------------------
# Dict variants (drop-in replacements for the old inline dictionaries)
# ---------------------------------------------------------------------------

TASK_COLORS_DICT: dict[str, str] = dataclasses.asdict(TASK_COLORS)
STATUS_COLORS_DICT: dict[str, str] = dataclasses.asdict(STATUS_COLORS)
TASK_STREAM_COLORS_DICT: dict[str, str] = dataclasses.asdict(TASK_STREAM_COLORS)
SHOW_COLORS_DICT: dict[str, str] = dataclasses.asdict(SHOW_COLORS)
UNMERGED_COLORS_DICT: dict[str, str] = dataclasses.asdict(UNMERGED_COLORS)
LINEAGE_COLORS_DICT: dict[str, str] = dataclasses.asdict(LINEAGE_COLORS)
NEXT_COLORS_DICT: dict[str, str] = dataclasses.asdict(NEXT_COLORS)
RUNNER_COLORS_DICT: dict[str, str] = dataclasses.asdict(RUNNER_COLORS)
ADVANCE_COLORS_DICT: dict[str, str] = dataclasses.asdict(ADVANCE_COLORS)

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

# Rich style-name overrides for Consoles built via :func:`build_rich_theme`.
# Populated from the active theme's ``rich`` dict by :func:`set_theme`. Keys
# are Rich style names (e.g. ``repr.number``, ``repr.path``); values are Rich
# color strings. Empty = fall through to Rich's built-in defaults.
RICH_STYLES_DICT: dict[str, str] = {}


def build_rich_theme() -> Any:
    """Return a ``rich.theme.Theme`` built from :data:`RICH_STYLES_DICT`.

    Returns ``None`` when the active gza theme has no ``rich`` overrides so
    callers can pass the result directly to ``Console(theme=...)`` without
    special-casing the empty dict.
    """
    if not RICH_STYLES_DICT:
        return None
    from rich.theme import Theme as RichTheme
    return RichTheme(dict(RICH_STYLES_DICT))


