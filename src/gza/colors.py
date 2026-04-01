"""Centralized color definitions for all gza CLI output.

This module provides a single source of truth for all colors used in gza's
terminal output, covering: task history/stats, task status, work output,
log display, next/pending lists, and review verdicts. Colors are chosen for
readability on both dark and light terminal backgrounds.

## Terminal Compatibility Notes
- ``#ff99cc`` (pink_prompt): Chosen specifically for prompt text — readable on
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
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Base palette — logical semantic names mapped to Rich color strings
# ---------------------------------------------------------------------------

# Pink used for prompt text — chosen for cross-terminal readability on both
# dark and light backgrounds (pure white vanishes on light; pure black on dark).
pink_prompt: str = "#ff99cc"

# Soft gray used for secondary/metadata text (task IDs, dates, labels).
# Visible on dark terminals without the harsh contrast of white on light ones.
gray_secondary: str = "#aaaaaa"

# Standard ANSI colors — adapt reasonably to most terminal themes.
blue_step: str = "blue"
cyan_header: str = "cyan"
green_success: str = "green"
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


# ---------------------------------------------------------------------------
# Per-domain color dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaskColors:
    """Colors for task history / stats output (``gza history``, ``gza stats``)."""

    task_id: str = gray_secondary    # light gray for task ID and date
    prompt: str = pink_prompt        # pink for prompt text
    branch: str = cyan_header        # cyan for branch name
    stats: str = cyan_header         # cyan for stats line
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
    in_progress: str = cyan_header   # cyan
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
    prompt: str = pink_prompt         # pink works on dark and light backgrounds
    branch: str = cyan_header         # cyan visible on dark and light
    stats: str = cyan_header          # cyan visible on dark and light
    status_pending: str = yellow_warning   # yellow for pending
    status_running: str = cyan_header      # cyan for running/in_progress
    status_completed: str = green_success  # green for completed
    status_failed: str = red_error         # red for failed
    status_default: str = bold_heading     # bold adapts to terminal background


@dataclass(frozen=True)
class UnmergedColors:
    """Colors for the ``gza unmerged`` command task list."""

    task_id: str = dim_secondary     # dim adapts to terminal background
    prompt: str = pink_prompt        # pink works on dark and light
    stats: str = cyan_header         # cyan visible on dark and light
    branch: str = cyan_header        # cyan for branch name
    review_approved: str = green_success      # green for approved
    review_changes: str = yellow_warning      # yellow for changes requested
    review_discussion: str = cyan_header      # cyan for discussion
    review_none: str = dim_yellow_note        # dim yellow for no review


@dataclass(frozen=True)
class NextColors:
    """Colors for the ``gza next`` command pending-task list."""

    task_id: str = dim_secondary     # dim for task ID
    prompt: str = pink_prompt        # pink for prompt text
    type: str = magenta_tool         # magenta for task type
    blocked: str = yellow_warning    # yellow for blocked indicator
    index: str = dim_secondary       # dim for row index


# ---------------------------------------------------------------------------
# Module-level singleton instances (typed, attribute-style access)
# ---------------------------------------------------------------------------

TASK_COLORS = TaskColors()
STATUS_COLORS = StatusColors()
WORK_OUTPUT_COLORS = WorkOutputColors()
SHOW_COLORS = ShowColors()
UNMERGED_COLORS = UnmergedColors()
NEXT_COLORS = NextColors()

# ---------------------------------------------------------------------------
# Dict variants (drop-in replacements for the old inline dictionaries)
# ---------------------------------------------------------------------------

# These module-level dict calls execute at import time (intentionally eager —
# the module is small and the dicts are always needed).
TASK_COLORS_DICT: dict[str, str] = dataclasses.asdict(TASK_COLORS)
STATUS_COLORS_DICT: dict[str, str] = dataclasses.asdict(STATUS_COLORS)
WORK_OUTPUT_COLORS_DICT: dict[str, str] = dataclasses.asdict(WORK_OUTPUT_COLORS)
SHOW_COLORS_DICT: dict[str, str] = dataclasses.asdict(SHOW_COLORS)
UNMERGED_COLORS_DICT: dict[str, str] = dataclasses.asdict(UNMERGED_COLORS)
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
LOG_TASK_STATUS_COLORS: dict[str, str] = {
    "completed": green_success,
    "unmerged": green_success,
    "failed": red_error,
    "dropped": red_error,
    "in_progress": yellow_warning,
}

# Log-command worker-status colors (keyed by worker.status string).
LOG_WORKER_STATUS_COLORS: dict[str, str] = {
    "running": yellow_warning,
    "in_progress": yellow_warning,
    "completed": green_success,
    "failed": red_error,
    "stale": yellow_warning,
}

# Review-verdict colors for the runner's post-task verdict display.
# Keys are uppercase verdict strings as returned by parse_review_verdict().
REVIEW_VERDICT_COLORS: dict[str, str] = {
    "APPROVED": green_success,
    "CHANGES_REQUESTED": yellow_warning,
    "NEEDS_DISCUSSION": blue_step,
}
