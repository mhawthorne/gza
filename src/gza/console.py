"""Rich console output helpers for gza."""

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console

import gza.colors as _colors

from .db import Task, TaskStats

if TYPE_CHECKING:
    from .db import SqliteTaskStore
    from .learnings import LearningsResult

__all__ = [
    "console",
    "truncate",
    "shorten_prompt",
    "prompt_available_width",
    "get_terminal_width",
    "format_duration",
    "task_header",
    "task_footer",
    "stats_line",
    "error_message",
    "warning_message",
    "info_line",
    "MAX_PROMPT_DISPLAY_SHORT",
    "MAX_PROMPT_DISPLAY",
    "MAX_PR_TITLE_LENGTH",
    "MAX_PR_BODY_LENGTH",
]

# Shared console instance for all output. ``highlight=False`` disables Rich's
# default ReprHighlighter so numbers, paths, strings etc. aren't implicitly
# bolded/colored — every style in this module is explicit via markup. The
# provider stream console is separate and keeps highlighting enabled because
# it renders arbitrary provider output we don't control.
console = Console(highlight=False)

# Display truncation constants
MAX_PROMPT_DISPLAY_SHORT = 50
MAX_PROMPT_DISPLAY = 60
MAX_PR_TITLE_LENGTH = 72
MAX_PR_BODY_LENGTH = 500


def truncate(text: str, max_len: int, suffix: str = '...') -> str:
    """Truncate text to max_len, adding suffix if truncated.

    Args:
        text: Text to truncate
        max_len: Maximum length including suffix
        suffix: Suffix to add when truncating (default: '...')

    Returns:
        Original text if within max_len, otherwise truncated text with suffix
    """
    if len(text) <= max_len:
        return text
    return text[:max_len - len(suffix)] + suffix


def prompt_available_width(prefix: int = 0, suffix: int = 0) -> int:
    """Compute available width for a prompt, given surrounding elements.

    Subtracts *prefix*, *suffix*, and a 5%-of-terminal-width padding
    per element from the terminal width.

    Args:
        prefix: Characters consumed before the prompt (e.g. status + task ID).
        suffix: Characters consumed after the prompt.
    """
    tw = get_terminal_width()
    pad = max(1, int(tw * 0.05))
    used = prefix + suffix
    # One pad between prefix and prompt; one between prompt and suffix if present.
    pads = (pad if prefix else 0) + (pad if suffix else 0)
    return max(20, tw - used - pads)


def shorten_prompt(text: str, available: int | None = None) -> str:
    """Shorten a prompt to fit in *available* columns.

    Collapses newlines into '. ' separators, then truncates.

    Args:
        text: The prompt text to shorten.
        available: Maximum character width for the prompt.  Callers should
                   compute this via :func:`prompt_available_width`.
                   When ``None``, falls back to 60% of the terminal width.
    """
    if available is None:
        available = int(get_terminal_width() * 0.6)
    available = max(20, available)
    flat = '. '.join(line.strip() for line in text.splitlines() if line.strip())
    return truncate(flat, available)


def get_terminal_width() -> int:
    """Get the current terminal width.

    Returns:
        Terminal width in characters, defaulting to 80 if unable to determine.
    """
    try:
        return shutil.get_terminal_size().columns
    except (AttributeError, ValueError, OSError):
        return 80


def format_duration(seconds: float, verbose: bool = False) -> str:
    """Format duration in human-readable form.

    Args:
        seconds: Duration in seconds
        verbose: If True, include hours for long durations

    Returns:
        Formatted duration string (e.g., '2m 30s', '1h 23m', or '45s')
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600 or not verbose:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def task_header(prompt: str, task_id: str, task_type: str, slug: str | None = None) -> None:
    """Print a styled task header with prompt, ID, type, and optional slug.

    Header format — a four-dash separator bounds a flat, unindented block:

        ----
        Task: <prompt>
        ID: <task_id>
        Slug: <slug>      (omitted when slug is None or empty)
        Type: <task_type>
        ----
    """
    rc = _colors.RUNNER_COLORS
    prompt_display = prompt[:80] + "..." if len(prompt) > 80 else prompt
    separator = f"[{rc.heading}]----[/{rc.heading}]"
    console.print(separator)
    console.print(f"[{rc.label}]Task:[/{rc.label}] [{rc.heading}]{prompt_display}[/{rc.heading}]")
    console.print(f"[{rc.label}]ID:[/{rc.label}] [{rc.task_id}]{task_id}[/{rc.task_id}]")
    if slug:
        console.print(f"[{rc.label}]Slug:[/{rc.label}] [{rc.slug}]{slug}[/{rc.slug}]")
    console.print(f"[{rc.label}]Type:[/{rc.label}] [{rc.task_type}]{task_type}[/{rc.task_type}]")
    console.print(separator)


def stats_line(stats: TaskStats, has_commits: bool | None = None) -> None:
    """Print task statistics in a formatted line."""
    rc = _colors.RUNNER_COLORS
    parts = []

    if stats.duration_seconds is not None:
        duration_str = format_duration(stats.duration_seconds)
        parts.append(f"[{rc.label}]Runtime:[/{rc.label}] [{rc.value}]{duration_str}[/{rc.value}]")

    if stats.num_steps_reported is not None:
        steps_display = str(stats.num_steps_reported)
        if stats.num_steps_computed is not None and stats.num_steps_computed != stats.num_steps_reported:
            steps_display += f" (computed: {stats.num_steps_computed})"
        if stats.num_turns_reported is not None and stats.num_turns_reported != stats.num_steps_reported:
            steps_display += f" (legacy turns: {stats.num_turns_reported})"
        parts.append(f"[{rc.label}]Steps:[/{rc.label}] [{rc.value}]{steps_display}[/{rc.value}]")
    elif stats.num_steps_computed is not None:
        parts.append(f"[{rc.label}]Steps:[/{rc.label}] [{rc.value}]{stats.num_steps_computed}[/{rc.value}]")
    elif stats.num_turns_reported is not None:
        turns_display = str(stats.num_turns_reported)
        if stats.num_turns_computed is not None and stats.num_turns_computed != stats.num_turns_reported:
            turns_display += f" (computed: {stats.num_turns_computed})"
        parts.append(f"[{rc.label}]Turns:[/{rc.label}] [{rc.value}]{turns_display}[/{rc.value}]")

    if stats.input_tokens is not None or stats.output_tokens is not None:
        input_tokens = stats.input_tokens if stats.input_tokens is not None else 0
        output_tokens = stats.output_tokens if stats.output_tokens is not None else 0
        estimated_suffix = f" [{rc.estimated}](estimated)[/{rc.estimated}]" if stats.tokens_estimated else ""
        parts.append(
            f"[{rc.label}]Tokens:[/{rc.label}] [{rc.value}]in {input_tokens:,} / out {output_tokens:,}[/{rc.value}]{estimated_suffix}"
        )

    if stats.cost_usd is not None:
        estimated_suffix = f" [{rc.estimated}](estimated)[/{rc.estimated}]" if stats.cost_estimated else ""
        parts.append(f"[{rc.label}]Cost:[/{rc.label}] [{rc.value}]${stats.cost_usd:.4f}[/{rc.value}]{estimated_suffix}")

    if has_commits is not None:
        commit_value = f"[{rc.commits_yes}]yes[/{rc.commits_yes}]" if has_commits else "no"
        parts.append(f"[{rc.label}]Commits:[/{rc.label}] {commit_value}")

    if parts:
        console.print(f"Stats: {' | '.join(parts)}")


def error_message(message: str) -> None:
    """Print an error message."""
    rc = _colors.RUNNER_COLORS
    console.print(f"[{rc.error}]{message}[/{rc.error}]")


def warning_message(message: str) -> None:
    """Print a warning message."""
    rc = _colors.RUNNER_COLORS
    console.print(f"[{rc.warning}]{message}[/{rc.warning}]")


def info_line(label: str, value: str) -> None:
    """Print an info line with label and value."""
    rc = _colors.RUNNER_COLORS
    console.print(f"[{rc.label}]{label}:[/{rc.label}] {value}")


def _status_is_failure(status: str) -> bool:
    """A status string represents a failure outcome when it doesn't start with 'Done'."""
    return not status.startswith("Done")


def _recommend_next_steps(
    task: Task,
    *,
    status: str,
    store: "SqliteTaskStore | None" = None,
) -> list[tuple[str, str]]:
    """Return suggested next-step commands for a task, based on type and outcome.

    This is the single place that decides what next-step hints get printed at
    the end of a task. All per-task-type dispatch lives here — callers pass the
    task, the status string, and (optionally) a task store, and this function
    decides.

    The ``store`` is optional so that simple consumers (tests, ad-hoc callers)
    don't need a database. When provided, it's available for lineage-aware
    recommendations — e.g., walking ``task.based_on`` / ``task.depends_on``
    chains to tailor suggestions to the surrounding task graph. Today's rules
    only need ``task.id`` / ``task.depends_on``, but the parameter is in place
    so new rules can grow here without another signature change.

    Returns a list of ``(command, description)`` tuples. An empty list means no
    suggestions should be printed.
    """
    _ = store  # reserved for lineage-aware recommendations; see docstring
    if task.id is None:
        return []

    if _status_is_failure(status):
        return [
            (f"gza retry {task.id}", "retry from scratch"),
            (f"gza resume {task.id}", "resume from where it left off"),
        ]

    # Success path: dispatch by task type.
    if task.task_type in ("implement", "improve", "rebase"):
        return [
            (f"gza merge {task.id}", "merge branch for task"),
            (f"gza pr {task.id}", "create a PR"),
        ]
    if task.task_type == "explore":
        return [(f"gza add --based-on {task.id}", "implement based on this exploration")]
    if task.task_type == "plan":
        return [(f"gza implement {task.id}", "implement this plan")]
    if task.task_type == "review" and task.depends_on is not None:
        return [(f"gza improve {task.depends_on}", "address review feedback")]
    return []


def task_footer(
    task: Task,
    stats: TaskStats | None = None,
    *,
    status: str,
    branch: str | None = None,
    report: str | None = None,
    verdict: str | None = None,
    worktree: str | Path | None = None,
    learnings: "LearningsResult | None" = None,
    store: "SqliteTaskStore | None" = None,
) -> None:
    """Print the end-of-task footer.

    Mirrors :func:`task_header`: the block is bounded by a four-dash separator,
    every line is flat (no indentation), and optional fields are omitted when
    not applicable. Suggested next-step commands (if any) are rendered inside
    the block, just before the closing separator, via
    :func:`_recommend_next_steps`.
    """
    rc = _colors.RUNNER_COLORS
    separator = f"[{rc.heading}]----[/{rc.heading}]"

    status_color = rc.error if _status_is_failure(status) else rc.success

    console.print(separator)
    console.print(f"[{rc.label}]Status:[/{rc.label}] [{status_color}]{status}[/{status_color}]")
    if task.id is not None:
        console.print(f"[{rc.label}]ID:[/{rc.label}] [{rc.task_id}]{task.id}[/{rc.task_id}]")
    if task.slug:
        console.print(f"[{rc.label}]Slug:[/{rc.label}] [{rc.slug}]{task.slug}[/{rc.slug}]")
    console.print(f"[{rc.label}]Type:[/{rc.label}] [{rc.task_type}]{task.task_type}[/{rc.task_type}]")

    if stats is not None:
        _print_stats_as_footer_line(stats)

    if branch:
        console.print(f"[{rc.label}]Branch:[/{rc.label}] [blue]{branch}[/blue]")
    if report:
        console.print(f"[{rc.label}]Report:[/{rc.label}] {report}")
    if verdict:
        from .colors import UNMERGED_COLORS_DICT
        verdict_key = {
            "APPROVED": "review_approved",
            "APPROVED_WITH_FOLLOWUPS": "review_followups",
            "CHANGES_REQUESTED": "review_changes",
            "NEEDS_DISCUSSION": "review_discussion",
        }.get(verdict)
        verdict_color = UNMERGED_COLORS_DICT.get(verdict_key, "white") if verdict_key else "white"
        console.print(f"[{rc.label}]Verdict:[/{rc.label}] [{verdict_color}]{verdict}[/{verdict_color}]")
    if worktree:
        console.print(f"[{rc.label}]Worktree:[/{rc.label}] {worktree}")
    if learnings is not None:
        console.print(
            f"[{rc.label}]Learnings:[/{rc.label}] "
            f"updated from {learnings.tasks_used} tasks "
            f"(+{learnings.added_count}/-{learnings.removed_count}/={learnings.retained_count}, "
            f"churn {learnings.churn_percent:.1f}%)"
        )

    steps = _recommend_next_steps(task, status=status, store=store)
    if steps:
        console.print(f"[{rc.label}]Next steps:[/{rc.label}]")
        for command, comment in steps:
            console.print(
                f"  [{rc.next_cmd}]{command}[/{rc.next_cmd}]  [{rc.next_comment}]{comment}[/{rc.next_comment}]"
            )

    console.print(separator)


def _print_stats_as_footer_line(stats: TaskStats) -> None:
    """Internal: render a single ``Stats: ...`` line inside the footer.

    Lives alongside :func:`task_footer` so the footer owns all of its own
    layout. ``stats_line()`` remains the public helper for callers that want
    the standalone, pre-refactor form (still used by a few log paths).
    """
    rc = _colors.RUNNER_COLORS
    parts = []
    if stats.duration_seconds is not None:
        parts.append(f"[{rc.value}]{format_duration(stats.duration_seconds)}[/{rc.value}]")
    if stats.num_steps_reported is not None:
        parts.append(f"[{rc.value}]{stats.num_steps_reported} steps[/{rc.value}]")
    elif stats.num_steps_computed is not None:
        parts.append(f"[{rc.value}]{stats.num_steps_computed} steps[/{rc.value}]")
    if stats.cost_usd is not None:
        estimated_suffix = f" [{rc.estimated}](estimated)[/{rc.estimated}]" if stats.cost_estimated else ""
        parts.append(f"[{rc.value}]${stats.cost_usd:.4f}[/{rc.value}]{estimated_suffix}")
    if parts:
        console.print(f"[{rc.label}]Stats:[/{rc.label}] {' | '.join(parts)}")
