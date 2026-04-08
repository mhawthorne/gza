"""Rich console output helpers for gza."""

import shutil

from rich.console import Console

import gza.colors as _colors

from .db import TaskStats

__all__ = [
    "console",
    "truncate",
    "shorten_prompt",
    "prompt_available_width",
    "get_terminal_width",
    "format_duration",
    "task_header",
    "stats_line",
    "success_message",
    "error_message",
    "warning_message",
    "info_line",
    "next_steps",
    "MAX_PROMPT_DISPLAY_SHORT",
    "MAX_PROMPT_DISPLAY",
    "MAX_PR_TITLE_LENGTH",
    "MAX_PR_BODY_LENGTH",
]

# Shared console instance for all output
console = Console()

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


def task_header(prompt: str, task_id: str, task_type: str) -> None:
    """Print a styled task header with prompt, ID, and type."""
    rc = _colors.RUNNER_COLORS
    prompt_display = prompt[:80] + "..." if len(prompt) > 80 else prompt
    console.print(f"[{rc.heading}]=== Task: {prompt_display} ===[/{rc.heading}]")
    console.print(f"    [{rc.label}]ID:[/{rc.label}] [{rc.task_id}]{task_id}[/{rc.task_id}]")
    console.print(f"    [{rc.label}]Type:[/{rc.label}] [{rc.task_type}]{task_type}[/{rc.task_type}]")


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


def success_message(title: str) -> None:
    """Print a success header."""
    rc = _colors.RUNNER_COLORS
    console.print(f"[{rc.success}]=== {title} ===[/{rc.success}]")


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


def next_steps(commands: list[tuple[str, str]]) -> None:
    """Print a list of next step commands with comments.

    Args:
        commands: List of (command, comment) tuples
    """
    rc = _colors.RUNNER_COLORS
    console.print("\nNext steps:")
    for command, comment in commands:
        console.print(f"  [{rc.next_cmd}]{command}[/{rc.next_cmd}]           [{rc.next_comment}]{comment}[/{rc.next_comment}]")
