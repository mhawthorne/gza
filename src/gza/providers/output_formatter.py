"""Shared stream output formatting for provider event logs."""

from __future__ import annotations

from rich.console import Console
from rich.markup import escape as rich_escape

from ..colors import TaskStreamColors


def format_runtime(seconds: int) -> str:
    """Format runtime seconds into compact human-readable text."""
    if seconds >= 60:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds}s"


def format_token_count(total_tokens: int) -> str:
    """Format token counts into compact units."""
    if total_tokens > 1_000_000:
        return f"{total_tokens / 1_000_000:.1f}M tokens"
    if total_tokens > 1000:
        return f"{total_tokens // 1000}k tokens"
    return f"{total_tokens} tokens"


def truncate_text(text: str, max_length: int) -> str:
    """Trim text to max_length with ellipsis when needed."""
    if max_length <= 0 or len(text) <= max_length:
        return text
    if max_length <= 3:
        return text[:max_length]
    return text[:max_length - 3] + "..."


class StreamOutputFormatter:
    """Shared formatter for provider event lines and step headers."""

    def __init__(self, console: Console | None = None, styles: TaskStreamColors | None = None):
        from ..colors import TASK_STREAM_COLORS, build_rich_theme
        self.styles = styles or TASK_STREAM_COLORS
        self.console = console or Console(theme=build_rich_theme())

    def print_step_header(
        self,
        step_num: int,
        total_tokens: int,
        cost_usd: float,
        runtime_seconds: int,
        *,
        blank_line_before: bool = False,
    ) -> None:
        """Print a standardized, colorized step header line."""
        if blank_line_before:
            self.console.print()
        token_str = format_token_count(total_tokens)
        runtime_str = format_runtime(runtime_seconds)
        self.console.print(
            f"| Step {step_num} | {token_str} | ${cost_usd:.2f} | {runtime_str} |",
            style=self.styles.step_header,
        )

    def print_turn_header(
        self,
        turn_count: int,
        total_tokens: int,
        cost_usd: float,
        runtime_seconds: int,
        *,
        blank_line_before: bool = False,
    ) -> None:
        """Backward-compatible alias for print_step_header."""
        self.print_step_header(
            turn_count,
            total_tokens,
            cost_usd,
            runtime_seconds,
            blank_line_before=blank_line_before,
        )

    def print_tool_event(self, label: str, detail: str = "", *, prefix: str = "") -> None:
        """Print a colorized tool usage line."""
        suffix = f" {rich_escape(detail)}" if detail else ""
        self.console.print(f"{prefix}→ {rich_escape(label)}{suffix}", style=self.styles.tool_use)

    def print_agent_message(self, text: str, *, prefix: str = "") -> None:
        """Print a colorized assistant message line."""
        self.console.print(f"{prefix}{rich_escape(text)}", style=self.styles.assistant_text)

    def print_error(self, message: str, *, prefix: str = "") -> None:
        """Print a colorized error line."""
        self.console.print(f"{prefix}{rich_escape(message)}", style=self.styles.error)

    def print_todo(self, status: str, content: str, *, prefix: str = "  ") -> None:
        """Print a TodoWrite entry with status color and icon."""
        status_icons = {
            "pending": "○",
            "in_progress": "◐",
            "completed": "●",
        }
        icon = status_icons.get(status, "○")
        style = getattr(self.styles, f"todo_{status}", self.styles.todo_pending)
        self.console.print(f"{prefix}{icon} {rich_escape(content)}", style=style)
