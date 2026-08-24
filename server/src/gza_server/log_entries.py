"""Map raw task-log JSONL records to a provider-neutral event model.

The provider renderers in ``gza.providers`` emit rich-markup line strings for a
terminal: the structure a web page needs -- which part is a tool call, which is
its result, what the exit code was -- is already flattened by the time they
return. Rather than refactor three renderers, this module consumes the same raw
records and reuses their *pure* helpers (``message_content_items``,
``summarize_tool_detail``, ``error_lines``) to build structured events the
templates can style.

Claude, Codex and Gemini all speak different JSONL dialects. Each is mapped as
far as its shape is known; anything unrecognized degrades to a readable
``unknown`` event carrying its own JSON rather than being dropped.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gza.providers.log_rendering import (
    error_lines,
    message_content_items,
    result_step_count,
    strip_shell_wrapper,
    summarize_tool_detail,
)

TEXT = "text"
THINKING = "thinking"
TOOL_USE = "tool_use"
TOOL_RESULT = "tool_result"
ERROR = "error"
RESULT = "result"
SYSTEM = "system"
OPS = "ops"
UNKNOWN = "unknown"


@dataclass(frozen=True)
class LogEvent:
    """One renderable unit of a task conversation."""

    kind: str
    offset: int
    title: str = ""
    body: str = ""
    role: str | None = None
    tool_name: str | None = None
    is_error: bool = False

    @property
    def has_body(self) -> bool:
        return bool(self.body.strip())


def events_for(offset: int, record: dict[str, Any] | None, raw: str) -> list[LogEvent]:
    """Convert one JSONL record into zero or more renderable events."""
    if record is None:
        return [LogEvent(kind=UNKNOWN, offset=offset, title="unparsed line", body=raw)]

    event_type = str(record.get("type") or "")

    if event_type == "gza":
        return _ops_events(offset, record)
    if event_type in {"assistant", "user"}:
        return _message_events(offset, record, role=event_type)
    if event_type == "system":
        return _system_events(offset, record)
    if event_type == "result":
        return _result_events(offset, record)
    if event_type in {"error", "tool_error"}:
        return [
            LogEvent(
                kind=ERROR,
                offset=offset,
                title="error",
                body="\n".join(_plain_error_lines(record.get("message", record))),
                is_error=True,
            )
        ]
    if event_type in {"message", "tool_use", "tool_output"}:
        return _gemini_events(offset, record, event_type)
    if event_type in {"item.started", "item.completed"}:
        return _codex_events(offset, record)
    if event_type in {"thread.started", "turn.started", "turn.completed", "init"}:
        return [LogEvent(kind=SYSTEM, offset=offset, title=event_type.replace(".", " "))]

    return [LogEvent(kind=UNKNOWN, offset=offset, title=event_type or "(no type)", body=raw)]


def _ops_events(offset: int, record: dict[str, Any]) -> list[LogEvent]:
    subtype = str(record.get("subtype") or "info")
    return [
        LogEvent(
            kind=OPS,
            offset=offset,
            title=subtype,
            body=str(record.get("message") or ""),
            is_error=subtype == "error",
        )
    ]


def _message_events(offset: int, record: dict[str, Any], *, role: str) -> list[LogEvent]:
    events: list[LogEvent] = []
    for item in message_content_items(record):
        item_type = str(item.get("type") or "")
        if item_type == "text":
            text = str(item.get("text") or "").strip()
            if text:
                events.append(LogEvent(kind=TEXT, offset=offset, role=role, body=text))
        elif item_type == "thinking":
            text = str(item.get("thinking") or item.get("text") or "").strip()
            if text:
                events.append(
                    LogEvent(kind=THINKING, offset=offset, role=role, title="thinking", body=text)
                )
        elif item_type == "tool_use":
            name = str(item.get("name") or "tool")
            tool_input = item.get("input")
            tool_input = tool_input if isinstance(tool_input, dict) else {}
            events.append(
                LogEvent(
                    kind=TOOL_USE,
                    offset=offset,
                    role=role,
                    tool_name=name,
                    title=summarize_tool_detail(name, tool_input),
                    body=_tool_input_body(name, tool_input),
                )
            )
        elif item_type == "tool_result":
            events.append(
                LogEvent(
                    kind=TOOL_RESULT,
                    offset=offset,
                    role=role,
                    title="tool result",
                    body=_content_text(item.get("content")),
                    is_error=bool(item.get("is_error")),
                )
            )
    return events


def _system_events(offset: int, record: dict[str, Any]) -> list[LogEvent]:
    subtype = str(record.get("subtype") or "system")
    model = record.get("model")
    title = f"{subtype} · {model}" if model else subtype
    return [LogEvent(kind=SYSTEM, offset=offset, title=title)]


def _result_events(offset: int, record: dict[str, Any]) -> list[LogEvent]:
    parts: list[str] = []
    steps = result_step_count(record)
    if steps is not None:
        parts.append(f"{steps} steps")
    duration = record.get("duration_ms")
    if isinstance(duration, (int, float)):
        parts.append(f"{duration / 1000:.1f}s")
    cost = record.get("total_cost_usd")
    if isinstance(cost, (int, float)):
        parts.append(f"${cost:.4f}")
    is_error = bool(record.get("is_error"))
    return [
        LogEvent(
            kind=RESULT,
            offset=offset,
            title="result" + (" (error)" if is_error else ""),
            body=" · ".join(parts),
            is_error=is_error,
        )
    ]


def _gemini_events(offset: int, record: dict[str, Any], event_type: str) -> list[LogEvent]:
    if event_type == "message":
        role = str(record.get("role") or "assistant")
        return [
            LogEvent(
                kind=TEXT, offset=offset, role=role, body=_content_text(record.get("content"))
            )
        ]
    if event_type == "tool_use":
        name = str(record.get("tool_name") or "tool")
        tool_input = record.get("tool_input")
        tool_input = tool_input if isinstance(tool_input, dict) else {}
        return [
            LogEvent(
                kind=TOOL_USE,
                offset=offset,
                tool_name=name,
                title=summarize_tool_detail(name, tool_input),
                body=_tool_input_body(name, tool_input),
            )
        ]
    return [
        LogEvent(
            kind=TOOL_RESULT,
            offset=offset,
            title="tool output",
            body=_content_text(record.get("content") or record.get("output")),
        )
    ]


def _codex_events(offset: int, record: dict[str, Any]) -> list[LogEvent]:
    item = record.get("item")
    if not isinstance(item, dict):
        return [LogEvent(kind=SYSTEM, offset=offset, title=str(record.get("type") or ""))]
    item_type = str(item.get("type") or "")
    if item_type in {"agent_message", "assistant_message"}:
        return [LogEvent(kind=TEXT, offset=offset, role="assistant", body=str(item.get("text") or ""))]
    if item_type == "reasoning":
        return [
            LogEvent(
                kind=THINKING,
                offset=offset,
                title="thinking",
                body=str(item.get("text") or item.get("summary") or ""),
            )
        ]
    if item_type == "command_execution":
        command = strip_shell_wrapper(str(item.get("command") or ""))
        exit_code = item.get("exit_code")
        return [
            LogEvent(
                kind=TOOL_USE,
                offset=offset,
                tool_name="Bash",
                title=command or "Bash",
                body=str(item.get("aggregated_output") or ""),
                is_error=isinstance(exit_code, int) and exit_code != 0,
            )
        ]
    return [LogEvent(kind=SYSTEM, offset=offset, title=item_type or "item")]


def _tool_input_body(name: str, tool_input: dict[str, Any]) -> str:
    """Show the part of a tool call worth reading in full."""
    if name == "Bash":
        return strip_shell_wrapper(str(tool_input.get("command") or ""))
    for key in ("pattern", "prompt", "content", "new_string", "description"):
        value = tool_input.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _content_text(content: object) -> str:
    """Flatten a provider content payload to displayable text."""
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        return str(content.get("text") or "")
    if isinstance(content, list):
        parts = [_content_text(item) for item in content]
        return "\n".join(part for part in parts if part)
    return "" if content is None else str(content)


def _plain_error_lines(message: object) -> list[str]:
    """Reuse the provider error formatter without its terminal prefixes."""
    return [line.removeprefix("[error] ").removeprefix("[error]").strip() for line in error_lines(message)]
