"""Shared log rendering dataclasses and helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from rich.markup import escape as rich_escape

from ..console import truncate


@dataclass
class RenderStats:
    step_count: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0


@dataclass
class RenderedLines:
    log_lines: list[str] = field(default_factory=list)
    tv_lines: list[str] = field(default_factory=list)
    starts_step: bool = False


class ProviderLogRenderer(Protocol):
    stats: RenderStats
    suppressed_count: int

    def handle_log(self, entry: dict[str, Any], *, live: bool) -> RenderedLines: ...

    def handle_tv(self, entry: dict[str, Any]) -> RenderedLines: ...


def normalize_model_name(model: str | None) -> str | None:
    """Normalize empty model values to None."""
    if not isinstance(model, str):
        return None
    normalized = model.strip()
    return normalized or None


def configured_model_from_gza_info(message: object) -> str | None:
    """Extract configured model from the shared gza provider info line."""
    if not isinstance(message, str):
        return None
    prefix = "Provider:"
    if not message.strip().startswith(prefix):
        return None
    _, _, model = message.partition("Model:")
    return normalize_model_name(model)


def model_parity_lines(configured_model: str | None, provider_model: str | None) -> list[str]:
    """Render configured-vs-provider model parity status for log surfaces."""
    configured = normalize_model_name(configured_model)
    if configured is None:
        return []
    provider = normalize_model_name(provider_model)
    provider_display = provider or "(not echoed by provider)"
    lines = [f"Model parity: configured={configured}; provider={provider_display}"]
    if provider is None:
        lines.append(f"Warning: provider did not echo model; configured={configured}")
    elif provider != configured:
        lines.append(
            f"Warning: provider model mismatch; configured={configured}; provider={provider}"
        )
    return lines


def result_step_count(result_entry: dict[str, Any]) -> int | None:
    """Resolve a result entry's step count using step-first fallback."""
    num_steps = result_entry.get("num_steps")
    if isinstance(num_steps, int):
        return num_steps
    num_steps_reported = result_entry.get("num_steps_reported")
    if isinstance(num_steps_reported, int):
        return num_steps_reported
    num_turns = result_entry.get("num_turns")
    if isinstance(num_turns, int):
        return num_turns
    return None


def message_content_items(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize assistant/user message content into a list of content items."""
    message = entry.get("message", {})
    if not isinstance(message, dict):
        return []
    content = message.get("content", [])
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if isinstance(content, dict):
        return [content]
    if isinstance(content, list):
        return [item for item in content if isinstance(item, dict)]
    return []


def error_lines(message: object) -> list[str]:
    """Format provider error payloads for readable log display."""
    if isinstance(message, str):
        raw_message = message.strip()
        if not raw_message:
            return ["[error]"]
        try:
            parsed = json.loads(raw_message)
        except json.JSONDecodeError:
            return [f"[error] {raw_message}"]
        if isinstance(parsed, dict):
            nested_error = parsed.get("error")
            if isinstance(nested_error, dict):
                nested_message = nested_error.get("message")
                if isinstance(nested_message, str) and nested_message.strip():
                    nested_message = nested_message.strip()
                    if nested_message != raw_message:
                        return [f"[error] {nested_message}", f"[error] payload: {raw_message}"]
        return [f"[error] {raw_message}"]

    rendered = json.dumps(message, ensure_ascii=True) if isinstance(message, (dict, list)) else str(message)
    rendered = rendered.strip()
    return [f"[error] {rendered}" if rendered else "[error]"]


def strip_shell_wrapper(cmd: str) -> str:
    """Strip common shell wrappers to show the inner command."""
    for prefix in ('/bin/bash -lc "', "/bin/bash -lc '", '/bin/sh -c "', "/bin/sh -c '"):
        if cmd.startswith(prefix):
            inner = cmd[len(prefix):]
            if inner and inner[-1] in ('"', "'"):
                inner = inner[:-1]
            return inner
    return cmd


def text_to_lines(text: str, *, max_lines: int = 6, max_chars: int = 200) -> list[str]:
    """Extract the last non-empty lines from a text block."""
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    lines = lines[-max_lines:]
    return [truncate(line, max_chars) for line in lines]


def summarize_tool_detail(tool_name: str, tool_input: dict[str, Any]) -> str:
    """Build a compact one-line tool summary for timeline and TV rendering."""
    if tool_name == "Bash":
        cmd = str(tool_input.get("command", ""))
        return truncate(strip_shell_wrapper(cmd), 100) if cmd else "Bash"
    if tool_name in {"Read", "Edit", "Write"}:
        path = str(tool_input.get("file_path", ""))
        return f"{tool_name} {path}".strip()
    if tool_name == "Grep":
        pattern = str(tool_input.get("pattern", ""))
        path = str(tool_input.get("path", ""))
        detail = f"{pattern} [{path}]".strip()
        return f"Grep {detail}".strip()
    if tool_name == "Glob":
        pattern = str(tool_input.get("pattern", ""))
        return f"Glob {pattern}".strip()
    if tool_name == "TodoWrite":
        todos = tool_input.get("todos", [])
        if isinstance(todos, list):
            return f"TodoWrite {len(todos)} todos"
        return "TodoWrite"
    return tool_name


def tool_one_liner(name: str, inp: dict[str, Any]) -> str:
    """Compact tool-use summary for TV."""
    if name == "Bash":
        cmd = str(inp.get("command", ""))
        return f"$ {truncate(strip_shell_wrapper(cmd), 120)}" if cmd else "$ (bash)"
    if name in ("Read", "Edit", "Write"):
        path = str(inp.get("file_path", ""))
        return f"{name} {path}"
    if name == "Grep":
        pattern = str(inp.get("pattern", ""))
        path = str(inp.get("path", ""))
        return f"Grep {truncate(pattern, 40)} [{path}]"
    if name == "Glob":
        return f"Glob {inp.get('pattern', '')}"
    if name == "TodoWrite":
        todos = inp.get("todos", [])
        if isinstance(todos, list):
            return f"TodoWrite {len(todos)} todos"
    return name


def compact_json(value: object, *, max_chars: int) -> str:
    """Render a value as a compact single-line JSON-ish string."""
    if isinstance(value, str):
        rendered = value.replace("\n", "\\n").replace("\r", "\\r")
    else:
        rendered = json.dumps(value, ensure_ascii=True, sort_keys=True)
    return truncate(rendered, max_chars)


def generic_log_summary(
    entry: dict[str, Any],
    *,
    max_value_chars: int = 120,
) -> str:
    """Build a compact generic fallback line for unknown provider events."""
    event_type = entry.get("type")
    parts = [f"[event:{rich_escape(str(event_type) if event_type is not None else '(missing)')}]"]
    remaining_keys = [key for key in entry if key != "type"]
    high_signal = (
        ("subtype", entry.get("subtype")),
        ("message", entry.get("message")),
        ("error", entry.get("error")),
        ("role", entry.get("role")),
        ("thread_id", entry.get("thread_id")),
        ("session_id", entry.get("session_id")),
        ("model", entry.get("model")),
        ("item.type", (entry.get("item") or {}).get("type") if isinstance(entry.get("item"), dict) else None),
        ("item.id", (entry.get("item") or {}).get("id") if isinstance(entry.get("item"), dict) else None),
        ("item.status", (entry.get("item") or {}).get("status") if isinstance(entry.get("item"), dict) else None),
    )
    used_keys: set[str] = set()
    shown = 0
    for label, value in high_signal:
        if value in (None, "", [], {}):
            continue
        used_keys.add(label.split(".")[0])
        rendered = compact_json(value, max_chars=max_value_chars)
        parts.append(f"{label}={rich_escape(rendered)}")
        shown += 1
        if shown >= 3:
            break
    extra_keys = [key for key in remaining_keys if key not in used_keys]
    if extra_keys:
        parts.append(f"keys={','.join(extra_keys[:6])}")
    return " ".join(parts)


def generic_tv_summary(entry: dict[str, Any], *, max_value_chars: int = 80) -> str:
    """Short TV fallback for unknown provider events."""
    event_type = entry.get("type")
    summary = f"event:{event_type if event_type is not None else '(missing)'}"
    message = entry.get("message")
    if message not in (None, ""):
        summary += f" message={compact_json(message, max_chars=max_value_chars)}"
    return summary


def pretty_json_lines(entry: dict[str, Any]) -> list[str]:
    """Pretty JSON lines for verbose fallback rendering."""
    return json.dumps(entry, ensure_ascii=True, indent=2, sort_keys=True).splitlines()


def truncated_json_lines(entry: dict[str, Any], *, max_lines: int = 8) -> list[str]:
    """Pretty JSON lines truncated for panel-constrained TV rendering."""
    lines = pretty_json_lines(entry)
    if len(lines) <= max_lines:
        return lines
    visible_lines = max(max_lines - 1, 0)
    truncated_count = len(lines) - visible_lines
    if visible_lines == 0:
        return [f"... (+{len(lines)} lines)"]
    return [*lines[:visible_lines], f"... (+{truncated_count} lines)"]
