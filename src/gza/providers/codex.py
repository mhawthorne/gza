"""OpenAI Codex CLI provider implementation."""

from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich.markup import escape as rich_escape

from .base import (
    DockerConfig,
    PreflightCheckResult,
    Provider,
    RunResult,
    build_docker_cmd,
    ensure_docker_image,
    verify_docker_credentials,
    write_ops_event,
    write_preflight_entry,
)
from .log_rendering import (
    RenderedLines,
    RenderStats,
    compact_json,
    configured_model_from_gza_info,
    error_lines,
    generic_log_summary,
    model_parity_lines,
    normalize_model_name,
    pretty_json_lines,
    strip_shell_wrapper,
    text_to_lines,
    truncated_json_lines,
    tv_error_lines,
)
from .output_formatter import StreamOutputFormatter, truncate_text

if TYPE_CHECKING:
    from ..config import Config


logger = logging.getLogger(__name__)

CODEX_EVENT_REGISTRY: dict[str, dict[str, object]] = {
    "gza": {"render": "_render_gza", "live": False},
    "raw": {"render": "_render_raw", "live": False},
    "error": {"render": "_render_error", "live": "_handle_live_error_event"},
    "thread.started": {"render": "_render_thread_started", "live": "_handle_live_thread_started_event"},
    "turn.started": {"render": "_render_turn_started", "live": "_handle_live_turn_started_event"},
    "turn.failed": {"render": "_render_turn_failed", "live": "_handle_live_turn_failed_event"},
    "turn.completed": {"render": "_render_turn_completed", "live": "_handle_live_turn_completed_event"},
    "item.started": {"render": "_render_item_started", "live": "_handle_live_item_started_event"},
    "item.updated": {"render": "_render_item_updated", "live": "_handle_live_item_updated_event"},
    "item.completed": {"render": "_render_item_completed", "live": "_handle_live_item_completed_event"},
}
CODEX_RENDER_EVENT_HANDLERS: dict[str, str] = {
    event_type: str(method_name)
    for event_type, metadata in CODEX_EVENT_REGISTRY.items()
    if (method_name := metadata.get("render")) is not None
}
CODEX_RENDER_KNOWN_EVENT_TYPES = frozenset(CODEX_RENDER_EVENT_HANDLERS)
CODEX_LIVE_EVENT_HANDLERS: dict[str, str] = {
    event_type: str(handler_name)
    for event_type, metadata in CODEX_EVENT_REGISTRY.items()
    if isinstance(handler_name := metadata.get("live"), str)
}
CODEX_LIVE_KNOWN_EVENT_TYPES = frozenset(CODEX_LIVE_EVENT_HANDLERS)
CODEX_ITEM_REGISTRY: dict[str, dict[str, object]] = {
    "agent_message": {"render": "_render_item_agent_message", "live": "_handle_live_item_agent_message"},
    "collab_tool_call": {"render": "_render_item_collab_tool_call", "live": "_handle_live_item_collab_tool_call"},
    "command_execution": {
        "render": "_render_item_command_execution",
        "live": "_handle_live_item_command_execution",
    },
    "file_change": {"render": "_render_item_file_change", "live": "_handle_live_item_file_change"},
    "mcp_tool_call": {"render": "_render_item_mcp_tool_call", "live": "_handle_live_item_mcp_tool_call"},
    "reasoning": {"render": "_render_item_reasoning", "live": "_handle_live_item_reasoning"},
    "todo_list": {"render": "_render_item_todo_list", "live": "_handle_live_item_todo_list"},
    "web_search": {"render": "_render_item_web_search", "live": "_handle_live_item_web_search"},
}
CODEX_RENDER_ITEM_HANDLERS: dict[str, str] = {
    item_type: str(handler_name)
    for item_type, metadata in CODEX_ITEM_REGISTRY.items()
    if isinstance(handler_name := metadata.get("render"), str)
}
CODEX_LIVE_ITEM_HANDLERS: dict[str, str] = {
    item_type: str(handler_name)
    for item_type, metadata in CODEX_ITEM_REGISTRY.items()
    if isinstance(handler_name := metadata.get("live"), str)
}
CODEX_RENDER_KNOWN_ITEM_TYPES = frozenset(
    CODEX_RENDER_ITEM_HANDLERS
)
CODEX_LIVE_KNOWN_ITEM_TYPES = frozenset(
    CODEX_LIVE_ITEM_HANDLERS
)
_CODEX_FILE_CHANGE_LIMIT = 5
_CODEX_TODO_LIMIT = 5


def _codex_tool_log_line(label: str, detail: str = "") -> str:
    suffix = f" {rich_escape(detail)}" if detail else ""
    return f"[green]\\[tool: {rich_escape(label)}][/green]{suffix}"


def _codex_tool_tv_line(label: str, detail: str = "") -> str:
    return f"-> {label}{f' {detail}' if detail else ''}"


def _codex_more_line(remaining: int) -> str | None:
    if remaining <= 0:
        return None
    noun = "item" if remaining == 1 else "items"
    return f"... (+{remaining} more {noun})"


def _codex_normalize_todo_status(todo: dict[str, Any]) -> str:
    status = todo.get("status")
    if isinstance(status, str) and status in {"pending", "in_progress", "completed"}:
        return status
    completed = todo.get("completed")
    if isinstance(completed, bool):
        return "completed" if completed else "pending"
    return "pending"


def _codex_todo_summary(todos: list[dict[str, Any]]) -> str:
    if not todos:
        return "0 todos"
    pending = sum(1 for todo in todos if _codex_normalize_todo_status(todo) == "pending")
    in_progress = sum(1 for todo in todos if _codex_normalize_todo_status(todo) == "in_progress")
    completed = sum(1 for todo in todos if _codex_normalize_todo_status(todo) == "completed")
    return (
        f"{len(todos)} todos "
        f"(pending: {pending}, in_progress: {in_progress}, completed: {completed})"
    )


def _codex_todo_lines(todos: list[dict[str, Any]], *, tv: bool) -> list[str]:
    status_icons = {"pending": "○", "in_progress": "◐", "completed": "●"}
    lines: list[str] = []
    for todo in todos[:_CODEX_TODO_LIMIT]:
        status = _codex_normalize_todo_status(todo)
        text = truncate_text(str(todo.get("text") or todo.get("content") or "").strip(), 80)
        if not text:
            continue
        line = f"  {status_icons.get(status, '○')} {text}"
        lines.append(line if tv else rich_escape(line))
    more_line = _codex_more_line(len(todos) - min(len(todos), _CODEX_TODO_LIMIT))
    if more_line:
        lines.append(more_line if tv else rich_escape(more_line))
    return lines


def _codex_receiver_summary(receiver_thread_ids: object) -> str:
    if not isinstance(receiver_thread_ids, list):
        return "unknown"
    receivers = [str(receiver).strip() for receiver in receiver_thread_ids if str(receiver).strip()]
    if not receivers:
        return "unknown"
    summary = receivers[0]
    if len(receivers) > 1:
        summary += f" +{len(receivers) - 1}"
    return summary


def _codex_file_change_lines(changes: list[dict[str, Any]], *, tv: bool) -> list[str]:
    lines: list[str] = []
    for change in changes[:_CODEX_FILE_CHANGE_LIMIT]:
        path = str(change.get("path") or "").strip()
        kind = str(change.get("kind") or "update").strip() or "update"
        if not path:
            continue
        detail = f"{path} ({kind})"
        lines.append(_codex_tool_tv_line("edit", detail) if tv else _codex_tool_log_line("edit", detail))
    more_line = _codex_more_line(len(changes) - min(len(changes), _CODEX_FILE_CHANGE_LIMIT))
    if more_line:
        lines.append(more_line if tv else rich_escape(more_line))
    return lines


def _codex_web_search_detail(item: dict[str, Any]) -> str:
    query = str(item.get("query") or "").strip()
    return truncate_text(query, 100) if query else "search"


def _codex_collab_tool_detail(item: dict[str, Any]) -> str:
    receiver = _codex_receiver_summary(item.get("receiver_thread_ids"))
    prompt = truncate_text(str(item.get("prompt") or "").strip(), 80)
    detail = f"{receiver}"
    if prompt:
        detail += f" prompt={prompt}"
    return detail


def _codex_mcp_tool_name(item: dict[str, Any]) -> str:
    server = str(item.get("server") or "unknown").strip() or "unknown"
    tool = str(item.get("tool") or "unknown").strip() or "unknown"
    return f"mcp:{server}/{tool}"


def _codex_mcp_detail(item: dict[str, Any]) -> str:
    arguments = item.get("arguments")
    if arguments in (None, "", {}, []):
        return ""
    return compact_json(arguments, max_chars=100)


def _unknown_live_type_message(provider: str, surface: str, type_name: object) -> str:
    """Build a stable operator-facing message for unhandled live provider output."""
    rendered_type = str(type_name).strip() if type_name not in (None, "") else "<missing>"
    return f"Unhandled {provider} {surface}: {rendered_type}"


# OpenAI Codex pricing per million tokens (input, output)
# https://openai.com/api/pricing/
CODEX_PRICING = {
    "gpt-5.2-codex": (2.50, 10.00),
    "gpt-5.3-codex": (2.50, 10.00),
    "o3": (10.00, 40.00),
    "default": (2.50, 10.00),
}

CODEX_HEADLESS_EXEC_BASE_ARGS = (
    "-c",
    "check_for_update_on_startup=false",
    "exec",
    "--json",
    "--dangerously-bypass-approvals-and-sandbox",
    "--skip-git-repo-check",
)


def _estimate_tokens_from_chars(char_count: int) -> int:
    """Estimate token count from character count using a simple 4-char heuristic."""
    if char_count <= 0:
        return 0
    return (char_count + 3) // 4


def _as_nonnegative_int(value: object) -> int:
    """Convert value to non-negative int with safe fallback."""
    if isinstance(value, (int, float)):
        return max(0, int(value))
    return 0


def _ensure_codex_step_store(data: dict[str, Any]) -> None:
    if "run_step_events" not in data:
        data["run_step_events"] = []
        data["_current_step_event"] = None
        data["_legacy_event_count_by_turn"] = {}


def _codex_step_count(data: dict[str, Any]) -> int:
    return len(data.get("run_step_events", []))


def _current_codex_turn_id(data: dict[str, Any]) -> str | None:
    turn_count = _as_nonnegative_int(data.get("turn_count"))
    return f"T{turn_count}" if turn_count > 0 else None


def _allocate_codex_legacy_event_id(data: dict[str, Any], legacy_turn_id: str | None) -> str | None:
    if not legacy_turn_id:
        return None
    counters = data.get("_legacy_event_count_by_turn")
    if not isinstance(counters, dict):
        counters = {}
        data["_legacy_event_count_by_turn"] = counters
    current = int(counters.get(legacy_turn_id, 0)) + 1
    counters[legacy_turn_id] = current
    return f"{legacy_turn_id}.{current}"


def _maybe_mark_codex_max_steps_exceeded(data: dict[str, Any], max_steps: int) -> None:
    if _codex_step_count(data) > max_steps:
        data["exceeded_max_steps"] = True
        data["__terminate_process__"] = True


def _codex_step_header_usage(data: dict[str, Any]) -> tuple[int, int]:
    turn_count = _as_nonnegative_int(data.get("turn_count"))
    turns_with_usage = data.get("turns_with_usage")
    has_real_usage_for_turn = isinstance(turns_with_usage, set) and turn_count in turns_with_usage

    if has_real_usage_for_turn:
        return (
            _as_nonnegative_int(data.get("input_tokens")),
            _as_nonnegative_int(data.get("output_tokens")),
        )

    base_input = _as_nonnegative_int(data.get("input_tokens"))
    base_output = _as_nonnegative_int(data.get("output_tokens"))
    approx_input_chars = _as_nonnegative_int(data.get("approx_input_chars"))
    approx_output_chars = _as_nonnegative_int(data.get("approx_output_chars"))
    baseline_input_chars = _as_nonnegative_int(data.get("estimate_input_chars_baseline"))
    baseline_output_chars = _as_nonnegative_int(data.get("estimate_output_chars_baseline"))
    delta_input_chars = max(0, approx_input_chars - baseline_input_chars)
    delta_output_chars = max(0, approx_output_chars - baseline_output_chars)
    est_input = base_input + _estimate_tokens_from_chars(delta_input_chars)
    est_output = base_output + _estimate_tokens_from_chars(delta_output_chars)
    return est_input, est_output


def _start_codex_step(
    data: dict[str, Any],
    message_text: str | None,
    legacy_turn_id: str | None,
    *,
    legacy_event_id: str | None = None,
    summary: str | None = None,
    on_step_count: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    _ensure_codex_step_store(data)
    event: dict[str, Any] = {
        "message_role": "assistant",
        "message_text": message_text,
        "legacy_turn_id": legacy_turn_id,
        "legacy_event_id": legacy_event_id,
        "substeps": [],
        "outcome": "completed",
        "summary": summary,
    }
    data["run_step_events"].append(event)
    data["_current_step_event"] = event
    if on_step_count:
        on_step_count(len(data["run_step_events"]))
    return event


def get_pricing_for_model(model: str) -> tuple[float, float]:
    """Get (input, output) pricing per million tokens for a model."""
    if not model:
        return CODEX_PRICING["default"]
    # Try exact match first
    if model in CODEX_PRICING:
        return CODEX_PRICING[model]
    # Try prefix match
    for model_prefix, pricing in CODEX_PRICING.items():
        if model_prefix != "default" and model.startswith(model_prefix):
            return pricing
    return CODEX_PRICING["default"]


def calculate_cost(input_tokens: int, output_tokens: int, model: str = "") -> float:
    """Calculate estimated cost in USD based on token counts and model."""
    input_price, output_price = get_pricing_for_model(model)
    cost = (
        (input_tokens * input_price / 1_000_000) +
        (output_tokens * output_price / 1_000_000)
    )
    return round(cost, 4)


def _codex_error_message(entry: dict[str, Any]) -> str:
    """Extract the most useful user-facing error message from a Codex event."""
    error = entry.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    message = entry.get("message")
    if isinstance(message, str) and message.strip():
        return message.strip()
    if error not in (None, "", [], {}):
        return str(error)
    return json.dumps(entry)


class CodexLogRenderer:
    """Render Codex JSONL events for log replay/live and TV surfaces."""

    def __init__(self, *, configured_model: str | None = None, verbose: bool = False) -> None:
        self.configured_model = configured_model
        self.verbose = verbose
        self.stats = RenderStats()
        self.suppressed_count = 0
        self._parity_ready = False
        self._last_parity_signature: tuple[str | None, str | None] | None = None

    def handle_log(self, entry: dict[str, Any], *, live: bool) -> RenderedLines:
        return self._handle(entry, tv=False)

    def handle_tv(self, entry: dict[str, Any]) -> RenderedLines:
        return self._handle(entry, tv=True)

    def _handle(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        self._maybe_accumulate_usage(entry)
        event_type = entry.get("type")
        if not isinstance(event_type, str):
            return self._render_unknown(entry, tv=tv)
        method_name = CODEX_RENDER_EVENT_HANDLERS.get(event_type)
        if method_name is None:
            return self._render_unknown(entry, tv=tv)
        method = getattr(self, method_name)
        return method(entry, tv=tv)

    def _render_raw(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        message = entry.get("message", "")
        if isinstance(message, str) and message:
            lines = [rich_escape(message)] if not tv else [message]
            return RenderedLines(log_lines=lines if not tv else [], tv_lines=lines if tv else [])
        return RenderedLines()

    def _render_error(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        if tv:
            return RenderedLines(tv_lines=tv_error_lines(entry.get("message", "")))
        return RenderedLines(log_lines=[f"[red]{rich_escape(line)}[/red]" for line in error_lines(entry.get("message", ""))])

    def _render_thread_started(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        thread_id = entry.get("thread_id")
        if thread_id:
            self._parity_ready = True
            line = f"Session started (thread: {thread_id})"
            log_lines = [line] if not tv else []
            if not tv:
                log_lines.extend(self._model_parity_lines())
            return RenderedLines(log_lines=log_lines, tv_lines=[line] if tv else [])
        return self._render_unknown(entry, tv=tv)

    def _render_turn_started(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        self.suppressed_count += 1
        return RenderedLines()

    def _render_turn_failed(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        message = _codex_error_message(entry)
        if tv:
            return RenderedLines(tv_lines=tv_error_lines(message))
        return RenderedLines(log_lines=[f"[red]{rich_escape(line)}[/red]" for line in error_lines(message)])

    def _render_turn_completed(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        self.suppressed_count += 1
        return RenderedLines()

    def _render_item_started(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        self.suppressed_count += 1
        return RenderedLines()

    def _render_item_updated(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        self.suppressed_count += 1
        return RenderedLines()

    def _render_gza(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        subtype = entry.get("subtype", "")
        message = entry.get("message", "")
        if subtype == "info":
            model_value = configured_model_from_gza_info(message)
            if model_value:
                self.configured_model = model_value
        if not message:
            return RenderedLines()
        line = f"[gza:{subtype}] {message}" if subtype else f"[gza] {message}"
        log_lines = [rich_escape(line)] if not tv else []
        if not tv:
            log_lines.extend(self._model_parity_lines())
        return RenderedLines(log_lines=log_lines, tv_lines=[line] if tv else [])

    def _maybe_accumulate_usage(self, entry: dict[str, Any]) -> None:
        event_type = entry.get("type")
        if not isinstance(event_type, str):
            return
        if not (event_type.endswith(".completed") or event_type.endswith(".error")):
            return
        usage = entry.get("usage")
        if not isinstance(usage, dict):
            return
        input_tokens = _as_nonnegative_int(usage.get("input_tokens"))
        output_tokens = _as_nonnegative_int(usage.get("output_tokens"))
        cached_tokens = _as_nonnegative_int(usage.get("cached_input_tokens"))
        self.stats.input_tokens += input_tokens + cached_tokens
        self.stats.output_tokens += output_tokens
        self.stats.cost_usd = calculate_cost(
            self.stats.input_tokens,
            self.stats.output_tokens,
            self.configured_model or "",
        )

    def _model_parity_lines(self) -> list[str]:
        if not self._parity_ready:
            return []
        signature = (normalize_model_name(self.configured_model), None)
        if signature == self._last_parity_signature:
            return []
        self._last_parity_signature = signature
        return model_parity_lines(*signature)

    def _render_item_completed(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        item = entry.get("item", {})
        if not isinstance(item, dict):
            return self._render_unknown(entry, tv=tv)
        item_type = item.get("type")
        method_name = CODEX_RENDER_ITEM_HANDLERS.get(str(item_type))
        if method_name is not None:
            return getattr(self, method_name)(entry, item=item, tv=tv)
        return self._render_unknown(entry, tv=tv)

    def _render_item_agent_message(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            self.stats.step_count += 1
            if tv:
                return RenderedLines(tv_lines=text_to_lines(text), starts_step=True)
            return RenderedLines(log_lines=[rich_escape(text.strip())], starts_step=True)
        return self._render_unknown(entry, tv=tv)

    def _render_item_command_execution(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        command = item.get("command")
        if not isinstance(command, str):
            command = ""
        output = item.get("aggregated_output")
        if not isinstance(output, str):
            output = ""
        output = output.strip()
        exit_code = item.get("exit_code")
        if not command and not output:
            return self._render_unknown(entry, tv=tv)
        if tv:
            lines = [f"-> $ {truncate_text(strip_shell_wrapper(command), 120)}"] if command else []
            lines.extend(f"  {line}" for line in text_to_lines(output, max_lines=3, max_chars=120))
            return RenderedLines(tv_lines=lines)
        lines = [f"[green]\\[tool: Bash][/green] {rich_escape(strip_shell_wrapper(command))}"] if command else []
        if output:
            rendered_output = rich_escape(output if len(output) <= 200 else output[:200] + "...")
            if isinstance(exit_code, int) and exit_code != 0:
                lines.append(f"[red]{rendered_output}[/red]")
            else:
                lines.append(rendered_output)
        return RenderedLines(log_lines=lines)

    def _render_item_file_change(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        changes = item.get("changes")
        if not isinstance(changes, list):
            return self._render_unknown(entry, tv=tv)
        dict_changes = [change for change in changes if isinstance(change, dict)]
        if not dict_changes:
            return self._render_unknown(entry, tv=tv)
        lines = _codex_file_change_lines(dict_changes, tv=tv)
        return RenderedLines(tv_lines=lines if tv else [], log_lines=lines if not tv else [])

    def _render_item_web_search(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        detail = _codex_web_search_detail(item)
        line = _codex_tool_tv_line("web_search", detail) if tv else _codex_tool_log_line("web_search", detail)
        return RenderedLines(tv_lines=[line] if tv else [], log_lines=[line] if not tv else [])

    def _render_item_todo_list(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        raw_items = item.get("items")
        if not isinstance(raw_items, list):
            return self._render_unknown(entry, tv=tv)
        todos = [todo for todo in raw_items if isinstance(todo, dict)]
        summary = _codex_todo_summary(todos)
        lines = [
            _codex_tool_tv_line("TodoWrite", summary) if tv else _codex_tool_log_line("TodoWrite", summary),
            *_codex_todo_lines(todos, tv=tv),
        ]
        return RenderedLines(tv_lines=lines if tv else [], log_lines=lines if not tv else [])

    def _render_item_collab_tool_call(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        tool = str(item.get("tool") or "collab_tool_call").strip() or "collab_tool_call"
        detail = _codex_collab_tool_detail(item)
        line = _codex_tool_tv_line(tool, detail) if tv else _codex_tool_log_line(tool, detail)
        return RenderedLines(tv_lines=[line] if tv else [], log_lines=[line] if not tv else [])

    def _render_item_mcp_tool_call(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        tool_name = _codex_mcp_tool_name(item)
        detail = _codex_mcp_detail(item)
        line = _codex_tool_tv_line(tool_name, detail) if tv else _codex_tool_log_line(tool_name, detail)
        return RenderedLines(tv_lines=[line] if tv else [], log_lines=[line] if not tv else [])

    def _render_item_reasoning(self, entry: dict[str, Any], *, item: dict[str, Any], tv: bool) -> RenderedLines:
        _ = entry, tv
        has_signal = any(item.get(key) for key in ("summary", "text", "error"))
        if not has_signal:
            self.suppressed_count += 1
            return RenderedLines()
        return self._render_unknown(entry, tv=tv)

    def _render_unknown(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        logger.debug("Unhandled Codex log payload: %s", entry.get("type"))
        if tv:
            return RenderedLines(tv_lines=truncated_json_lines(entry))
        lines = [generic_log_summary(entry)]
        lines.extend(rich_escape(line) for line in pretty_json_lines(entry))
        return RenderedLines(log_lines=lines)


def build_headless_exec_args(work_dir: str | Path) -> list[str]:
    """Build the shared non-resume Codex exec argv used across entry points."""
    return [
        *CODEX_HEADLESS_EXEC_BASE_ARGS,
        "-C",
        str(work_dir),
        "-",
    ]


def _has_codex_oauth() -> bool:
    """Check if OAuth credentials exist in ~/.codex."""
    auth_file = Path.home() / ".codex" / "auth.json"
    return auth_file.exists()


def _has_api_key() -> bool:
    """Check if an API key is configured.

    CODEX_API_KEY is the canonical variable; OPENAI_API_KEY is supported as a
    backward-compatible alias (the underlying Codex CLI also reads this variable).
    """
    return bool(os.getenv("CODEX_API_KEY") or os.getenv("OPENAI_API_KEY"))


def _get_docker_config(image_name: str) -> DockerConfig:
    """Get Docker configuration for Codex.

    Auth priority: API key (CODEX_API_KEY / OPENAI_API_KEY) takes precedence
    over OAuth (~/.codex). Explicit API key credentials are deterministic and
    portable; OAuth is used as a fallback when no API key is configured.
    """
    if _has_api_key():
        # API key takes precedence — pass through whichever key var(s) are set.
        env_vars: list[str] = []
        if os.getenv("CODEX_API_KEY"):
            env_vars.append("CODEX_API_KEY")
        if os.getenv("OPENAI_API_KEY"):
            env_vars.append("OPENAI_API_KEY")
        return DockerConfig(
            image_name=image_name,
            npm_package="@openai/codex",
            cli_command="codex",
            config_dir=None,
            env_vars=env_vars,
        )
    elif _has_codex_oauth():
        # Fall back to OAuth — mount ~/.codex into the container.
        return DockerConfig(
            image_name=image_name,
            npm_package="@openai/codex",
            cli_command="codex",
            config_dir=".codex",
            env_vars=[],
        )
    else:
        # No credentials found; default to API key mode (will fail at runtime
        # with a clear error message).
        return DockerConfig(
            image_name=image_name,
            npm_package="@openai/codex",
            cli_command="codex",
            config_dir=None,
            env_vars=["CODEX_API_KEY"],
        )


class CodexProvider(Provider):
    """OpenAI Codex CLI provider."""

    @property
    def name(self) -> str:
        return "Codex"

    @property
    def credential_setup_hint(self) -> str:
        return (
            "Set CODEX_API_KEY in ~/.gza/.env (OPENAI_API_KEY is also accepted as an alias) "
            "or run 'codex --login' to authenticate with OAuth"
        )

    def check_credentials(self) -> bool:
        """Check for Codex credentials (API key or OAuth).

        API key (CODEX_API_KEY or OPENAI_API_KEY alias) takes precedence.
        OAuth (~/.codex directory) is checked as a fallback.
        """
        if _has_api_key():
            return True
        codex_config = Path.home() / ".codex"
        if codex_config.is_dir():
            return True
        return False

    def verify_credentials(self, config: Config, log_file: Path | None = None) -> PreflightCheckResult:
        """Verify Codex credentials by testing the codex command."""
        if config.use_docker:
            return self._verify_docker(config, log_file=log_file)
        return self._verify_direct(log_file=log_file)

    def _verify_docker(self, config: Config, log_file: Path | None = None) -> PreflightCheckResult:
        """Verify credentials work in Docker."""
        docker_config = _get_docker_config(f"{config.docker_image}-codex")
        if not ensure_docker_image(
            docker_config,
            config.project_dir,
            log_file=log_file,
            provider_label="Codex",
        ):
            print("Error: Failed to build Docker image")
            return PreflightCheckResult.failure(
                failure_reason="INFRASTRUCTURE_ERROR",
                message="Preflight failed: failed to build Codex Docker image",
            )
        result = verify_docker_credentials(
            docker_config=docker_config,
            version_cmd=["codex", "--version"],
            error_patterns=["Invalid API key", "authentication", "unauthorized"],
            error_message=(
                "Error: Invalid or missing Codex credentials\n"
                "  Run 'codex login' or set CODEX_API_KEY in .env"
            ),
            log_file=log_file,
        )
        if not result.ok and result.failure_reason == "PROVIDER_UNAVAILABLE":
            return PreflightCheckResult.failure(
                failure_reason="PROVIDER_UNAVAILABLE",
                message="Preflight failed: Codex credential verification failed",
            )
        return result

    def _verify_direct(self, log_file: Path | None = None) -> PreflightCheckResult:
        """Verify credentials work directly."""
        cmd = ["codex", "--version"]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=5,
                text=True,
            )
            output = result.stdout + result.stderr
            write_preflight_entry(
                log_file,
                event="verify_credentials_direct",
                command=cmd,
                returncode=result.returncode,
                stdout_tail=result.stdout,
                stderr_tail=result.stderr,
                message=f"codex --version exited {result.returncode}",
            )
            if "Invalid API key" in output or "authentication" in output.lower() or "unauthorized" in output.lower():
                print("Error: Invalid or missing Codex credentials")
                print("  Run 'codex login' or set CODEX_API_KEY in .env")
                return PreflightCheckResult.failure(
                    failure_reason="PROVIDER_UNAVAILABLE",
                    message="Preflight failed: Codex credential verification failed",
                )
            if result.returncode == 0:
                return PreflightCheckResult.success()
        except subprocess.TimeoutExpired:
            write_preflight_entry(
                log_file,
                event="verify_credentials_timeout",
                command=cmd,
                returncode=None,
                stdout_tail="",
                stderr_tail="",
                message="codex --version timed out after 5s",
            )
            print("Error: 'codex --version' timed out (CLI may be hanging on an update prompt)")
            return PreflightCheckResult.failure(
                failure_reason="INFRASTRUCTURE_ERROR",
                message="Preflight failed: codex --version timed out",
            )
        except FileNotFoundError:
            write_preflight_entry(
                log_file,
                event="verify_credentials_missing_binary",
                command=cmd,
                returncode=None,
                stdout_tail="",
                stderr_tail="",
                message="codex binary not found on PATH",
            )
            print("Error: 'codex' command not found")
            print("  Install with: npm install -g @openai/codex")
            return PreflightCheckResult.failure(
                failure_reason="INFRASTRUCTURE_ERROR",
                message="Preflight failed: codex CLI not found on PATH",
            )
        return PreflightCheckResult.failure(
            failure_reason="INFRASTRUCTURE_ERROR",
            message=f"Preflight failed: codex --version exited {result.returncode}",
        )

    def run(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
        interactive: bool = False,
        ops_log_file: Path | None = None,
    ) -> RunResult:
        """Run Codex to execute a task."""
        _ = interactive
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        if config.use_docker:
            return self._run_docker(
                config,
                prompt,
                conversation_log_file,
                work_dir,
                resume_session_id,
                on_session_id,
                on_step_count,
                ops_log_file=ops_log_file,
            )
        return self._run_direct(
            config,
            prompt,
            conversation_log_file,
            work_dir,
            resume_session_id,
            on_session_id,
            on_step_count,
            ops_log_file=ops_log_file,
        )

    def _run_docker(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
        ops_log_file: Path | None = None,
    ) -> RunResult:
        """Run Codex in Docker container."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        docker_config = _get_docker_config(f"{config.docker_image}-codex")

        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return RunResult(exit_code=1)

        cmd = build_docker_cmd(
            docker_config,
            work_dir,
            config.timeout_minutes,
            config.docker_volumes,
            config.docker_setup_command,
            getattr(config, "docker_workdir", "/workspace"),
        )

        if resume_session_id:
            cmd.extend([
                "codex",
                "-c", "check_for_update_on_startup=false",
                "exec", "resume", "--json",
                "--dangerously-bypass-approvals-and-sandbox",
                resume_session_id,
                "-",  # Read resume prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])
            self._append_reasoning_effort_override(cmd, config.reasoning_effort)
        else:
            docker_cwd = getattr(config, "docker_workdir", "/workspace")
            cmd.extend([
                "codex",
                *build_headless_exec_args(docker_cwd),  # Worktree metadata may be unavailable inside containers
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])
            self._append_reasoning_effort_override(cmd, config.reasoning_effort)

        return self._run_with_output_parsing(
            cmd, conversation_log_file, config.timeout_minutes, stdin_input=prompt,
            model=config.model, max_steps=config.max_steps,
            chat_text_display_length=config.chat_text_display_length,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
            ops_log_file=ops_log_file,
        )

    def _run_direct(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
        ops_log_file: Path | None = None,
    ) -> RunResult:
        """Run Codex directly (no Docker)."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        cmd = self.build_noninteractive_command(config, work_dir, resume_session_id)

        return self._run_with_output_parsing(
            cmd,
            conversation_log_file,
            config.timeout_minutes,
            cwd=(getattr(config, "provider_cwd", None) or work_dir),
            stdin_input=prompt, model=config.model,
            max_steps=config.max_steps,
            chat_text_display_length=config.chat_text_display_length,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
            ops_log_file=ops_log_file,
        )

    @classmethod
    def build_noninteractive_command(
        cls,
        config: Config,
        work_dir: Path,
        resume_session_id: str | None = None,
    ) -> list[str]:
        """Build the direct non-interactive Codex command used by headless callers."""
        cmd = [
            "timeout", f"{config.timeout_minutes}m",
        ]

        if resume_session_id:
            cmd.extend([
                "codex",
                "-c", "check_for_update_on_startup=false",
                "exec", "resume", "--json",
                "--dangerously-bypass-approvals-and-sandbox",
                resume_session_id,
                "-",  # Read resume prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])
            cls._append_reasoning_effort_override(cmd, config.reasoning_effort)
        else:
            effective_work_dir = getattr(config, "provider_cwd", None) or work_dir
            cmd.extend([
                "codex",
                *build_headless_exec_args(effective_work_dir),  # Worktree metadata may be unavailable in detached review contexts
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])
            cls._append_reasoning_effort_override(cmd, config.reasoning_effort)

        return cmd

    @staticmethod
    def _append_reasoning_effort_override(cmd: list[str], reasoning_effort: str) -> None:
        """Append Codex model reasoning-effort override when configured."""
        if reasoning_effort:
            cmd.extend(["-c", f"model_reasoning_effort={reasoning_effort}"])

    def _run_with_output_parsing(
        self,
        cmd: list[str],
        log_file: Path,
        timeout_minutes: int,
        cwd: Path | None = None,
        stdin_input: str | None = None,
        model: str = "",
        max_steps: int = 50,
        chat_text_display_length: int = 0,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
        ops_log_file: Path | None = None,
    ) -> RunResult:
        """Run command and parse Codex's JSON output."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        formatter = StreamOutputFormatter()

        def _ensure_step_store(data: dict) -> None:
            if "run_step_events" not in data:
                data["run_step_events"] = []
                data["_current_step_event"] = None
                data["_legacy_event_count_by_turn"] = {}

        def _step_count(data: dict) -> int:
            return len(data.get("run_step_events", []))

        def _current_turn_id(data: dict) -> str | None:
            turn_count = int(data.get("turn_count", 0))
            return f"T{turn_count}" if turn_count > 0 else None

        def _allocate_legacy_event_id(data: dict, legacy_turn_id: str | None) -> str | None:
            if not legacy_turn_id:
                return None
            counters = data.get("_legacy_event_count_by_turn")
            if not isinstance(counters, dict):
                counters = {}
                data["_legacy_event_count_by_turn"] = counters
            current = int(counters.get(legacy_turn_id, 0)) + 1
            counters[legacy_turn_id] = current
            return f"{legacy_turn_id}.{current}"

        def _maybe_mark_max_steps_exceeded(data: dict) -> None:
            if _step_count(data) > max_steps:
                data["exceeded_max_steps"] = True
                data["__terminate_process__"] = True

        def _step_header_usage(data: dict) -> tuple[int, int]:
            """Return token totals to display in step header."""
            turn_count = _as_nonnegative_int(data.get("turn_count"))
            turns_with_usage = data.get("turns_with_usage")
            has_real_usage_for_turn = isinstance(turns_with_usage, set) and turn_count in turns_with_usage

            if has_real_usage_for_turn:
                return (
                    _as_nonnegative_int(data.get("input_tokens")),
                    _as_nonnegative_int(data.get("output_tokens")),
                )

            base_input = _as_nonnegative_int(data.get("input_tokens"))
            base_output = _as_nonnegative_int(data.get("output_tokens"))

            approx_input_chars = _as_nonnegative_int(data.get("approx_input_chars"))
            approx_output_chars = _as_nonnegative_int(data.get("approx_output_chars"))
            baseline_input_chars = _as_nonnegative_int(data.get("estimate_input_chars_baseline"))
            baseline_output_chars = _as_nonnegative_int(data.get("estimate_output_chars_baseline"))
            delta_input_chars = max(0, approx_input_chars - baseline_input_chars)
            delta_output_chars = max(0, approx_output_chars - baseline_output_chars)

            # Keep estimates cumulative only for character deltas that have not yet
            # been accounted for by real usage payloads.
            est_input = base_input + _estimate_tokens_from_chars(delta_input_chars)
            est_output = base_output + _estimate_tokens_from_chars(delta_output_chars)
            return est_input, est_output

        def _start_step(
            data: dict,
            message_text: str | None,
            legacy_turn_id: str | None,
            legacy_event_id: str | None = None,
            summary: str | None = None,
        ) -> dict:
            _ensure_step_store(data)
            event: dict[str, Any] = {
                "message_role": "assistant",
                "message_text": message_text,
                "legacy_turn_id": legacy_turn_id,
                "legacy_event_id": legacy_event_id,
                "substeps": [],
                "outcome": "completed",
                "summary": summary,
            }
            data["run_step_events"].append(event)
            data["_current_step_event"] = event
            if on_step_count:
                on_step_count(len(data["run_step_events"]))
            return event

        def parse_codex_output(line: str, data: dict, log_handle=None) -> None:
            try:
                if "approx_input_chars" not in data:
                    data["approx_input_chars"] = len(stdin_input or "")
                    data["approx_output_chars"] = 0
                    data["estimate_input_chars_baseline"] = 0
                    data["estimate_output_chars_baseline"] = 0
                    data["usage_events_seen"] = set()
                    data["turns_with_usage"] = set()
                _ensure_step_store(data)

                event: dict[str, Any] = json.loads(line)
                event_type = event.get("type")
                method_name = (
                    CODEX_LIVE_EVENT_HANDLERS.get(event_type)
                    if isinstance(event_type, str)
                    else None
                )
                if method_name is None:
                    formatter.print_error(
                        _unknown_live_type_message("Codex", "event type", event_type)
                    )
                    return
                getattr(self, method_name)(
                    event=event,
                    formatter=formatter,
                    data=data,
                    model=model,
                    max_steps=max_steps,
                    chat_text_display_length=chat_text_display_length,
                    log_handle=log_handle,
                    on_session_id=on_session_id,
                    on_step_count=on_step_count,
                    ops_log_file=ops_log_file,
                    ensure_step_store=_ensure_step_store,
                    current_turn_id=_current_turn_id,
                )

            except json.JSONDecodeError:
                # Non-JSON output, just display it
                if line == data.get("_startup_line"):
                    return
                formatter.print_error(line)

        result = self.run_with_logging(
            cmd,
            conversation_log_file,
            timeout_minutes,
            cwd=cwd,
            parse_output=parse_codex_output,
            stdin_input=stdin_input,
            ops_log_file=ops_log_file,
        )

        # Extract stats from accumulated data
        accumulated = getattr(result, "_accumulated_data", {})

        if accumulated:
            # Set num_turns_reported from turn_count
            if "turn_count" in accumulated:
                result.num_turns_reported = accumulated["turn_count"]
            if "computed_turn_count" in accumulated:
                result.num_turns_computed = accumulated["computed_turn_count"]
            result.num_steps_computed = _step_count(accumulated)
            result.num_steps_reported = result.num_steps_computed

            # Set token counts
            if "input_tokens" in accumulated:
                result.input_tokens = accumulated["input_tokens"]
            if "output_tokens" in accumulated:
                result.output_tokens = accumulated["output_tokens"]

            # Fallback estimate for interrupted one-turn runs with no usage events.
            if result.input_tokens is None and result.output_tokens is None:
                input_chars = _as_nonnegative_int(accumulated.get("approx_input_chars"))
                output_chars = _as_nonnegative_int(accumulated.get("approx_output_chars"))
                if input_chars > 0 or output_chars > 0:
                    result.input_tokens = _estimate_tokens_from_chars(input_chars)
                    result.output_tokens = _estimate_tokens_from_chars(output_chars)
                    result.tokens_estimated = True

            # Calculate cost
            if result.input_tokens is not None and result.output_tokens is not None:
                result.cost_usd = calculate_cost(
                    result.input_tokens,
                    result.output_tokens,
                    model,
                )
                if result.tokens_estimated:
                    result.cost_estimated = True

            # Check if we exceeded max steps
            if accumulated.get("exceeded_max_steps"):
                result.error_type = "max_steps"

            # Store session ID for resume capability
            if "thread_id" in accumulated:
                result.session_id = accumulated["thread_id"]

        return result

    def _handle_live_thread_started_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = formatter, model, max_steps, chat_text_display_length, log_handle, on_step_count, ops_log_file, ensure_step_store, current_turn_id
        thread_id = event.get("thread_id")
        if thread_id and "thread_id" not in data:
            data["thread_id"] = thread_id
            if on_session_id:
                on_session_id(thread_id)
        elif thread_id:
            data["thread_id"] = thread_id

    def _handle_live_turn_started_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = event, formatter, model, max_steps, chat_text_display_length, log_handle, on_session_id, on_step_count, ops_log_file
        if "turn_count" not in data:
            data["turn_count"] = 0
            data["start_time"] = time.time()
            data["item_count"] = 0
            data["item_count_in_turn"] = 0
            data["computed_turn_count"] = 0
            data["computed_step_count"] = 0
        data["turn_count"] += 1
        data["item_count_in_turn"] = 0
        data["_current_step_event"] = None
        ensure_step_store(data)
        legacy_turn_id = current_turn_id(data)
        if legacy_turn_id:
            counters = data.get("_legacy_event_count_by_turn")
            if isinstance(counters, dict):
                counters.setdefault(legacy_turn_id, 0)

    def _handle_live_turn_failed_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = data, model, max_steps, chat_text_display_length, log_handle, on_session_id, on_step_count, ops_log_file, ensure_step_store, current_turn_id
        formatter.print_error(f"Error: {_codex_error_message(event)}")

    def _handle_live_error_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = data, model, max_steps, chat_text_display_length, log_handle, on_session_id, on_step_count, ops_log_file, ensure_step_store, current_turn_id
        formatter.print_error(f"Error: {_codex_error_message(event)}")

    def _handle_live_turn_completed_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = formatter, model, max_steps, chat_text_display_length, log_handle, on_session_id, on_step_count, ops_log_file, ensure_step_store, current_turn_id
        if not isinstance(event.get("usage"), dict):
            return
        usage = event["usage"]
        input_tokens = _as_nonnegative_int(usage.get("input_tokens"))
        output_tokens = _as_nonnegative_int(usage.get("output_tokens"))
        cached_tokens = _as_nonnegative_int(usage.get("cached_input_tokens"))
        usage_key = (data.get("turn_count"), input_tokens, output_tokens, cached_tokens)
        usage_events_seen = data.get("usage_events_seen")
        if isinstance(usage_events_seen, set) and usage_key not in usage_events_seen:
            usage_events_seen.add(usage_key)
            if "input_tokens" not in data:
                data["input_tokens"] = 0
                data["output_tokens"] = 0
                data["cached_tokens"] = 0
            data["input_tokens"] += input_tokens
            data["output_tokens"] += output_tokens
            data["cached_tokens"] += cached_tokens
            turns_with_usage = data.get("turns_with_usage")
            if isinstance(turns_with_usage, set):
                turns_with_usage.add(_as_nonnegative_int(data.get("turn_count")))
            data["estimate_input_chars_baseline"] = _as_nonnegative_int(data.get("approx_input_chars"))
            data["estimate_output_chars_baseline"] = _as_nonnegative_int(data.get("approx_output_chars"))

    def _handle_live_item_started_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = event, formatter, data, model, max_steps, chat_text_display_length, log_handle, on_session_id, on_step_count, ops_log_file, ensure_step_store, current_turn_id

    def _handle_live_item_updated_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = event, formatter, data, model, max_steps, chat_text_display_length, log_handle, on_session_id, on_step_count, ops_log_file, ensure_step_store, current_turn_id

    def _handle_live_item_completed_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        current_turn_id: Callable[[dict[str, Any]], str | None],
    ) -> None:
        _ = on_session_id, ensure_step_store, current_turn_id
        self._handle_live_item_completed(
            event=event,
            formatter=formatter,
            data=data,
            model=model,
            max_steps=max_steps,
            chat_text_display_length=chat_text_display_length,
            log_handle=log_handle,
            on_step_count=on_step_count,
            ops_log_file=ops_log_file,
        )

    def _handle_live_item_completed(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        item = event.get("item", {})
        item_type = item.get("type") if isinstance(item, dict) else None
        method_name = CODEX_LIVE_ITEM_HANDLERS.get(str(item_type))
        if method_name is None:
            formatter.print_error(_unknown_live_type_message("Codex", "item type", item_type))
            return
        data["item_count"] = data.get("item_count", 0) + 1
        data["item_count_in_turn"] = data.get("item_count_in_turn", 0) + 1
        getattr(self, method_name)(
            item=item,
            formatter=formatter,
            data=data,
            model=model,
            max_steps=max_steps,
            chat_text_display_length=chat_text_display_length,
            log_handle=log_handle,
            on_step_count=on_step_count,
            ops_log_file=ops_log_file,
        )

    def _ensure_live_step_for_tool_activity(
        self,
        *,
        data: dict[str, Any],
        max_steps: int,
        on_step_count: Callable[[int], None] | None,
    ) -> tuple[dict[str, Any], str | None]:
        current_step = data.get("_current_step_event")
        legacy_turn_id = _current_codex_turn_id(data)
        if current_step is None:
            current_step = _start_codex_step(
                data,
                None,
                legacy_turn_id,
                legacy_event_id=_allocate_codex_legacy_event_id(data, legacy_turn_id),
                summary="Pre-message tool activity",
                on_step_count=on_step_count,
            )
            _maybe_mark_codex_max_steps_exceeded(data, max_steps)
        return current_step, legacy_turn_id

    def _append_live_substep(
        self,
        *,
        current_step: dict[str, Any],
        legacy_turn_id: str | None,
        data: dict[str, Any],
        substep_type: str,
        call_id: object = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        current_step["substeps"].append(
            {
                "type": substep_type,
                "source": "provider",
                "call_id": call_id,
                "payload": payload or {},
                "legacy_turn_id": legacy_turn_id,
                "legacy_event_id": _allocate_codex_legacy_event_id(data, legacy_turn_id),
            }
        )

    def _handle_live_item_command_execution(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        _ = model, chat_text_display_length, log_handle, on_step_count, ops_log_file
        command = item.get("command", "")
        aggregated_output = item.get("aggregated_output", "")
        data["approx_input_chars"] = data.get("approx_input_chars", 0) + len(command) + len(aggregated_output)
        current_step, legacy_turn_id = self._ensure_live_step_for_tool_activity(
            data=data,
            max_steps=max_steps,
            on_step_count=on_step_count,
        )
        call_id = item.get("id")
        retry_of_call_id = item.get("retry_of_call_id") or item.get("retry_of")

        if retry_of_call_id:
            self._append_live_substep(
                current_step=current_step,
                legacy_turn_id=legacy_turn_id,
                data=data,
                substep_type="tool_retry",
                call_id=call_id,
                payload={"retry_of_call_id": retry_of_call_id},
            )

        self._append_live_substep(
            current_step=current_step,
            legacy_turn_id=legacy_turn_id,
            data=data,
            substep_type="tool_call",
            call_id=call_id,
            payload={
                "tool_name": "Bash",
                "command": command,
                "tool_input": {"command": command},
                "retry_of_call_id": retry_of_call_id,
            },
        )
        exit_code = item.get("exit_code")
        if not isinstance(exit_code, int):
            maybe_exit = item.get("status_code")
            exit_code = maybe_exit if isinstance(maybe_exit, int) else None
        if isinstance(exit_code, int):
            substep_type = "tool_output" if exit_code == 0 else "tool_error"
            self._append_live_substep(
                current_step=current_step,
                legacy_turn_id=legacy_turn_id,
                data=data,
                substep_type=substep_type,
                call_id=call_id,
                payload={
                    "exit_code": exit_code,
                    "output": aggregated_output,
                },
            )
        elif aggregated_output:
            self._append_live_substep(
                current_step=current_step,
                legacy_turn_id=legacy_turn_id,
                data=data,
                substep_type="tool_output",
                call_id=call_id,
                payload={"output": aggregated_output},
            )
        formatter.print_tool_event("Bash", truncate_text(command, 80))

    def _handle_live_item_file_change(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        _ = model, chat_text_display_length, log_handle, ops_log_file
        raw_changes = item.get("changes")
        if not isinstance(raw_changes, list):
            return
        changes = [change for change in raw_changes if isinstance(change, dict)]
        if not changes:
            return
        current_step, legacy_turn_id = self._ensure_live_step_for_tool_activity(
            data=data,
            max_steps=max_steps,
            on_step_count=on_step_count,
        )
        for change in changes:
            path = str(change.get("path") or "").strip()
            kind = str(change.get("kind") or "update").strip() or "update"
            if not path:
                continue
            detail = f"{path} ({kind})"
            data["approx_output_chars"] = data.get("approx_output_chars", 0) + len(detail)
            self._append_live_substep(
                current_step=current_step,
                legacy_turn_id=legacy_turn_id,
                data=data,
                substep_type="tool_call",
                payload={
                    "tool_name": "edit",
                    "tool_input": {"detail": detail, "file_path": path, "kind": kind},
                    "changes": [{"path": path, "kind": kind}],
                },
            )
        for line in _codex_file_change_lines(changes, tv=True):
            formatter.print_tool_event("edit", line.removeprefix("-> edit ").strip() if line.startswith("-> edit ") else line)

    def _handle_live_item_web_search(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        _ = model, chat_text_display_length, log_handle, ops_log_file
        detail = _codex_web_search_detail(item)
        current_step, legacy_turn_id = self._ensure_live_step_for_tool_activity(
            data=data,
            max_steps=max_steps,
            on_step_count=on_step_count,
        )
        data["approx_input_chars"] = data.get("approx_input_chars", 0) + len(detail)
        self._append_live_substep(
            current_step=current_step,
            legacy_turn_id=legacy_turn_id,
            data=data,
            substep_type="tool_call",
            payload={"tool_name": "web_search", "tool_input": {"query": detail, "detail": detail}},
        )
        formatter.print_tool_event("web_search", detail)

    def _handle_live_item_todo_list(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        _ = model, chat_text_display_length, log_handle, ops_log_file
        raw_items = item.get("items")
        if not isinstance(raw_items, list):
            return
        todos = [todo for todo in raw_items if isinstance(todo, dict)]
        current_step, legacy_turn_id = self._ensure_live_step_for_tool_activity(
            data=data,
            max_steps=max_steps,
            on_step_count=on_step_count,
        )
        summary = _codex_todo_summary(todos)
        data["approx_output_chars"] = data.get("approx_output_chars", 0) + len(summary)
        self._append_live_substep(
            current_step=current_step,
            legacy_turn_id=legacy_turn_id,
            data=data,
            substep_type="tool_call",
            payload={"tool_name": "TodoWrite", "tool_input": {"todos": todos}},
        )
        formatter.print_tool_event("TodoWrite", summary)
        for todo in todos[:_CODEX_TODO_LIMIT]:
            text = truncate_text(str(todo.get("text") or todo.get("content") or "").strip(), 60)
            if text:
                formatter.print_todo(_codex_normalize_todo_status(todo), text)
        more_line = _codex_more_line(len(todos) - min(len(todos), _CODEX_TODO_LIMIT))
        if more_line:
            formatter.print_tool_event("TodoWrite", more_line, prefix="  ")

    def _handle_live_item_collab_tool_call(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        _ = model, chat_text_display_length, log_handle, ops_log_file
        tool = str(item.get("tool") or "collab_tool_call").strip() or "collab_tool_call"
        detail = _codex_collab_tool_detail(item)
        current_step, legacy_turn_id = self._ensure_live_step_for_tool_activity(
            data=data,
            max_steps=max_steps,
            on_step_count=on_step_count,
        )
        data["approx_input_chars"] = data.get("approx_input_chars", 0) + len(detail)
        self._append_live_substep(
            current_step=current_step,
            legacy_turn_id=legacy_turn_id,
            data=data,
            substep_type="tool_call",
            payload={
                "tool_name": tool,
                "tool_input": {
                    "detail": detail,
                    "receiver_thread_ids": item.get("receiver_thread_ids"),
                    "prompt_preview": truncate_text(str(item.get("prompt") or "").strip(), 80),
                },
            },
        )
        formatter.print_tool_event(tool, detail)

    def _handle_live_item_mcp_tool_call(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        _ = model, chat_text_display_length, log_handle, ops_log_file
        tool_name = _codex_mcp_tool_name(item)
        detail = _codex_mcp_detail(item)
        current_step, legacy_turn_id = self._ensure_live_step_for_tool_activity(
            data=data,
            max_steps=max_steps,
            on_step_count=on_step_count,
        )
        data["approx_input_chars"] = data.get("approx_input_chars", 0) + len(detail)
        self._append_live_substep(
            current_step=current_step,
            legacy_turn_id=legacy_turn_id,
            data=data,
            substep_type="tool_call",
            payload={"tool_name": tool_name, "tool_input": {"detail": detail, "arguments_preview": detail}},
        )
        if item.get("error") not in (None, "", {}, []):
            self._append_live_substep(
                current_step=current_step,
                legacy_turn_id=legacy_turn_id,
                data=data,
                substep_type="tool_error",
                payload={"error": compact_json(item.get("error"), max_chars=120)},
            )
        elif item.get("result") not in (None, "", {}, []):
            self._append_live_substep(
                current_step=current_step,
                legacy_turn_id=legacy_turn_id,
                data=data,
                substep_type="tool_output",
                payload={"output": compact_json(item.get("result"), max_chars=120)},
            )
        formatter.print_tool_event(tool_name, detail)

    def _handle_live_item_agent_message(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        data["computed_turn_count"] = data.get("computed_turn_count", 0) + 1
        raw_text = item.get("text", "")
        data["approx_output_chars"] = data.get("approx_output_chars", 0) + len(raw_text)
        data["computed_step_count"] = data.get("computed_step_count", 0) + 1
        step_num = data["computed_step_count"]

        elapsed_seconds = int(time.time() - data.get("start_time", time.time()))
        display_input_tokens, display_output_tokens = _codex_step_header_usage(data)
        total_tokens = display_input_tokens + display_output_tokens
        cost = calculate_cost(display_input_tokens, display_output_tokens, model)

        if log_handle:
            from datetime import datetime
            timestamp_str = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
            write_ops_event(
                ops_log_file,
                subtype="step_marker",
                source="provider",
                message=f"Step {step_num}",
                step=step_num,
                step_timestamp=timestamp_str,
            )

        formatter.print_step_header(
            step_num,
            total_tokens,
            cost,
            elapsed_seconds,
            blank_line_before=step_num > 1,
        )

        legacy_turn_id = _current_codex_turn_id(data)
        _start_codex_step(
            data,
            raw_text.strip() or None,
            legacy_turn_id,
            legacy_event_id=_allocate_codex_legacy_event_id(data, legacy_turn_id),
            on_step_count=on_step_count,
        )
        _maybe_mark_codex_max_steps_exceeded(data, max_steps)

        text = raw_text.strip()
        if text:
            if chat_text_display_length == 0:
                formatter.print_agent_message(text)
            else:
                first_line = text.split("\n")[0]
                formatter.print_agent_message(truncate_text(first_line, chat_text_display_length))

    def _handle_live_item_reasoning(
        self,
        *,
        item: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        max_steps: int,
        chat_text_display_length: int,
        log_handle: io.TextIOBase | None,
        on_step_count: Callable[[int], None] | None,
        ops_log_file: Path | None,
    ) -> None:
        _ = item, formatter, data, model, max_steps, chat_text_display_length, log_handle, on_step_count, ops_log_file
