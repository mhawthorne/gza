"""Claude Code provider implementation."""

from __future__ import annotations

import io
import json
import logging
import os
import pty
import select
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

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
    configured_model_from_gza_info,
    error_lines,
    generic_log_summary,
    message_content_items,
    model_parity_lines,
    normalize_model_name,
    pretty_json_lines,
    summarize_tool_detail,
    tool_one_liner,
    truncated_json_lines,
    tv_error_lines,
)
from .output_formatter import StreamOutputFormatter, truncate_text

if TYPE_CHECKING:
    from ..config import Config

logger = logging.getLogger(__name__)

CLAUDE_EVENT_REGISTRY: dict[str, dict[str, object]] = {
    "gza": {"render": ("_render_gza", False), "live": False},
    "raw": {"render": ("_render_raw", False), "live": False},
    "error": {"render": ("_render_error", False), "live": "_handle_live_error_event"},
    "system": {"render": ("_render_system", False), "live": "_handle_live_system_event"},
    "assistant": {"render": ("_render_assistant", True), "live": "_handle_live_assistant_event"},
    "user": {"render": ("_render_user", False), "live": "_handle_live_user_event"},
    "rate_limit_event": {"render": ("_render_rate_limit_event", False), "live": "_handle_live_rate_limit_event"},
    "result": {"render": ("_render_result", False), "live": "_handle_live_result_event"},
}
CLAUDE_RENDER_EVENT_HANDLERS: dict[str, tuple[str, bool]] = {
    event_type: cast(tuple[str, bool], dispatch)
    for event_type, metadata in CLAUDE_EVENT_REGISTRY.items()
    if isinstance(dispatch := metadata.get("render"), tuple)
}
CLAUDE_RENDER_KNOWN_EVENT_TYPES = frozenset(CLAUDE_RENDER_EVENT_HANDLERS)
CLAUDE_LIVE_EVENT_HANDLERS: dict[str, str] = {
    event_type: str(handler_name)
    for event_type, metadata in CLAUDE_EVENT_REGISTRY.items()
    if isinstance(handler_name := metadata.get("live"), str)
}
CLAUDE_LIVE_KNOWN_EVENT_TYPES = frozenset(CLAUDE_LIVE_EVENT_HANDLERS)
CLAUDE_ASSISTANT_BLOCK_REGISTRY: dict[str, dict[str, object]] = {
    "text": {"render": "_render_assistant_text_block", "live": "_handle_live_assistant_text_block"},
    "tool_use": {"render": "_render_assistant_tool_use_block", "live": "_handle_live_assistant_tool_use_block"},
    "tool_result": {"render": False, "live": "_handle_live_assistant_tool_result_block"},
    "tool_retry": {"render": False, "live": "_handle_live_assistant_tool_retry_block"},
}
CLAUDE_RENDER_ASSISTANT_BLOCK_HANDLERS: dict[str, str] = {
    block_type: str(handler_name)
    for block_type, metadata in CLAUDE_ASSISTANT_BLOCK_REGISTRY.items()
    if isinstance(handler_name := metadata.get("render"), str)
}
CLAUDE_LIVE_ASSISTANT_BLOCK_HANDLERS: dict[str, str] = {
    block_type: str(handler_name)
    for block_type, metadata in CLAUDE_ASSISTANT_BLOCK_REGISTRY.items()
    if isinstance(handler_name := metadata.get("live"), str)
}
CLAUDE_RENDER_KNOWN_ASSISTANT_BLOCK_TYPES = frozenset(
    CLAUDE_RENDER_ASSISTANT_BLOCK_HANDLERS
)
CLAUDE_LIVE_KNOWN_ASSISTANT_BLOCK_TYPES = frozenset(
    CLAUDE_LIVE_ASSISTANT_BLOCK_HANDLERS
)
CLAUDE_USER_BLOCK_REGISTRY: dict[str, dict[str, object]] = {
    "text": {"render": "_render_user_text_block", "live": "_handle_live_user_text_block"},
    "tool_result": {"render": "_render_user_tool_result_block", "live": "_handle_live_user_tool_result_block"},
}
CLAUDE_RENDER_USER_BLOCK_HANDLERS: dict[str, str] = {
    block_type: str(handler_name)
    for block_type, metadata in CLAUDE_USER_BLOCK_REGISTRY.items()
    if isinstance(handler_name := metadata.get("render"), str)
}
CLAUDE_LIVE_USER_BLOCK_HANDLERS: dict[str, str] = {
    block_type: str(handler_name)
    for block_type, metadata in CLAUDE_USER_BLOCK_REGISTRY.items()
    if isinstance(handler_name := metadata.get("live"), str)
}
CLAUDE_RENDER_KNOWN_USER_BLOCK_TYPES = frozenset(
    CLAUDE_RENDER_USER_BLOCK_HANDLERS
)
CLAUDE_LIVE_KNOWN_USER_BLOCK_TYPES = frozenset(
    CLAUDE_LIVE_USER_BLOCK_HANDLERS
)


def _unknown_live_type_message(provider: str, surface: str, type_name: object) -> str:
    """Build a stable operator-facing message for unhandled live provider output."""
    rendered_type = str(type_name).strip() if type_name not in (None, "") else "<missing>"
    return f"Unhandled {provider} {surface}: {rendered_type}"


def _parse_iso_datetime(value: object) -> datetime | None:
    """Parse an ISO-8601 timestamp with a permissive Z-suffix fallback."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _claude_rate_limit_message(entry: dict[str, Any]) -> str | None:
    """Summarize non-routine Claude rate limit telemetry for operators."""
    info = entry.get("rate_limit_info")
    if not isinstance(info, dict):
        return None
    status = str(info.get("status") or "").strip().lower()
    if status in {"", "allowed"}:
        return None
    rate_limit_type = str(info.get("rateLimitType") or "unknown").strip() or "unknown"
    message = f"Rate limit ({rate_limit_type})"
    if status != "denied":
        message = f"{message}: {status}"
    resets_at = _parse_iso_datetime(info.get("resetsAt"))
    if resets_at is not None:
        if resets_at.tzinfo is not None:
            resets_at = resets_at.astimezone(UTC)
            return f"{message}; resets at {resets_at.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        return f"{message}; resets at {resets_at.strftime('%Y-%m-%d %H:%M:%S')}"
    raw_resets_at = info.get("resetsAt")
    if raw_resets_at:
        return f"{message}; resets at {raw_resets_at}"
    return message


def _format_tool_param(value: object) -> str:
    """Format a tool input parameter value for display."""
    if isinstance(value, str):
        value = value.replace("\n", "\\n").replace("\r", "\\r")
        if len(value) > 60:
            value = value[:57] + "..."
        return value
    elif isinstance(value, list):
        return f"list[{len(value)}]"
    elif isinstance(value, dict):
        return "{...}"
    else:
        return str(value)


def _ensure_claude_step_store(data: dict[str, Any]) -> None:
    if "run_step_events" not in data:
        data["run_step_events"] = []
        data["_step_by_msg_id"] = {}
        data["_current_step_event"] = None
        data["_legacy_event_count_by_turn"] = {}


def _allocate_claude_legacy_event_id(data: dict[str, Any], legacy_turn_id: str | None) -> str | None:
    if not legacy_turn_id:
        return None
    counters = data.get("_legacy_event_count_by_turn")
    if not isinstance(counters, dict):
        counters = {}
        data["_legacy_event_count_by_turn"] = counters
    next_idx = int(counters.get(legacy_turn_id, 0)) + 1
    counters[legacy_turn_id] = next_idx
    return f"{legacy_turn_id}.{next_idx}"


def _start_claude_step(
    data: dict[str, Any],
    msg_id: str | None,
    legacy_turn_id: str | None,
    on_step_count: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    _ensure_claude_step_store(data)
    event: dict[str, Any] = {
        "message_role": "assistant",
        "message_text": None,
        "legacy_turn_id": legacy_turn_id,
        "legacy_event_id": _allocate_claude_legacy_event_id(data, legacy_turn_id),
        "substeps": [],
        "_seen_tool_use_ids": set(),
        "outcome": "completed",
        "summary": None,
    }
    data["run_step_events"].append(event)
    data["_current_step_event"] = event
    if msg_id:
        data["_step_by_msg_id"][msg_id] = event
    if on_step_count:
        on_step_count(len(data["run_step_events"]))
    return event


def _append_claude_tool_result_substep(
    data: dict[str, Any],
    current_step: dict[str, Any] | None,
    content: dict[str, Any],
) -> dict[str, Any]:
    if current_step is None:
        seen_msg_ids = data.get("seen_msg_ids", set())
        turn_count = len(seen_msg_ids) if isinstance(seen_msg_ids, set) else 0
        legacy_turn_id = f"T{turn_count}" if turn_count > 0 else None
        current_step = _start_claude_step(data, None, legacy_turn_id)
    legacy_turn_id = current_step.get("legacy_turn_id")
    is_error = bool(content.get("is_error"))
    current_step["substeps"].append(
        {
            "type": "tool_error" if is_error else "tool_output",
            "source": "provider",
            "call_id": content.get("tool_use_id") or content.get("id"),
            "payload": {
                "content": content.get("content"),
                "is_error": is_error,
            },
            "legacy_turn_id": legacy_turn_id,
            "legacy_event_id": _allocate_claude_legacy_event_id(data, legacy_turn_id),
        }
    )
    return current_step


# Claude pricing per million tokens (input, output)
# https://www.anthropic.com/pricing
CLAUDE_PRICING = {
    "claude-sonnet-4": (3.00, 15.00),
    "claude-opus-4": (15.00, 75.00),
    "claude-3-5-sonnet": (3.00, 15.00),
    "claude-3-opus": (15.00, 75.00),
    "claude-3-haiku": (0.25, 1.25),
}

# Default pricing when model is unknown (Sonnet)
DEFAULT_PRICING = (3.00, 15.00)


def get_pricing_for_model(model: str) -> tuple[float, float]:
    """Get (input, output) pricing per million tokens for a model."""
    if not model:
        return DEFAULT_PRICING
    # Try exact match first
    if model in CLAUDE_PRICING:
        return CLAUDE_PRICING[model]
    # Try prefix match
    for model_prefix, pricing in CLAUDE_PRICING.items():
        if model.startswith(model_prefix):
            return pricing
    return DEFAULT_PRICING


def calculate_cost(input_tokens: int, output_tokens: int, model: str = "") -> float:
    """Calculate estimated cost in USD based on token counts and model."""
    input_price, output_price = get_pricing_for_model(model)
    cost = (
        (input_tokens * input_price / 1_000_000) +
        (output_tokens * output_price / 1_000_000)
    )
    return round(cost, 4)


class ClaudeLogRenderer:
    """Render Claude JSONL events for log replay/live and TV surfaces."""

    def __init__(self, *, configured_model: str | None = None, verbose: bool = False) -> None:
        self.configured_model = configured_model
        self.verbose = verbose
        self.stats = RenderStats()
        self.suppressed_count = 0
        self._seen_message_ids: set[str] = set()
        self._seen_usage_message_ids: set[str] = set()
        self._provider_model: str | None = None
        self._parity_ready = False
        self._last_parity_signature: tuple[str | None, str | None] | None = None

    def handle_log(self, entry: dict[str, Any], *, live: bool) -> RenderedLines:
        return self._handle(entry, live=live, tv=False)

    def handle_tv(self, entry: dict[str, Any]) -> RenderedLines:
        return self._handle(entry, live=False, tv=True)

    def _handle(self, entry: dict[str, Any], *, live: bool, tv: bool) -> RenderedLines:
        event_type = entry.get("type")
        if not isinstance(event_type, str):
            return self._render_unknown(entry, tv=tv)
        dispatch = CLAUDE_RENDER_EVENT_HANDLERS.get(event_type)
        if dispatch is None:
            return self._render_unknown(entry, tv=tv)
        method_name, pass_live = dispatch
        method = getattr(self, method_name)
        if pass_live:
            return method(entry, live=live, tv=tv)
        return method(entry, tv=tv)

    def _render_raw(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        message = entry.get("message", "")
        if isinstance(message, str) and message:
            lines = [rich_escape(message)] if not tv else [message]
            return RenderedLines(log_lines=lines if not tv else [], tv_lines=lines if tv else [])
        return RenderedLines()

    def _render_error(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        lines = error_lines(entry.get("message", ""))
        if tv:
            return RenderedLines(tv_lines=tv_error_lines(entry.get("message", "")))
        return RenderedLines(log_lines=[f"[red]{rich_escape(line)}[/red]" for line in lines])

    def _render_gza(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        subtype = entry.get("subtype", "")
        message = entry.get("message", "")
        if subtype == "info":
            model_value = configured_model_from_gza_info(message)
            if model_value:
                self.configured_model = model_value
        if not message:
            return RenderedLines()
        prefix = f"[gza:{subtype}]" if subtype else "[gza]"
        line = f"{prefix} {message}"
        log_lines = [rich_escape(line)] if not tv else []
        if not tv:
            log_lines.extend(self._model_parity_lines())
        return RenderedLines(log_lines=log_lines, tv_lines=[line] if tv else [])

    def _render_system(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        subtype = entry.get("subtype", "")
        provider_model = normalize_model_name(entry.get("model"))
        if provider_model:
            self._provider_model = provider_model
        if subtype == "init":
            self._parity_ready = True
            line = f"Session initialized (model: {self._provider_model or 'unknown'})"
            log_lines = [line] if not tv else []
            if not tv:
                log_lines.extend(self._model_parity_lines())
            return RenderedLines(
                log_lines=log_lines,
                tv_lines=[line] if tv else [],
            )
        if self._is_routine_system_event(entry):
            self.suppressed_count += 1
            return RenderedLines()
        return self._render_unknown(entry, tv=tv)

    def _model_parity_lines(self) -> list[str]:
        if not self._parity_ready:
            return []
        signature = (
            normalize_model_name(self.configured_model),
            normalize_model_name(self._provider_model),
        )
        if signature == self._last_parity_signature:
            return []
        self._last_parity_signature = signature
        return model_parity_lines(*signature)

    def _render_assistant(self, entry: dict[str, Any], *, live: bool, tv: bool) -> RenderedLines:
        message = entry.get("message", {})
        message_id = message.get("id") if isinstance(message, dict) else None
        if isinstance(message, dict):
            self._accumulate_usage_once(message)
        log_lines: list[str] = []
        tv_lines: list[str] = []
        text_found = False
        tool_found = False
        content_items = message_content_items(entry)
        unknown_block_found = False
        unknown_block_types: list[str] = []
        for item in content_items:
            item_type = item.get("type")
            method_name = CLAUDE_RENDER_ASSISTANT_BLOCK_HANDLERS.get(str(item_type))
            if method_name is None:
                if item:
                    unknown_block_found = True
                    if item_type not in (None, ""):
                        unknown_block_types.append(str(item_type))
                continue
            rendered_log_lines, rendered_tv_lines, rendered_text_found, rendered_tool_found = getattr(
                self, method_name
            )(item, tv=tv)
            log_lines.extend(rendered_log_lines)
            tv_lines.extend(rendered_tv_lines)
            text_found = text_found or rendered_text_found
            tool_found = tool_found or rendered_tool_found
        if not text_found and not tool_found:
            if unknown_block_found:
                starts_step = self._mark_step_start(message_id)
                rendered = self._render_unknown_assistant(entry, tv=tv, unknown_block_types=unknown_block_types)
                rendered.starts_step = starts_step
                return rendered
            self.suppressed_count += 1
            return RenderedLines()
        starts_step = self._mark_step_start(message_id)
        if unknown_block_found:
            fallback = self._render_unknown_assistant(entry, tv=tv, unknown_block_types=unknown_block_types)
            log_lines.extend(fallback.log_lines)
            tv_lines.extend(fallback.tv_lines)
        return RenderedLines(log_lines=log_lines, tv_lines=tv_lines, starts_step=starts_step)

    def _render_assistant_text_block(self, item: dict[str, Any], *, tv: bool) -> tuple[list[str], list[str], bool, bool]:
        text = item.get("text", "")
        if not isinstance(text, str) or not text.strip():
            return [], [], False, False
        if tv:
            return [], [line for line in text.splitlines() if line.strip()][-6:], True, False
        return [rich_escape(text.strip())], [], True, False

    def _render_assistant_tool_use_block(self, item: dict[str, Any], *, tv: bool) -> tuple[list[str], list[str], bool, bool]:
        name = str(item.get("name", "unknown"))
        tool_input = item.get("input", {})
        if not isinstance(tool_input, dict):
            tool_input = {}
        if tv:
            return [], [f"-> {tool_one_liner(name, tool_input)}"], False, True
        return [
            f"[green]\\[tool: {rich_escape(name)}][/green] {rich_escape(summarize_tool_detail(name, tool_input))}"
        ], [], False, True

    def _render_user(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        log_lines: list[str] = []
        tv_lines: list[str] = []
        unknown_block_found = False
        for item in message_content_items(entry):
            item_type = item.get("type")
            method_name = CLAUDE_RENDER_USER_BLOCK_HANDLERS.get(str(item_type))
            if method_name is None:
                if item:
                    unknown_block_found = True
                continue
            rendered_log_lines, rendered_tv_lines = getattr(self, method_name)(item, tv=tv)
            log_lines.extend(rendered_log_lines)
            tv_lines.extend(rendered_tv_lines)

        if log_lines or tv_lines:
            if unknown_block_found:
                fallback = self._render_unknown(entry, tv=tv)
                log_lines.extend(fallback.log_lines)
                tv_lines.extend(fallback.tv_lines)
            return RenderedLines(log_lines=log_lines, tv_lines=tv_lines)

        if unknown_block_found:
            return self._render_unknown(entry, tv=tv)

        self.suppressed_count += 1
        return RenderedLines(log_lines=log_lines, tv_lines=tv_lines)

    def _render_user_tool_result_block(self, item: dict[str, Any], *, tv: bool) -> tuple[list[str], list[str]]:
        result = item.get("content", "")
        if isinstance(result, str):
            result = result.replace("\\n", "\n").replace("\\t", "\t")
        is_error = bool(item.get("is_error", False))
        rendered = str(result).strip()
        if not rendered:
            return [], []
        if tv:
            prefix = "tool_error" if is_error else "tool_output"
            return [], [f"{prefix} {rendered}"]
        if is_error:
            return [f"[red]{rich_escape(rendered)}[/red]"], []
        return [rich_escape(rendered)], []

    def _render_user_text_block(self, item: dict[str, Any], *, tv: bool) -> tuple[list[str], list[str]]:
        text = item.get("text", "")
        if not isinstance(text, str) or not text.strip():
            return [], []
        rendered_text = text.strip()
        prefix = "user: "
        if tv:
            return [], [f"{prefix}{rendered_text}"]
        return [rich_escape(f"{prefix}{rendered_text}")], []

    def _render_result(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        cost = entry.get("total_cost_usd") or entry.get("cost_usd")
        if isinstance(cost, (int, float)):
            self.stats.cost_usd = float(cost)
        result_text = str(entry.get("result", "") or "").strip()
        subtype = str(entry.get("subtype") or "")
        is_error = bool(entry.get("is_error", False))
        if tv:
            if result_text:
                return RenderedLines(tv_lines=[f"result {result_text}"])
            if subtype:
                return RenderedLines(tv_lines=[f"result {subtype}"])
            return RenderedLines()
        if is_error:
            return RenderedLines(log_lines=[f"[red]\\[result] ERROR:[/red] {rich_escape(result_text)}"])
        if subtype and subtype != "success":
            if result_text:
                return RenderedLines(log_lines=[f"[yellow]\\[result] {rich_escape(subtype)}:[/yellow] {rich_escape(result_text)}"])
            return RenderedLines(log_lines=[f"[yellow]\\[result] {rich_escape(subtype)}[/yellow]"])
        if result_text:
            return RenderedLines(log_lines=[f"[green]\\[result][/green] {rich_escape(result_text)}"])
        return RenderedLines()

    def _mark_step_start(self, message: object) -> bool:
        if isinstance(message, str) and message:
            if message in self._seen_message_ids:
                return False
            self._seen_message_ids.add(message)
            self.stats.step_count += 1
            return True
        self.stats.step_count += 1
        return True

    def _accumulate_usage_once(self, message: dict[str, Any]) -> None:
        message_id = message.get("id")
        if isinstance(message_id, str) and message_id:
            if message_id in self._seen_usage_message_ids:
                return
            self._seen_usage_message_ids.add(message_id)
        self._accumulate_usage(message)

    def _accumulate_usage(self, message: dict[str, Any]) -> None:
        usage = message.get("usage", {})
        if not isinstance(usage, dict):
            return
        self.stats.input_tokens += int(usage.get("input_tokens", 0) or 0)
        self.stats.input_tokens += int(usage.get("cache_creation_input_tokens", 0) or 0)
        self.stats.input_tokens += int(usage.get("cache_read_input_tokens", 0) or 0)
        self.stats.output_tokens += int(usage.get("output_tokens", 0) or 0)
        self.stats.cost_usd = calculate_cost(
            self.stats.input_tokens,
            self.stats.output_tokens,
            self._provider_model or self.configured_model or "",
        )

    def _is_routine_system_event(self, entry: dict[str, Any]) -> bool:
        return not self._has_error_signal(entry)

    def _has_error_signal(self, entry: dict[str, Any]) -> bool:
        error_value = entry.get("error")
        if error_value not in (None, "", [], {}):
            return True
        subtype = str(entry.get("subtype") or "").lower()
        if "error" in subtype:
            return True
        message = entry.get("message")
        if isinstance(message, str) and "error" in message.lower():
            return True
        return False

    def _render_rate_limit_event(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        message = _claude_rate_limit_message(entry)
        if not message:
            self.suppressed_count += 1
            return RenderedLines()
        if tv:
            return RenderedLines(tv_lines=tv_error_lines(message))
        return RenderedLines(log_lines=[f"[red]{rich_escape(message)}[/red]"])

    def _render_unknown(self, entry: dict[str, Any], *, tv: bool) -> RenderedLines:
        logger.debug("Unhandled Claude log payload: %s", entry.get("type"))
        if tv:
            return RenderedLines(tv_lines=truncated_json_lines(entry))
        lines = [generic_log_summary(entry)]
        lines.extend(rich_escape(line) for line in pretty_json_lines(entry))
        return RenderedLines(log_lines=lines)

    def _render_unknown_assistant(
        self,
        entry: dict[str, Any],
        *,
        tv: bool,
        unknown_block_types: list[str],
    ) -> RenderedLines:
        if tv and unknown_block_types:
            block_types = ",".join(dict.fromkeys(unknown_block_types))
            return RenderedLines(
                tv_lines=[
                    f"event:assistant block={block_types}",
                    *truncated_json_lines(entry, max_lines=7),
                ]
            )
        return self._render_unknown(entry, tv=tv)

def sync_keychain_credentials() -> bool:
    """Extract Claude OAuth credentials from macOS Keychain and write to ~/.claude/.credentials.json.

    Returns True if credentials were written, False otherwise.
    """
    if sys.platform != "darwin":
        logger.warning("sync_keychain_credentials: not on macOS, skipping")
        return False

    if not shutil.which("security"):
        logger.warning("sync_keychain_credentials: 'security' command not found, skipping")
        return False

    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-l", "Claude Code-credentials", "-w"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        logger.warning("sync_keychain_credentials: failed to run security command")
        return False

    if result.returncode != 0:
        logger.warning("sync_keychain_credentials: no keychain entry found for 'Claude Code-credentials'")
        return False

    raw = result.stdout.strip()
    if not raw:
        logger.warning("sync_keychain_credentials: keychain entry is empty")
        return False

    try:
        creds = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("sync_keychain_credentials: keychain entry is not valid JSON")
        return False

    if "claudeAiOauth" not in creds:
        logger.warning("sync_keychain_credentials: keychain entry missing 'claudeAiOauth' key")
        return False

    claude_dir = Path.home() / ".claude"
    claude_dir.mkdir(exist_ok=True)
    creds_path = claude_dir / ".credentials.json"
    creds_path.write_text(json.dumps(creds, indent=2) + "\n")
    creds_path.chmod(0o600)

    logger.info("sync_keychain_credentials: wrote credentials to %s", creds_path)
    return True


def _get_docker_config(image_name: str) -> DockerConfig:
    """Get Docker configuration for Claude."""
    return DockerConfig(
        image_name=image_name,
        npm_package="@anthropic-ai/claude-code",
        cli_command="claude",
        config_dir=".claude",
        env_vars=["ANTHROPIC_API_KEY"],
    )


class ClaudeProvider(Provider):
    """Claude Code CLI provider."""

    @property
    def name(self) -> str:
        return "Claude"

    @property
    def supports_interactive_foreground(self) -> bool:
        return True

    @property
    def credential_setup_hint(self) -> str:
        return "Set ANTHROPIC_API_KEY in ~/.gza/.env or run 'claude login' to authenticate via OAuth"

    def check_credentials(self) -> bool:
        """Check for Claude credentials (OAuth or API key)."""
        claude_config = Path.home() / ".claude"
        if claude_config.is_dir():
            return True
        if os.getenv("ANTHROPIC_API_KEY"):
            return True
        return False

    def verify_credentials(self, config: Config, log_file: Path | None = None) -> PreflightCheckResult:
        """Verify Claude credentials by testing the claude command."""
        if config.use_docker:
            return self._verify_docker(config, log_file=log_file)
        return self._verify_direct(log_file=log_file)

    def _verify_docker(self, config: Config, log_file: Path | None = None) -> PreflightCheckResult:
        """Verify credentials work in Docker."""
        if config.claude.fetch_auth_token_from_keychain:
            sync_keychain_credentials()
        docker_config = _get_docker_config(f"{config.docker_image}-claude")
        if not ensure_docker_image(
            docker_config,
            config.project_dir,
            log_file=log_file,
            provider_label="Claude",
        ):
            print("Error: Failed to build Docker image")
            return PreflightCheckResult.failure(
                failure_reason="INFRASTRUCTURE_ERROR",
                message="Preflight failed: failed to build Claude Docker image",
            )
        result = verify_docker_credentials(
            docker_config=docker_config,
            version_cmd=["claude", "--version"],
            error_patterns=["Invalid API key", "Please run /login", "/login"],
            error_message=(
                "Error: Invalid or missing Claude credentials\n"
                "  Run 'claude login' or set ANTHROPIC_API_KEY in .env"
            ),
            log_file=log_file,
        )
        if not result.ok and result.failure_reason == "PROVIDER_UNAVAILABLE":
            return PreflightCheckResult.failure(
                failure_reason="PROVIDER_UNAVAILABLE",
                message="Preflight failed: Claude credential verification failed",
            )
        return result

    def _verify_direct(self, log_file: Path | None = None) -> PreflightCheckResult:
        """Verify credentials work directly."""
        cmd = ["claude", "--version"]
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
                message=f"claude --version exited {result.returncode}",
            )
            if "Invalid API key" in output or "Please run /login" in output or "/login" in output:
                print("Error: Invalid or missing Claude credentials")
                print("  Run 'claude login' or set ANTHROPIC_API_KEY in .env")
                return PreflightCheckResult.failure(
                    failure_reason="PROVIDER_UNAVAILABLE",
                    message="Preflight failed: Claude credential verification failed",
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
                message="claude --version timed out after 5s",
            )
            print("Error: 'claude --version' timed out (CLI may be hanging)")
            return PreflightCheckResult.failure(
                failure_reason="INFRASTRUCTURE_ERROR",
                message="Preflight failed: claude --version timed out",
            )
        except FileNotFoundError:
            write_preflight_entry(
                log_file,
                event="verify_credentials_missing_binary",
                command=cmd,
                returncode=None,
                stdout_tail="",
                stderr_tail="",
                message="claude binary not found on PATH",
            )
            print("Error: 'claude' command not found")
            print("  Install with: npm install -g @anthropic-ai/claude-code")
            return PreflightCheckResult.failure(
                failure_reason="INFRASTRUCTURE_ERROR",
                message="Preflight failed: claude CLI not found on PATH",
            )
        return PreflightCheckResult.failure(
            failure_reason="INFRASTRUCTURE_ERROR",
            message=f"Preflight failed: claude --version exited {result.returncode}",
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
        """Run Claude to execute a task."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        if interactive:
            return self._run_interactive(
                config,
                prompt,
                conversation_log_file,
                work_dir,
                resume_session_id=resume_session_id,
                on_session_id=on_session_id,
                on_step_count=on_step_count,
                ops_log_file=ops_log_file,
            )
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

    def _run_interactive(
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
        """Run Claude in foreground mode while preserving runner telemetry callbacks."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        if config.use_docker:
            return self._run_docker_interactive(
                config,
                prompt,
                conversation_log_file,
                work_dir,
                resume_session_id=resume_session_id,
                on_session_id=on_session_id,
                on_step_count=on_step_count,
                ops_log_file=ops_log_file,
            )
        return self._run_direct_interactive(
            config,
            prompt,
            conversation_log_file,
            work_dir,
            resume_session_id=resume_session_id,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
            ops_log_file=ops_log_file,
        )

    @staticmethod
    def _build_claude_args(config: Config, resume_session_id: str | None = None) -> list[str]:
        """Build the claude CLI arguments shared across docker and direct modes."""
        args = ["-p", "-", "--output-format", "stream-json", "--verbose"]

        if resume_session_id:
            args.extend(["--resume", resume_session_id])

        if config.model:
            args.extend(["--model", config.model])

        args.extend(config.claude.args)
        args.extend(["--max-turns", str(config.max_steps)])

        return args

    @staticmethod
    def _build_claude_interactive_args(
        config: Config,
        resume_session_id: str | None = None,
    ) -> list[str]:
        """Build args for true interactive Claude foreground sessions."""
        args: list[str] = []

        if resume_session_id:
            args.extend(["--resume", resume_session_id])

        if config.model:
            args.extend(["--model", config.model])

        args.extend(config.claude.args)
        args.extend(["--max-turns", str(config.max_steps)])

        return args

    @classmethod
    def build_noninteractive_command(
        cls,
        config: Config,
        work_dir: Path,
        resume_session_id: str | None = None,
    ) -> list[str]:
        """Build the direct non-interactive Claude command used by headless callers."""
        _ = work_dir
        return [
            "timeout",
            f"{config.timeout_minutes}m",
            "claude",
            *cls._build_claude_args(config, resume_session_id),
        ]

    def _run_docker_interactive(
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
        """Run Claude in foreground interactive mode in Docker."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        if config.claude.fetch_auth_token_from_keychain:
            sync_keychain_credentials()
        docker_config = _get_docker_config(f"{config.docker_image}-claude")

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
            interactive=True,
        )
        cmd.append("claude")
        cmd.extend(self._build_claude_interactive_args(config, resume_session_id))
        return self._run_interactive_command(
            cmd,
            conversation_log_file,
            ops_log_file,
            timeout_minutes=config.timeout_minutes,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
            stdin_input=prompt,
            resume_session_id=resume_session_id,
        )

    def _run_direct_interactive(
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
        """Run Claude in foreground interactive mode on host."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        cmd = ["timeout", f"{config.timeout_minutes}m", "claude"]
        cmd.extend(self._build_claude_interactive_args(config, resume_session_id))
        return self._run_interactive_command(
            cmd,
            conversation_log_file,
            ops_log_file,
            cwd=(getattr(config, "provider_cwd", None) or work_dir),
            timeout_minutes=config.timeout_minutes,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
            stdin_input=prompt,
            resume_session_id=resume_session_id,
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
        """Run Claude in Docker container."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        if config.claude.fetch_auth_token_from_keychain:
            sync_keychain_credentials()
        docker_config = _get_docker_config(f"{config.docker_image}-claude")

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
        cmd.append("claude")
        cmd.extend(self._build_claude_args(config, resume_session_id))

        return self._run_with_output_parsing(
            cmd, conversation_log_file, config.timeout_minutes, stdin_input=prompt, model=config.model,
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
        """Run Claude directly (no Docker)."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        # When running inside a tmux session, use interactive mode so the proxy
        # can auto-accept tool prompts and users can attach.
        if getattr(config.tmux, "session_name", None):
            return self._run_direct_tmux(
                config,
                prompt,
                conversation_log_file,
                work_dir,
                ops_log_file=ops_log_file,
            )

        cmd = self.build_noninteractive_command(config, work_dir, resume_session_id)

        return self._run_with_output_parsing(
            cmd,
            conversation_log_file,
            config.timeout_minutes,
            cwd=(getattr(config, "provider_cwd", None) or work_dir),
            stdin_input=prompt,
            model=config.model,
            chat_text_display_length=config.chat_text_display_length,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
            ops_log_file=ops_log_file,
        )

    def _run_direct_tmux(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        ops_log_file: Path | None = None,
    ) -> RunResult:
        """Run Claude in interactive mode for tmux sessions.

        The initial task prompt is delivered by the TmuxProxy via the PTY master
        fd (simulating typing), so Claude does not receive it as a positional
        argument.  Raw terminal output is captured to ``log_file`` via
        ``tmux pipe-pane``; structured proxy events go to a separate
        ``*-proxy.log`` file so existing log parsers see clean output.
        """
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")

        import json as _json
        import shlex as _shlex

        conversation_log_file.parent.mkdir(parents=True, exist_ok=True)

        # Proxy structured events (JSONL) go to a separate file so they do not
        # mix with the raw terminal output captured in the main log file.
        proxy_log_file = conversation_log_file.parent / f"{conversation_log_file.stem}-proxy.log"

        with open(proxy_log_file, "a") as f:
            f.write(_json.dumps({
                "type": "gza",
                "subtype": "tmux_start",
                "message": "Started in tmux interactive mode",
                "session": config.tmux.session_name,
            }) + "\n")
        write_ops_event(
            ops_log_file,
            subtype="tmux_start",
            source="provider",
            message="Started in tmux interactive mode",
            session=config.tmux.session_name,
        )

        # Capture raw terminal output from the tmux pane to the main log file.
        # This mirrors the output that humans see when they attach, and is what
        # ``gza log -f`` reads in tmux mode.
        if config.tmux.session_name:
            subprocess.run(
                [
                    "tmux", "pipe-pane", "-t", config.tmux.session_name,
                    f"cat >> {_shlex.quote(str(conversation_log_file))}",
                ],
                check=False,
            )

        # Run Claude in interactive mode — the proxy delivers the prompt via the
        # PTY so we do not pass it as a positional argument here.
        cmd = ["claude", "--max-turns", str(config.max_steps)]
        cmd.extend(config.claude.args)

        # Run with inherited stdin/stdout/stderr (connected to the PTY via the proxy)
        result = subprocess.run(cmd, cwd=(getattr(config, "provider_cwd", None) or work_dir))

        with open(proxy_log_file, "a") as f:
            f.write(_json.dumps({
                "type": "gza",
                "subtype": "tmux_end",
                "exit_code": result.returncode,
            }) + "\n")
        write_ops_event(
            ops_log_file,
            subtype="tmux_end",
            source="provider",
            message=f"tmux session exited with code {result.returncode}",
            exit_code=result.returncode,
        )

        return RunResult(exit_code=result.returncode)

    @staticmethod
    def _redact_interactive_launch_command(
        cmd: list[str],
        *,
        prompt_seed: str | None = None,
        resume_session_id: str | None = None,
    ) -> list[str]:
        """Return a log-safe command view for interactive launch events."""
        if not prompt_seed or resume_session_id:
            return cmd
        redacted = cmd.copy()
        for idx, arg in enumerate(redacted):
            if arg == prompt_seed:
                redacted[idx] = "<prompt_redacted>"
                break
        return redacted

    def _run_interactive_command(
        self,
        cmd: list[str],
        conversation_log_file: Path,
        ops_log_file: Path,
        *,
        cwd: Path | None = None,
        timeout_minutes: int,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
        stdin_input: str | None = None,
        resume_session_id: str | None = None,
    ) -> RunResult:
        """Run an interactive Claude command in the foreground terminal."""
        conversation_log_file.parent.mkdir(parents=True, exist_ok=True)
        launch_cmd = self._redact_interactive_launch_command(
            cmd,
            prompt_seed=stdin_input,
            resume_session_id=resume_session_id,
        )
        write_ops_event(
            ops_log_file,
            subtype="interactive_launch",
            source="provider",
            message="Launching interactive Claude session",
            command=launch_cmd,
        )

        start_time = time.time()
        session_id: str | None = resume_session_id
        if resume_session_id and on_session_id:
            on_session_id(resume_session_id)
        seen_message_ids: set[str] = set()
        computed_steps = 0
        recovered_result_text: str | None = None
        transcript_lines: list[str] = []

        def _process_interactive_line(line: str) -> None:
            nonlocal session_id, computed_steps, recovered_result_text
            stripped = line.strip()
            if not stripped:
                return
            transcript_lines.append(stripped)
            try:
                event = json.loads(stripped)
            except json.JSONDecodeError:
                return
            if not isinstance(event, dict):
                return

            event_type = event.get("type")
            if event_type == "system" and event.get("subtype") == "init":
                init_session_id = event.get("session_id")
                if isinstance(init_session_id, str) and init_session_id and not session_id:
                    session_id = init_session_id
                    if on_session_id:
                        on_session_id(session_id)
            elif event_type == "assistant":
                msg = event.get("message")
                if isinstance(msg, dict):
                    msg_id = msg.get("id")
                    if isinstance(msg_id, str) and msg_id and msg_id not in seen_message_ids:
                        seen_message_ids.add(msg_id)
                        computed_steps = len(seen_message_ids)
                        if on_step_count:
                            on_step_count(computed_steps)
            elif event_type == "result":
                result_session_id = event.get("session_id")
                if isinstance(result_session_id, str) and result_session_id and not session_id:
                    session_id = result_session_id
                    if on_session_id:
                        on_session_id(session_id)
                result_text = event.get("result")
                if isinstance(result_text, str) and result_text.strip():
                    recovered_result_text = result_text

        process: subprocess.Popen[bytes] | None = None
        master_fd: int | None = None
        slave_fd: int | None = None
        stdin_fd: int | None = None
        line_buffer = ""
        pty_eof = False
        try:
            master_fd, slave_fd = pty.openpty()
            process = subprocess.Popen(
                cmd,
                cwd=cwd,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                text=False,
                bufsize=0,
                env=os.environ.copy(),
            )
            os.close(slave_fd)
            slave_fd = None

            # Seed the initial task prompt via PTY stdin so it is never exposed in argv.
            # For true interactive mode, send only a newline-terminated prompt and
            # keep stdin connected for the rest of the live session.
            if stdin_input:
                seeded_prompt = stdin_input
                if not seeded_prompt.endswith("\n"):
                    seeded_prompt += "\n"
                try:
                    os.write(master_fd, seeded_prompt.encode("utf-8", errors="replace"))
                except OSError:
                    error_message = "Failed to seed interactive stdin prompt; aborting interactive run."
                    logger.error(error_message, exc_info=True)
                    write_ops_event(
                        ops_log_file,
                        subtype="outcome",
                        source="provider",
                        message=error_message,
                        exit_code=1,
                        failure_reason="UNKNOWN",
                    )
                    sys.stderr.write(f"{error_message}\n")
                    sys.stderr.flush()
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                    return RunResult(
                        exit_code=1,
                        duration_seconds=round(time.time() - start_time, 1),
                        session_id=session_id,
                        num_steps_computed=computed_steps or None,
                        error_type="startup_failed",
                    )

            try:
                maybe_stdin_fd = sys.stdin.fileno()
            except (AttributeError, io.UnsupportedOperation, ValueError):
                maybe_stdin_fd = None
            if isinstance(maybe_stdin_fd, int) and os.isatty(maybe_stdin_fd):
                stdin_fd = maybe_stdin_fd

            with open(conversation_log_file, "a", encoding="utf-8") as f:
                while True:
                    read_fds = [master_fd]
                    if stdin_fd is not None:
                        read_fds.append(stdin_fd)
                    ready, _, _ = select.select(read_fds, [], [], 0.1)

                    if master_fd in ready:
                        try:
                            chunk = os.read(master_fd, 4096)
                        except OSError:
                            chunk = b""
                        if not chunk:
                            pty_eof = True
                        else:
                            text = chunk.decode("utf-8", errors="replace")
                            f.write(text)
                            f.flush()
                            sys.stdout.write(text)
                            sys.stdout.flush()
                            line_buffer += text
                            while "\n" in line_buffer:
                                line, line_buffer = line_buffer.split("\n", 1)
                                _process_interactive_line(line)

                    if stdin_fd is not None and stdin_fd in ready:
                        try:
                            user_input = os.read(stdin_fd, 1024)
                        except OSError:
                            stdin_fd = None
                        else:
                            if user_input:
                                try:
                                    os.write(master_fd, user_input)
                                except OSError:
                                    pty_eof = True

                    if process.poll() is not None and pty_eof:
                        break

                process.wait()
                if line_buffer.strip():
                    _process_interactive_line(line_buffer.strip())
                if not recovered_result_text:
                    transcript_text = "\n".join(transcript_lines).strip()
                    if transcript_text:
                        recovered_result_text = transcript_text
                if recovered_result_text:
                    f.write(
                        json.dumps({
                            "type": "result",
                            "subtype": "interactive_capture",
                            "result": recovered_result_text,
                        }) + "\n"
                    )
        except KeyboardInterrupt:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            raise
        finally:
            if slave_fd is not None:
                os.close(slave_fd)
            if master_fd is not None:
                os.close(master_fd)

        duration_seconds = round(time.time() - start_time, 1)
        return RunResult(
            exit_code=process.returncode if process is not None else 1,
            duration_seconds=duration_seconds,
            session_id=session_id,
            num_steps_computed=computed_steps or None,
        )

    def _run_with_output_parsing(
        self,
        cmd: list[str],
        log_file: Path,
        timeout_minutes: int,
        cwd: Path | None = None,
        stdin_input: str | None = None,
        model: str = "",
        chat_text_display_length: int = 0,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
        ops_log_file: Path | None = None,
    ) -> RunResult:
        """Run command and parse Claude's stream-json output."""
        conversation_log_file = log_file
        if ops_log_file is None:
            ops_log_file = log_file.with_name(f"{log_file.stem}.ops.jsonl")
        formatter = StreamOutputFormatter()

        def _ensure_step_store(data: dict) -> None:
            _ensure_claude_step_store(data)

        def _allocate_legacy_event_id(data: dict, legacy_turn_id: str | None) -> str | None:
            return _allocate_claude_legacy_event_id(data, legacy_turn_id)

        def _start_step(data: dict, msg_id: str | None, legacy_turn_id: str | None) -> dict:
            return _start_claude_step(data, msg_id, legacy_turn_id, on_step_count)

        def _append_tool_result_substep(data: dict, current_step: dict | None, content: dict[str, Any]) -> dict:
            return _append_claude_tool_result_substep(data, current_step, content)

        def parse_claude_output(line: str, data: dict, log_handle=None) -> None:
            try:
                event: dict[str, Any] = json.loads(line)
                event_type = event.get("type")
                method_name = CLAUDE_LIVE_EVENT_HANDLERS.get(str(event_type))
                if method_name is None:
                    formatter.print_error(
                        _unknown_live_type_message("Claude", "event type", event_type)
                    )
                    return
                getattr(self, method_name)(
                    event=event,
                    formatter=formatter,
                    data=data,
                    model=model,
                    log_handle=log_handle,
                    on_session_id=on_session_id,
                    on_step_count=on_step_count,
                    chat_text_display_length=chat_text_display_length,
                    ops_log_file=ops_log_file,
                    ensure_step_store=_ensure_step_store,
                    start_step=_start_step,
                )

            except json.JSONDecodeError:
                # Non-JSON output, just display it
                if line == data.get("_startup_line"):
                    return
                print(line)

        result = self.run_with_logging(
            cmd,
            conversation_log_file,
            timeout_minutes,
            cwd=cwd,
            parse_output=parse_claude_output,
            stdin_input=stdin_input,
            ops_log_file=ops_log_file,
        )

        # Extract stats and error info from result event
        accumulated_data = getattr(result, "_accumulated_data", {}) or {}
        result_data = accumulated_data.get("result", {})
        if result_data:
            if "num_turns" in result_data:
                result.num_turns_reported = result_data["num_turns"]
            if "total_cost_usd" in result_data:
                result.cost_usd = result_data["total_cost_usd"]
            # Check for error subtypes (e.g., error_max_turns)
            subtype = result_data.get("subtype", "")
            if subtype == "error_max_turns":
                result.error_type = "max_turns"

        # Expose accumulated session_id (captured from system/init or result event)
        if "session_id" in accumulated_data:
            result.session_id = accumulated_data["session_id"]

        # Store our internally computed turn count (unique assistant message IDs)
        seen_msg_ids = accumulated_data.get("seen_msg_ids", set())
        if seen_msg_ids:
            result.num_turns_computed = len(seen_msg_ids)

        step_count = len(accumulated_data.get("run_step_events", []))
        result.num_steps_computed = step_count
        result.num_steps_reported = step_count

        # Store accumulated token counts
        if "total_input_tokens" in accumulated_data:
            result.input_tokens = accumulated_data["total_input_tokens"]
        if "total_output_tokens" in accumulated_data:
            result.output_tokens = accumulated_data["total_output_tokens"]

        return result

    def _handle_live_assistant_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        chat_text_display_length: int,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        start_step: Callable[[dict[str, Any], str | None, str | None], dict[str, Any]],
    ) -> None:
        _ = on_session_id, on_step_count
        message = event.get("message", {})
        msg_id = message.get("id")
        ensure_step_store(data)

        if "seen_msg_ids" not in data:
            data["seen_msg_ids"] = set()
            data["start_time"] = time.time()
        turn_count = len(data["seen_msg_ids"])
        if msg_id and msg_id not in data["seen_msg_ids"]:
            data["seen_msg_ids"].add(msg_id)
            turn_count = len(data["seen_msg_ids"])
            start_step(data, msg_id, f"T{turn_count}")

            usage = message.get("usage", {})
            if "total_input_tokens" not in data:
                data["total_input_tokens"] = 0
                data["total_output_tokens"] = 0
            data["total_input_tokens"] += usage.get("input_tokens", 0)
            data["total_input_tokens"] += usage.get("cache_creation_input_tokens", 0)
            data["total_input_tokens"] += usage.get("cache_read_input_tokens", 0)
            data["total_output_tokens"] += usage.get("output_tokens", 0)

            elapsed_seconds = int(time.time() - data["start_time"])
            total_tokens = data["total_input_tokens"] + data["total_output_tokens"]
            cost = calculate_cost(
                data["total_input_tokens"],
                data["total_output_tokens"],
                model,
            )

            if log_handle:
                timestamp_str = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                write_ops_event(
                    ops_log_file,
                    subtype="step_marker",
                    source="provider",
                    message=f"Step {turn_count}",
                    step=turn_count,
                    step_timestamp=timestamp_str,
                )

            formatter.print_step_header(
                turn_count,
                total_tokens,
                cost,
                elapsed_seconds,
                blank_line_before=turn_count > 1,
            )
        current_step = data["_step_by_msg_id"].get(msg_id) if msg_id else data.get("_current_step_event")
        if current_step is None:
            legacy_turn_id = f"T{turn_count}" if turn_count > 0 else None
            current_step = start_step(data, msg_id, legacy_turn_id)

        for content in message.get("content", []):
            current_step = self._handle_live_assistant_block(
                content=content,
                formatter=formatter,
                data=data,
                current_step=current_step,
                chat_text_display_length=chat_text_display_length,
            )

    def _handle_live_user_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        chat_text_display_length: int,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        start_step: Callable[[dict[str, Any], str | None, str | None], dict[str, Any]],
    ) -> None:
        _ = model, log_handle, on_session_id, on_step_count, chat_text_display_length, ops_log_file, ensure_step_store, start_step
        current_step = data.get("_current_step_event")
        for content in message_content_items(event):
            current_step = self._handle_live_user_block(
                content=content,
                formatter=formatter,
                data=data,
                current_step=current_step,
            )

    def _handle_live_system_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        chat_text_display_length: int,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        start_step: Callable[[dict[str, Any], str | None, str | None], dict[str, Any]],
    ) -> None:
        _ = formatter, model, log_handle, on_step_count, chat_text_display_length, ops_log_file, ensure_step_store, start_step
        subtype = event.get("subtype")
        if subtype == "init":
            session_id = event.get("session_id")
            if session_id and "session_id" not in data:
                data["session_id"] = session_id
                if on_session_id:
                    on_session_id(session_id)

    def _handle_live_result_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        chat_text_display_length: int,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        start_step: Callable[[dict[str, Any], str | None, str | None], dict[str, Any]],
    ) -> None:
        _ = formatter, model, log_handle, on_step_count, chat_text_display_length, ops_log_file, ensure_step_store, start_step
        data["result"] = event
        session_id = event.get("session_id")
        if session_id and "session_id" not in data:
            data["session_id"] = session_id
            if on_session_id:
                on_session_id(session_id)

    def _handle_live_error_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        chat_text_display_length: int,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        start_step: Callable[[dict[str, Any], str | None, str | None], dict[str, Any]],
    ) -> None:
        _ = data, model, log_handle, on_session_id, on_step_count, chat_text_display_length, ops_log_file, ensure_step_store, start_step
        for line in error_lines(event.get("message", "")):
            formatter.print_error(line.removeprefix("[error] ").removeprefix("[error]").strip() or "error", prefix="Error: ")

    def _handle_live_rate_limit_event(
        self,
        *,
        event: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        model: str,
        log_handle: io.TextIOBase | None,
        on_session_id: Callable[[str], None] | None,
        on_step_count: Callable[[int], None] | None,
        chat_text_display_length: int,
        ops_log_file: Path | None,
        ensure_step_store: Callable[[dict[str, Any]], None],
        start_step: Callable[[dict[str, Any], str | None, str | None], dict[str, Any]],
    ) -> None:
        _ = data, model, log_handle, on_session_id, on_step_count, chat_text_display_length, ops_log_file, ensure_step_store, start_step
        message = _claude_rate_limit_message(event)
        if message:
            formatter.print_error(message)

    def _handle_live_assistant_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any],
        chat_text_display_length: int,
    ) -> dict[str, Any]:
        content_type = content.get("type")
        method_name = CLAUDE_LIVE_ASSISTANT_BLOCK_HANDLERS.get(str(content_type))
        if method_name is None:
            formatter.print_error(_unknown_live_type_message("Claude", "assistant block type", content_type))
            return current_step
        return getattr(self, method_name)(
            content=content,
            formatter=formatter,
            data=data,
            current_step=current_step,
            chat_text_display_length=chat_text_display_length,
        )

    def _handle_live_assistant_tool_use_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any],
        chat_text_display_length: int,
    ) -> dict[str, Any]:
        _ = chat_text_display_length
        tool_name = content.get("name", "unknown")
        tool_input = content.get("input", {})
        call_id = content.get("id")
        seen_tool_use_ids = current_step.setdefault("_seen_tool_use_ids", set())
        dedupe_key = call_id or f"{tool_name}:{json.dumps(tool_input, sort_keys=True, default=str)}"
        if dedupe_key in seen_tool_use_ids:
            return current_step
        seen_tool_use_ids.add(dedupe_key)
        current_step["substeps"].append(
            {
                "type": "tool_call",
                "source": "provider",
                "call_id": call_id,
                "payload": {
                    "tool_name": tool_name,
                    "tool_input": tool_input,
                },
                "legacy_turn_id": current_step.get("legacy_turn_id"),
                "legacy_event_id": _allocate_claude_legacy_event_id(data, current_step.get("legacy_turn_id")),
            }
        )

        file_path = tool_input.get("file_path") or tool_input.get("path")
        if tool_name == "Bash":
            command = truncate_text(tool_input.get("command", ""), 80)
            formatter.print_tool_event(tool_name, command)
        elif tool_name == "Glob":
            formatter.print_tool_event(tool_name, tool_input.get("pattern", ""))
        elif tool_name == "TodoWrite":
            todos = tool_input.get("todos", [])
            todos_summary = f"{len(todos)} todos"
            dict_todos = [t for t in todos if isinstance(t, dict)]
            if dict_todos:
                pending = sum(1 for t in dict_todos if t.get("status") == "pending")
                in_progress = sum(1 for t in dict_todos if t.get("status") == "in_progress")
                completed = sum(1 for t in dict_todos if t.get("status") == "completed")
                todos_summary += f" (pending: {pending}, in_progress: {in_progress}, completed: {completed})"
            formatter.print_tool_event(tool_name, todos_summary)
            for todo in todos:
                if isinstance(todo, dict):
                    status = todo.get("status", "pending")
                    todo_content = todo.get("content", "")
                else:
                    status = "pending"
                    todo_content = str(todo)
                formatter.print_todo(status, truncate_text(todo_content, 60))
        elif tool_name == "Edit":
            parts = [tool_name]
            if file_path:
                parts.append(file_path)
            old_string = tool_input.get("old_string", "")
            new_string = tool_input.get("new_string", "")
            old_lines = old_string.count("\n") + (1 if old_string else 0)
            new_lines = new_string.count("\n") + (1 if new_string else 0)
            if old_lines > 0 or new_lines > 0:
                added = max(0, new_lines - old_lines)
                removed = max(0, old_lines - new_lines)
                if added > 0 and removed > 0:
                    parts.append(f"(+{added}/-{removed} lines)")
                elif added > 0:
                    parts.append(f"(+{added} lines)")
                elif removed > 0:
                    parts.append(f"(-{removed} lines)")
            if tool_input.get("replace_all"):
                parts.append("[replace_all]")
            if old_string:
                first_line = old_string.split("\n")[0]
                preview = truncate_text(first_line, 40).replace("\r", "\\r").replace("\t", "\\t")
                parts.append(f'"{preview}"')
            formatter.print_tool_event(" ".join(parts))
        elif file_path:
            formatter.print_tool_event(tool_name, file_path)
        else:
            parts = [tool_name]
            for key, value in tool_input.items():
                parts.append(f"{key}={_format_tool_param(value)}")
            formatter.print_tool_event(" ".join(parts))
        return current_step

    def _handle_live_assistant_tool_result_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any],
        chat_text_display_length: int,
    ) -> dict[str, Any]:
        _ = formatter, data, chat_text_display_length
        return _append_claude_tool_result_substep(data, current_step, content)

    def _handle_live_assistant_tool_retry_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any],
        chat_text_display_length: int,
    ) -> dict[str, Any]:
        _ = formatter, chat_text_display_length
        legacy_turn_id = current_step.get("legacy_turn_id")
        current_step["substeps"].append(
            {
                "type": "tool_retry",
                "source": "provider",
                "call_id": content.get("id"),
                "payload": {
                    "retry_of_call_id": content.get("retry_of_call_id"),
                },
                "legacy_turn_id": legacy_turn_id,
                "legacy_event_id": _allocate_claude_legacy_event_id(data, legacy_turn_id),
            }
        )
        return current_step

    def _handle_live_assistant_text_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any],
        chat_text_display_length: int,
    ) -> dict[str, Any]:
        _ = data
        text = content.get("text", "").strip()
        if not text:
            return current_step
        previous = current_step.get("message_text")
        current_step["message_text"] = text if not previous else f"{previous}\n{text}"
        if chat_text_display_length == 0:
            formatter.print_agent_message(text)
        else:
            first_line = text.split("\n")[0]
            formatter.print_agent_message(truncate_text(first_line, chat_text_display_length))
        return current_step

    def _handle_live_user_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        item_type = content.get("type")
        method_name = CLAUDE_LIVE_USER_BLOCK_HANDLERS.get(str(item_type))
        if method_name is None:
            formatter.print_error(_unknown_live_type_message("Claude", "user block type", item_type))
            return current_step
        return getattr(self, method_name)(
            content=content,
            formatter=formatter,
            data=data,
            current_step=current_step,
        )

    def _handle_live_user_tool_result_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        current_step = _append_claude_tool_result_substep(data, current_step, content)
        result_text = str(content.get("content", "") or "").strip()
        if result_text:
            if bool(content.get("is_error")):
                formatter.print_error(result_text)
            else:
                formatter.print_agent_message(result_text)
        return current_step

    def _handle_live_user_text_block(
        self,
        *,
        content: dict[str, Any],
        formatter: StreamOutputFormatter,
        data: dict[str, Any],
        current_step: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        _ = data
        text = str(content.get("text", "") or "").strip()
        if text:
            formatter.print_agent_message(f"user: {text}")
        return current_step
