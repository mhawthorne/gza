"""Log display and timeline rendering for ``gza log``."""

import argparse
import json
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

from rich.markup import escape as rich_escape

from ..colors import (
    LINEAGE_STATUS_COLORS,
    PS_STATUS_COLORS,
    SHOW_COLORS_DICT,
    blue,
    pink,
)
from ..config import Config
from ..console import console, format_duration, truncate
from ..db import SqliteTaskStore, Task as DbTask
from ..log_events import is_new_step
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import _parse_iso, get_store, pager_context, resolve_id


def _lc() -> str:
    """Return the themed label color for log output."""
    return SHOW_COLORS_DICT["label"]


def _result_step_count(result_entry: dict) -> int | None:
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


def _summarize_tool_detail(tool_name: str, tool_input: dict) -> str:
    """Build a compact one-line tool summary for timeline rendering."""
    if tool_name == "Bash":
        cmd = str(tool_input.get("command", ""))
        return truncate(cmd, 100) if cmd else "Bash"
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


def _append_timeline_step(steps: list[dict], message_text: str | None, summary: str | None = None) -> dict:
    """Append a timeline step and return it."""
    step_index = len(steps) + 1
    step: dict = {
        "step_id": f"S{step_index}",
        "message_text": (message_text or "").strip() or None,
        "summary": summary,
        "substeps": [],
    }
    steps.append(step)
    return step


def _append_substep(step: dict, detail: str) -> None:
    """Append a substep line to a timeline step."""
    detail = detail.strip()
    if not detail:
        return
    substeps = step["substeps"]
    substeps.append(
        {
            "substep_id": f"{step['step_id']}.{len(substeps) + 1}",
            "detail": detail,
        }
    )


def _ensure_current_step(steps: list[dict], current_step: dict | None) -> dict:
    """Ensure a current step exists for pre-message tool activity."""
    if current_step is not None:
        return current_step
    return _append_timeline_step(steps, None, summary="Pre-message tool activity")


def _message_content_items(entry: dict) -> list[dict]:
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


def _build_step_timeline(entries: list[dict]) -> list[dict]:
    """Build step-first timeline model from mixed historical log entry shapes."""
    steps: list[dict] = []
    current_step: dict | None = None

    for entry in entries:
        entry_type = entry.get("type")

        if entry_type == "assistant":
            content_items = _message_content_items(entry)
            text_chunks: list[str] = []
            tool_items: list[dict] = []
            for item in content_items:
                item_type = item.get("type")
                if item_type == "text":
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        text_chunks.append(text.strip())
                elif item_type == "tool_use":
                    tool_items.append(item)

            if text_chunks:
                current_step = _append_timeline_step(steps, "\n".join(text_chunks))
            elif tool_items:
                current_step = _ensure_current_step(steps, current_step)

            for item in tool_items:
                if current_step is None:
                    current_step = _ensure_current_step(steps, current_step)
                tool_name = str(item.get("name", "unknown"))
                tool_input = item.get("input", {})
                if not isinstance(tool_input, dict):
                    tool_input = {}
                _append_substep(current_step, f"tool_call {_summarize_tool_detail(tool_name, tool_input)}")

        elif entry_type == "user":
            content_items = _message_content_items(entry)
            for item in content_items:
                if item.get("type") != "tool_result":
                    continue
                current_step = _ensure_current_step(steps, current_step)
                is_error = bool(item.get("is_error", False))
                result_type = "tool_error" if is_error else "tool_output"
                content = item.get("content", "")
                if isinstance(content, str):
                    detail = truncate(content.replace("\\n", "\n"), 120)
                else:
                    detail = truncate(json.dumps(content, ensure_ascii=True), 120)
                _append_substep(current_step, f"{result_type} {detail}".strip())

        elif entry_type == "message":
            role = entry.get("role")
            if role == "user":
                current_step = None
                continue
            if role == "assistant":
                content = entry.get("content", "")
                if isinstance(content, str) and content.strip() and not entry.get("delta"):
                    current_step = _append_timeline_step(steps, content.strip())

        elif entry_type == "tool_use":
            current_step = _ensure_current_step(steps, current_step)
            tool_name = str(entry.get("tool_name", "unknown"))
            tool_input = entry.get("tool_input", {})
            if not isinstance(tool_input, dict):
                tool_input = {}
            _append_substep(current_step, f"tool_call {_summarize_tool_detail(tool_name, tool_input)}")

        elif entry_type in {"tool_output", "tool_error", "tool_retry"}:
            current_step = _ensure_current_step(steps, current_step)
            payload = {
                key: value
                for key, value in entry.items()
                if key not in {"type", "id", "call_id"}
            }
            detail = truncate(json.dumps(payload, ensure_ascii=True), 120)
            _append_substep(current_step, f"{entry_type} {detail}".strip())

        elif entry_type == "turn.started":
            current_step = None

        elif entry_type == "gza":
            subtype = entry.get("subtype", "")
            if subtype in ("branch", "stats", "outcome"):
                pass  # metadata only — skip timeline
            else:
                message = entry.get("message", "")
                if message:
                    label = f"[gza:{subtype}] {message}" if subtype else f"[gza] {message}"
                    current_step = _append_timeline_step(steps, label)

        elif entry_type == "item.completed":
            item = entry.get("item", {})
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")
            if item_type == "agent_message":
                text = item.get("text")
                if isinstance(text, str) and text.strip():
                    current_step = _append_timeline_step(steps, text.strip())
            elif item_type == "command_execution":
                current_step = _ensure_current_step(steps, current_step)
                command = truncate(str(item.get("command", "")), 100)
                _append_substep(current_step, f"tool_call Bash {command}".strip())
                if "aggregated_output" in item or "exit_code" in item:
                    exit_code = item.get("exit_code")
                    is_error = isinstance(exit_code, int) and exit_code != 0
                    substep_type = "tool_error" if is_error else "tool_output"
                    output = str(item.get("aggregated_output", ""))
                    _append_substep(
                        current_step,
                        f"{substep_type} {truncate(output, 120)}".strip(),
                    )
        elif entry_type == "raw":
            message = entry.get("message", "")
            if isinstance(message, str) and message.strip():
                current_step = _append_timeline_step(steps, f"[raw] {message.strip()}")

    return steps


def _display_step_timeline(entries: list[dict], *, verbose: bool) -> None:
    """Render a step-first timeline in compact or verbose mode."""
    steps = _build_step_timeline(entries)
    if not steps:
        console.print("No step entries found.", soft_wrap=True)
        return

    for step in steps:
        title = f"[{_lc()}]\\[Step {step['step_id']}][/{_lc()}]"
        message_text = step.get("message_text")
        summary = step.get("summary")
        if message_text:
            console.print(f"{title} {rich_escape(message_text)}", soft_wrap=True)
        elif summary:
            console.print(f"{title} {rich_escape(summary)}", soft_wrap=True)
        else:
            console.print(title, soft_wrap=True)
        if verbose:
            for substep in step["substeps"]:
                console.print(f"  [green]\\[{substep['substep_id']}][/green] {rich_escape(substep['detail'])}", soft_wrap=True)


class _LiveLogPrinter:
    """Stateful printer that renders log entries using the same style as ``gza work``.

    Tracks step boundaries so it can emit step headers with token/cost/runtime
    info, while also showing tool results (which ``gza work`` omits).
    """

    def __init__(self, *, live: bool = True) -> None:
        from ..providers.output_formatter import StreamOutputFormatter, truncate_text as _trunc
        self._fmt = StreamOutputFormatter()
        self._console = self._fmt.console
        self._trunc = _trunc
        self._live = live
        self._seen_msg_ids: set[str] = set()
        self._step_count = 0
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self._start_time: float | None = None
        self._configured_provider: str | None = None
        self._configured_model: str | None = None
        self._provider_reported_model: str | None = None
        self._last_model_display_key: tuple[str | None, str | None, str | None] | None = None

    _PROVIDER_MODEL_INFO_RE = re.compile(
        r"^Provider:\s*(?P<provider>[^,]+),\s*Model:\s*(?P<model>.+?)\s*$"
    )

    @classmethod
    def _parse_configured_provider_model(cls, message: str) -> tuple[str | None, str | None]:
        """Parse configured provider/model from standard gza info line."""
        match = cls._PROVIDER_MODEL_INFO_RE.match(message.strip())
        if not match:
            return None, None
        provider = match.group("provider").strip() or None
        model = match.group("model").strip() or None
        return provider, model

    @staticmethod
    def _extract_provider_model(entry: dict) -> str | None:
        """Extract provider-reported model from a provider event when present."""
        candidate_keys = ("model", "model_name", "provider_model")
        for key in candidate_keys:
            value = entry.get(key)
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped
        return None

    def _render_model_parity_if_changed(self, *, include_missing_provider_note: bool = False) -> None:
        """Render configured/provider model parity when state changes."""
        key = (
            self._configured_provider,
            self._configured_model,
            self._provider_reported_model,
        )
        if key == self._last_model_display_key:
            return

        if not self._configured_model and not self._provider_reported_model:
            return

        provider_label = self._configured_provider or "Provider"
        configured = self._configured_model or "unknown"
        provider_reported = self._provider_reported_model

        if provider_reported:
            self._last_model_display_key = key
            self._fmt.print_agent_message(
                f"{provider_label} model parity: configured={configured}, provider_reported={provider_reported}"
            )
            if configured != provider_reported:
                self._console.print(
                    f"[yellow]WARNING: Model mismatch ({configured} != {provider_reported})[/yellow]",
                    soft_wrap=True,
                )
            return

        if include_missing_provider_note and self._configured_model:
            self._last_model_display_key = key
            self._fmt.print_agent_message(
                f"{provider_label} model parity: configured={configured}, provider_reported=(provider did not echo model)"
            )

    def process(self, entry: dict) -> None:
        """Process a single JSON log entry and print it."""
        entry_type = entry.get("type")

        if entry_type == "system":
            subtype = entry.get("subtype", "")
            if subtype == "init":
                self._fmt.print_agent_message("Session initialized")
                provider_model = self._extract_provider_model(entry)
                if provider_model:
                    self._provider_reported_model = provider_model
                self._render_model_parity_if_changed(include_missing_provider_note=True)

        elif entry_type == "init":
            # Gemini stream-json init event.
            self._fmt.print_agent_message("Session initialized")
            provider_model = self._extract_provider_model(entry)
            if provider_model:
                self._provider_reported_model = provider_model
            self._render_model_parity_if_changed(include_missing_provider_note=True)

        elif entry_type == "assistant":
            message = entry.get("message", {})

            if self._start_time is None:
                self._start_time = time.time()

            # New step on new message ID
            if is_new_step(entry, self._seen_msg_ids):
                self._step_count += 1

                if self._step_count > 1:
                    self._console.print()

                if self._live:
                    usage = message.get("usage", {})
                    self._total_input_tokens += usage.get("input_tokens", 0)
                    self._total_input_tokens += usage.get("cache_creation_input_tokens", 0)
                    self._total_input_tokens += usage.get("cache_read_input_tokens", 0)
                    self._total_output_tokens += usage.get("output_tokens", 0)

                    total_tokens = self._total_input_tokens + self._total_output_tokens
                    elapsed = int(time.time() - self._start_time)

                    self._fmt.print_step_header(
                        self._step_count, total_tokens, 0.0, elapsed,
                        blank_line_before=False,
                    )
                else:
                    self._console.print(
                        f"| Step {self._step_count} |",
                        style=blue,
                    )

            raw_content = message.get("content", [])
            if isinstance(raw_content, str):
                # Simple string content (e.g. "Working...")
                if raw_content.strip():
                    self._fmt.print_agent_message(raw_content.strip())
            elif isinstance(raw_content, list):
                for content in raw_content:
                    if not isinstance(content, dict):
                        continue
                    if content.get("type") == "tool_use":
                        self._console.print()
                        self._print_tool_use(content)
                    elif content.get("type") == "text":
                        text = content.get("text", "").strip()
                        if text:
                            self._fmt.print_agent_message(text)

        elif entry_type == "user":
            # Tool results
            content_items = _message_content_items(entry)
            for item in content_items:
                if item.get("type") == "tool_result":
                    result = item.get("content", "")
                    is_error = item.get("is_error", False)
                    if isinstance(result, str):
                        result = result.replace("\\n", "\n").replace("\\t", "\t")
                        if len(result) > 200:
                            result = result[:200] + "..."
                        if is_error:
                            self._fmt.print_error(result)
                        else:
                            self._console.print(rich_escape(result), style=SHOW_COLORS_DICT["label"], soft_wrap=True)

        elif entry_type == "gza":
            subtype = entry.get("subtype", "")
            message = entry.get("message", "")
            if subtype == "info" and isinstance(message, str):
                provider_name, configured_model = self._parse_configured_provider_model(message)
                if provider_name:
                    self._configured_provider = provider_name
                if configured_model:
                    self._configured_model = configured_model
                self._render_model_parity_if_changed()
            if message:
                if subtype:
                    self._console.print(f"[{_lc()}]\\[gza:{rich_escape(subtype)}][/{_lc()}] {rich_escape(message)}", soft_wrap=True)
                else:
                    self._console.print(f"[{_lc()}]\\[gza][/{_lc()}] {rich_escape(message)}", soft_wrap=True)

        elif entry_type == "thread.started":
            thread_id = entry.get("thread_id", "")
            provider_model = self._extract_provider_model(entry)
            if provider_model:
                self._provider_reported_model = provider_model
            if thread_id:
                self._fmt.print_agent_message(f"Session started (thread: {thread_id})")
            self._render_model_parity_if_changed(include_missing_provider_note=True)

        elif entry_type == "turn.started":
            # Codex emits turn.started once per session (not per logical step).
            # Step headers are printed when agent_message items arrive instead.
            if self._start_time is None:
                self._start_time = time.time()

        elif entry_type == "item.completed":
            item = entry.get("item", {})
            if not isinstance(item, dict):
                pass
            elif item.get("type") == "agent_message":
                text = item.get("text", "")
                if is_new_step(entry, self._seen_msg_ids):
                    if self._start_time is None:
                        self._start_time = time.time()
                    self._step_count += 1
                    if self._step_count > 1:
                        self._console.print()
                    if self._live:
                        total_tokens = self._total_input_tokens + self._total_output_tokens
                        elapsed = int(time.time() - self._start_time)
                        self._fmt.print_step_header(
                            self._step_count, total_tokens, 0.0, elapsed,
                            blank_line_before=False,
                        )
                    else:
                        self._console.print(
                            f"| Step {self._step_count} |",
                            style=blue,
                        )
                    self._fmt.print_agent_message(text.strip())
            elif item.get("type") == "command_execution":
                command = item.get("command", "")
                self._console.print()
                self._fmt.print_tool_event("Bash", self._trunc(command, 80))
                aggregated_output = item.get("aggregated_output", "")
                exit_code = item.get("exit_code")
                if isinstance(aggregated_output, str) and aggregated_output.strip():
                    is_error = isinstance(exit_code, int) and exit_code != 0
                    output = aggregated_output.strip("\n")
                    if len(output) > 200:
                        output = output[:200] + "..."
                    if is_error:
                        self._fmt.print_error(output)
                    else:
                        self._console.print(rich_escape(output), style=SHOW_COLORS_DICT["label"], soft_wrap=True)

        elif entry_type == "result":
            self._render_model_parity_if_changed(include_missing_provider_note=True)
            is_error = entry.get("is_error", False)
            subtype = str(entry.get("subtype") or "")
            result_text = entry.get("result", "")
            if is_error:
                self._fmt.print_error(f"[result] ERROR: {result_text}")
            elif subtype and subtype != "success":
                if isinstance(result_text, str) and result_text.strip():
                    self._console.print(f"[yellow]\\[result] {rich_escape(subtype)}:[/yellow] {rich_escape(result_text.strip())}", soft_wrap=True)
                else:
                    self._console.print(f"[yellow]\\[result] {rich_escape(subtype)}[/yellow]", soft_wrap=True)
            else:
                # Success result
                if isinstance(result_text, str) and result_text.strip():
                    self._console.print(f"[green]\\[result][/green] {rich_escape(result_text.strip())}", soft_wrap=True)
        elif entry_type == "raw":
            message = entry.get("message", "")
            if isinstance(message, str) and message:
                self._console.print(rich_escape(message), soft_wrap=True)

    def _print_tool_use(self, content: dict) -> None:
        tool_name = content.get("name", "unknown")
        tool_input = content.get("input", {})

        if tool_name == "Bash":
            cmd = tool_input.get("command", "")
            self._fmt.print_tool_event(tool_name, self._trunc(cmd, 80))
        elif tool_name == "Edit":
            parts = [tool_name]
            file_path = tool_input.get("file_path", "")
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
            self._fmt.print_tool_event(" ".join(parts))
        elif tool_name == "Glob":
            self._fmt.print_tool_event(tool_name, tool_input.get("pattern", ""))
        elif tool_name == "Grep":
            pattern = tool_input.get("pattern", "")
            path = tool_input.get("path", "")
            detail = pattern
            if path:
                detail += f" [{path}]"
            self._fmt.print_tool_event(tool_name, detail)
        elif tool_name == "TodoWrite":
            todos = tool_input.get("todos", [])
            self._fmt.print_tool_event(tool_name, f"{len(todos)} todos")
            for todo in todos:
                if isinstance(todo, dict):
                    status = todo.get("status", "pending")
                    todo_content = todo.get("content", "")
                else:
                    status = "pending"
                    todo_content = str(todo)
                self._fmt.print_todo(status, self._trunc(todo_content, 60))
        else:
            file_path = tool_input.get("file_path") or tool_input.get("path")
            if file_path:
                self._fmt.print_tool_event(tool_name, file_path)
            else:
                self._fmt.print_tool_event(tool_name)


def _format_log_entry(entry: dict) -> str | None:
    """Format a single JSON log entry for display.

    Returns formatted string or None to skip the entry.
    """
    entry_type = entry.get("type")

    if entry_type == "system":
        subtype = entry.get("subtype", "")
        if subtype == "init":
            model = rich_escape(str(entry.get("model", "unknown")))
            return f"[{_lc()}]\\[system][/{_lc()}] Session initialized (model: {model})"
        return None  # Skip other system messages

    elif entry_type == "user":
        # User messages contain tool results
        content = _message_content_items(entry)
        parts = []
        for item in content:
            if item.get("type") == "tool_result":
                result = item.get("content", "")
                if isinstance(result, str):
                    # Unescape literal \n from double-escaped JSON (Claude Code logging artifact)
                    result = result.replace("\\n", "\n").replace("\\t", "\t")
                    if len(result) > 200:
                        result = result[:200] + "..."
                parts.append(rich_escape(str(result)))
        if parts:
            return "\n".join(parts)
        return None

    elif entry_type == "assistant":
        content = _message_content_items(entry)
        parts = []
        for item in content:
            if item.get("type") == "text":
                text = item.get("text", "")
                parts.append(rich_escape(text))
            elif item.get("type") == "tool_use":
                name = rich_escape(item.get("name", "unknown"))
                tool_input = item.get("input", {})
                # Show condensed tool use info
                if item.get("name") == "Bash":
                    cmd = tool_input.get("command", "")
                    if len(cmd) > 100:
                        cmd = cmd[:100] + "..."
                    parts.append(f"[green]\\[tool: {name}][/green] {rich_escape(cmd)}")
                elif item.get("name") == "Read":
                    path = tool_input.get("file_path", "")
                    parts.append(f"[green]\\[tool: {name}][/green] {rich_escape(path)}")
                elif item.get("name") == "Edit":
                    path = tool_input.get("file_path", "")
                    parts.append(f"[green]\\[tool: {name}][/green] {rich_escape(path)}")
                elif item.get("name") == "Write":
                    path = tool_input.get("file_path", "")
                    parts.append(f"[green]\\[tool: {name}][/green] {rich_escape(path)}")
                elif item.get("name") == "Grep":
                    pattern = tool_input.get("pattern", "")
                    path = tool_input.get("path", "")
                    glob = tool_input.get("glob", "")
                    type_filter = tool_input.get("type", "")

                    # Format: pattern [path] (filter)
                    detail = rich_escape(pattern)
                    if path:
                        detail += f" \\[{rich_escape(path)}]"
                    if glob:
                        detail += f" (glob: {rich_escape(glob)})"
                    elif type_filter:
                        detail += f" (type: {rich_escape(type_filter)})"
                    parts.append(f"[green]\\[tool: {name}][/green] {detail}")
                elif item.get("name") == "Glob":
                    pattern = tool_input.get("pattern", "")
                    parts.append(f"[green]\\[tool: {name}][/green] {rich_escape(pattern)}")
                elif item.get("name") == "TodoWrite":
                    todos = tool_input.get("todos", [])
                    in_progress = [t for t in todos if t.get("status") == "in_progress"]
                    if in_progress:
                        parts.append(f"[green]\\[tool: {name}][/green] {rich_escape(in_progress[0].get('activeForm', ''))}")
                    else:
                        parts.append(f"[green]\\[tool: {name}][/green]")
                else:
                    parts.append(f"[green]\\[tool: {name}][/green]")
        if parts:
            return "\n".join(parts)
        return None

    elif entry_type == "result":
        result = entry.get("result", "")
        is_error = entry.get("is_error", False)
        subtype = str(entry.get("subtype") or "")
        if is_error:
            return f"[red]\\[result] ERROR:[/red] {rich_escape(str(result))}"
        if subtype and subtype != "success":
            if isinstance(result, str) and result.strip():
                return f"[yellow]\\[result] {rich_escape(subtype)}:[/yellow] {rich_escape(result.strip())}"
            return f"[yellow]\\[result] {rich_escape(subtype)}[/yellow]"
        # For success, show summary if available
        duration = entry.get("duration_ms", 0)
        num_steps = _result_step_count(entry) or 0
        cost = entry.get("total_cost_usd", 0)
        summary = f"[green]\\[result][/green] Completed in {num_steps} steps, {duration/1000:.1f}s, ${cost:.4f}"
        if isinstance(result, str) and result.strip():
            return f"{summary}\n{rich_escape(result.strip())}"
        return summary

    elif entry_type == "gza":
        subtype = entry.get("subtype", "")
        message = entry.get("message", "")
        if not message:
            return None
        if subtype:
            return f"[{_lc()}]\\[gza:{rich_escape(subtype)}][/{_lc()}] {rich_escape(message)}"
        return f"[{_lc()}]\\[gza][/{_lc()}] {rich_escape(message)}"
    elif entry_type == "raw":
        message = entry.get("message", "")
        if message:
            return rich_escape(str(message))

    return None


def _task_log_candidates(config: Config, task: DbTask) -> list[Path]:
    """Build ordered candidate log paths for a task."""
    candidates: list[Path] = []

    if task.log_file:
        path = Path(task.log_file)
        if not path.is_absolute():
            path = config.project_dir / path
        candidates.append(path)

    if task.slug:
        inferred = config.log_path / f"{task.slug}.log"
        candidates.append(inferred)

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _task_startup_log_path(config: Config, task: DbTask | None, worker: WorkerMetadata | None = None) -> Path | None:
    """Resolve deterministic startup log path from task slug, with legacy fallback."""
    if task and task.slug:
        deterministic = config.workers_path / f"{task.slug}.startup.log"
        if deterministic.exists():
            return deterministic
    if worker and worker.startup_log_file:
        startup_path = Path(worker.startup_log_file)
        if not startup_path.is_absolute():
            startup_path = config.project_dir / startup_path
        return startup_path
    if task and task.slug:
        return config.workers_path / f"{task.slug}.startup.log"
    return None


def _resolve_worker_log_path(
    config: Config,
    worker: WorkerMetadata,
    task: DbTask | None,
) -> tuple[Path | None, bool]:
    """Resolve log path for worker lookups, preferring main task logs then startup logs."""
    main_candidates: list[Path] = []

    if worker.log_file:
        worker_log = Path(worker.log_file)
        if not worker_log.is_absolute():
            worker_log = config.project_dir / worker_log
        main_candidates.append(worker_log)

    if task is not None:
        main_candidates.extend(_task_log_candidates(config, task))

    for candidate in main_candidates:
        if candidate.exists():
            return candidate, False

    startup_log_path = _task_startup_log_path(config, task, worker)
    if startup_log_path and startup_log_path.exists():
        return startup_log_path, True

    # Prefer returning a main log candidate (even if missing) over a non-existent
    # startup log, so error messages reference the expected main log path.
    if main_candidates:
        return main_candidates[0], False
    return None, False


def _latest_worker_for_task(registry: WorkerRegistry, task_id: str) -> WorkerMetadata | None:
    """Return most recent worker metadata for a task."""
    workers = [w for w in registry.list_all(include_completed=True) if w.task_id == task_id]
    if not workers:
        return None
    workers.sort(key=lambda w: (_parse_iso(w.started_at) or datetime.min.replace(tzinfo=UTC), w.worker_id))
    return workers[-1]


def _resolve_task_log_path(
    config: Config,
    registry: WorkerRegistry,
    task: DbTask,
) -> tuple[Path | None, bool]:
    """Resolve log path for task/slug lookups with worker startup fallback."""
    main_candidates: list[Path] = []
    main_candidates.extend(_task_log_candidates(config, task))

    for candidate in main_candidates:
        if candidate.exists():
            return candidate, False

    if task.id is not None:
        latest_worker = _latest_worker_for_task(registry, task.id)
        if latest_worker is not None:
            worker_log_path, using_startup_log = _resolve_worker_log_path(config, latest_worker, task)
            if worker_log_path is not None:
                return worker_log_path, using_startup_log

    if main_candidates:
        return main_candidates[0], False
    return None, False


def _running_worker_id_for_task(registry: WorkerRegistry, task_id: str) -> str | None:
    """Return a running worker ID for a task when available."""
    # Note: legacy worker JSON files created before the INTEGER→TEXT PK migration
    # may have task_id stored as a bare stringified integer (e.g. "123") rather than
    # the canonical prefixed form (e.g. "gza-123"). Such workers won't match here.
    # This is acceptable since worker metadata is ephemeral and old JSON files are
    # cleaned up after the worker process exits.
    workers = [w for w in registry.list_all(include_completed=True) if w.task_id == task_id]
    running = [w for w in workers if w.status == "running" and registry.is_running(w.worker_id)]
    if not running:
        return None
    running.sort(key=lambda w: (_parse_iso(w.started_at) or datetime.min.replace(tzinfo=UTC), w.worker_id))
    return running[-1].worker_id


def _load_log_file_entries(log_path: Path) -> tuple[dict | None, list[dict], str]:
    """Load log file as old JSON object or JSONL entries."""
    with open(log_path) as f:
        content = f.read().strip()

    log_data = None
    entries: list[dict] = []

    if not content:
        return None, [], content

    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            log_data = parsed
            if parsed.get("type"):
                entries.append(parsed)
            return log_data, entries, content
    except json.JSONDecodeError:
        pass

    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            if isinstance(entry, dict):
                entries.append(entry)
                if entry.get("type") == "result":
                    log_data = entry
            else:
                entries.append({"type": "raw", "message": str(entry)})
        except json.JSONDecodeError:
            entries.append({"type": "raw", "message": line})

    return log_data, entries, content


def _print_log_header(
    *,
    task: DbTask | None,
    worker: WorkerMetadata | None,
    log_path: Path,
    is_running: bool,
    using_startup_log: bool,
) -> None:
    """Print the static task/worker header banner for ``gza log``."""
    _sep = f"[{_lc()}]" + "━" * 70 + f"[/{_lc()}]"
    console.print(_sep, soft_wrap=True)
    if task:
        prompt_display = task.prompt[:100] if task.prompt else "(no prompt)"
        console.print(f"[{pink}]Task: {rich_escape(prompt_display)}[/{pink}]", soft_wrap=True)
        console.print(f"[{_lc()}]ID:[/{_lc()}] {task.id} | [{_lc()}]Slug:[/{_lc()}] {rich_escape(task.slug or '')}", soft_wrap=True)
        _status_color = LINEAGE_STATUS_COLORS.get(task.status, "")
        _status_val = f"[{_status_color}]{rich_escape(task.status)}[/{_status_color}]" if _status_color else rich_escape(task.status)
        console.print(f"[{_lc()}]Status:[/{_lc()}] {_status_val}", soft_wrap=True)
        console.print(f"[{_lc()}]Log:[/{_lc()}] {rich_escape(str(log_path))}", soft_wrap=True)
        if using_startup_log:
            console.print("[yellow]Using worker startup log (main task log not available).[/yellow]", soft_wrap=True)
        if task.branch:
            console.print(f"[{_lc()}]Branch:[/{_lc()}] {rich_escape(task.branch)}", soft_wrap=True)
    elif worker:
        console.print(f"[{_lc()}]Worker:[/{_lc()}] {rich_escape(worker.worker_id)}", soft_wrap=True)
        _w_status = worker.status if worker.status else "unknown"
        if is_running and _w_status != "running":
            # Prefer live process state when worker metadata is stale.
            _w_status = "running"
        _w_color = PS_STATUS_COLORS.get(_w_status, "white")
        console.print(f"[{_lc()}]Status:[/{_lc()}] [{_w_color}]{_w_status}[/{_w_color}]", soft_wrap=True)
        console.print(f"[{_lc()}]Log:[/{_lc()}] {rich_escape(str(log_path))}", soft_wrap=True)
        if using_startup_log:
            console.print("[yellow]Using startup log (main task log not available).[/yellow]", soft_wrap=True)
    console.print(_sep, soft_wrap=True)
    console.print()


def cmd_log(args: argparse.Namespace) -> int:
    """Display the log for a task or worker."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    registry = WorkerRegistry(config.workers_path)

    query = args.identifier
    task = None
    worker = None
    log_path = None
    using_startup_log = False
    is_running = False
    worker_id_for_follow: str | None = None

    if args.worker:
        # Look up by worker ID
        worker = registry.get(query)
        if not worker:
            print(f"Error: Worker '{query}' not found")
            return 1
        is_running = registry.is_running(query)
        if worker.task_id:
            task = store.get(worker.task_id)
        log_path, using_startup_log = _resolve_worker_log_path(config, worker, task)
        worker_id_for_follow = worker.worker_id

    elif args.slug:
        # Look up by slug (exact or partial match)
        task = store.get_by_slug(query)
        if not task:
            # Try partial match
            all_tasks = store.get_all()
            for t in all_tasks:
                if t.slug and query in t.slug:
                    task = t
                    break
        if not task:
            print(f"Error: No task found matching slug '{query}'")
            return 1

        log_path, using_startup_log = _resolve_task_log_path(config, registry, task)

        if task.id is not None:
            worker_id_for_follow = _running_worker_id_for_task(registry, task.id)
            is_running = worker_id_for_follow is not None

    else:
        # Default: look up by task ID
        task_id: str = resolve_id(config, query)
        task = store.get(task_id)
        if not task:
            print(f"Error: Task {query} not found")
            return 1

        log_path, using_startup_log = _resolve_task_log_path(config, registry, task)

        if task.id is not None:
            worker_id_for_follow = _running_worker_id_for_task(registry, task.id)
            is_running = worker_id_for_follow is not None

    if not log_path:
        print("Error: No log file found")
        return 1

    if not log_path.exists():
        if is_running and not using_startup_log:
            print(f"Log file not yet created: {log_path}")
            print("Worker is still starting up...")
        elif using_startup_log:
            print(f"Error: Startup log not found at {log_path}")
        else:
            print(f"Error: Log file not found at {log_path}")
        return 1

    # Determine mode: follow (live tail) vs static display
    follow = hasattr(args, 'follow') and args.follow
    if follow and not is_running:
        follow = False  # Can't follow a completed task

    # Check for raw mode
    raw_mode = hasattr(args, 'raw') and args.raw

    if follow and not raw_mode:
        _print_log_header(
            task=task,
            worker=worker,
            log_path=log_path,
            is_running=is_running,
            using_startup_log=using_startup_log,
        )

    if follow or raw_mode:
        # Live streaming mode - use the formatted streaming output
        return _tail_log_file(
            log_path,
            args,
            registry,
            worker_id_for_follow if is_running else None,
            task.id if task else None,
            store if task else None,
        )

    use_page = getattr(args, 'page', False)
    with pager_context(use_page, config.project_dir):
        # Static display mode - show summary or full turns
        try:
            log_data, entries, content = _load_log_file_entries(log_path)

            if log_data is None and not entries:
                # If we have content but couldn't parse any JSON, it's likely a startup error
                if content:
                    if using_startup_log:
                        console.print(f"[{_lc()}]Startup log:[/{_lc()}] {rich_escape(str(log_path))}", soft_wrap=True)
                        console.print("[yellow]Using startup log (main task log not available).[/yellow]", soft_wrap=True)
                    console.print("[red]Task failed during startup (no Claude session):[/red]", soft_wrap=True)
                    # Display the raw error message, indented for clarity
                    for line in content.split('\n'):
                        console.print(f"  {rich_escape(line)}", soft_wrap=True)
                    return 1
                else:
                    console.print("Error: No log entries found in log file", soft_wrap=True)
                    return 1
        except Exception as e:
            print(f"Error: Failed to read log file: {e}")
            return 1

        # Display header
        _print_log_header(
            task=task,
            worker=worker,
            log_path=log_path,
            is_running=is_running,
            using_startup_log=using_startup_log,
        )

        _sep = f"[{_lc()}]" + "━" * 70 + f"[/{_lc()}]"

        timeline_mode = getattr(args, "timeline_mode", None)
        if timeline_mode and entries:
            _display_step_timeline(entries, verbose=timeline_mode == "verbose")
        elif entries:
            printer = _LiveLogPrinter(live=False)
            any_printed = False
            for entry in entries:
                printer.process(entry)
                any_printed = True
            if not any_printed:
                if log_data:
                    if "result" in log_data:
                        console.print(rich_escape(log_data["result"]), soft_wrap=True)
                    else:
                        subtype = log_data.get("subtype", "unknown")
                        console.print(f"Run ended with: {rich_escape(subtype)}", soft_wrap=True)
                        if log_data.get("errors"):
                            console.print(f"[red]Errors:[/red] {rich_escape(str(log_data['errors']))}", soft_wrap=True)
                else:
                    console.print("No displayable log entries found.", soft_wrap=True)
        elif log_data:
            # Extract and display the result field (which contains markdown)
            if "result" in log_data:
                console.print(rich_escape(log_data["result"]), soft_wrap=True)
            else:
                # No result - show the subtype (e.g., error_max_turns)
                subtype = log_data.get("subtype", "unknown")
                console.print(f"Run ended with: {rich_escape(subtype)}", soft_wrap=True)
                if log_data.get("errors"):
                    console.print(f"[red]Errors:[/red] {rich_escape(str(log_data['errors']))}", soft_wrap=True)
        else:
            # No result entry yet - show compact step timeline
            _display_step_timeline(entries, verbose=False)

        console.print()
        console.print(_sep, soft_wrap=True)

        # Display summary stats if available
        if log_data:
            if "duration_ms" in log_data:
                duration_sec = log_data["duration_ms"] / 1000
                console.print(f"[{_lc()}]Duration:[/{_lc()}] {format_duration(duration_sec, verbose=True)}", soft_wrap=True)
            step_count = _result_step_count(log_data)
            if step_count is not None:
                console.print(f"[{_lc()}]Steps:[/{_lc()}] {step_count}", soft_wrap=True)
                if "num_steps" not in log_data and "num_steps_reported" not in log_data and "num_turns" in log_data:
                    console.print(f"[{_lc()}]Legacy turns:[/{_lc()}] {log_data['num_turns']}", soft_wrap=True)
            if "total_cost_usd" in log_data:
                console.print(f"[{_lc()}]Cost:[/{_lc()}] ${log_data['total_cost_usd']:.4f}", soft_wrap=True)

        return 0


def _tail_log_file(
    log_path: Path,
    args: argparse.Namespace,
    registry: WorkerRegistry,
    worker_id: str | None,
    task_id: str | None = None,
    store: SqliteTaskStore | None = None,
) -> int:
    """Tail a log file with optional follow mode."""
    raw_mode = hasattr(args, 'raw') and args.raw
    follow = hasattr(args, 'follow') and args.follow

    if raw_mode:
        # Use tail directly for raw JSON output
        try:
            cmd = ["tail"]
            if hasattr(args, 'tail') and args.tail:
                cmd.extend(["-n", str(args.tail)])
            if follow:
                cmd.append("-f")
            cmd.append(str(log_path))
            subprocess.run(cmd)
            return 0
        except KeyboardInterrupt:
            return 0
        except Exception as e:
            print(f"Error tailing log: {e}")
            return 1

    # Formatted output mode
    try:
        tail_lines = args.tail if hasattr(args, 'tail') and args.tail else None
        printer = _LiveLogPrinter()

        def _process_lines(raw_lines: list[str]) -> None:
            """Parse JSON lines and feed them to the live printer."""
            for line in raw_lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("---"):
                    # Skip step timestamp markers written by the runner
                    continue
                try:
                    entry = json.loads(line)
                    printer.process(entry)
                except json.JSONDecodeError:
                    console.print(rich_escape(line), soft_wrap=True)

        # Initial read
        with open(log_path) as f:
            lines = f.readlines()
        if tail_lines:
            lines = lines[-tail_lines:]
        _process_lines(lines)

        if not follow:
            return 0

        # Follow mode - watch for new lines
        last_size = log_path.stat().st_size
        with open(log_path) as f:
            last_line_count = sum(1 for _ in f)

        while True:
            time.sleep(0.5)

            current_size = log_path.stat().st_size
            if current_size > last_size:
                with open(log_path) as f:
                    lines = f.readlines()

                new_lines = lines[last_line_count:]
                last_line_count = len(lines)
                last_size = current_size
                _process_lines(new_lines)

            # Check if worker is still running
            if worker_id and not registry.is_running(worker_id):
                time.sleep(0.5)
                with open(log_path) as f:
                    lines = f.readlines()
                _process_lines(lines[last_line_count:])
                break

            # Fallback for task-based follow without a running worker ID.
            if task_id is not None and store is not None and worker_id is None:
                latest_task = store.get(task_id)
                if latest_task is None or latest_task.status != "in_progress":
                    time.sleep(0.5)
                    with open(log_path) as f:
                        lines = f.readlines()
                    _process_lines(lines[last_line_count:])
                    break

        return 0

    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error tailing log: {e}")
        return 1


def _display_conversation_turns(entries: list[dict]) -> None:
    """Deprecated compatibility wrapper for legacy call sites."""
    _display_step_timeline(entries, verbose=True)
