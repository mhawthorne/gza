"""Claude Code provider implementation."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .base import (
    DockerConfig,
    Provider,
    RunResult,
    build_docker_cmd,
    ensure_docker_image,
    verify_docker_credentials,
    write_preflight_entry,
)
from .output_formatter import StreamOutputFormatter, truncate_text

if TYPE_CHECKING:
    from ..config import Config

logger = logging.getLogger(__name__)


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

    def verify_credentials(self, config: Config, log_file: Path | None = None) -> bool:
        """Verify Claude credentials by testing the claude command."""
        if config.use_docker:
            return self._verify_docker(config, log_file=log_file)
        return self._verify_direct(log_file=log_file)

    def _verify_docker(self, config: Config, log_file: Path | None = None) -> bool:
        """Verify credentials work in Docker."""
        if config.claude.fetch_auth_token_from_keychain:
            sync_keychain_credentials()
        docker_config = _get_docker_config(f"{config.docker_image}-claude")
        if not ensure_docker_image(docker_config, config.project_dir):
            write_preflight_entry(
                log_file,
                event="docker_image_build_failed",
                command=["docker", "build", "-t", docker_config.image_name],
                returncode=None,
                stdout_tail="",
                stderr_tail="",
                message="Failed to build Claude Docker image",
            )
            print("Error: Failed to build Docker image")
            return False
        return verify_docker_credentials(
            docker_config=docker_config,
            version_cmd=["claude", "--version"],
            error_patterns=["Invalid API key", "Please run /login", "/login"],
            error_message=(
                "Error: Invalid or missing Claude credentials\n"
                "  Run 'claude login' or set ANTHROPIC_API_KEY in .env"
            ),
            log_file=log_file,
        )

    def _verify_direct(self, log_file: Path | None = None) -> bool:
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
                return False
            if result.returncode == 0:
                return True
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
            return False
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
            return False
        return False

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
    ) -> RunResult:
        """Run Claude to execute a task."""
        if interactive:
            return self._run_interactive(
                config,
                prompt,
                log_file,
                work_dir,
                resume_session_id=resume_session_id,
                on_session_id=on_session_id,
                on_step_count=on_step_count,
            )
        if config.use_docker:
            return self._run_docker(config, prompt, log_file, work_dir, resume_session_id, on_session_id, on_step_count)
        return self._run_direct(config, prompt, log_file, work_dir, resume_session_id, on_session_id, on_step_count)

    def _run_interactive(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
    ) -> RunResult:
        """Run Claude in foreground mode while preserving session/step telemetry."""
        if config.use_docker:
            return self._run_docker(
                config,
                prompt,
                log_file,
                work_dir,
                resume_session_id,
                on_session_id,
                on_step_count,
            )
        return self._run_direct(
            config,
            prompt,
            log_file,
            work_dir,
            resume_session_id,
            on_session_id,
            on_step_count,
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

    def _run_docker(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
        on_session_id: Callable[[str], None] | None = None,
        on_step_count: Callable[[int], None] | None = None,
    ) -> RunResult:
        """Run Claude in Docker container."""
        if config.claude.fetch_auth_token_from_keychain:
            sync_keychain_credentials()
        docker_config = _get_docker_config(f"{config.docker_image}-claude")

        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return RunResult(exit_code=1)

        cmd = build_docker_cmd(docker_config, work_dir, config.timeout_minutes, config.docker_volumes, config.docker_setup_command)
        cmd.append("claude")
        cmd.extend(self._build_claude_args(config, resume_session_id))

        return self._run_with_output_parsing(
            cmd, log_file, config.timeout_minutes, stdin_input=prompt, model=config.model,
            chat_text_display_length=config.chat_text_display_length,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
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
    ) -> RunResult:
        """Run Claude directly (no Docker)."""
        # When running inside a tmux session, use interactive mode so the proxy
        # can auto-accept tool prompts and users can attach.
        if getattr(config.tmux, "session_name", None):
            return self._run_direct_tmux(config, prompt, log_file, work_dir)

        cmd = ["timeout", f"{config.timeout_minutes}m", "claude"]
        cmd.extend(self._build_claude_args(config, resume_session_id))

        return self._run_with_output_parsing(
            cmd, log_file, config.timeout_minutes, cwd=work_dir, stdin_input=prompt, model=config.model,
            chat_text_display_length=config.chat_text_display_length,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
        )

    def _run_direct_tmux(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
    ) -> RunResult:
        """Run Claude in interactive mode for tmux sessions.

        The initial task prompt is delivered by the TmuxProxy via the PTY master
        fd (simulating typing), so Claude does not receive it as a positional
        argument.  Raw terminal output is captured to ``log_file`` via
        ``tmux pipe-pane``; structured proxy events go to a separate
        ``*-proxy.log`` file so existing log parsers see clean output.
        """
        import json as _json
        import shlex as _shlex

        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Proxy structured events (JSONL) go to a separate file so they do not
        # mix with the raw terminal output captured in the main log file.
        proxy_log_file = log_file.parent / f"{log_file.stem}-proxy.log"

        with open(proxy_log_file, "a") as f:
            f.write(_json.dumps({
                "type": "gza",
                "subtype": "tmux_start",
                "message": "Started in tmux interactive mode",
                "session": config.tmux.session_name,
            }) + "\n")

        # Capture raw terminal output from the tmux pane to the main log file.
        # This mirrors the output that humans see when they attach, and is what
        # ``gza log -f`` reads in tmux mode.
        if config.tmux.session_name:
            subprocess.run(
                [
                    "tmux", "pipe-pane", "-t", config.tmux.session_name,
                    f"cat >> {_shlex.quote(str(log_file))}",
                ],
                check=False,
            )

        # Run Claude in interactive mode — the proxy delivers the prompt via the
        # PTY so we do not pass it as a positional argument here.
        cmd = ["claude", "--max-turns", str(config.max_steps)]
        cmd.extend(config.claude.args)

        # Run with inherited stdin/stdout/stderr (connected to the PTY via the proxy)
        result = subprocess.run(cmd, cwd=work_dir)

        with open(proxy_log_file, "a") as f:
            f.write(_json.dumps({
                "type": "gza",
                "subtype": "tmux_end",
                "exit_code": result.returncode,
            }) + "\n")

        return RunResult(exit_code=result.returncode)

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
    ) -> RunResult:
        """Run command and parse Claude's stream-json output."""
        formatter = StreamOutputFormatter()

        def _ensure_step_store(data: dict) -> None:
            if "run_step_events" not in data:
                data["run_step_events"] = []
                data["_step_by_msg_id"] = {}
                data["_current_step_event"] = None
                data["_legacy_event_count_by_turn"] = {}

        def _allocate_legacy_event_id(data: dict, legacy_turn_id: str | None) -> str | None:
            if not legacy_turn_id:
                return None
            counters = data.get("_legacy_event_count_by_turn")
            if not isinstance(counters, dict):
                counters = {}
                data["_legacy_event_count_by_turn"] = counters
            next_idx = int(counters.get(legacy_turn_id, 0)) + 1
            counters[legacy_turn_id] = next_idx
            return f"{legacy_turn_id}.{next_idx}"

        def _start_step(data: dict, msg_id: str | None, legacy_turn_id: str | None) -> dict:
            _ensure_step_store(data)
            event: dict[str, Any] = {
                "message_role": "assistant",
                "message_text": None,
                "legacy_turn_id": legacy_turn_id,
                "legacy_event_id": _allocate_legacy_event_id(data, legacy_turn_id),
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

        def parse_claude_output(line: str, data: dict, log_handle=None) -> None:
            try:
                event: dict[str, Any] = json.loads(line)
                event_type = event.get("type")

                if event_type == "assistant":
                    message = event.get("message", {})
                    msg_id = message.get("id")
                    _ensure_step_store(data)

                    # Track unique message IDs as turn proxy
                    if "seen_msg_ids" not in data:
                        data["seen_msg_ids"] = set()
                        data["start_time"] = time.time()
                    turn_count = len(data["seen_msg_ids"])
                    if msg_id and msg_id not in data["seen_msg_ids"]:
                        data["seen_msg_ids"].add(msg_id)
                        turn_count = len(data["seen_msg_ids"])
                        _start_step(data, msg_id, f"T{turn_count}")

                        # Accumulate token usage for cost estimation
                        usage = message.get("usage", {})
                        if "total_input_tokens" not in data:
                            data["total_input_tokens"] = 0
                            data["total_output_tokens"] = 0
                        data["total_input_tokens"] += usage.get("input_tokens", 0)
                        data["total_input_tokens"] += usage.get("cache_creation_input_tokens", 0)
                        data["total_input_tokens"] += usage.get("cache_read_input_tokens", 0)
                        data["total_output_tokens"] += usage.get("output_tokens", 0)

                        # Calculate runtime
                        elapsed_seconds = int(time.time() - data["start_time"])
                        total_tokens = data["total_input_tokens"] + data["total_output_tokens"]

                        # Calculate estimated cost
                        cost = calculate_cost(
                            data["total_input_tokens"],
                            data["total_output_tokens"],
                            model,
                        )

                        # Log timestamp to log file at start of each step
                        if log_handle:
                            timestamp_str = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                            log_handle.write(f"--- Step {turn_count} at {timestamp_str} ---\n")
                            log_handle.flush()

                        # Add blank line before step (except first step)
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
                        current_step = _start_step(data, msg_id, legacy_turn_id)

                    for content in message.get("content", []):
                        if content.get("type") == "tool_use":
                            tool_name = content.get("name", "unknown")
                            tool_input = content.get("input", {})
                            call_id = content.get("id")
                            seen_tool_use_ids = current_step.setdefault("_seen_tool_use_ids", set())
                            dedupe_key = call_id or f"{tool_name}:{json.dumps(tool_input, sort_keys=True, default=str)}"
                            if dedupe_key in seen_tool_use_ids:
                                continue
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
                                    "legacy_event_id": _allocate_legacy_event_id(data, current_step.get("legacy_turn_id")),
                                }
                            )

                            # Extract file path for file-related tools
                            file_path = tool_input.get("file_path") or tool_input.get("path")

                            # Enhanced logging for specific tools
                            if tool_name == "Bash":
                                command = tool_input.get("command", "")
                                # Truncate to 80 chars
                                command = truncate_text(command, 80)
                                formatter.print_tool_event(tool_name, command)
                            elif tool_name == "Glob":
                                pattern = tool_input.get("pattern", "")
                                formatter.print_tool_event(tool_name, pattern)
                            elif tool_name == "TodoWrite":
                                todos = tool_input.get("todos", [])
                                todos_summary = f"{len(todos)} todos"
                                # Show status breakdown if available (todos may be dicts or strings)
                                dict_todos = [t for t in todos if isinstance(t, dict)]
                                if dict_todos:
                                    pending = sum(1 for t in dict_todos if t.get("status") == "pending")
                                    in_progress = sum(1 for t in dict_todos if t.get("status") == "in_progress")
                                    completed = sum(1 for t in dict_todos if t.get("status") == "completed")
                                    todos_summary += f" (pending: {pending}, in_progress: {in_progress}, completed: {completed})"
                                formatter.print_tool_event(tool_name, todos_summary)
                                # Print each todo with status icon and truncated content.
                                for todo in todos:
                                    if isinstance(todo, dict):
                                        status = todo.get("status", "pending")
                                        content = todo.get("content", "")
                                    else:
                                        status = "pending"
                                        content = str(todo)
                                    formatter.print_todo(status, truncate_text(content, 60))
                            elif tool_name == "Edit":
                                # Enhanced logging for Edit tool
                                parts = [tool_name]
                                if file_path:
                                    parts.append(file_path)

                                # Calculate line count changes
                                old_string = tool_input.get("old_string", "")
                                new_string = tool_input.get("new_string", "")
                                old_lines = old_string.count("\n") + (1 if old_string else 0)
                                new_lines = new_string.count("\n") + (1 if new_string else 0)

                                # Show line count delta
                                if old_lines > 0 or new_lines > 0:
                                    added = max(0, new_lines - old_lines)
                                    removed = max(0, old_lines - new_lines)
                                    if added > 0 and removed > 0:
                                        parts.append(f"(+{added}/-{removed} lines)")
                                    elif added > 0:
                                        parts.append(f"(+{added} lines)")
                                    elif removed > 0:
                                        parts.append(f"(-{removed} lines)")

                                # Show replace_all indicator
                                if tool_input.get("replace_all"):
                                    parts.append("[replace_all]")

                                # Show truncated preview of old_string
                                if old_string:
                                    # Get first line of old_string, truncate if needed
                                    first_line = old_string.split("\n")[0]
                                    preview = truncate_text(first_line, 40)
                                    # Escape newlines and quotes for display
                                    preview = preview.replace("\r", "\\r").replace("\t", "\\t")
                                    parts.append(f'"{preview}"')

                                formatter.print_tool_event(" ".join(parts))
                            elif file_path:
                                formatter.print_tool_event(tool_name, file_path)
                            else:
                                parts = [tool_name]
                                for k, v in tool_input.items():
                                    parts.append(f"{k}={_format_tool_param(v)}")
                                formatter.print_tool_event(" ".join(parts))
                        elif content.get("type") == "tool_result":
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
                                    "legacy_event_id": _allocate_legacy_event_id(data, legacy_turn_id),
                                }
                            )
                        elif content.get("type") == "tool_retry":
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
                                    "legacy_event_id": _allocate_legacy_event_id(data, legacy_turn_id),
                                }
                            )
                        elif content.get("type") == "text":
                            text = content.get("text", "").strip()
                            if text:
                                previous = current_step.get("message_text")
                                current_step["message_text"] = (
                                    text if not previous else f"{previous}\n{text}"
                                )
                                # Display text to console (configurable length, 0 = unlimited)
                                if chat_text_display_length == 0:
                                    # Show full text
                                    formatter.print_agent_message(text)
                                else:
                                    # Truncate to first line and max length
                                    first_line = text.split("\n")[0]
                                    formatter.print_agent_message(truncate_text(first_line, chat_text_display_length))

                elif event_type == "system":
                    subtype = event.get("subtype")
                    if subtype == "init":
                        session_id = event.get("session_id")
                        if session_id and "session_id" not in data:
                            data["session_id"] = session_id
                            if on_session_id:
                                on_session_id(session_id)

                elif event_type == "result":
                    data["result"] = event
                    # Also capture session_id from result event if not already seen
                    session_id = event.get("session_id")
                    if session_id and "session_id" not in data:
                        data["session_id"] = session_id
                        if on_session_id:
                            on_session_id(session_id)

            except json.JSONDecodeError:
                # Non-JSON output, just display it
                if line == data.get("_startup_line"):
                    return
                print(line)

        result = self.run_with_logging(
            cmd, log_file, timeout_minutes, cwd=cwd, parse_output=parse_claude_output, stdin_input=stdin_input
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
                result.error_type = "max_steps"

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
